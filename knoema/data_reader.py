"""This module contains data definitions for Knoema client"""

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import pandas
import knoema.api_definitions as definition
from urllib.parse import quote

class DataReader(object):
    """This class read data from Knoema and transform it to pandas frame"""

    def __init__(self, client):
        self.client = client
        self.dataset = None
        self.include_metadata = False
        self.dimensions = []     
        self.separator = ';'

    def _get_series_name(self, series_point):
        names = []
        for dim in self.dimensions:
            names.append(series_point[dim.id])
        names.append(series_point['Frequency'])
        return tuple(names) 

    def _get_series_with_metadata(self, series_point):
        names = []
        for dim in self.dimensions:
            for item in dim.items:
                if item.name == series_point[dim.id]:
                    dim_attrs = item.fields
                    break
            for attr in dim.fields:          
                if not attr['isSystemField']: 
                    value_empty = True
                    for key, value in dim_attrs.items(): 
                        if definition.is_equal_strings_ignore_case(key, attr['name']):
                            names.append(value)
                            value_empty = False
                    if value_empty:
                        names.append(float("NaN"))
        names.append(series_point.get('Unit'))
        names.append(series_point.get('Scale'))
        names.append(series_point.get('Mnemonics'))
        for attr in self.dataset.timeseries_attributes:
            names.append(series_point.get(attr.name))  
        return tuple(names)     

    def _get_attribute_names(self):
        names = []
        for dim in self.dimensions:
            for attr in dim.fields:
                if not attr['isSystemField']:
                    names.append(dim.name +' '+ attr['displayName'])
        names.append('Unit')            
        names.append('Scale') 
        names.append('Mnemonics')
        for attr in self.dataset.timeseries_attributes:
            names.append(attr.name)     
        return names

    def _get_dimension_names(self):
        names = []
        for dim in self.dimensions:
            names.append(dim.name)
        names.append('Frequency')             
        return names

    def _get_metadata_series(self, resp, names_of_attributes):
        series = {}
        for series_point in resp.tuples:
            val = series_point['Value']
            if val is None:
                continue
            serie_name = self._get_series_name(series_point)
            if serie_name not in series:
                serie_attrs = self._get_series_with_metadata(series_point)
                series[serie_name] = KnoemaSeries(serie_name, serie_attrs, names_of_attributes)
        return series

    def _get_data_series(self, resp):
        series = {}
        for series_point in resp.tuples:
            val = series_point['Value']
            if val is None:
                continue
            series_name = self._get_series_name(series_point)
            if series_name not in series:
                series[series_name] = KnoemaSeries(series_name,[],[])

            if 'Time' in series_point:
                curr_date_val = series_point['Time']
                try:
                    curr_date_val = datetime.strptime(series_point['Time'], '%Y-%m-%dT%H:%M:%SZ')
                except ValueError:
                    pass
            else:
                curr_date_val = 'All time'

            if (series_point['Frequency'] == "W"):
                curr_date_val = curr_date_val - timedelta(days = curr_date_val.weekday())
            series[series_name].add_value(series_point['Value'], curr_date_val)
        return series

    def creates_pandas_series(self, series, pandas_series):
        for series_name, series_content in series.items():
            pandas_series[series_name] = series_content.get_pandas_series()
        return pandas_series

    def create_pandas_dataframe(self, pandas_series, names_of_dimensions):
        pandas_data_frame = pandas.DataFrame(pandas_series)
        pandas_data_frame.sort_index()
        if isinstance(pandas_data_frame.columns, pandas.MultiIndex):
            pandas_data_frame.columns.names = names_of_dimensions
        return pandas_data_frame

    def _load_dimensions(self):
        for dim in self.dataset.dimensions:
            self.dimensions.append(self.client.get_dimension(self.dataset.id, dim.id))
    

class SelectionDataReader(DataReader):

    def __init__(self, client, dim_values, transform = None):
        super().__init__(client)
        self.dim_values = dim_values
        self.transform = transform

    def _get_dim_members(self, dim, splited_values):
        members = []
        for value in splited_values:
            if value is None or isinstance(value, str) and not value:
                raise ValueError('Selection for dimension {} is empty'.format(dim.name))

            member = dim.find_member_by_id(value)
            if member is None:
                member = dim.find_member_by_name(value)

            if member is None:
                member = dim.find_member_by_regionid(value)

            if member is None:
                member = dim.find_member_by_ticker(value)

            if member is None and value.isnumeric():
                member = dim.find_member_by_key(int(value))

            if member:
                members.append(member.key)
            else:
                raise ValueError('Selection for dimension {} contains invalid elements'.format(dim.name))

        return members    

    def _find_dimension(self, dim_name_or_id):

        dim = self.dataset.find_dimension_by_name(dim_name_or_id)
        if dim is None:
            dim = self.dataset.find_dimension_by_id(dim_name_or_id)

        return dim

    def _add_full_selection_by_empty_dim_values(self, filter_dims, pivot_req):
        out_of_filter_dim_id = [dim.id for dim in self.dataset.dimensions if dim not in filter_dims]
        for id in out_of_filter_dim_id:
            pivot_req.stub.append(definition.PivotItem(id, []))

    def _create_pivot_request(self):

        pivot_req = definition.PivotRequest(self.dataset.id)

        filter_dims = []
        time_range = None
        for name, value in self.dim_values.items():
            if definition.is_equal_strings_ignore_case(name, 'timerange'):
                time_range = value
                continue

            splited_values = value.split(self.separator) if isinstance(value, str) else value
            if definition.is_equal_strings_ignore_case(name, 'frequency'):
                pivot_req.frequencies = splited_values
                continue

            dim = self._find_dimension(name)
            if dim is None:
                raise ValueError('Dimension with id or name {} is not found'.
                                 format(name))

            filter_dims.append(dim)

            for dimension in self.dimensions:
                if dimension.id == dim.id:
                    dim = dimension
                    break

            aggregations = None
            if len(splited_values) > 0 and splited_values[0].startswith('@'):
                aggregations = splited_values[0][1:]
                splited_values = splited_values[1:]

            members = self._get_dim_members(dim, splited_values)
            if not members and aggregations == None:
                raise ValueError('Selection for dimension {} is empty'.format(dim.name))

            pivot_req.stub.append(definition.PivotItem(dim.id, members, aggregations = aggregations))

        self._add_full_selection_by_empty_dim_values(filter_dims, pivot_req)

        if time_range:
            pivot_req.header.append(definition.PivotTimeItem('Time', [time_range], 'range'))
        else:
            pivot_req.header.append(definition.PivotTimeItem('Time', [], 'AllData'))

        if self.transform is not None:
            pivot_req.transform = self.transform

        return pivot_req

class TransformationDataReader(SelectionDataReader):

    def __init__(self, client, dim_values, transform, frequency, group_by = None):
        if dim_values == None:
            dim_values = {}
        if transform:
            dim_values['transform'] = transform
        if frequency:
            dim_values['frequency'] = frequency
        self.group_by = group_by
        super().__init__(client, dim_values)

    def get_pandasframe_pivot(self, pivot_resp):
        names_of_dimensions = self._get_dimension_names()
            
        # create dataframe with data
        series = self._get_data_series(pivot_resp)
        pandas_series = self.creates_pandas_series(series, {})
        pandas_data_frame = self.create_pandas_dataframe(pandas_series, names_of_dimensions)
        if not self.include_metadata:
            return pandas_data_frame
            
        # create dataframe with metadata
        series_with_attr = self._get_metadata_series(pivot_resp)
        pandas_series_with_attr = self.creates_pandas_series(series_with_attr, {})
        pandas_data_frame_with_attr = self.create_pandas_dataframe(pandas_series_with_attr, names_of_dimensions)         
        return pandas_data_frame, pandas_data_frame_with_attr

    def get_pandasframe(self):
        pivot_resp = self.client.get_dataset_data(self.dataset.id, self._get_data_query())
        
        return self.get_pandasframe_pivot(pivot_resp)

    def _get_time_point_amount(self, start_date, end_date, frequency):
        count = 0
        dict_with_delta = {
            'A': relativedelta(years = 1),
            'H': relativedelta(months = 6),
            'Q': relativedelta(months = 3),
            'FQ': relativedelta(months = 3),
            'M': relativedelta(months = 1),
            'W': timedelta(days = 7),
            'D': timedelta(days = 1)
        }

        data_begin_val = datetime.strptime(start_date, '%Y-%m-%dT%H:%M:%S')    
        data_end_val = datetime.strptime(end_date, '%Y-%m-%dT%H:%M:%S')
        if (frequency == "W"):
            data_begin_val = data_begin_val - timedelta(days = data_begin_val.weekday())
            data_end_val = data_end_val - timedelta(days = data_end_val.weekday())
        delta = dict_with_delta[frequency]
        curr_date_val = data_begin_val
        while curr_date_val<=data_end_val:
            curr_date_val += delta
            count += 1

        # we have to increase amount of points for FQ freq because of export from OASIS issue
        if frequency == 'FQ':
           count += 10
                
        return count

    def _get_part_of_metadata(self, metadata, offset):
        limit = 50000
        member_count_limit = 200

        if offset >= len(metadata):
            return None

        curr_points = 0
        curr_member_count = 0
        time_points = 0
        dim_values = {}
        index = offset
        for i in range(offset, len(metadata)):
            curr_ts = metadata[i]
            curr_time_points = self._get_time_point_amount(curr_ts['startDate'], curr_ts['endDate'], curr_ts['frequency'])
            if curr_time_points > time_points:
                time_points = curr_time_points

            curr_points = time_points
            curr_member_count = 0
            for dim in self.dataset.dimensions:
                dim_id = dim.id

                if dim_id not in curr_ts:
                    raise ValueError('There is no value for dim: {}'.format(dim_id))

                member_key = str(curr_ts[dim_id]['key'])
                if dim_id in dim_values:
                    if member_key not in dim_values[dim_id]:
                        dim_values[dim_id].append(member_key)
                else:
                    dim_values[dim_id] = [member_key]

                curr_points = curr_points * len(dim_values[dim_id])
                curr_member_count = curr_member_count + len(dim_values[dim_id])

            if curr_points >= limit or curr_member_count >= member_count_limit:
                break

            index = i

        return metadata[offset:min(index + 1, len(metadata))]

    def _get_series_with_attr(self, series, series_with_attr):
        res = {}
        for series_name, _ in series.items():
            if series_name in series_with_attr:
                res[series_name] = series_with_attr[series_name]

        return res

    def _get_frequency_for_normalization(self, required_frequency, available_frequency):
        sorted_frequency = ['A', 'H', 'Q', 'M', 'W', 'D']
        sorted_available_frequency = []
        for f in sorted_frequency:
            if f in available_frequency:
                sorted_available_frequency.append(f)

        norm_index = sorted_frequency.index(sorted_available_frequency[-1])
        for f in sorted_available_frequency:
            if sorted_frequency.index(f) > sorted_frequency.index(required_frequency):
                norm_index = sorted_frequency.index(f)
                break

        return sorted_frequency[norm_index]


    def _get_all_series_for_group(self, group_name, group_name_index, frequency, series, metadata):
        series_by_group = {}
        for series_name, series_item in series.items():
            if series_name[group_name_index] == group_name:
                series_by_group[series_name] = series_item
        
        available_frequency = {}
        count = 0
        for md in metadata:
            if md[self.group_by]['name'] == group_name:
                freq = md['frequency']
                if freq not in available_frequency:
                    available_frequency[freq] = 0
                    
                available_frequency[freq] += 1
                count += 1

        if frequency == None:
            if len(series_by_group.keys()) == count:
                return series_by_group

        if len(available_frequency.keys()) == 0:
            return None

        norm_frequency = frequency
        if frequency not in available_frequency.keys():
            norm_frequency = self._get_frequency_for_normalization(frequency, available_frequency)
        
        if len(series_by_group.keys()) == available_frequency[norm_frequency]:
                return series_by_group

        return None

    def get_pandasframe_by_metadata(self, metadata, timerange):
        names_of_dimensions = self._get_dimension_names()

        series = {}
        series_with_attr = {}
        offset = 0
        while True:
            metadata_part = self._get_part_of_metadata(metadata, offset)
            if metadata_part == None:
                break

            offset += len(metadata_part)

            dim_values = {}
            for item in metadata_part:
                for dim in self.dataset.dimensions:
                    dim_id = dim.id

                    if dim_id not in item:
                        raise ValueError('There is no value for dim: {}'.format(dim_id))

                    member_key = str(item[dim_id]['key'])
                    if dim_id in dim_values:
                        if member_key not in dim_values[dim_id]:
                            dim_values[dim_id].append(member_key)
                    else:
                        dim_values[dim_id] = [member_key]

            if timerange != None:
                dim_values['timerange'] = timerange
            if 'transform' in self.dim_values:
                dim_values['transform'] = self.dim_values['transform']
            if 'frequency'in self.dim_values:
                dim_values['frequency'] = self.dim_values['frequency']

            self.dim_values = dim_values
            pivot_resp = self.client.get_dataset_data(self.dataset.id, self._get_data_query())

            part_series = self._get_data_series(pivot_resp)
            series.update(part_series)

            if self.include_metadata:
                part_series_with_attr = self._get_metadata_series(pivot_resp)
                series_with_attr.update(part_series_with_attr)

        pandas_series = self.creates_pandas_series(series, {})
        pandas_data_frame = self.create_pandas_dataframe(pandas_series, names_of_dimensions)
        if not self.include_metadata:
            return pandas_data_frame

        pandas_series_with_attr = self.creates_pandas_series(series_with_attr, {})
        pandas_data_frame_with_attr = self.create_pandas_dataframe(pandas_series_with_attr, names_of_dimensions)         
        return pandas_data_frame, pandas_data_frame_with_attr

    def get_pandasframe_by_metadata_grouped(self, metadata, timerange):
        names_of_dimensions = self._get_dimension_names()

        series = {}
        series_with_attr = {}
        offset = 0
        while True:
            metadata_part = self._get_part_of_metadata(metadata, offset)
            if metadata_part == None:
                break

            offset += len(metadata_part)

            dim_values = {}
            for item in metadata_part:
                for dim in self.dataset.dimensions:
                    dim_id = dim.id

                    if dim_id not in item:
                        raise ValueError('There is no value for dim: {}'.format(dim_id))

                    member_key = str(item[dim_id]['key'])
                    if dim_id in dim_values:
                        if member_key not in dim_values[dim_id]:
                            dim_values[dim_id].append(member_key)
                    else:
                        dim_values[dim_id] = [member_key]

            frequency = None
            if timerange != None:
                dim_values['timerange'] = timerange
            if 'transform' in self.dim_values:
                dim_values['transform'] = self.dim_values['transform']
            if 'frequency'in self.dim_values:
                frequency = self.dim_values['frequency']
                dim_values['frequency'] = self.dim_values['frequency']

            self.dim_values = dim_values
            pivot_resp = self.client.get_dataset_data(self.dataset.id, self._get_data_query())

            part_series = self._get_data_series(pivot_resp)
            series.update(part_series)

            if self.include_metadata:
                part_series_with_attr = self._get_metadata_series(pivot_resp)
                series_with_attr.update(part_series_with_attr)

            group_name_index = -1
            for i in range(len(self.dataset.dimensions)):
                if self.group_by == self.dataset.dimensions[i].id or self.group_by == self.dataset.dimensions[i].name:
                    self.group_by = self.dataset.dimensions[i].id
                    group_name_index = i
                    break

            groups_to_delete = []
            groups_checked = []
            for series_name, series_item in series.items():
                group_name = series_name[group_name_index]

                if group_name in groups_to_delete:
                    continue
                if group_name in groups_checked:
                    continue

                all_series_by_group = self._get_all_series_for_group(group_name, group_name_index, frequency, series, metadata)
                if all_series_by_group != None:
                    groups_to_delete.append(group_name)
                    all_panda_series_by_group = self.creates_pandas_series(all_series_by_group, {})
                    data_frame = definition.DataFrame()
                    data_frame.id = group_name
                    data_frame.data = self.create_pandas_dataframe(all_panda_series_by_group, names_of_dimensions)

                    if self.include_metadata:
                        all_series_with_attr_by_group = self._get_series_with_attr(all_series_by_group, series_with_attr)
                        all_pandes_series_with_attr_by_group = self.creates_pandas_series(all_series_with_attr_by_group, {})
                        data_frame.metadata = self.create_pandas_dataframe(all_pandes_series_with_attr_by_group, names_of_dimensions)
                        
                    yield data_frame
                else:
                    groups_checked.append(group_name)

            left_series = {}
            for series_name, series_item in series.items():
                group_name = series_name[group_name_index]
                if group_name not in groups_to_delete:
                    left_series[series_name] = series_item
                
            series = left_series

            left_series_with_attr = {}
            for series_name, series_item in series_with_attr.items():
                group_name = series_name[group_name_index]
                if group_name not in groups_to_delete:
                    left_series_with_attr[series_name] = series_item
                
            series_with_attr = left_series_with_attr

    def _get_series_with_metadata(self, series_point, resp):
        names = []
        resp_dims = [dim for dim in (resp.header + resp.stub + resp.filter) if dim.fields and any(dim.fields)]
        for dim in resp_dims:
            if dim.dimensionid == 'Time' or not dim.metadataFields:
                continue
            for item in dim.metadataFields:
                if item.name == series_point[dim.dimensionid]:
                    dim_attrs = item.fields
                    break

            for attr in dim.fields: 
                find_elem = False
                if attr['isSystemField']:
                    continue
                for key, value in dim_attrs.items(): 
                    if definition.is_equal_strings_ignore_case(key, attr['name']):
                        find_elem = True
                        names.append(value)
                        break
                if not find_elem:
                    names.append(None)

        names.append(series_point.get('Unit'))
        names.append(series_point.get('Scale'))
        names.append(series_point.get('Mnemonics'))
        for attr in self.dataset.timeseries_attributes:
            names.append(series_point.get(attr.name))  
        return tuple(names)     
        
    def _get_attribute_names(self, resp):
        resp_dims = [dim for dim in (resp.header + resp.stub + resp.filter) if dim.fields and any(dim.fields)]
        names = []
        for dim in resp_dims:
            for d in self.dataset.dimensions:
                if dim.dimensionid == d.id:
                    for attr in dim.fields:
                        if not attr['isSystemField']:
                            names.append(d.name +  ' ' + attr["displayName"])
                    break
        names.append('Unit')            
        names.append('Scale') 
        names.append('Mnemonics')
        for attr in self.dataset.timeseries_attributes:
            names.append(attr.name)     
        return names

    def _get_metadata_series(self, resp):
        series = {}
        names_of_attributes = self._get_attribute_names(resp)
        for series_point in resp.tuples:
            serie_name = self._get_series_name(series_point)
            if serie_name not in series:
                serie_attrs = self._get_series_with_metadata(series_point, resp)
                series[serie_name] = KnoemaSeries(serie_name, serie_attrs, names_of_attributes)
        return series  

    def _get_dimension_names(self):
        names = []
        for dim in self.dataset.dimensions:
            names.append(dim.name)
        if self.dataset.has_time:
            names.append('Frequency')             
        return names   

    def _get_series_name(self, series_point):
        names = []
        for dim in self.dataset.dimensions:
            if dim.id in series_point:
                names.append(series_point[dim.id])
        if self.dataset.has_time:
            names.append(series_point['Frequency'])
        return tuple(names) 

    def _get_data_query(self):
        filter_dims = {}
        passed_params = ['timerange', 'transform']

        for name, value in self.dim_values.items():
            if name.lower() in passed_params:
                filter_dims[name] = quote(value)
                continue
            if definition.is_equal_strings_ignore_case(name, 'datecolumn') and self.dataset.type != 'Regular':
                filter_dims['datecolumn'] = quote(value)
                continue

            splited_values = [x for x in value.split(self.separator) if x] if isinstance(value, str) else value
            if definition.is_equal_strings_ignore_case(name, 'frequency'):
                filter_dims["frequency"] = self.separator.join(splited_values)
                continue

            dim = self._find_dimension(name)
            if dim is None:
                raise ValueError('Dimension with id or name {} is not found'.
                                 format(name))

            if not splited_values:
                raise ValueError('Selection for dimension {} is empty'.format(dim.name))

            filter_dims[dim.id] =  self.separator.join(quote(s) for s in splited_values)

        if self.include_metadata:
            filter_dims['metadata'] = 'true'
        
        if self.separator != ',':
             filter_dims['separator'] = self.separator

        return "&".join("=".join((str(key), str(value))) for key, value in filter_dims.items())

class StreamingDataReader(SelectionDataReader):

    def __init__(self, client, dim_values, transform = None):
        super().__init__(client, dim_values, transform)
    
    def _get_series_name(self, series_point):
        names = []
        for dim in self.dimensions:
            names.append(series_point[dim.id]['name'])
        names.append(series_point['frequency'])
        return tuple(names)

    def _get_series_with_metadata(self, series_point):
        names = []
        for dim in self.dimensions:
            for item in dim.items:
                if item.name == series_point[dim.id]['name']:
                    dim_attrs = item.fields
                    break
            for attr in dim.fields: 
                find_elem = False
                if not attr['isSystemField']: 
                    for key, value in dim_attrs.items(): 
                        if definition.is_equal_strings_ignore_case(key, attr['name']):
                            find_elem = True
                            names.append(value)
                            break
                    if not find_elem:
                        names.append(None)        

        names.append(series_point.get('unit'))
        names.append(series_point.get('scale'))
        names.append(series_point.get('mnemonics'))
        for attr in self.dataset.timeseries_attributes:
            names.append(series_point['timeseriesAttributes'][attr.name])
        return tuple(names)

    def _get_metadata_series(self, resp, names_of_attributes):
        series = {}
        for series_point in resp.series:
            serie_name = self._get_series_name(series_point)
            if serie_name not in series:
                serie_attrs = self._get_series_with_metadata(series_point)
                series[serie_name] = KnoemaSeries(serie_name, serie_attrs, names_of_attributes)
        return series  

    def _get_data_series(self, resp):
        series = {}
        dict_with_delta = {
            'A': relativedelta(years = 1),
            'H': relativedelta(months = 6),
            'Q': relativedelta(months = 3),
            'FQ': relativedelta(months = 3),
            'M': relativedelta(months = 1),
            'W': timedelta(days = 7),
            'D': timedelta(days = 1)}
        for series_point in resp.series:  
            all_values = series_point['values']  
            series_name = self._get_series_name(series_point)
            date_format = '%Y-%m-%dT%H:%M:%SZ' if series_point['startDate'].endswith('Z') else '%Y-%m-%dT%H:%M:%S'
            data_begin_val = datetime.strptime(series_point['startDate'], date_format)
            data_end_val = datetime.strptime(series_point['endDate'], date_format)
            if (series_point['frequency'] == "W"):
                data_begin_val = data_begin_val - timedelta(days = data_begin_val.weekday())
                data_end_val = data_end_val - timedelta(days = data_end_val.weekday())
            delta = dict_with_delta[series_point['frequency']]
            curr_date_val = data_begin_val
            index = []
            values = []
            i = 0
            while curr_date_val<=data_end_val:
                val = all_values[i]
                if val is not None:
                    index.append(curr_date_val)
                    values.append(val)
                curr_date_val += delta
                i += 1
            series[series_name] = KnoemaSeries(series_name,values,index)
        return series

    def get_series_metadata(self):
        self._load_dimensions()
        pivot_req = self._create_pivot_request()

        data_streaming = self.client.get_data_raw(pivot_req, True)
        
        return data_streaming.series

    def get_pandasframe(self):
        self._load_dimensions()
        pandas_series = {}
        names_of_dimensions = self._get_dimension_names()
        if self.include_metadata:
            pandas_series_with_attr = {}
            names_of_attributes = self._get_attribute_names()

        pivot_req = self._create_pivot_request()
        data_streaming = self.client.get_data_raw(pivot_req)
        # create dataframe with data
        series = self._get_data_series(data_streaming)
        pandas_series = self.creates_pandas_series(series, pandas_series)
        pandas_data_frame = self.create_pandas_dataframe(pandas_series, names_of_dimensions)
        if not self.include_metadata:
            return pandas_data_frame
            
        # create dataframe with metadata
        series_with_attr = self._get_metadata_series(data_streaming, names_of_attributes)
        pandas_series_with_attr = self.creates_pandas_series(series_with_attr, pandas_series_with_attr)
        pandas_data_frame_with_attr = self.create_pandas_dataframe(pandas_series_with_attr, names_of_dimensions)         
        return pandas_data_frame, pandas_data_frame_with_attr

class MnemonicsDataReader(DataReader):

    def __init__(self, client, mnemonics, transform, frequency):
        super().__init__(client)
        self.mnemonics = mnemonics
        self.transform = transform
        self.frequency = frequency

    def _get_metadata_series(self, resp, names_of_attributes):
        series = {}
        for series_point in resp.tuples:
            val = series_point['Value']
            if val is None:
                continue
            serie_name = series_point['Mnemonics']
            if serie_name not in series:
                serie_attrs = self._get_series_with_metadata(series_point)
                series[serie_name] = KnoemaSeries(serie_name, serie_attrs, names_of_attributes)
        return series  

    def _get_data_series(self, resp):
        series = {}
        for series_point in resp.tuples:
            val = series_point['Value']
            if val is None:
                continue
            series_name = series_point['Mnemonics']
            if series_name not in series:
                series[series_name] = KnoemaSeries(series_name,[],[])

            curr_date_val = series_point['Time']
            try:
                curr_date_val = datetime.strptime(series_point['Time'], '%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                pass

            if (series_point['Frequency'] == "W"):
                curr_date_val = curr_date_val - timedelta(days = curr_date_val.weekday())
            series[series_name].add_value(series_point['Value'], curr_date_val)
        return series

    def _get_pandasframe_one_dataset(self):
        pandas_series = {}
        if self.include_metadata:
            pandas_series_with_attr = {}
            names_of_attributes = self._get_attribute_names()

        mnemonics = self.mnemonics
        mnemonics_string = self.separator.join(mnemonics) if isinstance(mnemonics, list) else mnemonics
        mnemonics_resp = self.client.get_mnemonics(mnemonics_string, self.transform, self.frequency)
            
        for item in mnemonics_resp.items:
            pivot_resp = item.pivot
            if not definition.is_equal_strings_ignore_case(self.dataset.id, pivot_resp.dataset):
                continue
            # create dataframe with data for mnemonics
            series = self._get_data_series(pivot_resp)
            pandas_series = self.creates_pandas_series(series, pandas_series)
            if self.include_metadata:
                # create dataframe with metadata for mnemonics
                series_with_attr = self._get_metadata_series(pivot_resp, names_of_attributes)
                pandas_series_with_attr = self.creates_pandas_series(series_with_attr, pandas_series_with_attr)     

        pandas_data_frame = self.create_pandas_dataframe(pandas_series, [])
        if not self.include_metadata:
            return pandas_data_frame
        pandas_data_frame_with_attr = self.create_pandas_dataframe(pandas_series_with_attr, [])         
        return pandas_data_frame, pandas_data_frame_with_attr

    def _get_pandasframe_across_datasets(self):
           
        mnemonics_string = self.separator.join(self.mnemonics) if isinstance(self.mnemonics, list) else self.mnemonics
        mnemonics_resp = self.client.get_mnemonics(mnemonics_string, self.transform, self.frequency)

        dict_datasets = {}
        dict_dimensions = {} 
        pandas_series = {}
        if self.include_metadata:
            pandas_series_with_attr = {}
            dict_attributes_names = {}

        for item in mnemonics_resp.items:
            pivot_resp = item.pivot
            if pivot_resp is None:
                continue
            dataset_id = pivot_resp.dataset
            if dataset_id  not in dict_datasets:
                dataset = self.client.get_dataset(dataset_id)
                self.dataset = dataset
                dict_datasets[dataset_id] = dataset
                dimensions = []
                for dim in dataset.dimensions:
                    dimensions.append(self.client.get_dimension(dataset_id, dim.id))
                dict_dimensions[dataset_id] = dimensions
                self.dimensions = dimensions
                if self.include_metadata:
                    names_of_attributes = self._get_attribute_names()
                    dict_attributes_names[dataset_id] = names_of_attributes
            else:
                self.dataset = dict_datasets[dataset_id]
                self.dimensions = dict_dimensions[dataset_id]
                if self.include_metadata:
                    names_of_attributes = dict_attributes_names[dataset_id]
                    
            # create dataframe with data for mnemonics
            series = self._get_data_series(pivot_resp)
            pandas_series = self.creates_pandas_series(series, pandas_series)
            if self.include_metadata:
                # create dataframe with metadata for mnemonics
                series_with_attr = self._get_metadata_series(pivot_resp, names_of_attributes)
                pandas_series_with_attr = self.creates_pandas_series(series_with_attr, pandas_series_with_attr)     

        pandas_data_frame = self.create_pandas_dataframe(pandas_series, [])
        if not self.include_metadata:
            return pandas_data_frame
        pandas_data_frame_with_attr = self.create_pandas_dataframe(pandas_series_with_attr, [])         
        return pandas_data_frame, pandas_data_frame_with_attr

    def get_pandasframe(self):
        """The method loads data from dataset"""
        if self.dataset:
            self._load_dimensions()
            return self._get_pandasframe_one_dataset()
        return self._get_pandasframe_across_datasets()


class KnoemaSeries(object):
    """This class combines values and index points for one time series"""

    def __init__(self, name, values=[], index=[]):
        self.name = name
        self.values = values
        self.index = index

    def add_value(self, value, index_point):
        """The function is addeing new value to provied index. If index does not exist"""
        if index_point not in self.index:
            self.values.append(value)
            self.index.append(index_point)

    def get_pandas_series(self):
        """The function creates pandas series based on index and values"""
        return pandas.Series(self.values, self.index, name=self.name)
