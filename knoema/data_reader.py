"""This module contains data definitions for Knoema client"""

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

import pandas
import knoema.api_definitions as definition
import knoema.view_definitions as view_definition

class DataReader(object):
    """This class read data from Knoema and transform it to pandas frame"""

    def __init__(self, client):
        self.client = client
        self.dataset = None
        self.include_metadata = False
        self.columns = None
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
        for dim in self.dataset.dimensions:
            names.append(dim.name)
        if self.dataset.has_time:
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
                series[serie_name] = KnoemaSeries(serie_name, serie_attrs, names_of_attributes, None)
        return series

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

        if self.columns is not None:
            pivot_req.columns = self.columns

        return pivot_req

class ResponseReader(object):
    def __init__(self, reader):
        self.include_metadata = reader.include_metadata
        self.dataset = reader.dataset
        self.reader = reader
        super().__init__()

    def _get_detail_columns(self, resp):
        detail_columns = []
        if resp.descriptor is not None and 'detailColumns' in resp.descriptor and resp.descriptor['detailColumns'] is not None:
            for column in resp.descriptor['detailColumns']:
                detail_columns.append(column['name'])

        return detail_columns if len(detail_columns) > 0 else None

class PivotResponseReader(ResponseReader):
    def __init__(self, reader, pivot_resp):
        self.pivot_resp = pivot_resp
        super().__init__(reader)
    
    def get_pandasframe(self):
        names_of_dimensions = self.reader._get_dimension_names()
            
        # create dataframe with data
        detail_columns = self._get_detail_columns(self.pivot_resp)
        series = self._get_data_series(self.pivot_resp, detail_columns)
        pandas_series = PandasHelper.creates_pandas_series(series, {}, detail_columns)
        pandas_data_frame = PandasHelper.create_pandas_dataframe(pandas_series, names_of_dimensions, detail_columns)
        if not self.include_metadata:
            return pandas_data_frame
            
        # create dataframe with metadata
        series_with_attr = self._get_metadata_series(self.pivot_resp)
        pandas_series_with_attr = PandasHelper.creates_pandas_series(series_with_attr, {}, None)
        pandas_data_frame_with_attr = PandasHelper.create_pandas_dataframe(pandas_series_with_attr, names_of_dimensions, None)
        return pandas_data_frame, pandas_data_frame_with_attr

    def _get_data_series(self, resp, detail_columns):
        series_map = {}

        columns = None
        frequency_list = []
        for series_point in resp.tuples:
            val = series_point['Value']
            if val is None:
                continue
            series_name = self._get_series_name(series_point)
            if series_name in series_map:
                series = series_map[series_name]
            else:
                series = KnoemaSeries(series_name, [], [], detail_columns)
                series_map[series_name] = series

            freq = series_point['Frequency']

            if 'Time' in series_point:
                curr_date_val = series_point['Time']
                try:
                    curr_date_val = datetime.strptime(series_point['Time'], '%Y-%m-%dT%H:%M:%SZ')

                    if (freq == "W"):
                        curr_date_val = curr_date_val - timedelta(days = curr_date_val.weekday())
                except ValueError:
                    pass
            else:
                curr_date_val = 'All time'

            if freq not in frequency_list:
                frequency_list.append(freq)
            if freq == "W":
                curr_date_val = curr_date_val - timedelta(days = curr_date_val.weekday())
            if freq == "FQ":
                curr_date_val = TimeFormat.format_statistical(curr_date_val, 'FQ')
            if detail_columns is not None:
                columns = []
                for column_name in detail_columns:
                    columns.append(series_point[column_name])

            series.add_value(series_point['Value'], curr_date_val, columns)

        if 'FQ' in frequency_list and len(frequency_list) > 1:
            raise ValueError('Please provide a valid frequency list. You can request FQ or others frequencies not together.')

        return series_map

    def _get_series_name(self, series_point):
        names = []
        for dim in self.dataset.dimensions:
            if dim.id in series_point:
                names.append(series_point[dim.id])
        if self.dataset.has_time:
            names.append(series_point['Frequency'])
        return tuple(names) 

    def _get_metadata_series(self, resp):
        series = {}
        names_of_attributes = self._get_attribute_names(resp)
        for series_point in resp.tuples:
            serie_name = self._get_series_name(series_point)
            if serie_name not in series:
                serie_attrs = self._get_series_with_metadata(series_point, resp)
                series[serie_name] = KnoemaSeries(serie_name, serie_attrs, names_of_attributes, None)
        return series  

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

class StreamingResponseReader(ResponseReader):
    def __init__(self, reader, data_streaming):
        self.data_streaming = data_streaming
        super().__init__(reader)
    
    def get_pandasframe(self):
        detail_columns = self._get_detail_columns(self.data_streaming)
        # create dataframe with data
        series = self._get_data_series(self.data_streaming, detail_columns)

        pandas_series = {}
        names_of_dimensions = self.reader._get_dimension_names()
        if self.include_metadata:
            pandas_series_with_attr = {}
            names_of_attributes = self._get_attribute_names()

        pandas_series = PandasHelper.creates_pandas_series(series, pandas_series, detail_columns)
        pandas_data_frame = PandasHelper.create_pandas_dataframe(pandas_series, names_of_dimensions, detail_columns)
        if not self.include_metadata:
            return pandas_data_frame
            
        # create dataframe with metadata
        series_with_attr = self._get_metadata_series(self.data_streaming, names_of_attributes)
        pandas_series_with_attr = PandasHelper.creates_pandas_series(series_with_attr, pandas_series_with_attr, None)
        pandas_data_frame_with_attr = PandasHelper.create_pandas_dataframe(pandas_series_with_attr, names_of_dimensions, None)         
        return pandas_data_frame, pandas_data_frame_with_attr

    def _get_data_series(self, resp, detail_columns):
        series_map = {}
        dict_with_delta = TimeFormat.get_frequencies_delta()

        frequency_list = []
        use_stat_format_for_date_label = self._get_use_stat_format_for_date_label(resp.series)

        detail_values = None
        for series_point in resp.series:  
            all_values = series_point['values']  
            series_name = self._get_series_name(series_point)
            if detail_columns is not None:
                detail_values = []
                for column_name in detail_columns:
                    detail_values.append(series_point[column_name])

            date_format = '%Y-%m-%dT%H:%M:%S' + ('Z' if series_point['startDate'].endswith('Z') else '')
            data_begin_val = datetime.strptime(series_point['startDate'], date_format)
            freq = series_point['frequency']
            if freq not in frequency_list:
                frequency_list.append(freq)
            if (freq == "W"):
                data_begin_val = data_begin_val - timedelta(days = data_begin_val.weekday())
            
            delta = dict_with_delta[freq]
            series = KnoemaSeries(series_name, [], [], detail_columns)
            date_labels = series_point['dateLabels'] if 'dateLabels' in series_point else None
            curr_date_val = data_begin_val
            for vi in range(0, len(all_values)):
                val = all_values[vi]
                if val is not None:
                    date = curr_date_val if freq != 'FQ' else TimeFormat.format_statistical(curr_date_val, 'FQ')
                    if freq in use_stat_format_for_date_label or date_labels is not None and date_labels[vi] is not None:
                        date = TimeFormat.format_statistical(curr_date_val, freq) \
                            if freq in use_stat_format_for_date_label else datetime.strptime(date_labels[vi], date_format)
                    series.index.append(date)
                    series.values.append(val)
                    for ai in range(0, series.column_count):
                        series.column_values[ai].append(detail_values[ai][vi])
                curr_date_val += delta

            series_map[series_name] = series

        if 'FQ' in frequency_list and len(frequency_list) > 1:
            raise ValueError('Please provide a valid frequency list. You can request FQ or others frequencies not together.')

        return series_map

    def _get_use_stat_format_for_date_label(self, series):
        use_stat_format_for_date_label = {}
        date_labels_by_freq = {}
        has_date_labels_by_freq = {}
        dict_with_delta = TimeFormat.get_frequencies_delta()
        for series_point in series:  
            freq = series_point['frequency']
            if freq in use_stat_format_for_date_label:
                continue
            has_date_labels = 'dateLabels' in series_point
            if freq in has_date_labels_by_freq and has_date_labels_by_freq[freq] != has_date_labels:
                use_stat_format_for_date_label[freq] = True
                continue
            has_date_labels_by_freq[freq] = has_date_labels
            if not has_date_labels:
                continue

            if freq not in date_labels_by_freq:
                date_labels_by_freq[freq] = {}

            date_labels = series_point['dateLabels']

            all_values = series_point['values']  

            date_format = '%Y-%m-%dT%H:%M:%S' + ('Z' if series_point['startDate'].endswith('Z') else '')
            data_begin_val = datetime.strptime(series_point['startDate'], date_format)
            if (freq == "W"):
                data_begin_val = data_begin_val - timedelta(days = data_begin_val.weekday())
            
            delta = dict_with_delta[freq]
            curr_date_val = data_begin_val
            for vi in range(0, len(all_values)):
                if curr_date_val in date_labels_by_freq[freq]:
                    if date_labels[vi] is not None and date_labels_by_freq[freq][curr_date_val] != date_labels[vi]:
                        use_stat_format_for_date_label[freq] = True
                        break
                else:
                    date_labels_by_freq[freq][curr_date_val] = date_labels[vi]
                curr_date_val += delta
        return use_stat_format_for_date_label

    def _get_series_name(self, series_point):
        names = []
        for dim in self.dataset.dimensions:
            names.append(series_point[dim.id]['name'] if 'name' in series_point[dim.id] else series_point[dim.id])
        if 'frequency' in series_point:
            names.append(series_point['frequency'])
        return tuple(names)

    def _get_attribute_names(self):
        names = []
        for dim in self.dataset.dimensions:
            for attr in self.data_streaming.dimensionFields[dim.id]:
                if not attr['isSystemField']:
                    names.append(dim.name +' '+ attr['displayName'])
        names.append('Unit')            
        names.append('Scale') 
        names.append('Mnemonics')
        for attr in self.dataset.timeseries_attributes:
            names.append(attr.name)     
        return names

    def _get_metadata_series(self, resp, names_of_attributes):
        series = {}
        for series_point in resp.series:
            serie_name = self._get_series_name(series_point)
            if serie_name not in series:
                serie_attrs = self._get_series_with_metadata(series_point, resp.dimensionFields)
                series[serie_name] = KnoemaSeries(serie_name, serie_attrs, names_of_attributes, None)
        return series  

    def _get_series_with_metadata(self, series_point, dimensionFields):
        names = []
        for dim in self.dataset.dimensions:
            dimFields = dimensionFields[dim.id]
            dimValues = series_point[dim.id]
            for attr in dimFields: 
                if not attr['isSystemField']: 
                    if attr['name'] in dimValues:
                        names.append(dimValues[attr['name']])
                    else:
                        names.append(None)        

        names.append(series_point.get('unit'))
        names.append(series_point.get('scale'))
        names.append(series_point.get('mnemonics'))
        for attr in self.dataset.timeseries_attributes:
            names.append(series_point['timeseriesAttributes'][attr.name])
        return tuple(names)

class DetailsResponseReader(ResponseReader):
    def __init__(self, reader, data_resp):
        self.data_resp = data_resp
        super().__init__(reader)
    
    def get_pandasframe(self):
        records = self.convert_pandasframe()
        if not self.include_metadata:
            return records
        return records, None

    def convert_pandasframe(self):
        titles = []
        columns = []
        indexes = []
        for col in self.data_resp.columns:
            indexes.append(col['index'])
            if col['dimensionId'] != None:
                if 'detailColumns' in col:
                    titles.append(col['name'])
                    columns.append([col['id'], 'name'])
                    for detail in col['detailColumns']:
                        indexes.append(detail['index'])
                        titles.append(detail['name'])
                        columns.append([col['id'], detail['id']])
                else:
                    titles.append(col['name'])
                    columns.append(col['id'])
            else:
                if col['type'] == 'Date':
                    titles.append(col['name'])
                    columns.append([col['id'], 'value'])
                    continue

                if col['type'] == 'Currency':
                    titles.append(col['name'])
                    columns.append([col['id'], 'value'])
                    continue

                titles.append(col['name'])
                columns.append(col['id'])

        #sort both array by indexes
        titles, columns = zip(*[x for _,x in sorted(zip(indexes, zip(titles, columns)))])
        records = []
        for tuple in self.data_resp.tuples:
            record = []

            for col in columns:
                if isinstance(col, str):
                    val = tuple[col]
                    record.append(val)
                else:
                    val = tuple[col[0]]
                    record.append(val[col[1]] if val != None else None)

            records.append(record)

        return pandas.DataFrame(data=records, columns=titles)        

class TransformationDataReader(SelectionDataReader):

    def __init__(self, client, dim_values, transform, frequency):
        if dim_values == None:
            dim_values = {}
        if transform:
            dim_values['transform'] = transform
        if frequency:
            dim_values['frequency'] = frequency
        super().__init__(client, dim_values)

    def get_pandasframe(self):
        data_resp = self.client.get_dataset_data(self.dataset.id, self._get_data_filters())
        if isinstance(data_resp, definition.DetailsResponse):
            response_reader = DetailsResponseReader(self, data_resp)
            return response_reader.get_pandasframe()

        if isinstance(data_resp, definition.RawDataResponse):
            token = data_resp.continuation_token
            while token is not None:
                res2 = self.client.get_data_raw_with_token(token)
                data_resp.series += res2.series
                token = res2.continuation_token

            response_reader = StreamingResponseReader(self, data_resp)
            return response_reader.get_pandasframe()

        response_reader = PivotResponseReader(self, data_resp)
        return response_reader.get_pandasframe()

    def _get_data_filters(self):
        filter_dims = {}
        passed_params = ['timerange', 'transform']

        for name, value in self.dim_values.items():
            if name.lower() in passed_params:
                filter_dims[name] = value
                continue
            if definition.is_equal_strings_ignore_case(name, 'datecolumn') and self.dataset.type != 'Regular':
                filter_dims['datecolumn'] = value
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

            filter_dims[dim.id] =  self.separator.join(s for s in splited_values)

        if self.include_metadata:
            filter_dims['metadata'] = 'true'
        
        if self.separator != ',':
             filter_dims['separator'] = self.separator

        return definition.DataAPIRequest(filter_dims)

class StreamingDataReader(SelectionDataReader):

    def __init__(self, client, dim_values, transform = None, group_by = None):
        self.group_by = group_by

        super().__init__(client, dim_values, transform)
    
    def get_series_metadata(self):
        self._load_dimensions()
        pivot_req = self._create_pivot_request()

        data_streaming = self.client.get_data_raw(pivot_req, True)
        
        return data_streaming.series

    def get_pandasframe(self):
        self._load_dimensions()
        pivot_req = self._create_pivot_request()
        data_streaming = self.client.get_data_raw(pivot_req)
        response_reader = StreamingResponseReader(self, data_streaming)
        return response_reader.get_pandasframe()

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
            else:
                return None

        if len(available_frequency.keys()) == 0:
            return None

        norm_frequency = frequency
        if frequency not in available_frequency.keys():
            norm_frequency = self._get_frequency_for_normalization(frequency, available_frequency)
        
        if len(series_by_group.keys()) == available_frequency[norm_frequency]:
                return series_by_group

        return None

    def get_pandasframe_by_metadata_grouped(self, metadata, frequency, timerange):
        self._load_dimensions()
        names_of_dimensions = self._get_dimension_names()
        
        series = {}
        series_with_attr = {}
        offset = 0
        detail_columns = None
        was = False
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

            if frequency != None:
                dim_values['frequency'] = frequency
            if timerange != None:
                dim_values['timerange'] = timerange

            self.dim_values = dim_values
            pivot_req = self._create_pivot_request()
            pivot_resp = self.client.get_data_raw(pivot_req)
            response_reader = StreamingResponseReader(self, pivot_resp)

            if was:
                if detail_columns is not None:
                    part_detail_columns = response_reader._get_detail_columns(pivot_resp)
                    if detail_columns != part_detail_columns:
                        detail_columns = None
            else:
                was = True
                detail_columns = response_reader._get_detail_columns(pivot_resp)

            part_series = response_reader._get_data_series(pivot_resp, detail_columns)
            series.update(part_series)

            if self.include_metadata:
                names_of_attributes = self._get_attribute_names()
                part_series_with_attr = response_reader._get_metadata_series(pivot_resp, names_of_attributes)
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
                    all_panda_series_by_group = PandasHelper.creates_pandas_series(all_series_by_group, {}, detail_columns)
                    data_frame = definition.DataFrame()
                    data_frame.id = group_name
                    data_frame.data = PandasHelper.create_pandas_dataframe(all_panda_series_by_group, names_of_dimensions, detail_columns)

                    if self.include_metadata:
                        all_series_with_attr_by_group = self._get_series_with_attr(all_series_by_group, series_with_attr)
                        all_pandes_series_with_attr_by_group = PandasHelper.creates_pandas_series(all_series_with_attr_by_group, {}, None)
                        data_frame.metadata = PandasHelper.create_pandas_dataframe(all_pandes_series_with_attr_by_group, names_of_dimensions, None)
                        
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

    def _get_time_point_amount(self, start_date, end_date, frequency):
        count = 0
        dict_with_delta = TimeFormat.get_frequencies_delta()

        date_format = '%Y-%m-%dT%H:%M:%S' + ('Z' if start_date.endswith('Z') else '')
        data_begin_val = datetime.strptime(start_date, date_format)
        date_format = '%Y-%m-%dT%H:%M:%S' + ('Z' if end_date.endswith('Z') else '')
        data_end_val = datetime.strptime(end_date, date_format)
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
                series[serie_name] = KnoemaSeries(serie_name, serie_attrs, names_of_attributes, None)
        return series  

    def _get_detail_columns(self, resp):
        return None

    def _get_data_series(self, resp, detail_columns):
        series = {}
        frequency_list = []
        for series_point in resp.tuples:
            val = series_point['Value']
            if val is None:
                continue
            series_name = series_point['Mnemonics']
            if series_name not in series:
                series[series_name] = KnoemaSeries(series_name, [], [], detail_columns)

            curr_date_val = series_point['Time']
            try:
                curr_date_val = datetime.strptime(series_point['Time'], '%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                pass

            freq = series_point['Frequency']
            if freq not in frequency_list:
                frequency_list.append(freq)
            if (freq == "W"):
                curr_date_val = curr_date_val - timedelta(days = curr_date_val.weekday())
            if (freq == 'FQ'):
                curr_date_val = TimeFormat.format_statistical(curr_date_val, 'FQ')
            series[series_name].add_value(series_point['Value'], curr_date_val, None)

        if 'FQ' in frequency_list and len(frequency_list) > 1:
            raise ValueError('Please provide a valid frequency list. You can request FQ or others frequencies not together.')

        return series

    def _get_pandasframe_one_dataset(self):
        pandas_series = {}
        if self.include_metadata:
            pandas_series_with_attr = {}
            names_of_attributes = self._get_attribute_names()

        mnemonics = self.mnemonics
        mnemonics_string = self.separator.join(mnemonics) if isinstance(mnemonics, list) else mnemonics
        mnemonics_resp = self.client.get_mnemonics(mnemonics_string, self.transform, self.frequency)
        detail_columns = None
            
        for item in mnemonics_resp.items:
            pivot_resp = item.pivot
            if not definition.is_equal_strings_ignore_case(self.dataset.id, pivot_resp.dataset):
                continue
            # create dataframe with data for mnemonics
            series = self._get_data_series(pivot_resp, detail_columns)
            pandas_series = PandasHelper.creates_pandas_series(series, pandas_series, detail_columns)
            if self.include_metadata:
                # create dataframe with metadata for mnemonics
                series_with_attr = self._get_metadata_series(pivot_resp, names_of_attributes)
                pandas_series_with_attr = PandasHelper.creates_pandas_series(series_with_attr, pandas_series_with_attr, None)

        pandas_data_frame = PandasHelper.create_pandas_dataframe(pandas_series, [], detail_columns)
        if not self.include_metadata:
            return pandas_data_frame
        pandas_data_frame_with_attr = PandasHelper.create_pandas_dataframe(pandas_series_with_attr, [], None)
        return pandas_data_frame, pandas_data_frame_with_attr

    def _get_pandasframe_across_datasets(self):
           
        mnemonics_string = self.separator.join(self.mnemonics) if isinstance(self.mnemonics, list) else self.mnemonics
        mnemonics_resp = self.client.get_mnemonics(mnemonics_string, self.transform, self.frequency)

        dict_datasets = {}
        pandas_series = {}
        if self.include_metadata:
            pandas_series_with_attr = {}

        detail_columns = None
        for item in mnemonics_resp.items:
            pivot_resp = item.pivot
            if pivot_resp is None:
                continue
            dataset_id = pivot_resp.dataset
            if dataset_id  not in dict_datasets:
                dataset = self.client.get_dataset(dataset_id)
                self.dataset = dataset
                dimensions = []
                for dim in dataset.dimensions:
                    dimensions.append(self.client.get_dimension(dataset_id, dim.id))
                self.dimensions = dimensions
                names_of_attributes = self._get_attribute_names() if self.include_metadata else None
                dict_datasets[dataset_id] = (dataset, dimensions, names_of_attributes)
            else:
                self.dataset, self.dimensions, names_of_attributes = dict_datasets[dataset_id]
                    
            # create dataframe with data for mnemonics
            series = self._get_data_series(pivot_resp, detail_columns)
            pandas_series = PandasHelper.creates_pandas_series(series, pandas_series, detail_columns)
            if self.include_metadata:
                # create dataframe with metadata for mnemonics
                series_with_attr = self._get_metadata_series(pivot_resp, names_of_attributes)
                pandas_series_with_attr = PandasHelper.creates_pandas_series(series_with_attr, pandas_series_with_attr, None)

        pandas_data_frame = PandasHelper.create_pandas_dataframe(pandas_series, [], detail_columns)
        if not self.include_metadata:
            return pandas_data_frame
        pandas_data_frame_with_attr = PandasHelper.create_pandas_dataframe(pandas_series_with_attr, [], None)         
        return pandas_data_frame, pandas_data_frame_with_attr

    def get_pandasframe(self):
        """The method loads data from dataset"""
        if self.dataset:
            self._load_dimensions()
            return self._get_pandasframe_one_dataset()
        return self._get_pandasframe_across_datasets()

class KnoemaSeries(object):
    """This class combines values and index points for one time series"""

    def __init__(self, name, values=[], index=[], detail_columns=None):
        self.name = name
        self.values = values
        self.index = index
        self.column_count = len(detail_columns) if detail_columns is not None else 0
        self.column_values = [] if self.column_count > 0 else None
        for _ in range(0, self.column_count):
            self.column_values.append([])

    def add_value(self, value, index_point, columns=None):
        """The function is addeing new value to provied index. If index does not exist"""
        if index_point not in self.index:
            self.values.append(value)
            self.index.append(index_point)
            if self.column_count > 0:
                for i in range(0, self.column_count):
                    self.column_values[i].append(None if columns is None else columns[i])

    def creates_pandas_series(self, pandas_series, detail_columns):
        """The function creates pandas series based on index and values"""
        if detail_columns is None:
            pandas_series[self.name] = pandas.Series(self.values, self.index, name=self.name)
        else:
            series_name = self.name + ('Value',)
            pandas_series[series_name] = pandas.Series(self.values, self.index, name=series_name)
            for i in range(0, self.column_count):
                column_name = detail_columns[i]
                series_name = self.name + (column_name,)
                pandas_series[series_name] = pandas.Series(self.column_values[i], self.index, name=series_name)

class PandasHelper(object):
    @staticmethod
    def creates_pandas_series(series, pandas_series, detail_columns):
        for _, series_content in series.items():
            series_content.creates_pandas_series(pandas_series, detail_columns)
        return pandas_series

    @staticmethod
    def create_pandas_dataframe(pandas_series, names_of_dimensions, detail_columns):
        pandas_data_frame = pandas.DataFrame(pandas_series)
        pandas_data_frame.sort_index()
        if isinstance(pandas_data_frame.columns, pandas.MultiIndex):
            column_names = names_of_dimensions
            if detail_columns is not None:
                column_names = list(column_names)
                column_names.append('Attribute')
            pandas_data_frame.columns.names = column_names

        return pandas_data_frame

class TimeFormat(object):
    @staticmethod
    def format_statistical(date_point, freq):
        return {
            'FQ': lambda d: '{}FQ{}'.format(d.year, (d.month - 1) // 3 + 1),
            'W': lambda d: TimeFormat.format_weekly(d)
        }.get(freq, date_point)(date_point)

    @staticmethod
    def format_weekly(date):
        iso_values = date.isocalendar()
        iso_year = iso_values[0]
        week_number = iso_values[1]
        return '{}W{}'.format(iso_year, week_number)

    @staticmethod
    def get_frequencies_delta():
        return {
            'A': relativedelta(years = 1),
            'H': relativedelta(months = 6),
            'Q': relativedelta(months = 3),
            'FQ': relativedelta(months = 3),
            'M': relativedelta(months = 1),
            'W': timedelta(days = 7),
            'D': timedelta(days = 1)}


class DimensionMetadataReader:

    def __init__(self, client, dataset, dimension):
        self.client = client
        self.dataset = dataset
        self.dimension = dimension

    def get(self):
        dim_info = self.client.get_dimension(self.dataset, self.dimension)
        view_model = view_definition.Dimension()
        view_model.key = dim_info.key
        view_model.id = dim_info.id
        view_model.name = dim_info.name
        view_model.isGeo = dim_info.is_geo
        view_model.datasetId = self.dataset

        for fields in dim_info.fields:
            view_model.fields.append(view_definition.Field(fields))

        frame_data = []
        headers = ['key', 'name', 'level', 'parent key', 'parent id', 'parent name', 'hasdata']
        header_filled = False

        parents = {}
        for item in dim_info.items:
            parents[item.level] = {
                'id': item.fields['id'] if 'id' in item.fields else None,
                'key': item.key,
                'name': item.name
            }

            parent = {'id': None, 'key': -1, 'name': None}
            if item.level > 0:
                parent = parents[item.level - 1]

            row = [item.key, item.name, item.level, parent['key'], parent['id'], parent['name'], item.hasdata]
            for field in dim_info.fields:
                if not header_filled:
                    headers.append(field['name'])

                if field['name'] in item.fields:
                    row.append(item.fields[field['name']])
                else:
                    row.append(None)

            if not header_filled:
                header_filled = True
            frame_data.append(row)
        
        view_model.members = pandas.DataFrame(frame_data, columns=headers)

        return view_model
