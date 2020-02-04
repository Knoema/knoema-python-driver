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

    def __init__(self, client, dim_values):
        super().__init__(client)
        self.dim_values = dim_values

    def _get_dim_members(self, dim, splited_values):
        members = []
        for value in splited_values:
            if value is None or isinstance(value, str) and not value:
                raise ValueError('Selection for dimension {} is empty'.format(dim.name))

            member = dim.find_member_by_id(value)
            if member is None:
                member = dim.find_member_by_name(value)

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

            splited_values = value.split(';') if isinstance(value, str) else value
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
            members = self._get_dim_members(dim, splited_values)
            if not members:
                raise ValueError('Selection for dimension {} is empty'.format(dim.name))

            pivot_req.stub.append(definition.PivotItem(dim.id, members))

        self._add_full_selection_by_empty_dim_values(filter_dims, pivot_req)

        if time_range:
            pivot_req.header.append(definition.PivotTimeItem('Time', [time_range], 'range'))
        else:
            pivot_req.header.append(definition.PivotTimeItem('Time', [], 'AllData'))

        return pivot_req    

class TransformationDataReader(SelectionDataReader):

    def __init__(self, client, dim_values, transform, frequency):
        if transform:
            dim_values['transform'] = transform
        if frequency:
            dim_values['frequency'] = frequency
        super().__init__(client, dim_values)

    def get_pandasframe(self):
        names_of_dimensions = self._get_dimension_names()

        pivot_resp = self.client.get_dataset_data(self.dataset.id, self._get_data_query())
            
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
                filter_dims[name] = value
                continue
            if definition.is_equal_strings_ignore_case(name, 'datecolumn') and self.dataset.type != 'Regular':
                filter_dims['datecolumn'] = quote(value)
                continue

            splited_values = [x for x in value.split(';') if x] if isinstance(value, str) else value
            if definition.is_equal_strings_ignore_case(name, 'frequency'):
                filter_dims["frequency"] = ",".join(splited_values)
                continue

            dim = self._find_dimension(name)
            if dim is None:
                raise ValueError('Dimension with id or name {} is not found'.
                                 format(name))

            if not splited_values:
                raise ValueError('Selection for dimension {} is empty'.format(dim.name))

            filter_dims[dim.id] =  ",".join(quote(s) for s in splited_values)

        if self.include_metadata:
            filter_dims['metadata'] = 'true'
        return "&".join("=".join((str(key), str(value))) for key, value in filter_dims.items())

class StreamingDataReader(SelectionDataReader):

    def __init__(self, client, dim_values):
        super().__init__(client, dim_values)
    
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
            'M': relativedelta(months = 1),
            'W': timedelta(days = 7),
            'D': timedelta(days = 1)}
        for series_point in resp.series:  
            all_values = series_point['values']  
            series_name = self._get_series_name(series_point)
            data_begin_val = datetime.strptime(series_point['startDate'], '%Y-%m-%dT%H:%M:%S')    
            data_end_val = datetime.strptime(series_point['endDate'], '%Y-%m-%dT%H:%M:%S')
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
        mnemonics_string = ';'.join(mnemonics) if isinstance(mnemonics, list) else mnemonics
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
           
        mnemonics_string = ';'.join(self.mnemonics) if isinstance(self.mnemonics, list) else self.mnemonics
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
            mnemonics = item.mnemonics
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
