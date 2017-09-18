"""This module contains data definitions for Knoema client"""

from datetime import datetime
import pandas
import knoema.api_definitions as definition

class DataReader(object):
    """This class read data from Knoema and transform it to pandas frame"""

    def __init__(self, client, dataset, dim_values, include_metadata):
        self.client = client
        self.dataset = dataset
        self.dim_values = dim_values
        self.dimensions = []
        self.include_metadata = include_metadata

    def _ensure_alldimenions_in_filter(self, filter_dims):
        out_of_filter_dim_names = [dim.name for dim in self.dataset.dimensions if dim not in filter_dims]
        if out_of_filter_dim_names:
            raise ValueError('The following dimension(s) are not set: {}'.format(', '.join(out_of_filter_dim_names)))

    def _get_dim_members(self, dim, splited_values):

        members = []
        for value in splited_values:
            if value is None:
                raise ValueError('Selection for dimension {} is empty'.format(dim.name))

            member = dim.findmember_by_id(value)
            if member is None:
                member = dim.findmember_by_name(value)

            if member is None and value.isnumeric():
                member = dim.findmember_by_key(int(value))

            if member:
                members.append(member.key)

        return members

    def _find_dmension(self, dim_name_or_id):

        dim = self.dataset.find_dimension_by_name(dim_name_or_id)
        if dim is None:
            dim = self.dataset.find_dimension_by_id(dim_name_or_id)

        return dim

    def _create_pivot_request(self):

        pivotreq = definition.PivotRequest(self.dataset.id)

        filter_dims = []
        time_range = None
        for name, value in self.dim_values.items():
            if definition.isequal_strings_ignorecase(name, 'timerange'):
                time_range = value
                continue

            splited_values = value.split(';') if isinstance(value, str) else value
            if definition.isequal_strings_ignorecase(name, 'frequency'):
                pivotreq.frequencies = splited_values
                continue

            dim = self._find_dmension(name)
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

            pivotreq.stub.append(definition.PivotItem(dim.id, members))

        self._ensure_alldimenions_in_filter(filter_dims)

        if time_range:
            pivotreq.header.append(definition.PivotTimeItem('Time', [time_range], 'range'))
        else:
            pivotreq.header.append(definition.PivotTimeItem('Time', [], 'AllData'))

        return pivotreq

    def _get_series_name(self, series_point):
        names = []
        for dim in self.dimensions:
            names.append(series_point[dim.id]) 
        names.append(series_point['Frequency'])               
        return tuple(names)  

    def _get_series_name_with_metadata(self, series_point):
        names = []
        for dim in self.dimensions:
            for item in dim.items:
                if item.name == series_point[dim.id]:
                    dim_attrs = item.fields
                    break
            for attr in dim.fields: 
                if not attr['isSystemField']: 
                    for key, value in dim_attrs.items(): 
                        if definition.isequal_strings_ignorecase(key, attr['name']):
                            names.append(value)
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
                serie_attrs = self._get_series_name_with_metadata(series_point)
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

            curr_date_val = series_point['Time']
            try:
                curr_date_val = datetime.strptime(series_point['Time'], '%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                pass

            series[series_name].add_value(series_point['Value'], curr_date_val)

        return series

    def get_pandasframe(self):
        """The method loads data from dataset"""
        for dim in self.dataset.dimensions:
            self.dimensions.append(self.client.get_dimension(self.dataset.id, dim.id))
            
        pivotreq = self._create_pivot_request()
        pitvotresp = self.client.get_data(pivotreq)
        pandas_series = {}

        # create dataframe with data
        names_of_dimensions = self._get_dimension_names()
        series = self._get_data_series(pitvotresp)
        for series_name, series_content in series.items():
            pandas_series[series_name] = series_content.get_pandas_series()

        pandas_data_frame = pandas.DataFrame(pandas_series)
        pandas_data_frame.sort_index()
        if isinstance(pandas_data_frame.columns, pandas.MultiIndex):
            pandas_data_frame.columns.names = names_of_dimensions  
        if not self.include_metadata:
            return pandas_data_frame
        
        # create dataframe with metadata
        names_of_attributes = self._get_attribute_names()
        series_with_attr = self._get_metadata_series(pitvotresp, names_of_attributes)
        pandas_series_with_attr = {}
        for series_name, series_content in series_with_attr.items():    
            pandas_series_with_attr[series_name] = series_content.get_pandas_series()
        pandas_data_frame_with_attr = pandas.DataFrame(pandas_series_with_attr)
        pandas_data_frame_with_attr.sort_index()
        if isinstance(pandas_data_frame_with_attr.columns, pandas.MultiIndex):
            pandas_data_frame_with_attr.columns.names = names_of_dimensions             
        return pandas_data_frame, pandas_data_frame_with_attr    
         


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
