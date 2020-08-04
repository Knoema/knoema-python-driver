"""This module contains metadata definitions for Knoema API"""

import json
from datetime import datetime

def is_equal_strings_ignore_case(first, second):
    """The function compares strings ignoring case"""
    if first and second:
        return first.upper() == second.upper()
    else:
        return not (first or second)


class DimensionMember(object):
    """The class contains dimension member information"""

    def __init__(self, data):
        self.key = data['key']
        self.name = data['name']
        self.level = data['level']
        self.hasdata = data['hasData']
        self.fields = data['fields']


class DimensionModel(object):
    """The class contains dimension description"""

    def __init__(self, data):
        self.key = data['key']
        self.id = data['id']
        self.name = data['name']
        self.is_geo = data['isGeo'] if 'isGeo' in data else False


class Dimension(DimensionModel):
    """The class contains dimension description and dimension items"""

    def __init__(self, data):
        super().__init__(data)
        self.fields = data['fields']
        self.items = [DimensionMember(item) for item in data['items']]

        # fill maps
        self.key_map = {}
        self.id_map = {}
        self.name_map = {}
        self.region_map = {}
        self.ticker_map = {}
        for item in self.items:
            self.key_map[item.key] = item
            self.name_map[item.name.upper()] = item
            if 'id' in item.fields:
                self.id_map[item.fields['id'].upper()] = item
            if 'regionid' in item.fields:
                self.region_map[item.fields['regionid'].upper()] = item
            if 'ticker' in item.fields:
                self.ticker_map[item.fields['ticker'].upper()] = item

        
    def find_member_by_key(self, member_key):
        """The method searches member of dimension by given member key"""
        return self.key_map.get(member_key)

    def find_member_by_id(self, member_id):
        """The method searches member of dimension by given member id"""
        return self.id_map.get(member_id.upper())

    def find_member_by_name(self, member_name):
        """The method searches member of dimension by given member name"""
        return self.name_map.get(member_name.upper())

    def find_member_by_regionid(self, member_name):
        """The method searches member of dimension by given region id"""
        return self.region_map.get(member_name.upper())

    def find_member_by_ticker(self, member_name):
        return self.ticker_map.get(member_name.upper())


class DateRange(object):
    """The class contains information about dataset's data range"""

    def __init__(self, data):
        #self.start_date = datetime.strptime(data['startDate'], '%Y-%m-%dT%H:%M:%SZ')
        #self.end_date = datetime.strptime(data['endDate'], '%Y-%m-%dT%H:%M:%SZ')
        self.frequencies = data['frequencies']

class TimeSeriesAttribute(object):
    """This class contains information about dataset' s timeseries attributes"""
    def __init__(self, data):
        self.name = data['name']
        self.type = data['type']
        self.allowed_values = data['allowedValues']

class Dataset(object):
    """The class contains dataset description"""

    def __init__(self, data):
        """The method loading data from json to class fields"""

        if 'id' not in data:
            raise ValueError(data)

        self.id = data['id']
        self.type = data['type']
        self.is_remote = data['isRemote'] if 'isRemote' in data else False
        self.dimensions = [DimensionModel(dim) for dim in data['dimensions']]
        self.timeseries_attributes = [TimeSeriesAttribute(attr) for attr in data['timeseriesAttributes']] if 'timeseriesAttributes' in data else []
        self.has_time = self.type == 'Regular' or any(x for x in data['columns'] if x['type'] == 'Date')
            
    def find_dimension_by_name(self, dim_name):
        """the method searching dimension with a given name"""

        for dim in self.dimensions:
            if is_equal_strings_ignore_case(dim.name, dim_name):
                return dim

            if dim.is_geo and is_equal_strings_ignore_case('region', dim_name):
                return dim
        return None

    def find_dimension_by_id(self, dim_id):
        """the method searching dimension with a given id"""

        for dim in self.dimensions:
            if is_equal_strings_ignore_case(dim.id, dim_id):
                return dim

            if dim.is_geo and is_equal_strings_ignore_case('region', dim_id):
                return dim

            if is_equal_strings_ignore_case('ticker', dim_id):
                return dim

        return None

class PivotItemMetadata(object):
#SimpleDimensionMember
    """The class contains metadata fields for pivot request item"""
    def __init__(self, key, name, parent=None, fields=None):
        self.key = key
        self.name = name
        self.parent = parent
        self.fields = fields

class PivotItem(object):
    """The class contains pivot request item"""

    def __init__(self, dimensionid=None, members=None, metadataFields=None, dimensionFields=None, aggregations=None):
        self.dimensionid = dimensionid
        self.members = members
        if aggregations != None:
            self.aggregation = aggregations
        if metadataFields:
            self.metadataFields = [PivotItemMetadata(metadata['key'], metadata['name'],
                metadata['parent'], metadata['fields']) for metadata in metadataFields]
        else:
            self.metadataFields = None
        self.fields = dimensionFields


class PivotTimeItem(PivotItem):
    """The class contains pivot request item"""

    def __init__(self, dimensionid=None, members=None, uimode=None):
        super().__init__(dimensionid, members)
        self.uimode = uimode


class PivotRequest(object):
    """The class contains pivot request"""

    def __init__(self, dataset):
        self.dataset = dataset
        self.header = []
        self.stub = []
        self.filter = []
        self.frequencies = []
        self.transform = None
        self.columns = None

    def _get_item_array(self, items):
        arr = []
        for item in items:
            itemvalues = {
                'DimensionId': item.dimensionid,
                'Members': item.members
            }
            if hasattr(item, 'aggregation'):
                itemvalues['Aggregation'] = item.aggregation
            if isinstance(item, PivotTimeItem):
                itemvalues['UiMode'] = item.uimode
            arr.append(itemvalues)
        return arr

    def save_to_json(self):
        """The method saves data to json from object"""
        requestvalues = {
            'Dataset': self.dataset,
            'Header' : self._get_item_array(self.header),
            'Filter' : self._get_item_array(self.filter),
            'Stub' : self._get_item_array(self.stub),
            'Frequencies': self.frequencies
        }
        if self.transform is not None:
            requestvalues['Transform'] = self.transform
        if self.columns is not None:
            requestvalues['DetailColumns'] = self.columns
            
        return json.dumps(requestvalues)

class DetailsResponse(object):
    def __init__(self, data):
        self.columns = data['columns']
        self.tuples = data['data']

class PivotResponse(object):
    """The class contains pivot response"""

    def __init__(self, data):

        self.dataset = data['dataset']

        self.header = []
        for item in data['header']:
            self.header.append(self._construct_dimension(item))

        self.stub = []
        for item in data['stub']:
            self.stub.append(self._construct_dimension(item))

        self.filter = []
        for item in data['filter']:
            self.filter.append(self._construct_dimension(item))

        self.tuples = data['data']
        self.descriptor = data['descriptor'] if 'descriptor' in data else None

    def _construct_dimension(self, item):
        return PivotItem(item['dimensionId'], item['members'], item['metadataFields'], item['dimensionFields'] if 'dimensionFields' in item else None)

class RawDataResponse(object):
    """The class contains raw data response"""

    def __init__(self, data):

        self.continuation_token = data['continuationToken']
        self.series = data['data']
        self.descriptor = data['descriptor'] if 'descriptor' in data else None

class MnemonicsResponseList(object):

    def __init__(self,data):
        self.items = []
        for item in data:
            self.items.append(MnemonicsResponse(item))   

class MnemonicsResponse(object):

    def __init__(self,data):
        self.mnemonics = data['mnemonics']
        self.pivot = PivotResponse(data['pivot']) if 'pivot' in data else None

class FileProperties(object):
    """The class contains response from upload post request"""

    def __init__(self, data):
        self.size = data['Size'] if 'Size' in data else None
        self.name = data['Name'] if 'Name' in data else None
        self.location = data['Location'] if 'Location' in data else None
        self.type = data['Type'] if 'Type' in data else None


class UploadPostResponse(object):
    """The class contains response from upload post request"""

    def __init__(self, data):
        self.successful = data['Successful'] if 'Successful' in data else False
        self.error = data['Error'] if 'Error' in data else None
        self.properties = FileProperties(data['Properties'])


class UploadDatasetDetails(object):
    """The class contains dataset details from verify result response"""

    def __init__(self, data):
        self.dataset_id = data['DatasetId']
        self.dataset_name = data['DatasetName']
        self.source = data['Source']
        self.description = data['Description']
        self.dataset_ref = data['DatasetRef']
        self.publication_date = data['PublicationDate'] if 'PublicationDate' in data else None
        self.accessed_on = data['AccessedOn'] if 'AccessedOn' in data else None


class UploadVerifyResponse(object):
    """The class contains response from upload post request"""

    def __init__(self, data):
        self.successful = data['Successful'] if 'Successful' in data else False
        self.upload_format_type = data['UploadFormatType'] if 'UploadFormatType' in data else None
        self.errors = data['ErrorList'] if 'ErrorList' in data else None
        self.columns = data['Columns'] if 'Columns' in data else None
        self.flat_ds_update_options = data['FlatDSUpdateOptions'] if 'FlatDSUpdateOptions' in data else None
        self.metadata_details = UploadDatasetDetails(data['MetadataDetails']) if 'MetadataDetails' in data and data['MetadataDetails'] is not None else None


class DatasetUpload(object):
    """The class contains request for UploadSubmit"""

    def __init__(self, verify_result, upload_result, dataset = None, public = False, name = None):
        self.dataset = dataset

        self.upload_format_type = verify_result.upload_format_type
        self.columns = verify_result.columns
        self.file_property = upload_result.properties
        self.flat_ds_update_options = verify_result.flat_ds_update_options

        dataset_details = verify_result.metadata_details
        self.name = dataset_details.dataset_name if dataset_details and dataset_details.dataset_name else None
        if self.name is None and dataset is None:
            self.name = name if name != None else 'New dataset'
        self.description = dataset_details.description if dataset_details else None
        self.source = dataset_details.source if dataset_details else None
        self.publication_date = dataset_details.publication_date if dataset_details else None
        self.accessed_on = dataset_details.accessed_on if dataset_details else None
        self.dataset_ref = dataset_details.dataset_ref if dataset_details else None
        self.public = public

    def save_to_json(self):
        """The method saves DatasetUpload to json from object"""
        requestvalues = {
            'DatasetId': self.dataset,
            'Name': self.name,
            'Description': self.description,
            'Source': self.source,
            'PubDate': self.publication_date,
            'AccessedOn': self.accessed_on,
            'Url': self.dataset_ref,
            'UploadFormatType': self.upload_format_type,
            'Columns': self.columns,
            'FileProperty': self.file_property.__dict__,
            'FlatDSUpdateOptions': self.flat_ds_update_options,
            'Public': self.public
        }
        return json.dumps(requestvalues)


class DatasetUploadResponse(object):
    """The class contains response for UploadSubmit"""

    def __init__(self, data):
        self.submit_id = data['Id'] if 'Id' in data else None
        self.dataset = data['DatasetId'] if 'DatasetId' in data else None
        self.status = data['Status'] if 'Status' in data else 'failed'
        self.errors = data['Errors'] if 'Errors' in data else None


class DatasetUploadStatusResponse(object):
    """The class contains response for UploadSubmit"""

    def __init__(self, data):
        self.submit_id = data['id'] if 'id' in data else None
        self.dataset = data['datasetId'] if 'datasetId' in data else None
        self.status = data['status'] if 'status' in data else 'failed'
        self.errors = data['errors'] if 'errors' in data else None


class DatasetVerifyRequest(object):
    """The class contains dataset verification request"""

    def __init__(self, dataset, publication_date, source, refernce_url):
        self.dataset = dataset
        self.publication_date = publication_date
        self.source = source
        self.refernce_url = refernce_url

    def save_to_json(self):
        """The method saves data to json from object"""

        requestvalues = {
            'id': self.dataset,
            'publicationDate': self.publication_date.strftime('%Y-%m-%d'),
            'source': self.source,
            'refUrl': self.refernce_url,
        }        
        return json.dumps(requestvalues)


class DatasetVerifyResponse(object):
    """The class contains response from dataset verification request"""

    def __init__(self, data):
        self.status = data['status']
        self.errors = data['errors'] if 'errors' in data else None

class DataFrame(object):

    def __init__(self):
        self.id = None
        self.data = None
        self.metadata = None
