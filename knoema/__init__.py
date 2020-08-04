"""This is main package module"""

from knoema.api_config import ApiConfig
from knoema.api_client import ApiClient
from knoema.data_reader import MnemonicsDataReader, StreamingDataReader, TransformationDataReader
from knoema.api_definitions import is_equal_strings_ignore_case
from knoema.api_definitions_sema import Company
from knoema.api_definitions_search import SearchResults
from knoema.upload_frame import FrameTransformerFactory, FileLayerWrapper

def get(dataset = None, include_metadata = False, mnemonics = None, transform = None, separator = None, group_by = None, columns = None, **dim_values):
    """Use this function to get data from Knoema dataset."""

    if not dataset and not mnemonics:
        raise ValueError('Dataset id is not specified')

    config = ApiConfig()
    client = ApiClient(config.host, config.app_id, config.app_secret)
    client.check_correct_host()

    ds = client.get_dataset(dataset) if dataset else None

    if columns is not None and isinstance(columns, str):
        columns = columns.split(';')

    frequency = None
    timerange = None
    has_agg = False
    for name, value in dim_values.copy().items():
        if is_equal_strings_ignore_case(name, 'frequency'):
            frequency = value
            del dim_values[name]
            continue
        if is_equal_strings_ignore_case(name, 'transform'):
            transform = value
            del dim_values[name]
            continue
        if is_equal_strings_ignore_case(name, 'timerange'):
            timerange = value
            continue

        if len(value) > 0 and value[0].startswith('@'):
            has_agg = True

    if mnemonics:
        reader =  MnemonicsDataReader(client, mnemonics, transform, frequency)
        reader.columns = columns
        reader.include_metadata = include_metadata
        reader.dataset = ds

        if separator:
            reader.separator = separator
            
        return reader.get_pandasframe()
 
    if not dataset:
        raise ValueError('Dataset id is not specified')

    if ds.type == 'Regular' and not has_agg and group_by:
        metadata_reader = StreamingDataReader(client, dim_values)
        metadata_reader.columns = columns
        metadata_reader.dataset = ds
        if separator:
            metadata_reader.separator = separator

        metadata = metadata_reader.get_series_metadata()

        reader = TransformationDataReader(client, None, transform, frequency, group_by)
        reader.columns = columns
        reader.include_metadata = include_metadata
        reader.dataset = ds

        return reader.get_pandasframe_by_metadata_grouped(metadata, timerange)

    reader = None
    if ds.is_remote or (ds.type == 'Flat' and 'datecolumn' in dim_values):
        reader = TransformationDataReader(client, dim_values, transform, frequency, None)
    else:
        if ds.type == 'Regular' and frequency != None:
            dim_values['frequency'] = frequency

        reader = StreamingDataReader(client, dim_values, transform)

    reader.columns = columns
    reader.include_metadata = include_metadata
    reader.dataset = ds

    if separator:
        reader.separator = separator
            
    return reader.get_pandasframe()

def ticker(ticker):
    """Use this function to get data about company"""

    if not ticker:
        raise ValueError('Ticker or company name is not specified')

    config = ApiConfig()
    client = ApiClient(config.host, config.app_id, config.app_secret)
    client.check_correct_host()

    company_int = client.get_company_info(ticker)

    return Company(company_int, client)

def search(query):
    """Use this function to make search request"""

    if not query:
        raise ValueError('Query is not specified')

    config = ApiConfig()
    client = ApiClient(config.host, config.app_id, config.app_secret)
    client.check_correct_host()

    search_results_int = client.search(query)
    search_results = SearchResults(search_results_int, client)

    if search_results.instant != None:
        if search_results.instant.type == 'ConceptBind':
            company_int = client.get_company_info(search_results.instant.id)

            search_results.instant.company = Company(company_int, client)

    return search_results

def upload(file_path_or_frame, dataset=None, public=False, name = None):
    """Use this function to upload data to Knoema dataset."""

    config = ApiConfig()
    client = ApiClient(config.host, config.app_id, config.app_secret)

    if isinstance(file_path_or_frame, str):
        return client.upload(file_path_or_frame, dataset, public)

    frame_transformer = FrameTransformerFactory(file_path_or_frame).get_transformer()

    with FileLayerWrapper() as fw:
        file = frame_transformer.prepare(fw, dataset, name)
        return client.upload(file, dataset, public, name)

def delete(dataset):
    """Use this function to delete dataset by it's id."""
    
    config = ApiConfig()
    client = ApiClient(config.host, config.app_id, config.app_secret)
    client.check_correct_host()
    client.delete(dataset)
    return ('Dataset {} has been deleted successfully'.format(dataset))

def verify(dataset, publication_date, source, refernce_url):
    """Use this function to verify a dataset."""

    config = ApiConfig()
    client = ApiClient(config.host, config.app_id, config.app_secret)
    client.check_correct_host()
    client.verify(dataset, publication_date, source, refernce_url)
