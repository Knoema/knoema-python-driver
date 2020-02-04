"""This is main package module"""

from knoema.api_config import ApiConfig
from knoema.api_client import ApiClient
from knoema.data_reader import MnemonicsDataReader, StreamingDataReader, TransformationDataReader
from knoema.api_definitions import is_equal_strings_ignore_case

def get(dataset = None, include_metadata = False, mnemonics = None, transform = None, **dim_values):
    """Use this function to get data from Knoema dataset."""

    if not dataset and not mnemonics:
        raise ValueError('Dataset id is not specified')

    frequency = None
    for name, value in dim_values.copy().items():
        if is_equal_strings_ignore_case(name, 'frequency'):
            frequency = value
            del dim_values[name]
            continue
        if is_equal_strings_ignore_case(name, 'transform'):
            transform = value
            del dim_values[name]
            continue

    if mnemonics and dim_values:
        raise ValueError('The function does not support specifying mnemonics and selection in a single call')

    config = ApiConfig()
    client = ApiClient(config.host, config.app_id, config.app_secret)
    client.check_correct_host()

    ds = client.get_dataset(dataset) if dataset else None

    reader =  MnemonicsDataReader(client, mnemonics, transform, frequency) if mnemonics \
        else TransformationDataReader(client, dim_values, transform, frequency) if ds.type != 'Regular' or frequency or transform\
        else StreamingDataReader(client, dim_values)

    reader.include_metadata = include_metadata
    reader.dataset = ds

    return reader.get_pandasframe()
 
def upload(file_path, dataset=None, public=False):
    """Use this function to upload data to Knoema dataset."""

    config = ApiConfig()
    client = ApiClient(config.host, config.app_id, config.app_secret)
    return client.upload(file_path, dataset, public)

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
