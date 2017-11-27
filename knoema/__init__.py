"""This is main package module"""

from knoema.api_config import ApiConfig
from knoema.api_client import ApiClient
from knoema.data_reader import DataReader, ReaderByMnemonics

def get(dataset = None, include_metadata = False, mnemonics = None, **dim_values):
    """Use this function to get data from Knoema dataset."""

    if not dataset and not mnemonics:
        raise ValueError('Dataset id is not specified')

    if dataset and not mnemonics and not dim_values:
        raise ValueError('Dimensions members are not specified')

    if mnemonics and dim_values:
        raise ValueError('The function does not support the simultaneous use of mnemonic and selection')

    config = ApiConfig()
    client = ApiClient(config.host, config.app_id, config.app_secret)
    client.check_correct_host()
    if not dataset and mnemonics:     
        reader_with_mnemo = ReaderByMnemonics(client, dataset, dim_values, include_metadata, mnemonics, [])
        return reader_with_mnemo.get_pandasframe_by_mnemonics_in_all_datasets()
      
    dataset = client.get_dataset(dataset)
    data_reader = DataReader(client, dataset, dim_values, include_metadata, mnemonics)

    return data_reader.get_pandasframe()

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
