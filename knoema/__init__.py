"""This is main package module"""

from knoema.api_config import ApiConfig
from knoema.api_client import ApiClient
from knoema.data_reader import DataReader

def get(dataset, **dim_values):
    """Use this function to get data from Knoema dataset."""

    if not dataset:
        raise ValueError('Dataset id is not specified')

    if not dim_values:
        raise ValueError('Dimensions members are not specified')

    config = ApiConfig()
    client = ApiClient(config.host, config.app_id, config.app_secret)

    dataset = client.get_dataset(dataset)
    data_reader = DataReader(client, dataset, dim_values)

    return data_reader.get_pandasframe()

def upload(file_path, dataset=None):
    """Use this function to upload data to Knoema dataset."""

    config = ApiConfig()
    client = ApiClient(config.host, config.app_id, config.app_secret)
    return client.upload(file_path, dataset)
