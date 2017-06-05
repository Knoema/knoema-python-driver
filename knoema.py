"""This is main package module"""

import time
import api
import apy_definitions as definition
import data

def get(dataset, **dim_values):
    """Use this function to get data from Knoema dataset."""

    if not dataset:
        raise ValueError('Dataset id is not specified')

    if not dim_values:
        raise ValueError('Dimensions members are not specified')

    config = ApiConfig()
    client = api.Client(config.host, config.app_id, config.app_secret)

    dataset = client.get_dataset(dataset)
    data_reader = data.DataReader(client, dataset, dim_values)

    return data_reader.get_pandasframe()


def upload(file_path, dataset=None):
    """Use this function to upload data to Knoema dataset."""

    config = ApiConfig()
    client = api.Client(config.host, config.app_id, config.app_secret)

    upload_status = client.upload_file(file_path)
    err_msg = 'Dataset has not been uploaded to the remote host'
    if not upload_status.successful:
        msg = '{}, because of the the following error: {}'.format(err_msg, upload_status.error)
        raise Exception(msg)

    err_msg = 'Dataset has been verified with errors'
    upload_ver_status = client.upload_verify(upload_status.properties.location, dataset)
    if not upload_ver_status.successful:
        ver_err = '\r\n'.join(upload_ver_status.errors)
        msg = '{}, because of the the following error(s): {}'.format(err_msg, ver_err)
        raise Exception(msg)

    ds_upload = definition.DatasetUpload()
    ds_upload.upload_format_type = upload_ver_status.upload_format_type
    ds_upload.columns = upload_ver_status.columns
    ds_upload.file_property = upload_status.properties
    ds_upload.flat_ds_update_options = upload_ver_status.flat_ds_update_options
    ds_upload.dataset = dataset
    if not dataset:
        ds_upload.name = 'New dataset'

    ds_upload_submit_result = client.upload_submit(ds_upload)
    err_msg = 'Dataset has been saved to the database'
    if ds_upload_submit_result.status == 'failed':
        ver_err = '\r\n'.join(ds_upload_submit_result.errors)
        msg = '{}, because of the the following error(s): {}'.format(err_msg, ver_err)
        raise Exception(msg)

    ds_upload_result = None
    while True:
        ds_upload_result = client.upload_status(ds_upload_submit_result.submit_id)
        if ds_upload_result.status == 'pending' or ds_upload_result.status == 'processing':
            time.sleep(5)
        else:
            break

    if ds_upload_result.status != 'successful':
        ver_err = '\r\n'.join(ds_upload_result.errors)
        msg = '{}, because of the the following error(s): {}'.format(err_msg, ver_err)
        raise Exception(msg)

    return ds_upload_result.dataset


class ApiConfig(object):
    """
    This class configures knoema api.

    The class contains fields:

    host -- the host where kneoma is going to connect

    app_id -- application id that will have access to knoema.
    Application should be created by knoema user or administrator

    app_secret -- code that can be done after application will be created.
    Should be set up together with app_id
    """

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(ApiConfig, cls).__new__(cls)
            cls.instance.host = 'knoema.com'
            cls.instance.app_id = None
            cls.instance.app_secret = None
        return cls.instance

    def __init__(self):
        self.host = self.instance.host
        self.app_id = self.instance.app_id
        self.app_secret = self.instance.app_secret
