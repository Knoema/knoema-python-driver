"""This module contains client that wrap requests and response to Knoema API"""

import json
import urllib.request
import time
import datetime
import hmac
import base64
import hashlib
import random
import string
import io
import os
import knoema.api_definitions as definition

def _random_string(length):
    return ''.join(random.choice(string.ascii_letters) for ii in range(length + 1))

def _string_to_binary(string_data):
    return string_data.encode()

def _crlf():
    return _string_to_binary('\r\n')

def _response_to_json(resp):
    str_response = resp.read().decode('utf-8')

    if resp.status < 200 or resp.status >= 300:
        raise ValueError('Error {} from server:{}', resp.status, str_response)

    obj_resp = json.loads(str_response)
    if isinstance(obj_resp, str):
        raise ValueError(obj_resp)

    return obj_resp

class ApiClient:
    """This is client that wrap requests and response to Knoema API"""

    def __init__(self, host, appid=None, appsecret=None):
        self._host = host
        self._appid = appid
        self._appsecret = appsecret

    def _get_url(self, apipath):
        return 'http://{}{}'.format(self._host, apipath)

    def _get_request_headers(self):

        if not self._appid or not self._appsecret:
            return {'Content-Type' : 'application/json'}

        key = datetime.datetime.utcnow().strftime('%d-%m-%y-%H').encode()
        hashed = hmac.new(key, self._appsecret.encode(), hashlib.sha1)
        secrethash = base64.b64encode(hashed.digest()).decode('utf-8')
        auth = 'Knoema {}:{}:1.2'.format(self._appid, secrethash)

        return {
            'Content-Type' : 'application/json',
            'Authorization' : auth
            }

    def _api_get(self, obj, apipath, query=None):

        url = self._get_url(apipath)
        if query:
            url = '{}?{}'.format(url, query)

        headers = self._get_request_headers()
        req = urllib.request.Request(url, headers=headers)
        resp = urllib.request.urlopen(req)

        return obj(_response_to_json(resp))

    def _api_post(self, responseobj, apipath, requestobj):

        url = self._get_url(apipath)

        json_data = requestobj.save_to_json()
        binary_data = json_data.encode()

        headers = self._get_request_headers()
        req = urllib.request.Request(url, binary_data, headers)
        resp = urllib.request.urlopen(req)

        return responseobj(_response_to_json(resp))

    def get_dataset(self, datasetid):
        """The method is getting information about dataset byt it's id"""

        path = '/api/1.0/meta/dataset/{}'
        return self._api_get(definition.Dataset, path.format(datasetid))

    def get_dimension(self, dataset, dimension):
        """The method is getting information about dimension with items"""

        path = '/api/1.0/meta/dataset/{}/dimension/{}'
        return self._api_get(definition.Dimension, path.format(dataset, dimension))

    def get_daterange(self, dataset):
        """The method is getting information about date range of dataset"""

        path = '/api/1.0/meta/dataset/{}/daterange'
        return self._api_get(definition.DateRange, path.format(dataset))

    def get_data(self, pivotrequest):
        """The method is getting data by pivot request"""

        path = '/api/1.0/data/pivot/'
        return self._api_post(definition.PivotResponse, path, pivotrequest)

    def upload_file(self, file):
        """The method is posting file to the remote server"""

        url = self._get_url('/api/1.0/upload/post')

        fcontent = FileContent(file)
        binary_data = fcontent.get_binary()

        headers = self._get_request_headers()
        req = urllib.request.Request(url, binary_data, headers)
        req.add_header('Content-type', fcontent.get_content_type())
        req.add_header('Content-length', len(binary_data))

        resp = urllib.request.urlopen(req)

        return definition.UploadPostResponse(_response_to_json(resp))

    def upload_verify(self, file_location, dataset=None):
        """This method is verifiing posted file on server"""

        path = '/api/1.0/upload/verify'
        query = 'doNotGenerateAdvanceReport=true&filePath={}'.format(file_location)
        if dataset:
            query = 'doNotGenerateAdvanceReport=true&filePath={}&datasetId={}'.format(file_location, dataset)

        return self._api_get(definition.UploadVerifyResponse, path, query)

    def upload_submit(self, upload_request):
        """The method is submitting dataset upload"""

        path = '/api/1.0/upload/save'
        return self._api_post(definition.DatasetUploadResponse, path, upload_request)

    def upload_status(self, upload_id):
        """The method is checking status of uploaded dataset"""

        path = '/api/1.0/upload/status'
        query = 'id={}'.format(upload_id)
        return self._api_get(definition.DatasetUploadStatusResponse, path, query)

    def upload(self, file_path, dataset=None, public=False):
        """Use this function to upload data to Knoema dataset."""

        upload_status = self.upload_file(file_path)
        err_msg = 'Dataset has not been uploaded to the remote host'
        if not upload_status.successful:
            msg = '{}, because of the following error: {}'.format(err_msg, upload_status.error)
            raise ValueError(msg)

        err_msg = 'File has not been verified'
        upload_ver_status = self.upload_verify(upload_status.properties.location, dataset)
        if not upload_ver_status.successful:
            ver_err = '\r\n'.join(upload_ver_status.errors)
            msg = '{}, because of the following error(s): {}'.format(err_msg, ver_err)
            raise ValueError(msg)

        ds_upload = definition.DatasetUpload(upload_ver_status, upload_status, dataset, public)
        ds_upload_submit_result = self.upload_submit(ds_upload)
        err_msg = 'Dataset has not been saved to the database'
        if ds_upload_submit_result.status == 'failed':
            ver_err = '\r\n'.join(ds_upload_submit_result.errors)
            msg = '{}, because of the following error(s): {}'.format(err_msg, ver_err)
            raise ValueError(msg)

        ds_upload_result = None
        while True:
            ds_upload_result = self.upload_status(ds_upload_submit_result.submit_id)
            if ds_upload_result.status == 'pending' or ds_upload_result.status == 'processing':
                time.sleep(5)
            else:
                break

        if ds_upload_result.status != 'successful':
            ver_err = '\r\n'.join(ds_upload_result.errors)
            msg = '{}, because of the following error(s): {}'.format(err_msg, ver_err)
            raise ValueError(msg)

        return ds_upload_result.dataset

    def delete(self, dataset):
        """The method is deleting dataset by it's id"""

        url = self._get_url('/api/1.0/meta/dataset/{}/delete'.format(dataset))

        json_data = ''
        binary_data = json_data.encode()

        headers = self._get_request_headers()
        req = urllib.request.Request(url, binary_data, headers)
        resp = urllib.request.urlopen(req)
        str_response = resp.read().decode('utf-8')
        if str_response != '"successful"' or resp.status < 200 or resp.status >= 300:
            msg = 'Dataset has not been deleted, because of the following error(s): {}'.format(str_response)
            raise ValueError(msg)

    def verify(self, dataset, publication_date, source, refernce_url):
        """The method is verifying dataset by it's id"""

        path = '/api/1.0/meta/verifydataset'
        req = definition.DatasetVerifyRequest(dataset, publication_date, source, refernce_url)
        result = self._api_post(definition.DatasetVerifyResponse, path, req)
        if result.status == 'failed':
            ver_err = '\r\n'.join(result.errors)
            msg = 'Dataset has not been verified, because of the following error(s): {}'.format(ver_err)
            raise ValueError(msg)


class FileContent(object):
    """Accumulate the data to be used when posting a form."""

    def __init__(self, file):
        self.file_name = os.path.basename(file)
        self.body = open(file, mode='rb').read()
        self.boundary = _random_string(30)

    def get_content_type(self):
        """Return a content type"""
        return 'multipart/form-data; boundary="{}"'.format(self.boundary)

    def get_binary(self):
        """Return a binary buffer containing the file content"""

        content_disp = 'Content-Disposition: form-data; name="file"; filename="{}"'

        stream = io.BytesIO()
        stream.write(_string_to_binary('--{}'.format(self.boundary)))
        stream.write(_crlf())
        stream.write(_string_to_binary(content_disp.format(self.file_name)))
        stream.write(_crlf())
        stream.write(_crlf())
        stream.write(self.body)
        stream.write(_crlf())
        stream.write(_string_to_binary('--{}--'.format(self.boundary)))
        stream.write(_crlf())

        return stream.getvalue()
