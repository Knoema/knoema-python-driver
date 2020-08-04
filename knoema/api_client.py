"""This module contains client that wrap requests and response to Knoema API"""

import json
import urllib.parse
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
import knoema.api_definitions_sema as definition_sema
import knoema.api_definitions_search as definition_search
from urllib.error import HTTPError

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

    #api response can starts with BOM symbol and it break json parser, so have to strip the symbol
    obj_resp = json.loads(str_response.strip('\ufeff'))
    if isinstance(obj_resp, str):
        raise ValueError(obj_resp)

    return obj_resp

class ApiClient:
    """This is client that wrap requests and response to Knoema API"""

    def __init__(self, host, appid=None, appsecret=None):
        splitted = urllib.parse.urlsplit(host)
        self._host = splitted.netloc.strip()
        if not self._host:
            self._host = splitted.path.strip()
        self._schema = splitted.scheme
        if not self._schema:
            self._schema = 'http'

        self._appid = appid
        self._appsecret = appsecret
        self._opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor)

        self._search_config = None

    def _get_url(self, apipath):
        return urllib.parse.urlunsplit((self._schema, self._host, apipath, '', ''))

    def _get_request_headers(self):

        if not self._appid or not self._appsecret:
            return {
                'Content-Type' : 'application/json',
                'Accept': 'application/json'
                }

        key = datetime.datetime.utcnow().strftime('%d-%m-%y-%H').encode()
        hashed = hmac.new(key, self._appsecret.encode(), hashlib.sha1)
        secrethash = base64.b64encode(hashed.digest()).decode('utf-8')
        auth = 'Knoema {}:{}:1.2'.format(self._appid, secrethash)

        return {
            'Content-Type' : 'application/json',
            'Accept': 'application/json',
            'Authorization' : auth
            }

    def _api_get(self, obj, apipath, query=None):

        url = self._get_url(apipath)
        if query:
            url = '{}?{}'.format(url, query)

        headers = self._get_request_headers()
        req = urllib.request.Request(url, headers=headers)
        resp = self._opener.open(req)
        return obj(_response_to_json(resp))

    def _api_post(self, responseobj, apipath, requestobj):

        json_data = requestobj.save_to_json()
        
        return self._api_post_json(responseobj, apipath, json_data)

    def _api_post_json(self, responseobj, apipath, requestjson):

        url = self._get_url(apipath)

        binary_data = requestjson.encode()

        headers = self._get_request_headers()
        req = urllib.request.Request(url, binary_data, headers)
        resp = self._opener.open(req)
        return responseobj(_response_to_json(resp))

    def check_correct_host(self):
        """The method checks whether the host is correctly set and whether it can configure the connection to this host. Does not check the base host knoema.com """
        if self._host == 'knoema.com':
            return
        url = self._get_url('/api/1.0/frontend/tags')
        headers = self._get_request_headers()
        req = urllib.request.Request(url, headers=headers)
        try:
            _ = urllib.request.urlopen(req)
        except:
            raise ValueError('The specified host {} does not exist'.format(self._host))  

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

    def get_data_by_json(self, pivotrequest_json):
        """The method is getting data by pivot request (json)"""

        path = '/api/1.0/data/pivot/'
        return self._api_post_json(definition.PivotResponse, path, pivotrequest_json)

    def get_dataset_data(self, dataset_id, query):
        """The method is getting JSON by URL and parses it to specified object"""
        try:
            return self._api_get(definition.PivotResponse, '/api/1.2/data/{}'.format(dataset_id), query)
        except HTTPError as ex:
            if ex.code == 400:
                raise ValueError(ex.read().decode('utf-8'))
            else:
                raise

    def get_data_raw(self, request, metadata_only = False):
        """The method is getting data by raw request"""
        path = '/api/1.2/data/raw/' + ('?metadataOnly=true' if metadata_only else '')
        res = self._api_post(definition.RawDataResponse, path, request)
        token = res.continuation_token
        while token is not None:
           res2 = self.get_data_raw_with_token(token, metadata_only)
           res.series += res2.series
           token = res2.continuation_token 
        return res

    def get_data_raw_with_token(self, token, metadata_only = False):
        path = '/api/1.0/data/raw/?continuationToken={0}' + ('&metadataOnly=true' if metadata_only else '')
        return self._api_get(definition.RawDataResponse, path.format(token))

    def get_mnemonics(self, mnemonics, transform, frequency):
        """The method get series by mnemonics"""
        path = '/api/1.0/data/mnemonics?mnemonics={0}'
        if transform:
            path += '&transform=' + transform
        if frequency:
            path += '&frequency=' + frequency
        return self._api_get(definition.MnemonicsResponseList, path.format(mnemonics))

    def get_details(self, request):
        """The method is getting data details by request"""
        path = '/api/1.1/data/details/'
        return self._api_post(definition.DetailsResponse, path, request)

    def get_company_info(self, ticker):
        """The method get company data"""

        path = 'api/1.0/sema/{0}'
        return self._api_get(definition_sema.CompanyInt, path.format(ticker))

    def get_indicator_info(self, path):
        path = 'api/1.0/sema/{0}'.format(path)
        url = self._get_url(path)

        headers = self._get_request_headers()
        req = urllib.request.Request(url, headers=headers)
        resp = self._opener.open(req)
        return _response_to_json(resp)

    def search(self, query):
        if self._search_config == None:
            path = '/api/1.0/search/config'
            self._search_config = self._api_get(definition_search.SearchConfig, path)

        headers = self._get_request_headers()
        url = self._search_config.build_search_url(query)
        req = urllib.request.Request(url, headers=headers)
        req = urllib.request.Request(url)
        resp = self._opener.open(req)

        return definition_search.SearchResultsInt(_response_to_json(resp))

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

    def upload(self, file_path, dataset=None, public=False, name = None):
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

        ds_upload = definition.DatasetUpload(upload_ver_status, upload_status, dataset, public, name)
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
