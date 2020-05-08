"""This module contains metadata definitions for Knoema API for semantic atlas"""

import json
from knoema.data_reader import TransformationDataReader
import urllib.parse

class SearchConfig(object):

    def __init__(self, data):
        self.host = data['host']
        self.search_host = data['searchHost']
        self.lang = data['lang']
        self.access_token = data['accessToken']
        self.session_id = data['sessionId']
        self.community_id = data['communityId']

    def build_search_url(self, query):
        quoted_query = urllib.parse.quote(query)
        url = 'https://{}/api/1.0/search?query={}&scope=instant,semantic&count=8&version=4&host={}&lang={}&sessionId={}&access_token={}'
        url = url.format(self.search_host, quoted_query, self.host, self.lang, self.session_id, self.access_token)

        if self.community_id:
           url += '&communityId={}'.format(self.community_id)

        return url


class Dimension(object):

    def __init__(self, data):
        self.name = data['dimension']
        self.key = data['key']
        self.value = data['name']

class SearchResults(object):

    def __init__(self, results, client):
        self.instant = None
        if results.instant != None:
            if results.instant.type == 'ConceptBind':
                self.instant = CompanyResult(results.instant)

        self.series = []
        for series in results.series:
            self.series.append(TimeseriesSearchResult(series, client))


class SearchResult(object):

    def __init__(self, result):
        self.type = result.type


class TimeseriesSearchResult(SearchResult):

    def __init__(self, result, client):
        super().__init__(result)

        self._client = client

        self.title = result.title
        self.dataset = result.dataset
        self.frequency = result.frequency
        self.start_date = result.start_date
        self.end_date = result.end_date
        self.dimensions = []
        self._dim_values = {}
        for dim in result.dimensions:
            self._dim_values[dim.name] = dim.value
            self.dimensions.append(dim)

    def get(self, transform = None):
        ds = self._client.get_dataset(self.dataset)

        reader =  TransformationDataReader(self._client, self._dim_values, transform, self.frequency, None)
        reader.dataset = ds
        
        return reader.get_pandasframe()


class CompanyResult(SearchResult):

    def __init__(self, company):
        super().__init__(company)

        self.name = company.name
        self.id = company.id
        self.company = None


class SearchResultsInt(object):

    def __init__(self, data):

        self.instant = None
        self.series = []

        if data['totalItems'] == 0:
            return

        items = data['items']
        for item in items:
            if item['type'] == 'ConceptBind':
                if item['concepts'][0]['conceptType'] == 'Company':
                    self.instant = CompanyResultInt(item)

            if item['type'] == 'TimeSeries':
                self.series.append(TimeseriesSearchResultInt(item))


class SearchResultInt(object):

    def __init__(self, data):
        self.type = data['type']

class TimeseriesSearchResultInt(SearchResultInt):

    def __init__(self, data):
        super().__init__(data)

        self.title = data['title']
        self.dataset = data['dataset']['id']
        self.frequency = data['frequency']
        self.start_date = data['startDate']
        self.end_date = data['endDate']
        self.dimensions = []
        self._dim_values = {}
        for dim in data['dimensions']:
            dimension = Dimension(dim)
            self._dim_values[dimension.name] = dimension.value
            self.dimensions.append(dimension)


class CompanyResultInt(SearchResultInt):

    def __init__(self, data):
        super().__init__(data)

        self.name = data['concepts'][0]['title']
        self.id = data['concepts'][0]['id']
