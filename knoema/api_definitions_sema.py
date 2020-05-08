"""This module contains metadata definitions for Knoema API for semantic atlas"""

import json
from knoema.data_reader import TransformationDataReader
from datetime import datetime

class Company(object):
    """"The class contains data related to a company like name and groups of indicators"""

    def __init__(self, company, client):
        self.name = company.name

        self.groups = []

        for group in company.groups:
            self.groups.append(CompanyIndicatorsGroup(group, client))

    def get_indicator(self, name, group = None):
        for grp in self.groups:
            if group != None and grp.name != group:
                continue

            for indicator in grp.indicators:
                if indicator.name == name:
                    return indicator
        
        return None


class CompanyIndicatorsGroup(object):
    """"The class contains information about named group of indicator"""

    def __init__(self, group, client):
        self.name = group.name
        
        self.indicators = []
        for indicator in group.indicators:
            self.indicators.append(CompanyIndicator(indicator, client))


class CompanyIndicator(object):
    """The class contains information about company indicator"""

    def __init__(self, indicator, client):
        self._client = client

        self._id = indicator._id
        self._full_id = indicator._full_id
        self.name = indicator.name
        self.count = indicator.count

    def get(self, transform = None):
        ind_info = self._client.get_indicator_info(self._full_id)
        if len(ind_info['groups']) < 1:
            return None

        first_group = ind_info['groups'][0]

        dataset = first_group['id']
        ds = self._client.get_dataset(dataset) if dataset else None

        desc = first_group['batchDesctiptor']
        pivot = self._client.get_data_by_json(desc)

        reader =  TransformationDataReader(self._client, None, transform, None)
        reader.dataset = ds
        frame = reader.get_pandasframe_pivot(pivot)

        return frame


class CompanyInt(object):

    def __init__(self, data):
        self.name = data['title']

        self.groups = []
        for group in data['groupHierarchies']:
            self.groups.append(CompanyIndicatorsGroupInt(group))
        

class CompanyIndicatorsGroupInt(object):

    def __init__(self, data):
        self.name = data['topParent']['name']

        hierarchy = data['hierarchy']
        item_data = data['itemData']
        self.indicators = []
        for _, indicators_list in hierarchy.items():
            for ind in indicators_list:
                indicator = {
                    'id': ind['id'],
                    'name': ind['name']
                }

                ind_key = str(ind['key'])
                if ind_key in item_data:
                    indicator['full_id'] = item_data[ind_key]['conceptsQuery']
                    indicator['count'] = item_data[ind_key]['count']

                self.indicators.append(CompanyIndicatorInt(indicator))


class CompanyIndicatorInt(object):
    
    def __init__(self, data):
        self._id = data['id']
        self._full_id = data['full_id']
        self.name = data['name']
        self.count = data['count']
