import pytest
import knoema
import os

class TestKnoemaDataset:

    base_host = 'knoema.com'

    @pytest.fixture(scope="class")
    def get_client(self):
        apicfg = knoema.ApiConfig()
        apicfg.host = self.base_host
        apicfg.app_id = os.environ['KNOEMA_APP_ID'] if 'KNOEMA_APP_ID' in os.environ else 'FzOYqDg'
        apicfg.app_secret = os.environ['KNOEMA_APP_SECRET'] if 'KNOEMA_APP_SECRET' in os.environ else 'SPrvmY8eGRcGA'

    @pytest.mark.parametrize('dataset_id', ['IMFWEO2020Apr', 'CDIACTACHIINDUSAA'])
    def test_metadata(self, get_client, dataset_id):
        assert knoema.dataset(dataset_id)['id'] == dataset_id
