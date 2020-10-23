import pytest
import knoema
import os

base_host = 'knoema.com'

@pytest.fixture()
def get_client():
    apicfg = knoema.ApiConfig()
    apicfg.host = base_host
    apicfg.app_id = os.environ['KNOEMA_APP_ID'] if 'KNOEMA_APP_ID' in os.environ else 'DguAM8'
    apicfg.app_secret = os.environ['KNOEMA_APP_SECRET'] if 'KNOEMA_APP_SECRET' in os.environ else 'Io0OgIy1TNwtAA'