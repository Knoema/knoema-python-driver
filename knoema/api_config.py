"""This module contains Api configuration class"""

import os

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
            cls.instance.host = os.environ['KNOEMA_HOST'] if 'KNOEMA_HOST' in os.environ else 'knoema.com'
            cls.instance.app_id = None
            cls.instance.app_secret = None
        return cls.instance

    def __init__(self):
        self.host = self.instance.host
        self.app_id = self.instance.app_id
        self.app_secret = self.instance.app_secret
