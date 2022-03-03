import requests
import requests_cache

class Extract:
    def __init__(self, data_source):
        self.data_source = data_source
        self.request_cache = requests_cache.install_cache(cache_name='github_cache', backend='sqlite', expire_after=3600)

    def getDataFromAPI(self, endpoint, **kwargs):
        endpoint_string = self.data_source + endpoint
        req = requests.get(endpoint_string, params=kwargs)
        return req.json()
