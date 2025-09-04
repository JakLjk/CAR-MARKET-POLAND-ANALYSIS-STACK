import requests
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from urllib3.util.retry import Retry

class SSLv1Adapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context(ciphers="DEFAULT:@SECLEVEL=1")
        kwargs["ssl_context"]=ctx
        return super().init_poolmanager(*args, **kwargs)
    

def requests_session_farbic(https_ssl_adapter:HTTPAdapter) -> requests.session:
    s = requests.session()
    s.mount("https://", https_ssl_adapter)
    retries = Retry(
        total=5, backoff_factor=0.5,
        status_forcelist = [429, 500, 502, 5003, 504],
        allowed_method = ["GET"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s