from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context

class SSLv1Adapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = create_urllib3_context(ciphers="DEFAULT:@SECLEVEL=1")
        kwargs["ssl_context"]=ctx
        return super().init_poolmanager(*args, **kwargs)
    