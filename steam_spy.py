import requests
from types import SimpleNamespace
import json

from log import log


class SteamSpy:
    @staticmethod
    def get_app_details(appid: int) -> object:
        url = f'https://steamspy.com/api.php?request=appdetails&appid={appid}'
        try:
            resp = requests.get(url)
            code = resp.status_code
            if code != 200:
                log.warning(f'got response code {code} for {url}')
                return None
            return json.loads(resp.text, object_hook=lambda d: SimpleNamespace(**d))
        except Exception as e:
            log.critical(f'while trying to get {url} from steamspy: {e}')
            return None
