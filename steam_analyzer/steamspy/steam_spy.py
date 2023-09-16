from typing import Type

import requests

from steam_analyzer.log import log
from steam_analyzer.steamspy.app_details import AppDetails


class SteamSpy:
    @staticmethod
    def get_app_details(appid: int) -> Type[AppDetails]:
        result = AppDetails
        url = f'https://steamspy.com/api.php?request=appdetails&appid={appid}'
        try:
            resp = requests.get(url)
            code = resp.status_code
            if code != 200:
                log.warning(f'got response code {code} for {url}')
            result.Tags = resp.json()['tags']
        except Exception as e:
            log.critical(f'while trying to get {url} from steamspy: {e}')
        finally:
            return result
