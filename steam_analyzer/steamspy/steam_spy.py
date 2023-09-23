from typing import Type

import requests

from steam_analyzer.log import Log
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
                Log.warning(f'got response code {code} for {url}')
                return result
            data = resp.json()
            # parse tags
            result.Tags = data['tags']
            # parse owners
            values = data['owners'].split(' .. ')
            if len(values) != 2:
                raise ValueError('invalid owners format')
            result.OwnersFrom = int(values[0].replace(',', ''))
            result.OwnersTo = int(values[1].replace(',', ''))
            # parse players
            result.Avg2Weeks = data['average_2weeks']
            result.AvgForever = data['average_forever']
            result.Median2Weeks = data['median_2weeks']
            result.MedianForever = data['median_forever']
            result.Ccu = data['ccu']
            # parse reviews
            result.ReviewsNegative = data['negative']
            result.ReviewsPositive = data['positive']
        except Exception as e:
            Log.critical(f'while trying to get {url} from steamspy: {e}')
        finally:
            return result
