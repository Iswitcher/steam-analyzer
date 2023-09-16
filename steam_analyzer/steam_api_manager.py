from review_loader import ReviewLoader

from steam.webapi import WebAPI
import requests


class SteamAPIManager:
    @staticmethod
    def get_reviews(gameid: int):
        return ReviewLoader(gameid).load()

    def get_current_players(self, gameid: int) -> int:
        response = self._api.ISteamUserStats.GetNumberOfCurrentPlayers(key=self._key, appid=gameid)
        return response['response']['player_count']

    def __init__(self, key: str):
        self._key = key
        self._api = WebAPI(key=key)

