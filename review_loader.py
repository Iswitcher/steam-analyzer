import requests
import json
from types import SimpleNamespace

REVIEW_FILTER_ALL = 'all'
REVIEW_FILTER_RECENT = 'recent'
REVIEW_FILTER_UPDATED = 'updated'

REVIEW_TYPE_ALL = 'all'
REVIEW_TYPE_POSITIVE = 'positive'
REVIEW_TYPE_NEGATIVE = 'negative'

PURCHASE_TYPE_ALL = 'all'
PURCHASE_TYPE_NON_STEAM = 'non_steam_purchase'
PURCHASE_TYPE_STEAM = 'steam'


class ReviewLoader:
    def load(self):
        result = []
        url = f'https://store.steampowered.com/appreviews/{self._appid}'
        processed = 0
        while processed <= self._total_reviews and processed <= 10:
            params = {
                'json': 1,
                'num_per_page': self._num_per_page,
                'filter': self._filter_type,
                'review_type': self._review_type,
            }
            if self._cursor is not None:
                params['cursor'] = self._cursor
            resp = requests.get(url, params=params)
            if resp.status_code != 200:
                break
            body = resp.json()
            if body['success'] != 1:
                break
            if self._cursor is None:
                self._cursor = body['cursor']
            self.parse_summary(body['query_summary'])
            reviews = self.parse_reviews(body['reviews'])
            result.append(reviews)
            processed += len(reviews)
        return result

    def parse_summary(self, data):
        if self._parsed_summary:
            return
        self._num_reviews = data['num_reviews']
        self._review_score = data['review_score']
        self._review_score_desc = data['review_score_desc']
        self._total_positive = data['total_positive']
        self._total_negative = data['total_negative']
        self._total_reviews = data['total_reviews']
        self._parsed_summary = True

    @staticmethod
    def parse_reviews(data) -> []:
        result = []
        for r in data:
            review = json.loads(json.dumps(r), object_hook=lambda d: SimpleNamespace(**d))
            review.author = json.loads(json.dumps(r['author']), object_hook=lambda d: SimpleNamespace(**d))
            result.append(review)
        return result

    def __init__(self,
                 appid: int,
                 max_per_page: int = 100,
                 filter_type: str = REVIEW_FILTER_ALL,
                 review_type: str = REVIEW_TYPE_ALL):
        self._appid = appid
        self._filter_type = filter_type
        self._review_type = review_type
        self._num_per_page = max_per_page
        self._cursor = None
        self._num_reviews = None
        self._review_score = None
        self._review_score_desc = None
        self._total_positive = None
        self._total_negative = None
        self._total_reviews = 0
        self._parsed_summary = False
