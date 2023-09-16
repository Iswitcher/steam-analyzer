import hashlib
import json
import os
import re
import time
import traceback

import requests
import undetected_chromedriver as uc
from fake_useragent import UserAgent
from fp.errors import FreeProxyException
from fp.fp import FreeProxy

from log import Log
from db_ctrl import DBCtrl
from steamspy.steam_spy import SteamSpy
from config import settings


class Main:
    table_game_tags = "tags"
    table_all_games = "all_games"
    table_game = "game"
    proxy = {'http': ''}

    # Main class constructor
    def __init__(self):
        self.url_steam_games = settings['url_steam_games']
        self.json_steam_games = settings['json_steam_games']
        self.url_game_info = settings['url_game_info']
        self.json_game_info = settings['json_game_info']
        self.url_game_store_page = settings['url_game_store_page']
        self.db_path = settings['db_path']
        self.ignored_game_att = settings['ignored_game_att']

        self._db_ctrl = DBCtrl(self.db_path)

        self.log = Log()
        # init the uc driver and set is to private field
        ua = UserAgent()
        options = uc.ChromeOptions()
        options.add_argument('--auto-open-devtools-for-tabs')
        options.add_argument('--no-sandbox')
        options.add_argument('--blink-settings=imagesEnabled=false')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('User-Agent={0}'.format(ua.chrome))
        self._driver = uc.Chrome(headless=False, use_subprocess=True, options=options)
        self._driver.maximize_window()
        self._driver.get('https://steamdb.info/app/570/charts/')

        self._steamspy = SteamSpy()

    # main execution flow
    def run(self):
        # get all steam games json if not have already
        # self.get_all_steam_games_json()

        # parse all games list into db
        # self.save_all_games_to_db()

        # get steam game info jsons
        # self.get_appdetails_json()

        # parse appdetails json into db
        # self.save_appdetails_to_db()

        # get game tags
        # self.save_game_tags_to_db()
        pass

    # check if file exists
    def check_file(self, fpath):
        if os.path.exists(fpath):
            return True
        return False

    # get random proxy
    def getproxy(self):
        try:
            self.proxy['http'] = FreeProxy(rand=True).get()
            self.log.warning(f"Updated proxy to {self.proxy['http']}")
        except FreeProxyException as e:
            self.log.critical(f'failed to get a new proxy: {e}')
            self.proxy['http'] = ''

            # get json from web by path

    def get_json_from_url(self, url):
        try:
            proxies = None
            if self.proxy['http'] != '':
                proxies = self.proxy
            response = requests.get(url, allow_redirects=True, proxies=proxies)
            if response.status_code == 200:
                data = json.loads(response.text)
                return data
            if response.status_code == 429:
                # timeout = 5
                # log.critical(f'Error 429, waiting for {timeout} seconds...')
                # time.sleep(timeout)
                self.log.critical(f'Error 429, changing proxy...')
                self.getproxy()
                data = self.get_json_from_url(url)
                return data
            else:
                self.log.warning(f'HTTP response code not 200: {response.status_code} {response.reason}')
                return None
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            self.log.critical(f'ERROR in {method_name}: {e}')

    # go to steam and dump all games list
    def get_all_steam_games_json(self):
        if self.check_file(self.json_steam_games):
            self.log.warning(f'Delete old file first! Skipping.')
            return
        raw = self.get_json_from_url(self.url_steam_games)
        data = json.dumps(raw['applist']['apps'])
        with open(self.json_steam_games, 'w') as json_file:
            json_file.write(data)

    # dump all games to db
    def save_all_games_to_db(self):
        if not self.check_file(self.json_steam_games):
            self.log.critical(f'No steam games JSON found! Skipping.')
            return
        table = self.table_all_games
        with open(self.json_steam_games, 'r') as json_file:
            data = json.load(json_file)
        if not self._db_ctrl.check_table(table):
            self._db_ctrl.create_table(table)
        for app in data:
            # db_ctrl.check_columns(conn, table, app)
            id = app['appid']
            h = self.get_md5_hash(app)
            if self._db_ctrl.check_hash(table, id, h):
                continue
            self._db_ctrl.close_old_record(table, id)
            self._db_ctrl.add_new_record(table, 'game_id', id, app, h)
            self.log.info(f'Added game {app}')
        self._db_ctrl.disconnect()

    # get all app details jsons
    def get_appdetails_json(self):
        appids = self.get_app_list()
        cnt = len(appids)
        i = 0
        if not os.path.exists(self.json_game_info):
            current_dir = os.getcwd()
            new_dir = os.path.join(current_dir, self.json_game_info)
            os.mkdir(new_dir)
        files = os.listdir(self.json_game_info)
        for app in appids:
            i += 1
            url = self.url_game_info + str(app)
            filepath = self.json_game_info + '/' + str(app) + '.json'
            if (str(app) + '.json') in files:
                self.log.info(f'App {app} exists, skipping  {i}/{cnt}')
                continue
            raw = self.get_json_from_url(url)
            if raw == None:
                self.log.critical(f'Bad output for {app}, stopping')
                break
            data = json.dumps(raw)
            with open(filepath, 'w') as json_file:
                json_file.write(data)
            self.log.info(f'added {filepath} {i}/{cnt}')
            time.sleep(1.1)  # because fucking steam timeout

    # parse appdetails json into db
    def save_appdetails_to_db(self):
        try:
            files = os.listdir(self.json_game_info)
            f_cnt = len(files)
            i = 0
            table = self.table_game
            for file in files:
                i += 1
                id = re.sub('.json', '', file)
                path = self.json_game_info + '/' + file
                with open(path, 'r') as json_file:
                    data = json.load(json_file)
                    if data[id]['success'] == False:
                        continue
                    appdata = data[id]['data']
                    row = self.delete_appdetails_ignored_att(appdata)
                    h = self.get_md5_hash(row)
                    if self._db_ctrl.check_hash(table, id, h):
                        self.log.info(f'Game {id} (hash:{h}) already exists, skipping {i}/{f_cnt}')
                        continue
                    self._db_ctrl.add_new_record(table, 'game_id', id, row, h)
                    self.log.info(f'Game {id} (hash:{h}) added {i}/{f_cnt}')
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            self.log.critical(f'ERROR in {method_name}: {e}')

    # delete ignored appdetails attributes
    def delete_appdetails_ignored_att(self, data):
        output = data.copy()
        for att in data:
            if att in self.ignored_game_att:
                del output[att]
        return output

    # get apps to download details
    def get_app_list(self):
        output = []
        q = "SELECT game_id from all_games where end_date > DATE('now')"
        ids = self._db_ctrl.execute_query(q)
        for row in ids:
            output.append(row[0])
        return output

        # get hash in md5

    def get_md5_hash(self, data):
        j = json.dumps(data, sort_keys=True).encode()
        dhash = hashlib.md5()
        dhash.update(j)
        return dhash.hexdigest()

        # get game tags by crawling the browser

    def save_game_tags_to_db(self):
        appids = self.get_app_list()
        table = self.get_tags_table()
        i = 0
        cnt = len(appids)
        for app in appids:
            i += 1
            app_url = self.url_game_store_page + app
            if self._db_ctrl.check_if_records_exist(table, app):
                self.log.warning(f'Skipped tags for {app} {i}/{cnt}')
                continue
            tags = self.get_tags_from_url(self._driver, app_url)
            if tags is None:
                self.log.warning(f'No tags for {app} {i}/{cnt}')
                # adding dummy record
                t = {}
                t['tag'] = ''
                self._db_ctrl.add_new_record(table, 'game_id', app, t, '')
                continue
            h = self.get_md5_hash(tags)
            if self._db_ctrl.check_hash(table, app, h):
                self.log.warning(f'Skipped tags for {app} {i}/{cnt}')
                continue
            for tag in tags['tags']:
                t = {}
                t['tag'] = tag
                self._db_ctrl.add_new_record(table, 'game_id', app, t, h)
            # db_ctrl.add_new_record(conn, table, 'game_id', app, tags, h)
            self.log.info(f'Added tags for {app} (hash: {h}) {i}/{cnt}')

    # find or create tags table
    def get_tags_table(self):
        table = self.table_game_tags
        if not self._db_ctrl.check_table(table):
            self._db_ctrl.create_table(table)
            # db_ctrl.add_table_column(conn, table, 'game_id', 'TEXT')
            # db_ctrl.add_table_column(conn, table, 'tag', 'TEXT')
        return table

    # get tags via browser instance
    def get_tags_from_url(self, appid: int):
        result = {'tags': []}
        try:
            app_details = self._steamspy.get_app_details(appid)
            for tag in app_details.Tags:
                result['tags'].append(tag)
            return result
        except Exception as e:
            # NoSuchElementException
            self.log.critical(f'Exception on {url}')
            return None


