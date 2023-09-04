import requests
import json
import traceback
import os
import sqlite3
import hashlib
import time
import re

from log import log
from datetime import datetime
from fp.fp import FreeProxy
from fp.errors import FreeProxyException

class Main:
    url_steam_games     = "https://api.steampowered.com/ISteamApps/GetAppList/v0002/?format=json"
    json_steam_games    = "all_games.json"
        
    url_game_info       = "https://store.steampowered.com/api/appdetails?appids="
    json_game_info      = "appdetails"
    
    db_path             = "steam.db"
    table_all_games     = 'all_games'
    table_game          = 'game'
    
    proxy = {'http': ''}
    
    ignored_game_att    = [
        'short_description',
        'detailed_description',
        'about_the_game',
        'supported_languages',
        'pc_requirements',
        'mac_requirements',
        'linux_requirements',
        'screenshots',
        'movies',
        'background',
        'background_raw',
        'achievements',
        'content_descriptors',
        'package_groups',
        'recomendations'
    ]    
    
    # main execution flow
    def run(self):
        # get all steam games json if not have already
        self.get_all_steam_games_json()
        
        # parse all games list into db
        self.save_all_games_to_db()
    
        # get steam game info jsons
        self.get_appdetails_json() 
        
        # parse appdetails json into db
        # self.save_appdetails_to_json()
    
    
    # check if file exists
    def check_file(self, fpath):
        if os.path.exists(fpath):
            return True
        return False
    
    
    #get random proxy
    def getproxy(self):
        try:
            self.proxy['http'] = FreeProxy(rand=True).get()
            log.warning(f"Updated proxy to {self.proxy['http']}")
        except FreeProxyException as e:
            log.critical(f'failed to get a new proxy: {e}')
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
                log.critical(f'Error 429, changing proxy...')
                self.getproxy()
                data = self.get_json_from_url(url)
                return data
            else:
                log.warning(f'HTTP response code not 200: {response.status_code} {response.reason}')
                return None
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            log.critical(f'ERROR in {method_name}: {e}')
    
        
    # go to steam and dump all games list
    def get_all_steam_games_json(self):
        if self.check_file(self.json_steam_games):
            log.warning(f'Delete old file first! Skipping.')
            return
        raw =  self.get_json_from_url(self.url_steam_games)
        data = json.dumps(raw['applist']['apps'])
        with open(self.json_steam_games, 'w') as json_file:
            json_file.write(data)
    
            
    # dump all games to db
    def save_all_games_to_db(self):
        if not self.check_file(self.json_steam_games):
            log.critical(f'No steam games JSON found! Skipping.')
            return
        table = self.table_all_games
        db_ctrl.check_db_file(self.db_path)
        conn = db_ctrl.connect(self.db_path)
        with open(self.json_steam_games, 'r') as json_file:
            data = json.load(json_file)
        if not db_ctrl.check_table(conn, table):
            db_ctrl.create_table(conn, table)
        for app in data:
            # db_ctrl.check_columns(conn, table, app)
            id  = app['appid']
            h   = self.get_md5_hash(app)
            if db_ctrl.check_hash(conn, table, id, h):
                continue
            db_ctrl.close_old_record(conn, table, id)
            db_ctrl.add_new_record(conn, table, 'game_id', id, app, h)
            log.info(f'Added game {app}') 
        db_ctrl.disconnect(conn)
        
        
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
            url = self.url_game_info+str(app)
            filepath = self.json_game_info + '/' + str(app) + '.json'
            if (str(app)+'.json') in files:
                log.info(f'App {app} exists, skipping  {i}/{cnt}')
                continue
            raw = self.get_json_from_url(url)
            if raw == None:
                log.critical(f'Bad output for {app}, stopping')
                break
            data = json.dumps(raw)
            with open(filepath, 'w') as json_file:
                json_file.write(data)
            log.info(f'added {filepath} {i}/{cnt}')
            time.sleep(1.5)  # because fucking steam

    
    # parse appdetails json into db
    def save_appdetails_to_json(self):
        try:
            files = os.listdir(self.json_game_info)
            db_ctrl.check_db_file(self.db_path)
            conn = db_ctrl.connect(self.db_path)
            table = self.table_game
            for file in files:
                id = re.sub('.json', '', file)
                path = self.json_game_info + '/' + file
                with open(path, 'r') as json_file:
                    data = json.load(json_file)
                    if data[id]['success'] == False:
                        continue
                    appdata = data[id]['data']
                    row = self.delete_appdetails_ignored_att(appdata)
                    h = self.get_md5_hash(row)
                    db_ctrl.add_new_record(conn, table, 'game_id', id, row, h)
                    log.info(f'Game {id} (hash:{h}) added')
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            log.critical(f'ERROR in {method_name}: {e}')
    
    
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
        conn = db_ctrl.connect(self.db_path)
        q = "SELECT game_id from all_games where end_date > DATE('now')"
        ids = db_ctrl.execute_query(conn, q)
        for row in ids:
            output.append(row[0])
        return output       
            
    
    # get hash in md5
    def get_md5_hash(self, data):
       j = json.dumps(data, sort_keys=True).encode()
       dhash = hashlib.md5()
       dhash.update(j) 
       return dhash.hexdigest()    

class db_ctrl:
    # check or create db
    def check_db_file(path):
        try:
            if not os.path.exists(path):
                log.warning('No DB file found! Creating a new one')
                conn = sqlite3.connect(path)
                conn.commit()
                conn.close
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            log.critical(f'ERROR in {method_name}: {e}')
    
            
    # connect to db
    def connect(path):
        try:
            log.info(f'Connecting to database {path}')
            db_ctrl.check_db_file(path)
            conn = sqlite3.connect(path)
            log.info('DB connection established')
            return conn
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            log.critical(f'ERROR in {method_name}: {e}')
            
    
    # Disconnect        
    def disconnect(conn):
        conn.close()
        log.info('DB disconnected')
        return
    
    
    # check if table exists
    def check_table(conn, name):
        try: 
            cursor = conn.cursor()
            q = f"""
                SELECT name 
                FROM sqlite_master 
                WHERE type='table' 
                AND name='{name}'
                """
            cursor.execute(q)
            if cursor.fetchone() is None:
                return False        
            # log.info(f'Table {name} found')
            cursor.close()
            return True
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            log.critical(f'ERROR in {method_name}: {e}')   
    
    
    # create new table
    def create_table(conn, name):
        try:
            cursor = conn.cursor()
            q = (f"""
                CREATE TABLE {name}
                (
                    uid INTEGER PRIMARY KEY,
                    hash        INTEGER,
                    start_date  DATE,
                    end_date    DATE
                )""")
            cursor.execute(q)
            cursor.close()
            log.info(f'Table {name} created')
            return
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            log.critical(f'ERROR in {method_name}: {e}')
            
            
    # check by row data if table column exists
    def check_column(conn, table, att):
        try:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table})")
            rows = cursor.fetchall()
            column_names = [row[1] for row in rows]
            if db_ctrl.cnt_col_occur(att, column_names) > 0:
                return
            t = type(att)
            db_ctrl.add_table_column(conn, table, att, t)
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            log.critical(f'ERROR in {method_name}: {e}') 
    
    
    # check by row data if table column exists
    def check_columns(conn, table, row):
        try:
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table})")
            rows = cursor.fetchall()
            column_names = [row[1] for row in rows]
            for att in row:
                if db_ctrl.cnt_col_occur(att, column_names) > 0:
                    continue
                t = type(row[att])
                db_ctrl.add_table_column(conn, table, att, t)
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            log.critical(f'ERROR in {method_name}: {e}')         
    
    
    # counts the occurence of column name in array of names   
    def cnt_col_occur(value, array):
        cnt = 0
        for i in array:
            if i == value:
                cnt = cnt + 1
        return cnt
        # return sum([string.count(value) for string in array])        
    
    
    # adds new table column of specified type
    def add_table_column(conn, table, att_name, att_type):
        try:
            col_type = db_ctrl.get_column_type(att_type)
            if att_type == None:
                return 
            cursor = conn.cursor()
            alter_query = f"""
                ALTER TABLE {table} 
                ADD COLUMN {att_name} {col_type}
                """
            cursor.execute(alter_query)
            conn.commit()
            cursor.close()
            log.info(f'Added column {att_name}:{att_type} to {table}')
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            log.critical(f'ERROR in {method_name}: {e}')
            
            
    # parse json type to sql        
    def get_column_type(att_type):
        if att_type == int:
            return 'INTEGER'
        elif att_type == float:
            return 'REAL'
        elif att_type == str:
            return 'TEXT'
        elif att_type == bool:
            return 'TEXT'
        else: 
            return None
        
    
    # check if a row can be skipped by comparing hash
    def check_hash(conn, table, id, h):
        try:
            cursor = conn.cursor()
            q = f"""
                SELECT hash
                FROM {table}
                WHERE game_id = {id}
                AND end_date > DATE('now')
                """ 
            cursor.execute(q)
            row = cursor.fetchone()
            cursor.close()
            if row is not None:
                return True
            return False
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            log.critical(f'ERROR in {method_name}: {e}')
    
    
    # set enddate for deprecated record        
    def close_old_record(conn, table, id):
        try:
            cursor = conn.cursor()
            q = f"""
                UPDATE {table}
                SET end_date = DATE('now')
                WHERE game_id = {id}
                AND end_date > DATE('now')
                """ 
            cursor.execute(q)
            cursor.close()
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            log.critical(f'ERROR in {method_name}: {e}')
            
            
    # adds new record
    def add_new_record(conn, table, parent_name, parent_id, row, h):
        try:
            if db_ctrl.check_hash(conn, table, parent_id, h):
                log.info(f'Table {table}: game {parent_id} skipped, same hash')
                return
            if not db_ctrl.check_table(conn, table):
                db_ctrl.create_table(conn, table)
            if not parent_name == None:
                db_ctrl.check_column(conn, table, parent_name)
            db_ctrl.check_columns(conn, table, row)
            columns = []
            values = []
            for att in row:
                if type(row[att]) in (int, float, str):
                    columns.append(att)
                    values.append(row[att])
                elif type(row[att]) == bool:
                    columns.append(att)
                    values.append(str(row[att]))
                elif type(row[att]) == dict:
                    parent_name = table + '_id'
                    db_ctrl.add_new_record(conn, att, parent_name, parent_id, row[att], h)
                elif type(row[att]) == list:
                    parent_name = table + '_id'
                    for item in row[att]:
                        if type(item) == dict:
                            db_ctrl.add_new_record(conn, att, parent_name, parent_id, item, h)
                        else:
                            v = {}
                            v['value'] = item
                            db_ctrl.add_new_record(conn, att, parent_name, parent_id, v, h)
            q_obj = db_ctrl.get_insert_query(table, parent_name, parent_id, h, columns, values) 
            q = q_obj['query']
            values = q_obj['values']
            cursor = conn.cursor()
            cursor.execute(q, values)
            cursor.close()
            conn.commit()
            # log.info(f'Table {table}: record {id} (hash:{h}) added')
            return
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            log.critical(f'ERROR in {method_name}: {e}')
            
            
    # returns insert query string and tuple with standart and table-specific values
    def get_insert_query(table, parent_name, parent_id, h, columns, values): 
        # # add id
        # columns.append("game_id")
        # values.append(id)
        
        # add parent
        if not parent_name == None:
            columns.append(parent_name)
            values.append(parent_id)
        
        # add hash
        columns.append("hash")
        values.append(h)
        
        # add dates
        columns.append("start_date")
        values.append(datetime.now())
        columns.append("end_date")
        values.append(datetime(9999, 12, 31, 23, 59, 59, 0))
        
        query = f"""
            INSERT INTO {table} ({", ".join(columns)})
            VALUES ({", ".join(["?" for _ in values])})
        """                    
        return {'query': query, 'values': tuple(values)}
    
    
    # execute raw inbound query and return the data
    def execute_query(conn, q):
        try:
            cursor = conn.cursor()
            cursor.execute(q)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            log.critical(f'ERROR in {method_name}: {e}')

    
if __name__ == '__main__':
    Main().run()
