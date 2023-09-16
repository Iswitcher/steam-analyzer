import sqlite3
import traceback
from datetime import datetime

from steam_analyzer.log import Log


class DBCtrl:
    def __init__(self, path: str, log: Log):
        self.log = log
        try:
            self.log.info(f'Connecting to database {path}')
            self._conn = sqlite3.connect(path)
            self.log.info('DB connection established')
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            self.log.critical(f'ERROR in {method_name}: {e}')

    # Disconnect
    def disconnect(self):
        self._conn.close()
        self.log.info('DB disconnected')

    # check if table exists
    def check_table(self, name: str):
        try:
            cursor = self._conn.cursor()
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
            self.log.critical(f'ERROR in {method_name}: {e}')

    # create new table
    def create_table(self, name: str):
        try:
            cursor = self._conn.cursor()
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
            self.log.info(f'Table {name} created')
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            self.log.critical(f'ERROR in {method_name}: {e}')

    # check by row data if table column exists
    def check_column(self, table, att):
        try:
            cursor = self._conn.cursor()
            cursor.execute(f"PRAGMA table_info({table})")
            rows = cursor.fetchall()
            column_names = [row[1] for row in rows]
            if self.cnt_col_occur(att, column_names) > 0:
                return
            t = type(att)
            self.add_table_column(table, att, t)
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            self.log.critical(f'ERROR in {method_name}: {e}')

    # check by row data if table column exists
    def check_columns(self, table, row):
        try:
            cursor = self._conn.cursor()
            cursor.execute(f"PRAGMA table_info({table})")
            rows = cursor.fetchall()
            column_names = [row[1] for row in rows]
            for att in row:
                if self.cnt_col_occur(att, column_names) > 0:
                    continue
                t = type(row[att])
                self.add_table_column(table, att, t)
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            self.log.critical(f'ERROR in {method_name}: {e}')

    # counts the occurence of column name in array of names
    def cnt_col_occur(self, value, array):
        cnt = 0
        for i in array:
            if i == value:
                cnt = cnt + 1
        return cnt
        # return sum([string.count(value) for string in array])

    # adds new table column of specified type
    def add_table_column(self, table, att_name, att_type):
        try:
            col_type = self.get_column_type(att_type)
            if att_type is None:
                return
            cursor = self._conn.cursor()
            alter_query = f"""
                ALTER TABLE {table} 
                ADD COLUMN {att_name} {col_type}
                """
            cursor.execute(alter_query)
            self._conn.commit()
            cursor.close()
            self.log.info(f'Added column {att_name}:{att_type} to {table}')
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            self.log.critical(f'ERROR in {method_name}: {e}')

    # parse json type to sql
    def get_column_type(self, att_type):
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
    def check_hash(self, table, id, h):
        try:
            cursor = self._conn.cursor()
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
            self.log.critical(f'ERROR in {method_name}: {e}')

    # set enddate for deprecated record
    def close_old_record(self, table, id):
        try:
            cursor = self._conn.cursor()
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
            self.log.critical(f'ERROR in {method_name}: {e}')

    # adds new record
    def add_new_record(self, table, parent_name, parent_id, row, h):
        try:
            # if db_ctrl.check_hash(conn, table, parent_id, h):
            #     log.info(f'Table {table}: game {parent_id} skipped, same hash')
            #     return
            if not self.check_table(table):
                self.create_table(table)
            if parent_name is not None:
                self.check_column(table, parent_name)
            self.check_columns(table, row)
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
                    self.add_new_record(att, parent_name, parent_id, row[att], h)
                elif type(row[att]) == list:
                    parent_name = table + '_id'
                    for item in row[att]:
                        if type(item) == dict:
                            self.add_new_record(att, parent_name, parent_id, item, h)
                        else:
                            v = {}
                            v['value'] = item
                            self.add_new_record(att, parent_name, parent_id, v, h)
            q_obj = self.get_insert_query(table, parent_name, parent_id, h, columns, values)
            q = q_obj['query']
            values = q_obj['values']
            cursor = self._conn.cursor()
            cursor.execute(q, values)
            cursor.close()
            self._conn.commit()
            # log.info(f'Table {table}: record {id} (hash:{h}) added')
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            self.log.critical(f'ERROR in {method_name}: {e}')

    # returns insert query string and tuple with standart and table-specific values
    def get_insert_query(self, table, parent_name, parent_id, h, columns, values):
        # # add id
        # columns.append("game_id")
        # values.append(id)

        # add parent
        if parent_name is not None:
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
    def execute_query(self, q):
        try:
            cursor = self._conn.cursor()
            cursor.execute(q)
            rows = cursor.fetchall()
            cursor.close()
            return rows
        except Exception as e:
            method_name = traceback.extract_stack(None, 2)[0][2]
            self.log.critical(f'ERROR in {method_name}: {e}')
