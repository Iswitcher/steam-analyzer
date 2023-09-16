import os
import sqlite3
import traceback
from datetime import datetime

from steam_analyzer.log import log


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
            # if db_ctrl.check_hash(conn, table, parent_id, h):
            #     log.info(f'Table {table}: game {parent_id} skipped, same hash')
            #     return
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


