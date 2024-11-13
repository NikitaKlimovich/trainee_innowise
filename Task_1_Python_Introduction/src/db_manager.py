import json
import logging

import mysql.connector


logging.basicConfig(
    level=logging.INFO,
    filename="log//py_log.log",
    filemode="w",
    format="%(asctime)s %(levelname)s %(message)s",
)


class DatabaseManager:
    def __init__(self, config_mysql):
        self.conn = mysql.connector.connect(
            host=config_mysql["host"],
            user=config_mysql["user"],
            password=config_mysql["password"],
        )
        logging.info("Successfully connected to database")
        self.path_sql = config_mysql["sql_folder"]

    def read_sql(self, file_name):
        with open(self.path_sql + file_name, "r", encoding="utf-8") as sql_file:
            return sql_file.read().split(";")[:-1]

    def execute_query(self, query, args=None, dict_bool=False):
        cur = self.conn.cursor(buffered=True, dictionary=dict_bool)
        if args:
            if len(args) > 1:
                cur.executemany(query, args)
            else:
                cur.execute(query, args)
        else:
            cur.execute(query)
        self.conn.commit()
        return cur

    def close_conn(self):
        self.conn.close()


class SchemaManager:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def create(self):
        for query in self.db_manager.read_sql("create.sql"):
            try:
                self.db_manager.execute_query(query)
            except (
                mysql.connector.errors.ProgrammingError,
                mysql.connector.errors.DatabaseError,
            ):
                logging.info(f"Query {query} is not running: the entity is already exists")


class DataLoader:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def load_data_from_file(self, path_json):
        with open(path_json, "r", encoding="utf-8") as json_file:
            try:
                info = json.load(json_file)
            except json.JSONDecodeError:
                logging.error("Failed to unpack json file")
                return 0
        sql_check = self.db_manager.read_sql("check.sql")
        sql_insert = self.db_manager.read_sql("insert.sql")
        for check, query, tuple_data in zip(sql_check, sql_insert, info):
            cur = self.db_manager.execute_query(check)
            if cur.fetchone():
                continue
            self.db_manager.execute_query(query, tuple_data)
        logging.info("Successfully created necessary entities")


class DataSelector:
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def select_info(self, q_num):
        sql_select = self.db_manager.read_sql("select.sql")[q_num]
        return self.db_manager.execute_query(sql_select, dict_bool=True).fetchall()
