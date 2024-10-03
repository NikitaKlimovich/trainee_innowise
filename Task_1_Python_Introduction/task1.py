import logging
import configparser
import argparse
import mysql.connector
import json
from pathlib import Path
from dicttoxml import dicttoxml

logging.basicConfig(level=logging.INFO, filename="log//py_log.log",filemode="w",format="%(asctime)s %(levelname)s %(message)s")


class Config():
    config: list
    
    def __init__(self):
        self.config = configparser.ConfigParser()
        try:
            self.config.read('config.ini') 
        except FileNotFoundError:
            logging.error('No config file')
            self.config = 0
    
    def get_host(self) -> str:
        return self.config["MySql"]["host"]
    
    def get_user(self) -> str:
        return self.config["MySql"]["user"]
    
    def get_password(self)-> str:
        return self.config["MySql"]["password"]

    def get_sql_folder(self)-> str:
        return self.config["FilePath"]["sql_folder"]

    def get_write_folder(self)-> str:
        return self.config["FilePath"]["write_folder"]




class Parser():
    parser:argparse.ArgumentParser

    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('-r', metavar='rooms_file_path', help='Path to file rooms.json')
        self.parser.add_argument('-s',  metavar='students_file_path', help='Path to file students.json')
        self.parser.add_argument('-f',  metavar='format', help='Choose output file format (json or xml)')
        self.parser = self.parser.parse_args()


class Args(Parser):
    path_rooms:str
    path_students:str
    output_format:str

    def __init__(self):
        super().__init__()
        self.path_rooms = self.parser.r
        self.path_students = self.parser.s
        self.output_format = self.parser.f
        
    def check_if_exists(self) -> bool:
        if FilePath.check_path(self.path_rooms) and FilePath.check_path(self.path_students):
            return 1
        logging.error("Specify the correct path to file")
        return 0
    
    def check_output_format(self) -> bool:
        if self.output_format in ('json','xml'):
            return 1
        logging.error('Unsupported format, choose one of them: json or xml')
        return 0
    
    def is_valid(self) -> bool:
        if self.check_if_exists() and self.check_output_format():
            return 1
        return 0



class Info (Args):

    @staticmethod
    def open_json(path_json:str) -> list:
        with open(path_json,'r',encoding='utf-8') as json_file:
            return json.load(json_file)
    
    def get_output_format(self) -> str:
        return self.output_format

    def get_info_from_path(self) -> tuple:
        files_info = list()
        if self.is_valid():
            for path_info in (self.path_rooms,self.path_students):
                data = Info.open_json(path_info)
                files_info.append([tuple(item.values()) for item in data])
            return tuple(files_info)
        else:
            return 0




class Connection():
    host:str
    user:str
    password:str
    conn:mysql.connector.MySQLConnection

    def __init__(self,config:Config):
        self.host = config.get_host()
        self.user = config.get_user()
        self.password = config.get_password()
        self.conn = ''

    def get_conn(self) -> None:
        try:
            self.conn = mysql.connector.connect(host=self.host, user=self.user, password=self.password)
            logging.info("Successfully connected to database")
        except mysql.connector.Error as e:
            self.conn = 0
            logging.error(f"Can not establish connection to database: {e}")


    def get_cur(self,dictionary=False) -> mysql.connector.MySQLConnection:
        return self.conn.cursor(buffered=True,dictionary=dictionary)


    def commit_changes(self) -> None:
        self.conn.commit()

    def close_conn(self) -> None:
        self.conn.close()




class DatabaseObject():
    sql_folder:str
    cur:mysql.connector.MySQLConnection

    def __init__(self,conn:mysql.connector.MySQLConnection,sql_folder:str,dict_bool=False):
        self.sql_folder = sql_folder
        self.cur = conn.get_cur(dictionary=dict_bool)
        try:
            self.cur.execute('create database rs_db')
        except mysql.connector.errors.DatabaseError:
            pass
        self.cur.execute('use rs_db')

    @staticmethod
    def open_sql(path_sql:str) -> list:
        with open(path_sql,'r',encoding='utf-8') as sql_file:
            return sql_file.read().split(';')[:-1]

   
    def execute(self,query:str,args=None) -> None:
        if args:
            if len(args) > 1:
                self.cur.executemany(query,args)
            else:
                self.cur.execute(query,args)
        else:
            self.cur.execute(query)

    
    def is_not_exists(self,query:str) -> None:
        self.cur.execute(query)
        if self.cur.fetchone()[0] == 0:
            return True
        return False


    def create(self) -> None:
        sql_create = DatabaseObject.open_sql(self.sql_folder+'//create.sql')
        for each in sql_create:
            try:
                self.execute(each)
            except mysql.connector.errors.ProgrammingError:
                pass

    
    def insert(self,info:list) -> None:
        sql_check = DatabaseObject.open_sql(self.sql_folder+'//check.sql')
        sql_insert = DatabaseObject.open_sql(self.sql_folder+'//insert.sql')
        for check, query,tuple_data in zip(sql_check,sql_insert,info):
            if self.is_not_exists(check):
                self.execute(query,tuple_data)

    
    def get(self) -> list:
        res_info = list()
        sql_get = DatabaseObject.open_sql(self.sql_folder+'//get.sql')
        for query in sql_get:
            self.execute(query)
            info = self.cur.fetchall()
            res_info.append(info)
        return res_info




class FilePath():
    write_folder_full:str
    output_format:str

    def __init__(self,write_folder_full:str,output_format:str):
        self.write_folder_full = write_folder_full
        self.output_format = output_format
        Path(self.write_folder_full).mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def check_path(path:str) -> bool:
        if Path(path).exists():
            logging.info(f"File {path} exists")
            return 1
        logging.info(f"File {path} doesn't exist")
        return 0
        
    def write(self,res_info:list) -> None:
        for i, dict_info in enumerate(res_info):
            file_name = '//query'+'.'.join([str(i+1),self.output_format])
            path_to_write = self.write_folder_full + file_name
            if FilePath.check_path(path_to_write):
                continue
            logging.info(f"Added new file {path_to_write}")
            if self.output_format == 'json':
                info = json.dumps(dict_info,indent=4)
                with open(path_to_write,'w',encoding='utf-8') as res_file:
                    res_file.write(info)
            if self.output_format == 'xml':
                info = dicttoxml(path_to_write, custom_root='info', attr_type=False)
                with open(path_to_write,'wb') as res_file:
                    res_file.write(info)
    


