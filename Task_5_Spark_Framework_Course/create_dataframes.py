import configparser

import pandas as pd
import psycopg2
from pyspark.context import SparkContext
from pyspark.sql import SparkSession


def connect():
    config = configparser.ConfigParser()
    config.read("config.ini")
    config = config["postgresql"]
    conn = psycopg2.connect(
        host=config["db_host"],
        port=config["db_port"],
        dbname=config["db_name"],
        user=config["db_user"],
        password=config["db_password"],
    )
    return conn


def create_tables(conn):
    query = "select tablename FROM pg_catalog.pg_tables where schemaname = 'public' and tablename not like 'payment_%';"
    table_names = pd.read_sql_query(query, conn)["tablename"].to_list()
    spark = SparkSession(SparkContext.getOrCreate())
    table_dict = dict()
    for table_name in table_names:
        query = f"select * from {table_name}"
        df_pandas = pd.read_sql_query(query, conn)
        df_pandas_dropna = df_pandas.dropna(axis="columns", how="all")
        if table_name == "staff":
            df_pandas_dropna = df_pandas_dropna.drop("picture", axis=1)
        table_dict[table_name] = spark.createDataFrame(df_pandas_dropna)
    return table_dict
