import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
from sqlalchemy import inspect
from sqlalchemy import text
from pandasgui import show
import numpy as np 
import tabula
import requests
import boto3


class DatabaseConnector:
    def __init__(self, yaml_file_path):
        self.yaml_file_path = yaml_file_path
        self.engine = self.init_db_engine()
        self.db_creds = self.read_db_creds()

    def read_db_creds(self):
        with open(self.yaml_file_path, 'r') as file:
            db_creds = yaml.safe_load(file)
            return db_creds

    def init_db_engine(self):
        db_creds = self.read_db_creds()
        engine = create_engine(f"postgresql://{db_creds['USER']}:{db_creds['PASSWORD']}@{db_creds['HOST']}:{db_creds['PORT']}/{db_creds['DATABASE']}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        return engine
    
    def list_db_tables(self):
        inspector = inspect(self.engine) 
        db_tables = inspector.get_table_names()
        return db_tables
    
    def upload_to_db(self, df, table_name):
        df.to_sql(table_name, con=self.engine, if_exists='replace', index=False)