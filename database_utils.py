import os
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-22'
import yaml

from pandasgui import show
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import text

import boto3
import numpy as np 
import pandas as pd
import requests
import tabula
import yaml

class DatabaseConnector:
    def __init__(self, yaml_file_path):
        '''
        Initialize the DatabaseConnector object with the path to the YAML file containing database credentials.

        Parameters:
        - yaml_file_path (str): The file path to the YAML file containing database credentials.

        This method initializes the DatabaseConnector object by setting up the path to the YAML file
        containing the database credentials. It also initializes the database engine using the provided credentials.
        '''
        self.yaml_file_path = yaml_file_path
        self.engine = self.init_db_engine()
        self.db_creds = self.read_db_creds()

    def read_db_creds(self):
        '''
        Read database credentials from the YAML file.

        Returns:
        - db_creds (dict): A dictionary containing database credentials.

        This method reads database credentials from the YAML file specified during initialization.
        The credentials are returned as a dictionary.
        '''
        with open(self.yaml_file_path, 'r') as file:
            db_creds = yaml.safe_load(file)
            return db_creds

    def init_db_engine(self):
        '''
        Initialize the SQLAlchemy database engine using the database credentials.

        Returns:
        - engine (Engine): The SQLAlchemy database engine.

        This method initializes the SQLAlchemy database engine using the database credentials
        obtained from the YAML file. The engine is configured to connect to the database.
        '''
        db_creds = self.read_db_creds()
        engine = create_engine(f"postgresql://{db_creds['USER']}:{db_creds['PASSWORD']}@{db_creds['HOST']}:{db_creds['PORT']}/{db_creds['DATABASE']}")
        engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        return engine
    
    def list_db_tables(self):
        '''
        List the tables in the connected database.

        Returns:
        - db_tables (list): A list of table names in the connected database.

        This method retrieves the list of tables in the connected database using SQLAlchemy's inspect function.
        The list of table names is returned.
        '''
        inspector = inspect(self.engine) 
        db_tables = inspector.get_table_names()
        return db_tables
    
    def upload_to_db(self, df, table_name):
        '''
        Upload a DataFrame to the specified database table.

        Parameters:
        - df (DataFrame): The pandas DataFrame to upload.
        - table_name (str): The name of the database table to upload the data to.

        This method uploads the provided pandas DataFrame to the specified database table.
        If the table already exists, it will be replaced with the new data.
        '''
        df.to_sql(table_name, con=self.engine, if_exists='replace', index=False)