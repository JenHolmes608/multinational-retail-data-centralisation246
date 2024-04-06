import yaml
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import inspect
from sqlalchemy import text


class DatabaseConnector:
    def __init__(self, yaml_file_path = 'db_creds.yaml'):
        self.engine = self.init_db_engine()

    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            db_creds = yaml.safe_load(file)
            return db_creds

    def init_db_engine(self):
        engine = create_engine(f"postgresql://{self.read_db_creds()['RDS_USER']}:{self.read_db_creds()['RDS_PASSWORD']}@{self.read_db_creds()['RDS_HOST']}:{self.read_db_creds()['RDS_PORT']}/{self.read_db_creds()['RDS_DATABASE']}")
        engine.execution_options(isolation_level = 'AUTOCOMMIT').connect()
        return engine
        
    def list_db_tables(self):
        inspector = inspect(self.engine) 
        db_tables = inspector.get_table_names()
        return db_tables
    
    def upload_to_db(self, df, table_name):
        df.to_sql(table_name, con=self.engine, if_exists='replace', index=False)
    
RDS_CONNECTOR = DatabaseConnector()




