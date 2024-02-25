import yaml
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import inspect
from sqlachemy import text


class DatabaseConnector:
    def __init__(self, yaml_file_path = 'db_creds.yaml'):
        self.engine = init_db_engine

    def read_db_creds(self):
        with open('db_creds.yaml', 'r') as file:
            db_creds = yaml.safe_load(file)
            print(db_creds)
            return db_creds

    def init_db_engine(self):
        RDS_HOST = 'data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com'
        RDS_PASSWORD = 'AiCore2022'
        RDS_USER = 'aicore_admin'
        RDS_DATABASE = 'postgres'
        RDS_PORT = 5432
        engine = create_engine(f"postgresql://{self.read_db_creds()['RDS_USER']}:{self.read_db_creds()['RDS_PASSWORD']}@{self.read_db_creds()['RDS_HOST']}:{self.read_db_creds()['RDS_PORT']}/{self.read_db_creds()['RDS_DATABASE']}")
        return engine
        
    def list_db_tables(self):
        inspector = inspect(engine) 
        db_tables = inspector.get_table_names()
        return db_tables
    
RDS_CONNECTOR = DatabaseConnector()

RDS_CONNECTOR.read_db_creds()



