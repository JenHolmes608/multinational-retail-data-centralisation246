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
            print(db_creds)
            return db_creds

    def init_db_engine(self):
        engine = create_engine(f"postgresql://{self.read_db_creds()['RDS_USER']}:{self.read_db_creds()['RDS_PASSWORD']}@{self.read_db_creds()['RDS_HOST']}:{self.read_db_creds()['RDS_PORT']}/{self.read_db_creds()['RDS_DATABASE']}")
        engine.execution_options(isolation_level = 'AUTOCOMMIT').connect()
        return engine
        
    def list_db_tables(self):
        inspector = inspect(self.engine) 
        db_tables = inspector.get_table_names()
        return db_tables
    
RDS_CONNECTOR = DatabaseConnector()


RDS_CONNECTOR.init_db_engine()


class DataExtractor:
    def __init__(self, engine):
        self.engine = engine
        
    def reads_rds_table(self, table_name):
        data = pd.read_sql_table(table_name, self.engine)
        df = pd.DataFrame(data)
        return df
        
Display_Data = DataExtractor(DatabaseConnector.engine)

df = Display_Data.reads_rds_table()

df.head()
