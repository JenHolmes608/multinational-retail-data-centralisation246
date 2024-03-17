import pandas as pd

class DataExtractor:
    def __init__(self, engine):
        self.engine = engine
        
    
    def reads_rds_table(self, table_name):
        data = pd.read_sql_table(table_name, self.engine)
        df = pd.DataFrame(data)
        return df
        