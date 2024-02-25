class DataExtractor:
    def __init__(self, engine):
        self.engine = engine
        
    
    def reads_rds_table(self, table_name):
        pd.read_sql("SELECT * FROM table_name", self.engine)
        