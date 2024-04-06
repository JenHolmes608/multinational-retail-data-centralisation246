import pandas as pd
import tabula

class DataExtractor:
    def __init__(self, engine):
        self.engine = engine
        
    def reads_rds_table(self, table_name):
        data = pd.read_sql_table(table_name, self.engine)
        df = pd.DataFrame(data)
        return df
        
    def retrieve_pdf_data(self, https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf):
        df_list = tabula.read_pdf(https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf, pages='all')
        extracted_data = pd.concat(df_list, ignore_index=True)
        return extracted_data
       