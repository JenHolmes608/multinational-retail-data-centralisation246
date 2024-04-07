import pandas as pd
import tabula
import requests

class DataExtractor:
    def __init__(self, engine):
        self.engine = engine
        self.header_dictionary = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        self.base_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/'
        
    def reads_rds_table(self, table_name):
        data = pd.read_sql_table(table_name, self.engine)
        df = pd.DataFrame(data)
        return df
        
    def retrieve_pdf_data(self, pdf_url):
        df_list = tabula.read_pdf(pdf_url, pages='all')
        extracted_data = pd.concat(df_list, ignore_index=True)
        return extracted_data
       
    def list_number_of_stores(self, number_of_stores_endpoint):
        response = requests.get(number_of_stores_endpoint, headers=self.header_dictionary)
        number_of_stores = response.json()['number_of_stores']
        return number_of_stores
    
    def retrieve_stores_data(self, retrieve_store_endpoint, number_of_stores):
        store_data_list = []
        for store_number in range(1, number_of_stores + 1):
            endpoint_url = f"{self.base_url}{retrieve_store_endpoint}/{store_number}"
            response = requests.get(endpoint_url, headers=self.header_dictionary)
        
        store_df = pd.DataFrame(store_data_list)
        return store_df
    
pdf_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
retrieve_store_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
