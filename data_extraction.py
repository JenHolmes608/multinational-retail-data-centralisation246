import yaml
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import inspect
from sqlalchemy import text
from pandasgui import show
import numpy as np 
import tabula
import requests
import boto3

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
        print(response.json())
        number_of_stores = response.json()['number_stores']
        return number_of_stores
    
    def retrieve_stores_data(self, retrieve_store_endpoint, number_of_stores):
        store_data_list = []
        for store_number in range(0, number_of_stores):
            endpoint_url = f"{self.base_url}{retrieve_store_endpoint}/{store_number}"
            response = requests.get(endpoint_url, headers=self.header_dictionary)
            store_data_list.append(response.json())
        
        store_df = pd.DataFrame(store_data_list)
        return store_df
    
    def extract_from_s3(self, s3_address):
        s3 = boto3.client('s3')
        bucket, key = s3_address.split('//')[1].split('/', 1)
        s3.download_file(bucket, key, 'products.csv')
        df = pd.read_csv('products.csv')
        return df
    
    def extract_date_events_data(self, date_events_url):
        df = pd.read_json(date_events_url)
        return df