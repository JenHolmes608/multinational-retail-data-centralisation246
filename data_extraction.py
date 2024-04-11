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


class DataExtractor:
    def __init__(self, engine):
        '''
        Initialize the data extraction object with the specified database engine.

        Parameters:
        - engine (str): The engine string specifies the database to use for data extraction.

        This method initializes the data extraction object by setting up the database engine, API headers, and base URL.
        The database engine is provided as input, typically created in the 'data_utils.py' file.
        The API headers include an 'x-api-key' for authentication.
        The base URL points to the root endpoint of the API for data retrieval.
        '''
        self.engine = engine
        self.header_dictionary = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
        self.base_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/'
        
    def reads_rds_table(self, table_name):
        '''
        Extract data from a database table using the provided engine and table name.

        Parameters:
        - table_name (str): The name of the table from which to extract data.

        Returns:
        - df (DataFrame): A pandas DataFrame containing the extracted data from the specified table.

        This method reads data from a database table using the SQLAlchemy engine associated with the object.
        It requires the name of the table as input to specify which table to extract data from.
        The extracted data is returned as a pandas DataFrame.
        '''
        data = pd.read_sql_table(table_name, self.engine)
        df = pd.DataFrame(data)
        return df
        
    def retrieve_pdf_data(self, pdf_url):
        '''
        Retrieve data from a PDF file located at the specified URL.

        Parameters:
        - pdf_url (str): The URL pointing to the PDF file.

        Returns:
        - extracted_data (DataFrame): A pandas DataFrame containing the extracted data from the PDF.

        This method extracts tabular data from a PDF file located at the provided URL using the tabula-py library.
        It reads all pages of the PDF and concatenates the extracted DataFrames into a single DataFrame.
        The resulting DataFrame contains the tabular data extracted from the PDF.
        '''
        df_list = tabula.read_pdf(pdf_url, pages='all')
        extracted_data = pd.concat(df_list, ignore_index=True)
        return extracted_data
       
    def list_number_of_stores(self, number_of_stores_endpoint):
        '''
        Retrieve the number of stores from an API endpoint.

        Parameters:
        - number_of_stores_endpoint (str): The URL endpoint to retrieve the number of stores information.

        Returns:
        - number_of_stores (int): The number of stores retrieved from the API endpoint.

        This method sends a GET request to the specified API endpoint to retrieve information about the number of stores.
        It expects the API to respond with JSON data containing a key named 'number_stores', which represents the number of stores.
        The retrieved number of stores is returned as an integer.
        '''
        response = requests.get(number_of_stores_endpoint, headers=self.header_dictionary)
        print(response.json())
        number_of_stores = response.json()['number_stores']
        return number_of_stores
    
    def retrieve_stores_data(self, retrieve_store_endpoint, number_of_stores):
        '''
        Retrieve data for multiple stores from an API endpoint.

        Parameters:
        - retrieve_store_endpoint (str): The URL endpoint to retrieve store data.
        - number_of_stores (int): The total number of stores to retrieve data for.

        Returns:
        - store_df (DataFrame): A pandas DataFrame containing the retrieved data for all stores.

        This method sends multiple GET requests to the specified API endpoint to retrieve data for each store.
        It iterates over each store number from 0 to (number_of_stores - 1) and constructs the endpoint URL accordingly.
        For each store, it sends a GET request with the appropriate store number appended to the endpoint URL.
        The JSON responses from each request are appended to a list to aggregate the data for all stores.
        Finally, the aggregated data is converted into a pandas DataFrame.
        '''
        store_data_list = []
        
        for store_number in range(0, number_of_stores):
            endpoint_url = f"{self.base_url}{retrieve_store_endpoint}/{store_number}"
            response = requests.get(endpoint_url, headers=self.header_dictionary)
            store_data_list.append(response.json())
        
        store_df = pd.DataFrame(store_data_list)
        return store_df
    
    def extract_from_s3(self, s3_address):
        '''
        Extract data from an object stored in Amazon S3.

        Parameters:
        - s3_address (str): The address of the object to extract, in the format 's3://bucket_name/object_key'.

        Returns:
        - df (DataFrame): A pandas DataFrame containing the extracted data.

        This method downloads an object from Amazon S3 specified by the provided address.
        The address should be in the format 's3://bucket_name/object_key'.
        It then reads the downloaded file (assumed to be in CSV format) into a pandas DataFrame.
        The DataFrame containing the extracted data is returned.
        '''
        s3 = boto3.client('s3')
        bucket, key = s3_address.split('//')[1].split('/', 1)
        s3.download_file(bucket, key, 'products.csv')
        df = pd.read_csv('products.csv')
        return df
    
    def extract_date_events_data(self, date_events_url):
        '''
        Extract data from a JSON file located at the provided URL.

        Parameters:
        - date_events_url (str): The URL pointing to the JSON file containing date events data.

        Returns:
        - df (DataFrame): A pandas DataFrame containing the extracted data.

        This method reads data from a JSON file located at the specified URL.
        The JSON data is assumed to be in a format that can be directly read into a pandas DataFrame using `pd.read_json()`.
        The DataFrame containing the extracted data is returned.
        '''
        df = pd.read_json(date_events_url)
        return df