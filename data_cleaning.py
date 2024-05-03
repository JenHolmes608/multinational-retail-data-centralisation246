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

class DataCleaning:
    def __init__(self, df):
        '''
        Initialize the DataCleaning object with the provided DataFrame.

        Parameters:
        - df (DataFrame): The pandas DataFrame containing the data to be cleaned.

        This method initializes the DataCleaning object with the provided DataFrame.
        '''
        self.df = df

    def clean_user_data(self):
        '''
        Clean and preprocess user data.

        Returns:
        - df (DataFrame): A pandas DataFrame containing the cleaned user data.

        This method performs cleaning and preprocessing tasks specific to user data.
        '''
        
        self.clean_legacy_users()
        self.clean_orders_table()
        self.clean_card_data()
        self.clean_store_data()
        self.convert_product_weights()
        self.clean_products_data()
        self.clean_date_events_data()
        

    def clean_legacy_users(self):
        '''
        Clean and preprocess user data.

        Returns:
        - df (DataFrame): A pandas DataFrame containing the cleaned user data.

        This method performs cleaning and preprocessing tasks specific to user data.
        '''
        
        self.df = self.df.replace('NULL', np.nan)
        self.df = self.df.replace('N/A', np.nan)
        self.df.drop(self.df.columns[0], axis=1, inplace=True)

        self.df['date_of_birth'] = pd.to_datetime(self.df['date_of_birth'], errors='coerce', utc=False, format='mixed').dt.date
        self.df['join_date'] = pd.to_datetime(self.df['join_date'], errors='coerce', utc=False, format='mixed').dt.date

        self.df.dropna(axis=0, how='all', subset=self.df.columns[1:], inplace=True)
        self.df = self.df.dropna(axis=1, how='all')
        self.df = self.df.replace('NaT', np.nan)
        self.df = self.df.dropna(subset=['date_of_birth'])

        self.df.drop_duplicates(inplace=True)

        phone_patterns = [r'^\+\d{1,3}-\d{3}-\d{3}-\d{4}$', r'^\d{3}-\d{3}-\d{4}$', r'^\+49-\d{3}-\d{6,}$',
                          r'^\+44\s?\d{1,5}\s?\d{4}\s?\d{4}$', r'^\+?[0-9()-]{7,}$']
        valid_phone_numbers = self.df['phone_number'].str.match('|'.join(phone_patterns))
        self.df.loc[~valid_phone_numbers, 'phone_number'] = np.nan

        valid_email_addresses = self.df['email_address'].str.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        self.df.loc[~valid_email_addresses, 'email_address'] = np.nan

        return self.df
    
    def clean_orders_data(self):
        '''
        Clean and preprocess orders data.

        Returns:
        - df (DataFrame): A pandas DataFrame containing the cleaned orders data.

        This method performs cleaning and preprocessing tasks specific to orders data.
        '''
        self.df.drop(columns=['first_name', 'last_name', '1'], inplace=True)

        self.df = self.df.replace('NULL', np.nan)
        self.df = self.df.replace('N/A', np.nan)
        self.df.drop(self.df.columns[0], axis=1, inplace=True)

        self.df.dropna(axis=0, how='all', subset=self.df.columns[1:], inplace=True)
        self.df = self.df.dropna(axis=1, how='all')

        valid_card_numbers = self.df['card_number'].astype(str).apply(len).between(11, 19)
        self.df['card_number'] = np.where(valid_card_numbers, self.df['card_number'].astype(str), np.nan)

        return self.df
    
    def clean_card_data(self):
        '''
        Clean and preprocess card data.

        Returns:
        - df (DataFrame): A pandas DataFrame containing the cleaned card data.

        This method performs cleaning and preprocessing tasks specific to card data.
        '''
        self.df = self.df.replace('NULL', np.nan)
        self.df = self.df.replace('N/A', np.nan)

        self.df['date_payment_confirmed'] = pd.to_datetime(self.df['date_payment_confirmed'], errors='coerce', utc=False, format='mixed').dt.date

        self.df.dropna(axis=0, how='all', subset=self.df.columns[1:], inplace=True)
        self.df = self.df.dropna(axis=1, how='all')
        self.df = self.df.replace('NaT', np.nan)
        self.df = self.df.dropna(subset=['date_payment_confirmed'])

        self.df.drop_duplicates(inplace=True)

        valid_card_numbers = self.df['card_number'].astype(str).apply(len).between(11, 19)
        self.df['card_number'] = np.where(valid_card_numbers, self.df['card_number'].astype(str), np.nan)

        return self.df
    
    def clean_store_data(self):
        '''
        Clean and preprocess store data.

        Returns:
        - df (DataFrame): A pandas DataFrame containing the cleaned store data.

        This method performs cleaning and preprocessing tasks specific to store data.
        '''
        self.df['lat'] = np.nan
        self.df = self.df.replace('NULL', np.nan)
        self.df = self.df.replace('N/A', np.nan)
        self.df.drop(self.df.columns[0], axis=1, inplace=True)

        self.df['opening_date'] = pd.to_datetime(self.df['opening_date'], errors='coerce', utc=False, format='mixed').dt.date

        self.df.dropna(axis=0, how='all', subset=self.df.columns[1:], inplace=True)
        self.df = self.df.dropna(axis=1, how='all')
        self.df = self.df.replace('NaT', np.nan)
        self.df = self.df.dropna(subset=['opening_date'])

        self.df['continent'] = self.df['continent'].replace('eeEurope', 'Europe')
        self.df['continent'] = self.df['continent'].replace('eeAmerica', 'America')
        self.df['staff_numbers'] = self.df['staff_numbers'].replace('e30', 30)

        return self.df
    
   
    def convert_product_weights(self):
        '''
        Convert product weights to kilograms.

        Returns:
        - df (DataFrame): A pandas DataFrame containing the converted product weights.

        This method converts product weights from various units to kilograms.
        '''
        def clean_and_convert(weight_str):
            weight_str = str(weight_str)
            weight_str = weight_str.replace('g', '').replace('ml', '').replace(' ', '').replace('k', '').replace('oz', '')
            parts = weight_str.split('x')
            total_weight = 0

            for part in parts:
                part = part.strip()
                if part.endswith('g'):
                    weight = float(part[:-1]) / 1000 
                elif part.endswith('ml'):
                    weight = float(part[:-2]) / 1000 
                else:
                    weight = float(part) 
                total_weight += weight

            return total_weight

        self.df['weight_in_kg'] = self.df['weight'].apply(clean_and_convert)
        return self.df
    
    def clean_products_data(self):
        '''
        Clean and preprocess products data.

        Returns:
        - df (DataFrame): A pandas DataFrame containing the cleaned products data.

        This method performs cleaning and preprocessing tasks specific to products data.
        '''
        self.df = self.df.replace('NULL', np.nan)
        self.df = self.df.replace('N/A', np.nan)
        self.df.drop(self.df.columns[0], axis=1, inplace=True)

        self.df['date_added'] = pd.to_datetime(self.df['date_added'], errors='coerce', utc=False, format='mixed').dt.date

        self.df.dropna(axis=0, how='all', subset=self.df.columns[1:], inplace=True)
        self.df = self.df.dropna(axis=1, how='all')
        self.df = self.df.replace('NaT', np.nan)
        self.df = self.df.dropna(subset=['date_added'])

        return self.df
    
    def clean_date_events_data(self):
        '''
        Clean and preprocess date events data.

        Returns:
        - df (DataFrame): A pandas DataFrame containing the cleaned date events data.

        This method performs cleaning and preprocessing tasks specific to date events data.
        '''
        self.df = self.df.replace('NULL', np.nan)
        self.df = self.df.replace('N/A', np.nan)

        self.df['day'] = self.df['day'].mask(pd.to_numeric(self.df['day'], errors='coerce').isna())

        self.df.dropna(axis=0, how='all', subset=self.df.columns[1:], inplace=True)
        self.df = self.df.dropna(axis=1, how='all')
        self.df = self.df.dropna(subset=['day'])
        
        return self.df