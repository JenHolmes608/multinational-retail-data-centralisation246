import yaml
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import inspect
from sqlalchemy import text
from pandasgui import show
import numpy as np

class DataCleaning:
    def __init__(self, df):
        self.df = df

    def clean_user_data(self):
        self.clean_legacy_store_details()
        self.clean_legacy_users()
        self.clean_orders_table()

    def clean_legacy_store_details(self):
        df['lat'] = np.nan
        df = df.replace('NULL', np.nan)
        df = df.replace('N/A', np.nan)
        df.drop(df.columns[0], axis=1, inplace=True)

        df['opening_date'] = pd.to_datetime(df['opening_date'], errors = 'coerce', utc = False, format ='mixed').dt.date

        df.dropna(axis=0, how='all', subset=df.columns[1:], inplace=True)
        df = df.dropna(axis=1, how='all')
        df = df.replace('NaT', np.nan)
        df = df.dropna(subset=['opening_date'])

        df['continent'] = df['continent'].replace('eeEurope', 'Europe')
        df['continent'] = df['continent'].replace('eeAmerica', 'America')
        df['staff_numbers'] = df['staff_numbers'].replace('e30', 30)
        pass

    def clean_legacy_users(self):
        df = df.replace('NULL', np.nan)
        df = df.replace('N/A', np.nan)
        df.drop(df.columns[0], axis=1, inplace=True)

        df['date_of_birth'] = pd.to_datetime(df['date_of_birth'], errors = 'coerce', utc = False, format ='mixed').dt.date
        df['join_date'] = pd.to_datetime(df['join_date'], errors = 'coerce', utc = False, format ='mixed').dt.date

        df.dropna(axis=0, how='all', subset=df.columns[1:], inplace=True)
        df = df.dropna(axis=1, how='all')
        df = df.replace('NaT', np.nan)
        df = df.dropna(subset=['date_of_birth'])

        df.drop_duplicates(inplace = True)

        phone_patterns = [ r'^\+\d{1,3}-\d{3}-\d{3}-\d{4}$', r'^\d{3}-\d{3}-\d{4}$', r'^\+49-\d{3}-\d{6,}$', r'^\+44\s?\d{1,5}\s?\d{4}\s?\d{4}$', r'^\+?[0-9()-]{7,}$']
        valid_phone_numbers = df['phone_number'].str.match('|'.join(phone_patterns))
        df.loc[~valid_phone_numbers, 'phone_number'] = np.nan

        valid_email_addresses = df['email_address'].str.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
        df.loc[~valid_email_addresses, 'email_address'] = np.nan
    
    def clean_orders_table(self):