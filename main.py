import datetime as dt
import os

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

import utils

# Defining path to the database key
key_path = 'gbq.json'

# Defining the credentials object
credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"])

# Creating empty dataframes to store bitcoin and ethereum data (É da logica principal)
bitcoin_df = pd.DataFrame(columns=['id', 'symbol', 'name', 'snapshot_date',
                          'current_price_usd', 'current_price_euro', 'current_price_brl'])
ethereum_df = pd.DataFrame(columns=['id', 'symbol', 'name', 'snapshot_date',
                           'current_price_usd', 'current_price_euro', 'current_price_brl'])

# If table exists, just append the new values for the day, else, create the whole table.
if utils.table_exists():
    # API request to get the bitcoin data
    bitcoin_df = utils.request_api(
        bitcoin_df, 'bitcoin', dt.datetime.today().strftime('%d-%m-%Y'))

    # API request to get the ethereum data
    ethereum_df = utils.request_api(
        ethereum_df, 'ethereum', dt.datetime.today().strftime('%d-%m-%Y'))

    crypto_df = pd.concat([bitcoin_df, ethereum_df], ignore_index=True)
    print(crypto_df)

    # Exporting to big query
    crypto_df.to_gbq(credentials=credentials,
                     destination_table='teste.cryptos', if_exists='append')

else:
    # Creating a list of datetime strings between 1st of january 2021 until today
    date_list = utils.date_list_generator()

    # temp test
    #date_list = ['01-01-2021', '02-01-2021', '03-01-2021', '04-01-2021']

    # API request to get the bitcoin data
    bitcoin_df = utils.request_api(bitcoin_df, 'bitcoin', date_list)

    # API request to get the ethereum data
    ethereum_df = utils.request_api(ethereum_df, 'ethereum', date_list)

    # Concatenating to create the full dataset and printing
    crypto_df = pd.concat([bitcoin_df, ethereum_df], ignore_index=True)
    print(crypto_df)

    # Exporting dataset to csv file
    output_folder = os.path.join(os.getcwd(), 'outputs')
    file_path = os.path.join(output_folder, 'cryptos.csv')
    crypto_df.to_csv(file_path)

    # Exporting to big query
    crypto_df.to_gbq(credentials=credentials,
                     destination_table='teste.cryptos', if_exists='replace')
