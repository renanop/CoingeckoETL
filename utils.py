import datetime as dt
import json
import os
import sys
import time as t

import requests
from google.cloud import bigquery
from google.cloud.exceptions import NotFound


def date_list_generator(start_date='01-01-2021', end_date=dt.datetime.today().strftime('%d-%m-%Y')):
    """This is a function that generates a list of date strings in the format %d-%m-%Y between a starting date
    and a ending date

    Args:
        start_date (str, optional): The day of the starting date. Defaults to '01-01-2021'.
        end_date (str, optional): The day of the ending date. Defaults to dt.datetime.today().strftime('%d-%m-%Y'),
        which gets the string for the current date.

    Returns:
        date_list (list): A list of strings in the format %d-%m-%Y
    """
    # Creating a list of datetime strings between 1st of january 2021 until today
    start_date = dt.datetime.strptime(start_date, '%d-%m-%Y')
    end_date = dt.datetime.strptime(end_date, '%d-%m-%Y')
    step = dt.timedelta(days=1)
    date_list = list()

    while start_date <= end_date:
        date_list.append(start_date.strftime('%d-%m-%Y'))
        start_date += step

    return date_list


def request_api(df, id, dates):
    """This is a function for making requests to the CoinGecko API for a chosen cryptocurrency and for a date range, in order to 
    populate a DataFrame.

    Args:
        df (Pandas DataFrame): An empty DataFrame that contains the following columns: ['id', 'symbol', 'name', 'snapshot_date', 'current_price_usd', 'current_price_euro', 'current_price_brl']
        id (str): The string for a cryptocurrency ID in the CoinGecko API. For more information, go to: https://www.coingecko.com/pt/api/documentation
        dates (list or str): If the desired behavior is to make multiple requests for a date range, it is a list of dates in the %d-%m-%Y format. Otherwise, 
                             if the desired behavior is to make a request for a single date, this is a string in the %d-%m-%Y format.

    Returns:
        [Pandas DataFrame]: Populated DataFrame, with data from the API.
    """
    def snapshot_generator(response_json, date):
        """Quick nested function for generating the snapshot for the day for one cryptocurrency

        Args:
            response_json (str): String for the JSON that comes from the API request.
            date (str): String for the day of the request in the %d-%m-%Y format.

        Returns:
            [type]: [description]
        """
        snapshot = {
            'id': response_json['id'],
            'symbol': response_json['symbol'],
            'name': response_json['name'],
            'snapshot_date': date,
            'current_price_usd': response_json['market_data']['current_price']['usd'],
            'current_price_euro': response_json['market_data']['current_price']['eur'],
            'current_price_brl': response_json['market_data']['current_price']['brl']
        }
        return snapshot
        
    # Making requests iterating the date list.
    if isinstance(dates, list):
        for date in dates:
            try:
                response = requests.get(
                    f"https://api.coingecko.com/api/v3/coins/{id}/history?date={date}&localization=false").text
                response = json.loads(response)
            except Exception as e:
                print(f"Could not make the request: {str(e)}")
                sys.exit()

            # Calling the snapshot_generator function in order to generate today's entry in the database
            snapshot = snapshot_generator(response, date)
            print(snapshot)

            # Trying to append to the current df
            try:
                df = df.append(snapshot, ignore_index=True)
            except Exception as e:
                print(f"Could not append new row to DataFrame: {str(e)}")
                sys.exit()

            # Free API only supports 50 requests/minute, so we wait a bit between each request
            t.sleep(2)

    elif isinstance(dates, str):
        try:
            response = requests.get(
                f"https://api.coingecko.com/api/v3/coins/{id}/history?date={dates}&localization=false").text
            response = json.loads(response)
        except Exception as e:
            print(f"Could not make the request: {str(e)}")
            sys.exit()
            
        # Calling the snapshot_generator function in order to generate today's entry in the database
        snapshot = snapshot_generator(response, dates)
        print(snapshot)

        try:
            df = df.append(snapshot, ignore_index=True)
        except Exception as e:
            print(f"Could not append new row to DataFrame: {str(e)}")
            sys.exit()

    else:
        print('A not supported format was passed to the dates variable. Please review the data and try again. Exiting...')
        sys.exit()

    return df


def table_exists():
    # Defining path to the database key
    key_path = 'gbq.json'
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\renan\\Desktop\\coingecko\\gbq.json"

    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to determine existence.
    table_id = "phrasal-chiller-323718.teste.cryptos"

    try:
        client.get_table(table_id)  # Make an API request.
        print("Table {} already exists.".format(table_id))
        return True
    except NotFound:
        print("Table {} is not found.".format(table_id))
        return False
