import os

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


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
