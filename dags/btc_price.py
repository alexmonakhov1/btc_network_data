from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import clickhouse_connect
import requests
from urllib.parse import urlparse

# https://api.coingecko.com/api/v3/coins/bitcoin/history?date=02-01-2025

CH_CLIENT = clickhouse_connect.get_client(
    host='clickhouse',
    port=8123,
    username='admin',
    password='clickhouse123',
    database='btc_network_data'
)

# TODO: переписать код на идемпотентный
def extract_load_btc_price(client, url, date, **kwargs):
    print("url: ", f"{url}{date}")
    response = requests.get(f"{url}{date}")
    if response.status_code == 200:
        btc_price_usd = int(round(response.json()["market_data"]["current_price"]["usd"]))
        source_name = urlparse(url).hostname
        date_clickhouse = kwargs['ds']
        client.query(
            f'CREATE TABLE IF NOT EXISTS src_btc_price (source_name String, date Date, currency_name String, current_price Int64) ENGINE = MergeTree PARTITION BY date ORDER BY (source_name, currency_name, date)')
        client.query(f"INSERT INTO src_btc_price VALUES ('{source_name}', '{date_clickhouse}', 'USD', '{btc_price_usd}')")
    else:
        # TODO: Alert to tg
        print("Failed to get btc_price_usd. Status code:", response.status_code)


with DAG(
    'btc_price',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    end_date=datetime(2025, 1, 10),
    tags=["btc_price"]
) as dag:

    extract_load_btc_price_task = PythonOperator(
        task_id='extract_load_btc_price',
        python_callable=extract_load_btc_price,
        op_kwargs={
            'client': CH_CLIENT,
            'url': 'https://api.coingecko.com/api/v3/coins/bitcoin/history?date=',
            'date': '{{ macros.ds_format(ds, "%Y-%m-%d", "%d-%m-%Y") }}',

        }
    )


extract_load_btc_price_task