from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import clickhouse_connect
import requests
from urllib.parse import urlparse
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.exceptions import AirflowException


CH_CLIENT = clickhouse_connect.get_client(
    host='clickhouse',
    port=8123,
    username='admin',
    password='clickhouse123',
    database='btc_network_data'
)


def extract_load_btc_price(client, url, date, **kwargs):
    response = requests.get(f"{url}{date}")
    if response.status_code == 200:
        btc_price_usd = int(round(response.json()["market_data"]["current_price"]["usd"]))
        source_name = urlparse(url).hostname
        date_clickhouse = kwargs['ds']

        client.query(
            f'CREATE TABLE IF NOT EXISTS src_btc_price (source_name String, date Date, currency_name String, current_price Int64) ENGINE = MergeTree PARTITION BY date ORDER BY (source_name, currency_name, date)')

        client.query(f"DELETE FROM src_btc_price WHERE date = '{date_clickhouse}'")

        client.query(f"INSERT INTO src_btc_price VALUES ('{source_name}', '{date_clickhouse}', 'USD', '{btc_price_usd}')")
    else:
        raise AirflowException(f"Failed to get btc_price_usd. Status code: {response.status_code}")


def on_failure_callback(context):
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    run_id = context.get("run_id")
    exception = context.get("exception")

    message = f"""
Airflow task failed 😱

DAG: {dag_id}
Task: {task_id}
Execution date: {execution_date}
Run ID: {run_id}
Exception:{exception}
    """.strip()

    send_message = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='telegram_id',
        chat_id='-1003370928329',
        text=message,
        dag=dag)
    return send_message.execute(context=context)


default_args = {
    'on_failure_callback': on_failure_callback
}

with DAG(
    'btc_price',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2025, 10, 10), # TODO: убрать это
    end_date=datetime(2025, 10, 19), # TODO: убрать это
    tags=["btc_price"]
) as dag:

    # TODO: может переписать под сенсор?
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