from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import clickhouse_connect
import requests
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.exceptions import AirflowException


CH_CLIENT = clickhouse_connect.get_client(
    host='clickhouse',
    port=8123,
    username='admin',
    password='clickhouse123',
    database='btc_network_data'
)

URL = 'https://api.blockchain.info/charts/'

# TODO: Bad response: 'fees-usd-per-transaction'. Разобраться с этим
charts = ['hash-rate', 'miners-revenue', 'transaction-fees-usd', 'transaction-fees',
          'n-transactions-per-block','difficulty']
task_list = list()


# TODO: Ещё раз проверить, правильные ли данные на дату. Смущает data_interval_start.int_timestamp
def extract_load(client, url, chart, start_unix_timestamp, timespan, rolling_average, **kwargs):
    response = requests.get(
        f"{url}{chart}?start={start_unix_timestamp}&timespan={timespan}days&rollingAverage={rolling_average}days&format=json")

    if response.status_code == 200:
        date = datetime.fromtimestamp(response.json()["values"][0]["x"]).date()
        value = round(response.json()["values"][0]["y"])

        client.query(
            f"CREATE TABLE IF NOT EXISTS src_{chart.replace('-', '_')} (date String, {chart.replace('-', '_')} String) ENGINE = MergeTree PARTITION BY date ORDER BY (date)")

        client.query(f"DELETE FROM src_{chart.replace('-', '_')} WHERE date = '{date}'")

        client.query(f"INSERT INTO src_{chart.replace('-', '_')} VALUES ('{date}', '{value}')")
    else:
        raise AirflowException(f"Failed to get {chart}. Status code: {response.status_code}")


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
    'btc_network_data',
    schedule_interval='@daily',
    default_args=default_args,
    start_date=datetime(2025, 10, 10), # TODO: убрать это
    end_date=datetime(2025, 10, 19), # TODO: убрать это
    tags=["btc_network_data"]
) as dag:

    for i in charts:
        task_list.append(
            PythonOperator(
                task_id=f'extract_load_{i.replace("-", "_")}',
                python_callable=extract_load,
                op_kwargs={
                    'client': CH_CLIENT,
                    'url': URL,
                    'chart': f'{i}',
                    'start_unix_timestamp': '{{ data_interval_start.int_timestamp }}',
                    'timespan': '1',
                    'rolling_average': '1'
                }
            )
        )