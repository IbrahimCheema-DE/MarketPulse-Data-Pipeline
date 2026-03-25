from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import sys

default_args = {
    'owner': 'ibrahim',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 25),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'marketpulse_pipeline',
    default_args=default_args,
    description='MarketPulse Stock Data Pipeline',
    schedule_interval='0 9 * * 1-5',
    catchup=False,
)


def run_fetch_stock_data():
    result = subprocess.run(
        [sys.executable, '/opt/airflow/scripts/fetch_stock_data.py'],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"fetch_stock_data failed:\n{result.stderr}")


def run_load_to_mysql():
    result = subprocess.run(
        [sys.executable, '/opt/airflow/scripts/load_to_mysql.py'],
        capture_output=True,
        text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"load_to_mysql failed:\n{result.stderr}")


def send_success_notification():
    print("=" * 50)
    print(" MarketPulse Pipeline Complete!")
    print(f" Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(" Stocks: AAPL, GOOGL, AMZN, MSFT")
    print(" Data saved to S3 and MySQL")
    print("=" * 50)


task_fetch = PythonOperator(
    task_id='fetch_stock_data',
    python_callable=run_fetch_stock_data,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load_to_mysql',
    python_callable=run_load_to_mysql,
    dag=dag,
)

task_notify = PythonOperator(
    task_id='send_notification',
    python_callable=send_success_notification,
    dag=dag,
)

task_fetch >> task_load >> task_notify