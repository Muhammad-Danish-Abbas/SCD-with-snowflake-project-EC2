"""
nifi_replacement_dag.py

Replaces the NiFi ListFile -> FetchFile -> PutS3Object flow from the original project.

Task 1: Generate synthetic customer records with Faker -> upload CSV to S3
Task 2: Fetch live TSLA stock data via yfinance -> upload CSV to S3
"""

from datetime import datetime, timedelta, timezone
import csv
import os

import boto3
import yfinance as yf
from airflow import DAG
from airflow.operators.python import PythonOperator
from faker import Faker

S3_BUCKET = os.environ.get("S3_BUCKET_NAME", "danish-airflow-snowflake-2026")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")
LOCAL_DATA_DIR = os.environ.get("SCD_LOCAL_DATA_DIR", "/tmp/FakeDataset")
RECORD_COUNT = int(os.environ.get("SCD_RECORD_COUNT", "1000"))

FIELDNAMES = [
    "customer_id", "first_name", "last_name", "email",
    "street", "city", "state", "country",
]

default_args = {
    "owner": "Muhammad Danish",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def generate_and_upload_customers(**context):
    fake = Faker()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    file_path = os.path.join(LOCAL_DATA_DIR, f"customer_{timestamp}.csv")

    with open(file_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writeheader()
        for customer_id in range(RECORD_COUNT):
            writer.writerow({
                "customer_id": customer_id,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.email(),
                "street": fake.street_address(),
                "city": fake.city(),
                "state": fake.state(),
                "country": fake.country(),
            })

    s3_key = f"customer_data/{os.path.basename(file_path)}"
    boto3.client("s3", region_name=AWS_REGION).upload_file(file_path, S3_BUCKET, s3_key)
    print(f"Uploaded {file_path} -> s3://{S3_BUCKET}/{s3_key}")
    os.remove(file_path)


def fetch_and_upload_tesla_stock(**context):
    ds = context["ds"]
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    ticker = yf.Ticker("TSLA")
    df = ticker.history(period="5d")
    df.reset_index(inplace=True)
    local_path = f"/tmp/TSLA_{ds}_{timestamp}.csv"
    df.to_csv(local_path, index=False)
    s3_key = f"tesla_stock/TSLA_{ds}_{timestamp}.csv"
    boto3.client("s3", region_name=AWS_REGION).upload_file(local_path, S3_BUCKET, s3_key)
    print(f"Uploaded {local_path} -> s3://{S3_BUCKET}/{s3_key}")
    os.remove(local_path)


with DAG(
    dag_id="nifi_replacement_fetch_to_s3",
    description="Replaces NiFi: generates customer data (Faker) + fetches Tesla stock data, uploads both to S3",
    default_args=default_args,
    start_date=datetime(2026, 8, 1, tzinfo=timezone.utc),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["snowflake", "s3", "nifi-replacement"],
) as dag:

    generate_customers_task = PythonOperator(
        task_id="generate_and_upload_customers",
        python_callable=generate_and_upload_customers,
    )

    fetch_tesla_task = PythonOperator(
        task_id="fetch_and_upload_tesla_stock",
        python_callable=fetch_and_upload_tesla_stock,
    )

    generate_customers_task >> fetch_tesla_task
