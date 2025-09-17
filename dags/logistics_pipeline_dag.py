import os
import pandas as pd
import boto3
import tempfile
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

# Configurations
MINIO_BUCKET = "logistics-data"
PREFIX = "smart_logistics_"
AWS_CONN_ID = "minio_conn"
POSTGRES_CONN_ID = "postgres_conn"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

def download_from_minio(**context):
    """Download the latest file from MinIO to local tmp dir."""
    conn = BaseHook.get_connection(AWS_CONN_ID)
    s3 = boto3.client(
        "s3",
        endpoint_url=conn.extra_dejson.get("endpoint_url"),
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )

    # List objects with prefix
    response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=PREFIX)
    if "Contents" not in response:
        raise ValueError("No matching file found in MinIO bucket.")

    # Pick the latest file
    latest_file = max(response["Contents"], key=lambda x: x["LastModified"])["Key"]

    # Download locally
    tmp_dir = tempfile.gettempdir()
    local_path = os.path.join(tmp_dir, os.path.basename(latest_file))
    s3.download_file(MINIO_BUCKET, latest_file, local_path)

    # Push path to XCom
    context["ti"].xcom_push(key="local_file", value=local_path)
    print(f"Downloaded {latest_file} to {local_path}")

def transform_data(**context):
    """Transform CSV data with pandas."""
    local_path = context["ti"].xcom_pull(key="local_file", task_ids="download_from_minio")
    df = pd.read_csv(local_path)

    # Example transformations
    df = df.dropna()
    df.columns = [c.strip().lower() for c in df.columns]

    # Save transformed file
    transformed_path = local_path.replace(".csv", "_clean.csv")
    df.to_csv(transformed_path, index=False)

    context["ti"].xcom_push(key="transformed_file", value=transformed_path)
    print(f"Transformed file saved at {transformed_path}")

def load_to_postgres(**context):
    """Load transformed CSV into PostgreSQL table."""
    transformed_path = context["ti"].xcom_pull(key="transformed_file", task_ids="transform_data")
    df = pd.read_csv(transformed_path)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    df.to_sql("logistics_data", engine, if_exists="replace", index=False)
    print("Data loaded into PostgreSQL table 'logistics_data'")

# DAG Definition
dag = DAG(
    dag_id="logistics_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline: MinIO to Transform to PostgreSQL",
    schedule_interval="@hourly",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["logistics", "minio", "postgres", "etl"],
)

wait_for_file = S3KeySensor(
    task_id="wait_for_file",
    bucket_name=MINIO_BUCKET,
    bucket_key="smart_logistics_*",
    aws_conn_id=AWS_CONN_ID,
    wildcard_match=True,
    poke_interval=30,
    timeout=60 * 60,
    mode="poke",
    dag=dag,
)

download_task = PythonOperator(
    task_id="download_from_minio",
    python_callable=download_from_minio,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_to_postgres",
    python_callable=load_to_postgres,
    dag=dag,
)

# Set dependencies using set_downstream to avoid Unicode issues
wait_for_file.set_downstream(download_task)
download_task.set_downstream(transform_task)
transform_task.set_downstream(load_task)