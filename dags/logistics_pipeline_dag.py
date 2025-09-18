import os
import pandas as pd
import boto3
import tempfile
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime, timedelta

# Configurations
MINIO_BUCKET = "logistics-data"
BACKFILL_PREFIX = "backfill/"
INCREMENTAL_PREFIX = "incremental/"
AWS_CONN_ID = "minio_conn"
POSTGRES_CONN_ID = "postgres_conn"

# Get DATA_YEAR dynamically to avoid parsing-time issues
def get_data_year():
    return Variable.get("DATA_YEAR", default_var=str(datetime.now().year))

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Utility Functions
def get_s3_client():
    conn = BaseHook.get_connection(AWS_CONN_ID)
    return boto3.client(
        "s3",
        endpoint_url=conn.extra_dejson.get("endpoint_url"),
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )

def list_files_in_prefix(prefix: str, year: str = None, **context):
    """List all files under a given prefix (optionally filtered by year)."""
    s3 = get_s3_client()
    target_prefix = f"{prefix}{year}/" if year else prefix
    response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=target_prefix)

    if "Contents" not in response:
        raise ValueError(f"No files found under {target_prefix}")

    file_keys = [obj["Key"] for obj in response["Contents"]]
    context["ti"].xcom_push(key="file_keys", value=file_keys)
    print(f"Found files: {file_keys}")

def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    # Required schema for logistics dataset
    required_cols = [
        "Timestamp", "Asset_ID", "Latitude", "Longitude", "Inventory_Level",
        "Shipment_Status", "Temperature", "Humidity", "Traffic_Status",
        "Waiting_Time", "User_Transaction_Amount", "User_Purchase_Frequency",
        "Logistics_Delay_Reason", "Asset_Utilization", "Demand_Forecast",
        "Logistics_Delay"
    ]

    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Business rules
    if not df["Asset_Utilization"].between(0, 100).all():
        raise ValueError("Asset_Utilization must be between 0 and 100.")

    if (df["Inventory_Level"] < 0).any():
        raise ValueError("Inventory_Level cannot be negative.")

    if (df["User_Transaction_Amount"] < 0).any():
        raise ValueError("User_Transaction_Amount cannot be negative.")

    if (df["User_Purchase_Frequency"] < 0).any():
        raise ValueError("User_Purchase_Frequency cannot be negative.")

    # Drop rows with nulls in critical columns
    df = df.dropna(subset=["Timestamp", "Asset_ID", "Asset_Utilization"])

    return df


def download_and_process(**context):
    s3 = get_s3_client()
    file_keys = context["ti"].xcom_pull(key="file_keys", task_ids=context["params"]["list_task"])

    transformed_files = []
    for key in file_keys:
        tmp_dir = tempfile.gettempdir()
        local_path = os.path.join(tmp_dir, os.path.basename(key))
        s3.download_file(MINIO_BUCKET, key, local_path)

        if key.endswith(".csv"):
            df = pd.read_csv(local_path)
        elif key.endswith(".json"):
            df = pd.read_json(local_path, lines=True)
        elif key.endswith(".parquet"):
            df = pd.read_parquet(local_path)
        else:
            raise ValueError(f"Unsupported file format: {key}")

        df = validate_data(df)
        df.columns = [c.strip().lower() for c in df.columns]

        clean_path = local_path.rsplit(".", 1)[0] + "_clean.parquet"
        df.to_parquet(clean_path, index=False)
        transformed_files.append(clean_path)

        print(f"Processed + saved {clean_path}")

    context["ti"].xcom_push(key="transformed_files", value=transformed_files)

def load_to_postgres(**context):
    transformed_files = context["ti"].xcom_pull(
        key="transformed_files", task_ids=context["params"]["process_task"]
    )
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    for file in transformed_files:
        df = pd.read_parquet(file)
        df.to_sql("logistics_data", engine, if_exists="append", index=False)
        print(f"Loaded {file} into PostgreSQL table 'logistics_data'")

# Backfill DAG (param-driven)
with DAG(
    dag_id="logistics_backfill_pipeline",
    default_args=default_args,
    description="ETL pipeline for backfill data from MinIO to PostgreSQL",
    schedule_interval=None,  # run manually
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["logistics", "minio", "postgres", "etl", "backfill"],
) as backfill_dag:

    list_backfill_files = PythonOperator(
        task_id="list_backfill_files",
        python_callable=list_files_in_prefix,
        op_kwargs={"prefix": BACKFILL_PREFIX, "year": get_data_year()},
        dag=backfill_dag,
    )

    download_backfill = PythonOperator(
        task_id="download_and_process_backfill",
        python_callable=download_and_process,
        params={"list_task": "list_backfill_files", "process_task": "download_and_process_backfill"},
        dag=backfill_dag,
    )

    load_backfill = PythonOperator(
        task_id="load_to_postgres_backfill",
        python_callable=load_to_postgres,
        params={"process_task": "download_and_process_backfill"},
        dag=backfill_dag,
    )

    list_backfill_files >> download_backfill >> load_backfill

# Incremental DAG (auto-current)
with DAG(
    dag_id="logistics_incremental_pipeline",
    default_args=default_args,
    description="ETL pipeline for daily incremental data",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["logistics", "minio", "postgres", "etl", "incremental"],
) as incremental_dag:

    wait_for_incremental_file = S3KeySensor(
        task_id="wait_for_file",
        bucket_name=MINIO_BUCKET,
        bucket_key=lambda: INCREMENTAL_PREFIX + f"{get_data_year()}/*",  # Dynamic bucket_key
        aws_conn_id=AWS_CONN_ID,
        wildcard_match=True,
        poke_interval=60,
        timeout=60 * 30,
        mode="poke",
        dag=incremental_dag,
    )

    list_incremental_files = PythonOperator(
        task_id="list_incremental_files",
        python_callable=list_files_in_prefix,
        op_kwargs={"prefix": INCREMENTAL_PREFIX, "year": get_data_year()},
        dag=incremental_dag,
    )

    download_incremental = PythonOperator(
        task_id="download_and_process_incremental",
        python_callable=download_and_process,
        params={"list_task": "list_incremental_files", "process_task": "download_and_process_incremental"},
        dag=incremental_dag,
    )

    load_incremental = PythonOperator(
        task_id="load_to_postgres_incremental",
        python_callable=load_to_postgres,
        params={"process_task": "download_and_process_incremental"},
        dag=incremental_dag,
    )

    wait_for_incremental_file >> list_incremental_files >> download_incremental >> load_incremental