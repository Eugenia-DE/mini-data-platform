import os
import pandas as pd
import boto3
import tempfile
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# UTF-8 safeguard
os.environ["PYTHONIOENCODING"] = "UTF-8"

# Config
MINIO_BUCKET = "logistics-data"
INCREMENTAL_PREFIX = "batch/"
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

# Utilities
def get_s3_client():
    conn = BaseHook.get_connection(AWS_CONN_ID)
    return boto3.client(
        "s3",
        endpoint_url=conn.extra_dejson.get("endpoint_url"),
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
    )

def list_files_in_prefix(prefix: str, year: str, date: str, **context):
    s3 = get_s3_client()
    target_prefix = f"{prefix}{year}/{date}/"
    response = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=target_prefix)

    if "Contents" not in response:
        logger.warning(f"No files under {target_prefix}")
        context["ti"].xcom_push(key="file_keys", value=[])
        return

    file_keys = [obj["Key"] for obj in response["Contents"]]
    context["ti"].xcom_push(key="file_keys", value=file_keys)
    logger.info(f"Found files: {file_keys}")

# Validation + Transformation
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    required_cols = [
        "Timestamp", "Asset_ID", "State", "Route_Distance_km",
        "Shipment_Status", "Planned_Delivery_Hours", "Actual_Delivery_Hours",
        "Delay_Flag", "Delay_Reason", "Traffic_Status", "Weather_Event",
        "Temperature", "Humidity", "Asset_Utilization",
        "Inventory_Level", "Demand_Forecast"
    ]
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    if not df["Asset_Utilization"].between(0, 100).all():
        raise ValueError("Asset_Utilization must be between 0 and 100")
    if (df["Inventory_Level"] < 0).any():
        raise ValueError("Inventory_Level cannot be negative")
    if (df["Planned_Delivery_Hours"] <= 0).any():
        raise ValueError("Planned_Delivery_Hours must be positive")
    if (df["Actual_Delivery_Hours"] <= 0).any():
        raise ValueError("Actual_Delivery_Hours must be positive")

    df = df.dropna(subset=["Timestamp", "Asset_ID", "Shipment_Status"])
    if df.empty:
        raise ValueError("DataFrame is empty after validation")
    return df

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])
    df["delivery_delay_hours"] = df["Actual_Delivery_Hours"] - df["Planned_Delivery_Hours"]
    df["is_underutilized"] = df["Asset_Utilization"] < 70
    df["is_overutilized"] = df["Asset_Utilization"] > 90
    df["delay_category"] = df.apply(
        lambda row: "On-Time" if row["Delay_Flag"] == 0 else row["Delay_Reason"],
        axis=1,
    )
    return df

# Idempotency helpers
def is_file_processed(pg_hook, file_key: str) -> bool:
    sql = "SELECT 1 FROM processed_files WHERE file_key = %s LIMIT 1"
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (file_key,))
            return cur.fetchone() is not None

def mark_file_processed(pg_hook, file_key: str):
    sql = "INSERT INTO processed_files(file_key, processed_at) VALUES (%s, NOW())"
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (file_key,))
        conn.commit()

# Processing + Loading
def download_and_process(**context):
    s3 = get_s3_client()
    file_keys = context["ti"].xcom_pull(key="file_keys", task_ids="list_files")
    if not file_keys:
        logger.warning("No files to process this run")
        return

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    transformed_files = []

    for key in file_keys:
        if is_file_processed(pg_hook, key):
            logger.info(f"Skipping already processed file: {key}")
            continue

        tmp_dir = tempfile.gettempdir()
        local_path = os.path.join(tmp_dir, os.path.basename(key))
        s3.download_file(MINIO_BUCKET, key, local_path)

        if key.endswith(".csv"):
            df = pd.read_csv(local_path, encoding="utf-8")
        elif key.endswith(".json"):
            df = pd.read_json(local_path, lines=True, encoding="utf-8")
        elif key.endswith(".parquet"):
            df = pd.read_parquet(local_path)
        else:
            raise ValueError(f"Unsupported file format: {key}")

        df = validate_data(df)
        df = transform_data(df)
        df.columns = [c.strip().lower() for c in df.columns]

        clean_path = local_path.rsplit(".", 1)[0] + "_clean.parquet"
        df.to_parquet(clean_path, index=False)
        transformed_files.append((clean_path, key))
        os.remove(local_path)

    context["ti"].xcom_push(key="transformed_files", value=transformed_files)

def load_to_postgres(**context):
    transformed_files = context["ti"].xcom_pull(key="transformed_files", task_ids="process_files")
    if not transformed_files:
        logger.warning("No transformed files to load")
        return

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = pg_hook.get_sqlalchemy_engine()

    for file, file_key in transformed_files:
        df = pd.read_parquet(file)
        if df.empty:
            logger.warning(f"Skipping empty file: {file}")
            continue

        df = df.drop_duplicates(subset=["timestamp", "asset_id"])
        df.to_sql("logistics_data", engine, if_exists="append", index=False)
        logger.info(f"Loaded {file} into PostgreSQL")

        mark_file_processed(pg_hook, file_key)
        os.remove(file)

# DAG
with DAG(
    dag_id="logistics_pipeline",
    default_args=default_args,
    description="ETL pipeline for logistics data with staggered partial file uploads",
    schedule_interval="0 8,14,20 * * *",  # 3x per day
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["logistics", "minio", "postgres", "etl"],
) as dag:

    wait_for_file = S3KeySensor(
        task_id="wait_for_file",
        bucket_name=MINIO_BUCKET,
        bucket_key=f"{INCREMENTAL_PREFIX}{{{{ execution_date.year }}}}/{{{{ execution_date.strftime('%Y-%m-%d') }}}}/*",
        aws_conn_id=AWS_CONN_ID,
        wildcard_match=True,
        poke_interval=60,
        timeout=60 * 15,
        mode="poke",
    )

    list_files = PythonOperator(
        task_id="list_files",
        python_callable=list_files_in_prefix,
        op_kwargs={
            "prefix": INCREMENTAL_PREFIX,
            "year": "{{ execution_date.year }}",
            "date": "{{ execution_date.strftime('%Y-%m-%d') }}",
        },
    )

    process_files = PythonOperator(
        task_id="process_files",
        python_callable=download_and_process,
    )

    load_postgres = PythonOperator(
        task_id="load_postgres",
        python_callable=load_to_postgres,
    )

    wait_for_file >> list_files >> process_files >> load_postgres
