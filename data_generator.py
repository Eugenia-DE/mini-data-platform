import os
import random
import csv
from datetime import datetime, timedelta
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")

# MinIO client
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

# Ensure bucket exists
def ensure_bucket(bucket_name):
    existing_buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket_name not in existing_buckets:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")

# Configurations
ASSET_IDS = [f"Truck_{i}" for i in range(1, 11)]
SHIPMENT_STATUSES = ["Delayed", "In Transit", "Delivered"]
TRAFFIC_STATUSES = ["Detour", "Heavy", "Clear"]
DELAY_REASONS = ["None", "Weather", "Traffic", "Mechanical Failure"]

# Random Value Generators
def random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_time = random.randint(0, 86399)  # seconds in day
    return (start_date + timedelta(days=random_days, seconds=random_time)).strftime("%Y-%m-%d %H:%M:%S")

def random_lat(): return round(random.uniform(-90, 90), 4)
def random_lon(): return round(random.uniform(-180, 180), 4)
def random_inventory(): return random.randint(100, 500)
def random_temp(): return round(random.uniform(18, 30), 1)
def random_humidity(): return round(random.uniform(50, 80), 1)
def random_waiting_time(): return random.randint(10, 60)
def random_transaction_amount(): return random.randint(100, 500)
def random_purchase_freq(): return random.randint(1, 10)
def random_utilization(): return round(random.uniform(60, 100), 1)
def random_demand_forecast(): return random.randint(100, 300)
def random_logistics_delay(): return random.choice([0, 1])

# Row and File Generators
def generate_row(start_date, end_date):
    return [
        random_date(start_date, end_date),
        random.choice(ASSET_IDS),
        random_lat(),
        random_lon(),
        random_inventory(),
        random.choice(SHIPMENT_STATUSES),
        random_temp(),
        random_humidity(),
        random.choice(TRAFFIC_STATUSES),
        random_waiting_time(),
        random_transaction_amount(),
        random_purchase_freq(),
        random.choice(DELAY_REASONS),
        random_utilization(),
        random_demand_forecast(),
        random_logistics_delay()
    ]

def generate_csv(file_path, num_rows, start_date, end_date):
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        # Header
        writer.writerow([
            "Timestamp", "Asset_ID", "Latitude", "Longitude", "Inventory_Level",
            "Shipment_Status", "Temperature", "Humidity", "Traffic_Status",
            "Waiting_Time", "User_Transaction_Amount", "User_Purchase_Frequency",
            "Logistics_Delay_Reason", "Asset_Utilization", "Demand_Forecast",
            "Logistics_Delay"
        ])
        for _ in range(num_rows):
            writer.writerow(generate_row(start_date, end_date))
    print(f"Generated {file_path} with {num_rows} rows.")
    return file_path

# Upload
def upload_to_minio(file_path, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_path)
    s3.upload_file(file_path, bucket, object_name)
    print(f"Uploaded {file_path} → {bucket}/{object_name}")

# Modes
def generate_backfill_2025(rows_per_month=1000):
    """Generate monthly files for 2025 YTD only."""
    year = 2025
    current_year = datetime.now().year
    current_month = datetime.now().month if year == current_year else 12

    for month in range(1, current_month + 1):
        start_date = datetime(year, month, 1)

        if month == 12:
            end_date = datetime(year, 12, 31, 23, 59, 59)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(seconds=1)

        filename = f"smart_logistics_{year}_{month:02d}.csv"
        generate_csv(filename, rows_per_month, start_date, end_date)
        upload_to_minio(filename, MINIO_BUCKET, f"backfill/{year}/{filename}")

def generate_incremental(rows_per_day=200):
    """Generate file for 'today' as incremental daily drop."""
    today = datetime.now().date()
    start_date = datetime(today.year, today.month, today.day)
    end_date = start_date + timedelta(hours=23, minutes=59, seconds=59)

    filename = f"smart_logistics_{today}.csv"
    generate_csv(filename, rows_per_day, start_date, end_date)
    upload_to_minio(filename, MINIO_BUCKET, f"incremental/{today.year}/{filename}")

# Main
if __name__ == "__main__":
    ensure_bucket(MINIO_BUCKET)

    mode = os.getenv("DATA_MODE", "backfill")  # backfill or incremental

    if mode == "backfill":
        generate_backfill_2025(rows_per_month=1000)
    elif mode == "incremental":
        generate_incremental(rows_per_day=200)
    else:
        print("Unknown mode. Use DATA_MODE=backfill or incremental.")
