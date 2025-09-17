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
YEARS = [2023, 2024, 2025]
ROWS_PER_YEAR = {2023: 1000, 2024: 1000, 2025: 600}

# Random Value Generators
def random_date(year):
    if year == 2025:
        start_date = datetime(2025, 1, 1)
        end_date = datetime(2025, 9, 16)
    else:
        start_date = datetime(year, 1, 1)
        end_date = datetime(year, 12, 31)
    
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    random_date = start_date + timedelta(days=random_days)
    random_time = random.randint(0, 86399)  # Seconds in a day
    return (random_date + timedelta(seconds=random_time)).strftime("%Y-%m-%d %H:%M:%S")

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
def generate_row(year):
    return [
        random_date(year),
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

def generate_csv(year, num_rows, version=1):
    filename = f"smart_logistics_{year}_v{version}.csv"
    with open(filename, 'w', newline='') as file:
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
            writer.writerow(generate_row(year))
    print(f"Generated {filename} with {num_rows} rows.")
    return filename

# Upload to MinIO
def upload_to_minio(file_path, bucket, object_name=None):
    if object_name is None:
        object_name = os.path.basename(file_path)
    s3.upload_file(file_path, bucket, object_name)
    print(f"Uploaded {file_path} → {bucket}/{object_name}")

# Main Entry Point
if __name__ == "__main__":
    ensure_bucket(MINIO_BUCKET)
    version = 1
    for year in YEARS:
        filename = generate_csv(2023, ROWS_PER_YEAR[2023], version=version)
        upload_to_minio(filename, MINIO_BUCKET)
