import os
import random
import csv
import argparse
from datetime import datetime, timedelta
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
ROWS_PER_DAY = int(os.getenv("DATA_ROWS_PER_DAY", 500))
PARTS_PER_DAY = int(os.getenv("DATA_PARTS_PER_DAY", 3))  # default = 3 parts/day

# MinIO client
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

def ensure_bucket(bucket_name):
    existing_buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if bucket_name not in existing_buckets:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")

# Configurations
ASSET_IDS = [f"Truck_{i}" for i in range(1, 11)]
SHIPMENT_STATUSES = ["Delayed", "In Transit", "Delivered"]
TRAFFIC_STATUSES = ["Clear", "Heavy", "Accident Detour"]
WEATHER_EVENTS = ["Normal", "Storm", "Extreme Heat"]
STATES = [
    "California", "Texas", "Florida", "New York", "Illinois",
    "Pennsylvania", "Ohio", "Georgia", "North Carolina", "Michigan"
]

def random_date(start_date, end_date):
    delta = end_date - start_date
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return (start_date + timedelta(seconds=random_seconds)).strftime("%Y-%m-%d %H:%M:%S")

def generate_row(start_date, end_date):
    # Random basic attributes
    ts = random_date(start_date, end_date)
    asset = random.choice(ASSET_IDS)
    state = random.choice(STATES)
    distance = round(random.uniform(20, 500), 1)

    traffic = random.choices(TRAFFIC_STATUSES, weights=[0.6, 0.3, 0.1])[0]
    weather = random.choices(WEATHER_EVENTS, weights=[0.7, 0.2, 0.1])[0]

    planned_hours = max(1, round(distance / random.uniform(40, 60)))
    actual_hours = planned_hours

    # Delay probability
    delay_flag = 0
    base_chance = 0.05
    if distance > 200: base_chance += 0.15
    if traffic != "Clear": base_chance += 0.25
    if weather != "Normal": base_chance += 0.3

    if random.random() < base_chance:
        delay_flag = 1
        actual_hours += random.randint(1, 4)

    # Reason for delay
    if delay_flag:
        if traffic != "Clear":
            reason = "Traffic"
        elif weather != "Normal":
            reason = "Weather"
        else:
            reason = random.choice(["Mechanical Failure", "Other"])
    else:
        reason = "None"

    return [
        ts,
        asset,
        state,
        distance,
        random.choice(SHIPMENT_STATUSES),
        planned_hours,
        actual_hours,
        delay_flag,
        reason,
        traffic,
        weather,
        round(random.uniform(20, 35), 1),
        round(random.uniform(50, 80), 1),
        round(random.uniform(60, 95), 1),
        random.randint(100, 500),
        random.randint(200, 350)
    ]

def generate_csv(file_path, num_rows, start_date, end_date):
    """
    Generate a CSV file with synthetic logistics data.
    """
    with open(file_path, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "Timestamp", "Asset_ID", "State", "Route_Distance_km",
            "Shipment_Status", "Planned_Delivery_Hours", "Actual_Delivery_Hours",
            "Delay_Flag", "Delay_Reason", "Traffic_Status", "Weather_Event",
            "Temperature", "Humidity", "Asset_Utilization", "Inventory_Level",
            "Demand_Forecast"
        ])
        for _ in range(num_rows):
            writer.writerow(generate_row(start_date, end_date))
    print(f"Generated {file_path} with {num_rows} rows.")
    return file_path

def upload_to_minio(file_path, bucket, object_name):
    """
    Upload generated CSV file to MinIO bucket.
    """
    s3.upload_file(file_path, bucket, object_name)
    print(f"Uploaded {file_path} → {bucket}/{object_name}")

def generate_partial_batches(date_str=None, rows=ROWS_PER_DAY, parts=PARTS_PER_DAY, part=None):
    if date_str:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        target_date = datetime.now().date()

    start_date = datetime(target_date.year, target_date.month, target_date.day)
    end_date = start_date + timedelta(hours=23, minutes=59, seconds=59)

    rows_per_part = rows // parts
    hours_per_part = 24 // parts

    selected_parts = [part] if part else range(1, parts + 1)

    for p in selected_parts:
        part_start = start_date + timedelta(hours=(p - 1) * hours_per_part)
        part_end = part_start + timedelta(hours=hours_per_part - 1, minutes=59, seconds=59)

        # File naming
        filename = f"smart_logistics_{target_date}_part{p}.csv"
        local_path = generate_csv(filename, rows_per_part, part_start, part_end)

        # Object storage path
        object_name = f"batch/{target_date.year}/{target_date.strftime('%Y-%m-%d')}/{filename}"
        upload_to_minio(local_path, MINIO_BUCKET, object_name)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate partial logistics data")
    parser.add_argument("--date", help="Target date (YYYY-MM-DD)", required=False)
    parser.add_argument("--part", type=int, help="Generate only this part (e.g., --part 2)", required=False)
    args = parser.parse_args()

    ensure_bucket(MINIO_BUCKET)
    generate_partial_batches(date_str=args.date, part=args.part)
