from dotenv import load_dotenv
import os
import boto3
import json
import time
import pandas as pd
from kafka import KafkaProducer
from uuid import uuid4

load_dotenv()

# --------------------------
# AWS Credentials
# --------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")

S3_BUCKET = "amzn-insurance-raw-dataset"
S3_PATH = "raw_events/"

# --------------------------
# Kafka Configuration
# --------------------------
CSV_FILE = "../data/insurance.csv"
KAFKA_TOPIC = "insurance_raw"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# --------------------------
# Initialize S3 Client
# --------------------------
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)

# --------------------------
# Load CSV and Stream
# --------------------------
df = pd.read_csv(CSV_FILE)

for _, row in df.iterrows():
    event = row.to_dict()
    event['event_time'] = time.time()

    producer.send(KAFKA_TOPIC, value=event)
    producer.flush()

    # s3.put_object(
    #     Bucket=S3_BUCKET,
    #     Key=f"{S3_PATH}{uuid4()}.json",
    #     Body=json.dumps(event)
    # )

    time.sleep(1)  # simulate streaming
