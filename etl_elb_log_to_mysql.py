import os
import gzip
import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# AWS S3 Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
AWS_LOG_PREFIX = os.getenv("AWS_LOG_PREFIX") or ""
AWS_REGION = os.getenv("AWS_REGION")

# MySQL Configuration
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DB_PORT = os.getenv("DB_PORT")
MYSQL_URL = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Set up AWS and MySQL clients
s3 = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                  region_name=AWS_REGION)
engine = create_engine(MYSQL_URL)

# Extract Logs from S3
def extract_elb_logs():
    resp = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix=AWS_LOG_PREFIX)
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(".gz"):
            continue
        body = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=key)["Body"].read()
        with gzip.GzipFile(fileobj=BytesIO(body)) as f:
            for raw in f:
                yield raw.decode('utf-8').strip(), key