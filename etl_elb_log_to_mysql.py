import os
import gzip
import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from io import BytesIO
from datetime import datetime, timezone
import pytz
import re
from user_agents import parse as ua_parse
from urllib.parse import urlparse

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

# Uitility function
EASTERN     = pytz.timezone("America/New_York")

def to_int(val):
    return int(val) if val.isdigit() else 0

def to_float(val):
    try:
        return float(val)
    except:
        return 0.0
    
def parse_timestamp(ts):
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            dt_naive = datetime.strptime(ts, fmt)
            dt_utc = dt_naive.replace(tzinfo=timezone.utc)
            return dt_utc.astimezone(EASTERN)
        except ValueError:
            continue
    return None

# EXTRACT: get .gz keys from S3
def extract_log_keys(bucket, prefix=''):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [obj['Key'] for obj in resp.get('Contents', []) if obj['Key'].endswith('.gz')]

# PARSE: read and parse each log file
def parse_log_entry(line, source_file):
    try:
        parts = shlex.split(line)
        if len(parts) < 15:
            return None
        # Timestamp
        ts = parse_timestamp(parts[1])
        if not ts:
            return None
        # Client IP
        client_ip = parts[3].split(":")[0]
        # ELB/backend status
        elb_code = to_int(parts[8])
        backend_code = to_int(parts[9])
        # Processing time in ms
        total_ms = round((to_float(parts[5]) + to_float(parts[6]) + to_float(parts[7])) * 1000, 3)
        # Bytes
        received_bytes = to_int(parts[10])
        sent_bytes = to_int(parts[11])
        # HTTP method + path
        try:
            req_split = parts[12].strip('"').split(" ", 2)
            http_method = req_split[0]
            full_url = req_split[1] if len(req_split) > 1 else ""
            requested_path = urlparse(full_url).path if full_url else ""
        except Exception:
            http_method, requested_path = "Unknown", ""
        # User agent
        user_agent_full = parts[13].strip('"')
        ua = ua_parse(user_agent_full) if user_agent_full and user_agent_full != "-" else None
        ua_browser = ua.browser.family if ua else "Unknown"
        ua_os = ua.os.family if ua else "Unknown"
        return {
            "log_timestamp": ts,
            "client_ip": client_ip,
            "http_method": http_method,
            "requested_path": requested_path,
            "elb_status_code": elb_code,
            "backend_status_code": backend_code,
            "total_processing_time_ms": total_ms,
            "received_bytes": received_bytes,
            "sent_bytes": sent_bytes,
            "user_agent_full": user_agent_full,
            "ua_browser_family": ua_browser,
            "ua_os_family": ua_os,
            "log_source_file": source_file,
        }
    except Exception as e:
        print(f"[Parse error] {e} | Line: {line[:80]}")
        return None


# Transform
def transform_elb_logs():
    records = []
    for line, src in extract_elb_logs():
        rec = parse_log_entry(line, src)
        if rec:
            records.append(rec)

    if not records:
        print("No valid records found.")
        return pd.DataFrame()

    df = pd.DataFrame(records)    
    print(f"Transformed {len(df)} entries.")
    return df

df_logs = transform_elb_logs()
# show first 5 rows
print(df_logs.head(5))
print(df_logs.shape)
print(df_logs.dtypes)

#Load to MySQL
def load_to_mysql(df):
    if df.empty:
        print("Nothing to load.")
    else:
        df.to_sql("elb_log_data", con=engine, if_exists="append", index=False)
        print(f"Loaded {len(df)} rows into MySQL.")

df_logs = transform_elb_logs()
load_to_mysql(df_logs)