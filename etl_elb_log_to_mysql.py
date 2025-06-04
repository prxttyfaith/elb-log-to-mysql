import os
import gzip
import boto3
import pandas as pd
import shlex
from dotenv import load_dotenv
from sqlalchemy import create_engine
from io import BytesIO
from datetime import datetime, timezone
import pytz
from user_agents import parse as ua_parse
from urllib.parse import urlparse

from logger import get_logger
logger = get_logger(__name__)

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

# Utility function
EASTERN = pytz.timezone("America/New_York")

def to_int(val):
    return int(val) if val.isdigit() else 0

def to_float(val):
    try:
        return float(val)
    except:
        return 0.0

# EXTRACT: get .gz keys from S3
def extract_log_keys(bucket, prefix=''):
    try:
        resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        keys = [obj['Key'] for obj in resp.get('Contents', []) if obj['Key'].endswith('.gz')]
        logger.info(f"Extracted {len(keys)} .gz log keys from S3.")
        return keys
    except Exception:
        logger.exception("Error extracting log keys from S3.")
        return []

# PARSE: read and parse each log file
def parse_log_entry(line, source_file):
    try:
        parts = shlex.split(line)
        if len(parts) < 15:
            logger.warning(f"Skipped line with insufficient parts (from {source_file}).")
            return None
        
        # Timestamp
        ts = None
        for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
            try:
                dt_naive = datetime.strptime(parts[1], fmt)
                dt_utc = dt_naive.replace(tzinfo=timezone.utc)
                ts = dt_utc.astimezone(EASTERN)
                break
            except ValueError:
                continue
        if not ts:
            logger.warning(f"Skipped line with invalid timestamp (from {source_file}).")
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
        
        # REQUEST: get full, then split out method + URL
        try:
            req_split = parts[12].strip('"').split(" ", 2)
            http_method = req_split[0]
            full_url = req_split[1] if len(req_split) > 1 else ""
            requested_path = urlparse(full_url).path if full_url else ""
        except Exception:
            http_method, requested_path = "Unknown", ""
            
        # USER-AGENT: full then families
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
    except Exception:
        logger.exception(f"[Parse error] | Source file: {source_file} | Line: {line[:80]}")
        return None

# TRANSFORM: read, parse, and transform logs into a DataFrame
def transform_elb_logs(bucket, keys):
    records = []
    for key in keys:
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            with gzip.GzipFile(fileobj=BytesIO(obj['Body'].read())) as gz:
                for line in gz:
                    line_decoded = line.decode('utf-8').strip()
                    parsed = parse_log_entry(line_decoded, key)
                    if parsed:
                        records.append(parsed)
            logger.info(f"Parsed log file: {key} ({len(records)} total records so far)")
        except Exception:
            logger.exception(f"Error parsing log file: {key}")
    df = pd.DataFrame(records)
    return df

# Load to MySQL
def load_to_mysql(df, table='elb_log_data'):
    if not df.empty:
        try:
            df.to_sql(table, con=engine, if_exists='append', index=False)
            logger.info(f"Loaded {len(df)} rows into MySQL table `{table}`.")
        except Exception:
            logger.exception("Failed to load to MySQL.")
    else:
        logger.warning("Nothing to load. DataFrame is empty.")

def run_etl():
    bucket = AWS_BUCKET_NAME
    prefix = AWS_LOG_PREFIX
    logger.info("Extracting log keys from S3...")
    keys = extract_log_keys(bucket, prefix)
    logger.info(f"Found {len(keys)} log files.")
    df_logs = transform_elb_logs(bucket, keys)

    # Show preview
    logger.info("=== Data Preview ===")
    logger.info(f"\n{df_logs.head(5)}")
    logger.info(f"Shape: {df_logs.shape}")

    # Attempt to load only if data is present (demo: just load 1 row)
    df_logs = df_logs.head(1)
    load_to_mysql(df_logs)

if __name__ == "__main__":
    run_etl()
