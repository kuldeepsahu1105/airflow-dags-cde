"""Shared vehicle health pipeline paths used by Airflow DAG templates."""

from __future__ import annotations

S3_BUCKET = "aws-ccf-smatyca-backup"
AWS_REGION = "us-east-1"

# Step 1 — raw input (zip under this prefix)
RAW_S3_ROOT = f"s3a://{S3_BUCKET}/telemetry_data"

# Spark writes curated Parquet here
CURATED_S3_ROOT = f"s3a://{S3_BUCKET}/curated/vehicle-health"
TEMP_S3_URI = f"s3a://{S3_BUCKET}/tmp/vehicle-health"

# Glue database registered by Airflow after Spark; queried by Trino (hive.metastore=glue)
GLUE_DATABASE = "vehicle_health_analytics"

# Airflow connection for Task 2 (Glue registration). Create in Admin → Connections.
# Connection Type: Amazon Web Services (or Generic with login=access key, password=secret key)
AWS_CONNECTION_ID = "aws_glue"


def input_zip_uri(business_date: str) -> str:
    """Default input path when the zip follows vehicle_health_<YYYY_MM_DD>.zip naming."""
    zip_date = business_date.replace("-", "_")
    return f"{RAW_S3_ROOT}/vehicle_health_{zip_date}.zip"
