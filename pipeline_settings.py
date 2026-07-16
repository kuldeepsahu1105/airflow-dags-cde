"""Settings for the Porsche Vehicle Health Iceberg analytics DAG."""

from __future__ import annotations

RAW_S3_ROOT = "s3a://porscheenv-buk-3b2308dc/data/porsche/raw/vehicle-health"
CURATED_S3_ROOT = "s3a://porscheenv-buk-3b2308dc/data/porsche/curated/vehicle-health"
TEMP_S3_URI = "s3a://porscheenv-buk-3b2308dc/data/porsche/tmp/vehicle-health"

CDE_CONNECTION_ID = "awc-cde"
CDE_SPARK_JOB_NAME = "spark-etl-pipeline"
IMPALA_CONNECTION_ID = "datahub_impala"

ANALYTICS_DATABASE = "porsche_vehicle_health_analytics"
STAGING_DATABASE = "porsche_vehicle_health_analytics_staging"


def input_zip_uri(business_date: str) -> str:
    zip_date = business_date.replace("-", "_")
    return f"{RAW_S3_ROOT}/vehicle_health_{zip_date}.zip"


def curated_dataset_uri(business_date: str, dataset_name: str) -> str:
    return f"{CURATED_S3_ROOT}/business_date={business_date}/{dataset_name}"
