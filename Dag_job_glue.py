
import logging
from datetime import datetime
from urllib.parse import urlparse

import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.standard.operators.python import ShortCircuitOperator

from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

DAG_ID = "callcenter_sync_prod_v1"

AWS_CONN_ID = "aws_glue"
AWS_REGION = "us-west-1"

CDE_CONN_ID = "awc-cde"
CDE_JOB_NAME = "CallCenterSummary"

GLUE_DATABASE = "callcenter_data_s3"
SOURCE_TABLE = "callcenter_interaction"
SUMMARY_TABLE = "callcenter_interaction_summary"

SOURCE_S3_LOCATION = (
    "s3://ccf-psecoe-ccf-artifacts/"
    "s3_flow_data/CCL/callcenter/"
)

DATE_COLUMN = "interactiondate"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


# ---------------------------------------------------------
# AWS clients
# ---------------------------------------------------------

def get_glue_client():
    """Create a regional Glue client using the Airflow AWS connection."""
    hook = AwsBaseHook(
        aws_conn_id=AWS_CONN_ID,
        client_type="glue",
        region_name=AWS_REGION,
    )

    session = hook.get_session()

    return session.client(
        "glue",
        region_name=AWS_REGION,
        endpoint_url=f"https://glue.{AWS_REGION}.amazonaws.com",
    )


def normalize_s3_uri(uri):
    """Normalize an S3 URI to avoid trailing-slash mismatches."""
    parsed = urlparse(uri)

    if parsed.scheme != "s3" or not parsed.netloc:
        raise AirflowException(f"Invalid S3 URI: {uri}")

    path = parsed.path.strip("/")

    return f"s3://{parsed.netloc}/{path}" if path else f"s3://{parsed.netloc}"


def normalize_date(value):
    """Convert supported date values into YYYY-MM-DD strings."""
    if value is None:
        return None

    if hasattr(value, "isoformat"):
        return value.isoformat()[:10]

    value = str(value).strip()

    return value[:10] if value else None


# ---------------------------------------------------------
# Discover dates present in the source Parquet data
# ---------------------------------------------------------

def get_source_dates(glue):
    """
    Read only the interactiondate column from source Parquet files.
    The source table is not partitioned in Glue, so dates are
    discovered from the actual data.
    """
    import pyarrow.parquet as pq
    import s3fs

    source_table = glue.get_table(
        DatabaseName=GLUE_DATABASE,
        Name=SOURCE_TABLE,
    )["Table"]

    actual_location = normalize_s3_uri(
        source_table["StorageDescriptor"]["Location"]
    )
    configured_location = normalize_s3_uri(SOURCE_S3_LOCATION)

    if actual_location != configured_location:
        raise AirflowException(
            "Source S3 location mismatch. "
            f"Glue table location: {actual_location}; "
            f"configured location: {configured_location}"
        )

    aws_hook = AwsBaseHook(
        aws_conn_id=AWS_CONN_ID,
        client_type="s3",
        region_name=AWS_REGION,
    )
    credentials = aws_hook.get_credentials()

    filesystem = s3fs.S3FileSystem(
        key=credentials.access_key,
        secret=credentials.secret_key,
        token=credentials.token,
        client_kwargs={
            "region_name": AWS_REGION,
            "endpoint_url": f"https://s3.{AWS_REGION}.amazonaws.com",
        },
    )

    parsed = urlparse(actual_location)
    prefix = f"{parsed.netloc}/{parsed.path.strip('/')}"

    parquet_files = [
        path
        for path in filesystem.find(prefix)
        if path.lower().endswith(".parquet")
    ]

    if not parquet_files:
        logging.warning(
            "No Parquet files found under %s", actual_location
        )
        return set()

    source_dates = set()

    for file_path in parquet_files:
        logging.info("Reading date column from %s", file_path)

        with filesystem.open(file_path, "rb") as file_obj:
            table = pq.read_table(
                file_obj,
                columns=[DATE_COLUMN],
            )

        for value in table.column(DATE_COLUMN).to_pylist():
            date_value = normalize_date(value)

            if date_value:
                source_dates.add(date_value)

    logging.info(
        "Discovered %d distinct source dates",
        len(source_dates),
    )

    return source_dates


# ---------------------------------------------------------
# Discover dates already summarized in Glue
# ---------------------------------------------------------

def get_summary_dates(glue):
    """Return interaction dates represented by summary table partitions."""
    from botocore.exceptions import ClientError

    try:
        summary_table = glue.get_table(
            DatabaseName=GLUE_DATABASE,
            Name=SUMMARY_TABLE,
        )["Table"]

    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")

        if error_code == "EntityNotFoundException":
            logging.info(
                "Summary table does not exist yet; "
                "treating all source dates as unprocessed."
            )
            return set()

        raise

    partition_keys = [
        item["Name"]
        for item in summary_table.get("PartitionKeys", [])
    ]

    if DATE_COLUMN not in partition_keys:
        raise AirflowException(
            f"Summary table {GLUE_DATABASE}.{SUMMARY_TABLE} "
            f"does not have the expected partition key "
            f"'{DATE_COLUMN}'. Found: {partition_keys}"
        )

    date_index = partition_keys.index(DATE_COLUMN)
    summary_dates = set()

    paginator = glue.get_paginator("get_partitions")

    for page in paginator.paginate(
        DatabaseName=GLUE_DATABASE,
        TableName=SUMMARY_TABLE,
    ):
        for partition in page.get("Partitions", []):
            values = partition.get("Values", [])

            if len(values) > date_index:
                date_value = normalize_date(values[date_index])

                if date_value:
                    summary_dates.add(date_value)

    logging.info(
        "Found %d dates already summarized",
        len(summary_dates),
    )

    return summary_dates


# ---------------------------------------------------------
# Select the earliest missing date
# ---------------------------------------------------------

def discover_missing_date():
    """
    Return the earliest source date without a summary partition.

    ShortCircuitOperator skips the CDE job if no date needs processing.
    """
    glue = get_glue_client()

    source_dates = get_source_dates(glue)

    if not source_dates:
        logging.info("No source dates found. Skipping the CDE job.")
        return False

    summary_dates = get_summary_dates(glue)

    missing_dates = sorted(source_dates - summary_dates)

    if not missing_dates:
        logging.info(
            "All source dates have summary partitions. Nothing to process."
        )
        return False

    selected_date = missing_dates[0]

    logging.info("Selected interaction date: %s", selected_date)
    return selected_date


# ---------------------------------------------------------
# DAG
# ---------------------------------------------------------

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="30 16 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["callcenter", "glue", "cde"],
) as dag:

    select_missing_date = ShortCircuitOperator(
        task_id="select_missing_date",
        python_callable=discover_missing_date,
    )

    run_spark = CDEJobRunOperator(
        task_id="run_callcenter_summary",
        job_name=CDE_JOB_NAME,
        connection_id=CDE_CONN_ID,
        variables={
            "interaction_date": (
                "{{ ti.xcom_pull(task_ids='select_missing_date') }}"
            )
        },
    )

    select_missing_date >> run_spark
