
"""
DAG: callcenter_sync_prod_v1

Workflow:
1. Read interactiondate values from source Parquet files.
2. Read registered interactiondate partitions from AWS Glue summary table.
3. Select the earliest date not yet summarized.
4. Run the existing CDE job: CallCenterSummary.

Airflow connection: aws_glue
CDE connection: awc-cde
AWS region: us-west-1
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from urllib.parse import urlparse

import pendulum

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)

from airflow.providers.cloudera.cde.operators.cde_operator import (
    CdeRunJobOperator,
)


# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------

DAG_ID = "callcenter_sync_prod_v1"

AWS_CONN_ID = "aws_glue"
CDE_CONN_ID = "awc-cde"
AWS_REGION = "us-west-1"

GLUE_DATABASE = "callcenter_data_s3"
SOURCE_TABLE = "callcenter_interaction"
SUMMARY_TABLE = "callcenter_interaction_summary"

SUMMARY_JOB_NAME = "CallCenterSummary"

# Keep or remove the trailing slash as desired; the code normalizes it.
SOURCE_S3_LOCATION = (
    "s3://ccf-psecoe-ccf-artifacts/s3_flow_data/CCL/callcenter/"
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------

def get_aws_client(client_type: str):
    """
    Create a boto3 client using the Airflow AWS connection and an
    explicitly configured regional endpoint.
    """
    endpoint_hosts = {
        "glue": f"https://glue.{AWS_REGION}.amazonaws.com",
        "s3": f"https://s3.{AWS_REGION}.amazonaws.com",
    }

    if client_type not in endpoint_hosts:
        raise ValueError(f"Unsupported AWS client type: {client_type}")

    hook = AwsBaseHook(
        aws_conn_id=AWS_CONN_ID,
        client_type=client_type,
        region_name=AWS_REGION,
    )

    session = hook.get_session()

    return session.client(
        client_type,
        region_name=AWS_REGION,
        endpoint_url=endpoint_hosts[client_type],
    )


def normalize_date(value) -> str | None:
    """Normalize supported date values to YYYY-MM-DD."""
    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    text_value = str(value).strip()

    if not text_value:
        return None

    # Handles ISO dates and timestamp strings.
    try:
        return date.fromisoformat(text_value[:10]).isoformat()
    except ValueError:
        logger.warning("Skipping unrecognized interactiondate: %r", value)
        return None


def normalize_s3_uri(uri: str) -> str:
    """Normalize an S3 URI so a trailing slash does not affect comparison."""
    parsed = urlparse(uri.strip())

    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Invalid S3 URI: {uri!r}")

    path = parsed.path.strip("/")
    return f"s3://{parsed.netloc}/{path}" if path else f"s3://{parsed.netloc}"


# ---------------------------------------------------------------------
# Discover source interaction dates
# ---------------------------------------------------------------------

def discover_source_dates() -> set[str]:
    """
    Read the interactiondate column from Parquet files under the
    source Glue table location.

    Requires pyarrow and s3fs to be available in the Airflow task
    environment.
    """
    try:
        import pyarrow.parquet as pq
        import s3fs
    except ImportError as exc:
        raise AirflowException(
            "The Airflow task environment needs pyarrow and s3fs "
            "to inspect the source Parquet files."
        ) from exc

    glue = get_aws_client("glue")

    try:
        response = glue.get_table(
            DatabaseName=GLUE_DATABASE,
            Name=SOURCE_TABLE,
        )
    except Exception as exc:
        raise AirflowException(
            f"Unable to retrieve Glue table "
            f"{GLUE_DATABASE}.{SOURCE_TABLE}: {exc}"
        ) from exc

    table = response["Table"]
    table_location = table.get("StorageDescriptor", {}).get("Location")

    if not table_location:
        raise AirflowException(
            f"Glue table {GLUE_DATABASE}.{SOURCE_TABLE} "
            "does not have a StorageDescriptor.Location."
        )

    # Fix: compare normalized URIs, not raw strings.
    normalized_table_location = normalize_s3_uri(table_location)
    normalized_source_location = normalize_s3_uri(SOURCE_S3_LOCATION)

    if normalized_table_location != normalized_source_location:
        raise AirflowException(
            f"The Glue table location is {table_location!r}, but the "
            f"configured source prefix is {SOURCE_S3_LOCATION!r}. "
            "Verify SOURCE_S3_LOCATION before processing."
        )

    logger.info("Source Glue table location: %s", table_location)

    # Use the same Airflow AWS connection for S3 access.
    s3_hook = AwsBaseHook(
        aws_conn_id=AWS_CONN_ID,
        client_type="s3",
        region_name=AWS_REGION,
    )
    credentials = s3_hook.get_credentials()

    filesystem = s3fs.S3FileSystem(
        key=credentials.access_key,
        secret=credentials.secret_key,
        token=credentials.token,
        client_kwargs={
            "region_name": AWS_REGION,
            "endpoint_url": f"https://s3.{AWS_REGION}.amazonaws.com",
        },
    )

    # s3fs expects bucket/key rather than s3://bucket/key.
    parsed_location = urlparse(table_location)
    s3_prefix = f"{parsed_location.netloc}/{parsed_location.path.strip('/')}"

    logger.info("Listing Parquet files under s3://%s", s3_prefix)

    try:
        all_objects = filesystem.find(s3_prefix)
    except Exception as exc:
        raise AirflowException(
            f"Unable to list objects under s3://{s3_prefix}: {exc}"
        ) from exc

    parquet_files = [
        path
        for path in all_objects
        if path.lower().endswith(".parquet")
    ]

    if not parquet_files:
        logger.warning(
            "No Parquet files found under s3://%s", s3_prefix
        )
        return set()

    discovered_dates: set[str] = set()
    files_without_date_column = 0

    for file_path in parquet_files:
        try:
            with filesystem.open(file_path, "rb") as file_obj:
                parquet_file = pq.ParquetFile(file_obj)

                if "interactiondate" not in parquet_file.schema_arrow.names:
                    files_without_date_column += 1
                    logger.warning(
                        "Skipping file without interactiondate column: %s",
                        file_path,
                    )
                    continue

                date_table = parquet_file.read(
                    columns=["interactiondate"]
                )

                for value in date_table.column("interactiondate").to_pylist():
                    normalized = normalize_date(value)
                    if normalized:
                        discovered_dates.add(normalized)

        except Exception as exc:
            raise AirflowException(
                f"Failed reading interactiondate from "
                f"s3://{file_path}: {exc}"
            ) from exc

    logger.info("Parquet files discovered: %d", len(parquet_files))
    logger.info(
        "Files without interactiondate column: %d",
        files_without_date_column,
    )
    logger.info(
        "Distinct source interaction dates discovered: %s",
        sorted(discovered_dates),
    )

    return discovered_dates


# ---------------------------------------------------------------------
# Read registered summary-table partitions from Glue
# ---------------------------------------------------------------------

def get_summary_dates() -> set[str]:
    """Return interactiondate values registered as Glue partitions."""
    glue = get_aws_client("glue")

    try:
        response = glue.get_table(
            DatabaseName=GLUE_DATABASE,
            Name=SUMMARY_TABLE,
        )
    except glue.exceptions.EntityNotFoundException:
        logger.info(
            "Summary table %s.%s does not exist yet; "
            "all discovered source dates will be considered missing.",
            GLUE_DATABASE,
            SUMMARY_TABLE,
        )
        return set()

    table = response["Table"]
    partition_keys = table.get("PartitionKeys", [])

    if not any(
        key.get("Name", "").lower() == "interactiondate"
        for key in partition_keys
    ):
        raise AirflowException(
            f"Glue summary table {GLUE_DATABASE}.{SUMMARY_TABLE} "
            "must have an interactiondate partition key. "
            f"Found: {partition_keys}"
        )

    summary_dates: set[str] = set()
    paginator = glue.get_paginator("get_partitions")

    try:
        for page in paginator.paginate(
            DatabaseName=GLUE_DATABASE,
            TableName=SUMMARY_TABLE,
        ):
            for partition in page.get("Partitions", []):
                values = partition.get("Values", [])

                if not values:
                    continue

                normalized = normalize_date(values[0])
                if normalized:
                    summary_dates.add(normalized)

    except Exception as exc:
        raise AirflowException(
            f"Unable to retrieve partitions for "
            f"{GLUE_DATABASE}.{SUMMARY_TABLE}: {exc}"
        ) from exc

    logger.info(
        "Registered summary interaction dates: %s",
        sorted(summary_dates),
    )

    return summary_dates


# ---------------------------------------------------------------------
# Select the earliest missing date
# ---------------------------------------------------------------------

def fetch_missing_date(**context) -> str | None:
    """
    Find the earliest source date that has no registered summary
    partition. The selected date is pushed to XCom automatically.
    """
    source_dates = discover_source_dates()

    if not source_dates:
        logger.info(
            "No valid interaction dates were found in the source. "
            "There is nothing to process."
        )
        return None

    summary_dates = get_summary_dates()
    missing_dates = sorted(source_dates - summary_dates)

    if not missing_dates:
        logger.info(
            "All discovered source dates already have summary "
            "partitions. No CDE job is needed."
        )
        return None

    selected_date = missing_dates[0]

    logger.info("Selected earliest missing interaction date: %s", selected_date)
    logger.info("Other missing dates, if any: %s", missing_dates[1:])

    return selected_date


def select_interaction_date(**context) -> bool:
    """Allow the CDE task to run only when a missing date was found."""
    selected_date = context["ti"].xcom_pull(
        task_ids="fetch_missing_date"
    )

    if not selected_date:
        logger.info("No missing date selected; skipping the CDE job.")
        return False

    logger.info("A missing date was selected: %s", selected_date)
    return True


# ---------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------

default_args = {
    "owner": "cloudera",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_ID,
    description=(
        "Discover unsummarized call-center interaction dates and "
        "run the existing CDE summary job."
    ),
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    dagrun_timeout=timedelta(hours=1),
    tags=["callcenter", "glue", "cde"],
) as dag:

    fetch_missing = PythonOperator(
        task_id="fetch_missing_date",
        python_callable=fetch_missing_date,
    )

    select_date = ShortCircuitOperator(
        task_id="select_interaction_date",
        python_callable=select_interaction_date,
    )

    run_summary = CdeRunJobOperator(
        task_id="run_callcenter_summary",
        job_name=SUMMARY_JOB_NAME,
        job_arguments=[
            "{{ ti.xcom_pull(task_ids='fetch_missing_date') }}",
            "--glue-database",
            GLUE_DATABASE,
            "--source-table",
            SOURCE_TABLE,
            "--summary-table",
            SUMMARY_TABLE,
            "--aws-region",
            AWS_REGION,
        ],
        cde_conn_id=CDE_CONN_ID,
    )

    fetch_missing >> select_date >> run_summary
