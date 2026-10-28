
"""
DAG: callcenter_sync_prod_v1

Workflow:
1. Read the source table location from AWS Glue.
2. Discover interaction dates directly from Parquet files in S3.
3. Compare discovered dates with registered summary-table partitions.
4. Select the earliest unprocessed date.
5. Run the existing CallCenterSummary CDE job.

No separate CDE discovery job is required.
"""

import pendulum

from datetime import date, datetime
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.standard.operators.python import (
    PythonOperator,
    ShortCircuitOperator,
)
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

AWS_CONN_ID = "aws_glue"
CDE_CONN_ID = "awc-cde"
AWS_REGION = "us-west-1"

GLUE_DATABASE = "callcenter_data_s3"
SOURCE_TABLE = "callcenter_interaction"
SUMMARY_TABLE = "callcenter_interaction_summary"

SOURCE_S3_LOCATION = (
    "s3://ccf-psecoe-ccf-artifacts/"
    "s3_flow_data/CCL/callcenter/"
)

SUMMARY_JOB_NAME = "CallCenterSummary"


# ---------------------------------------------------------
# AWS clients
# ---------------------------------------------------------

def get_aws_client(client_type):
    hook = AwsBaseHook(
        aws_conn_id=AWS_CONN_ID,
        client_type=client_type,
        region_name=AWS_REGION,
    )

    session = hook.get_session()

    return session.client(
        client_type,
        region_name=AWS_REGION,
        endpoint_url=(
            f"https://{client_type}.{AWS_REGION}.amazonaws.com"
        ),
    )


# ---------------------------------------------------------
# Date normalization
# ---------------------------------------------------------

def normalize_date(value):
    """Convert a Parquet interactiondate value into YYYY-MM-DD."""

    if value is None:
        return None

    if isinstance(value, datetime):
        return value.date().isoformat()

    if isinstance(value, date):
        return value.isoformat()

    value = str(value).strip()

    if not value:
        return None

    # Supports ISO date strings and ISO timestamps.
    # For other formats, adapt this to the actual source schema.
    try:
        return date.fromisoformat(value[:10]).isoformat()
    except ValueError:
        raise ValueError(
            f"Unsupported interactiondate value: {value!r}. "
            "Update normalize_date() for the source date format."
        )


# ---------------------------------------------------------
# Discover source dates directly from Parquet files in S3
# ---------------------------------------------------------

def discover_source_dates():
    """
    Read only the interactiondate column from source Parquet files.

    Requires pyarrow and s3fs in the Airflow task runtime.
    Uses the AWS credentials from the aws_glue Airflow connection.
    """

    try:
        import s3fs
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError(
            "Date discovery requires 's3fs' and 'pyarrow' in the "
            "Airflow task runtime. Install these dependencies in the "
            "Airflow environment before running this DAG."
        ) from exc

    hook = AwsBaseHook(
        aws_conn_id=AWS_CONN_ID,
        client_type="s3",
        region_name=AWS_REGION,
    )

    credentials = hook.get_credentials()

    filesystem = s3fs.S3FileSystem(
        key=credentials.access_key,
        secret=credentials.secret_key,
        token=credentials.token,
        client_kwargs={"region_name": AWS_REGION},
    )

    s3 = get_aws_client("s3")

    # Read Glue metadata to verify the table and its actual location.
    glue = get_aws_client("glue")

    table = glue.get_table(
        DatabaseName=GLUE_DATABASE,
        Name=SOURCE_TABLE,
    )["Table"]

    glue_location = table.get(
        "StorageDescriptor", {}
    ).get("Location", "")

    if not glue_location:
        raise ValueError(
            f"No S3 storage location found for "
            f"{GLUE_DATABASE}.{SOURCE_TABLE}"
        )

    expected_location = SOURCE_S3_LOCATION.rstrip("/") + "/"

    if not glue_location.startswith(expected_location):
        raise ValueError(
            f"The Glue table location is {glue_location!r}, but the "
            f"configured source prefix is {expected_location!r}. "
            "Verify SOURCE_S3_LOCATION before processing."
        )

    bucket = glue_location.replace("s3://", "", 1).split("/", 1)[0]
    prefix = glue_location.replace(
        f"s3://{bucket}/", "", 1
    ).rstrip("/") + "/"

    paginator = s3.get_paginator("list_objects_v2")

    parquet_keys = []

    for page in paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.lower().endswith(".parquet"):
                parquet_keys.append(key)

    if not parquet_keys:
        raise ValueError(
            f"No Parquet files found under {glue_location}"
        )

    print(f"Source table: {GLUE_DATABASE}.{SOURCE_TABLE}")
    print(f"Source location: {glue_location}")
    print(f"Parquet files found: {len(parquet_keys)}")

    source_dates = set()

    for key in parquet_keys:
        path = f"{bucket}/{key}"

        with filesystem.open(path, "rb") as parquet_file:
            parquet = pq.ParquetFile(parquet_file)

            if "interactiondate" not in parquet.schema_arrow.names:
                raise ValueError(
                    f"'interactiondate' column not found in "
                    f"s3://{bucket}/{key}. "
                    f"Available columns: {parquet.schema_arrow.names}"
                )

            # Read only the date column, not the entire dataset.
            date_table = parquet.read(
                columns=["interactiondate"]
            )

            for value in date_table.column(
                "interactiondate"
            ).to_pylist():
                normalized = normalize_date(value)

                if normalized:
                    source_dates.add(normalized)

    print(f"Distinct source dates found: {len(source_dates)}")
    print(f"Source dates: {sorted(source_dates)}")

    if not source_dates:
        raise ValueError(
            "Parquet files were found, but no valid interactiondate "
            "values were discovered."
        )

    return source_dates


# ---------------------------------------------------------
# Read processed dates from Glue summary partitions
# ---------------------------------------------------------

def get_summary_dates(glue):
    """
    Return registered interactiondate partitions.

    If the summary table does not exist yet, return an empty set.
    """

    try:
        table = glue.get_table(
            DatabaseName=GLUE_DATABASE,
            Name=SUMMARY_TABLE,
        )["Table"]

    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")

        if error_code == "EntityNotFoundException":
            print(
                f"Summary table {GLUE_DATABASE}.{SUMMARY_TABLE} "
                "does not exist yet. All source dates are candidates."
            )
            return set()

        raise

    partition_keys = [
        item["Name"]
        for item in table.get("PartitionKeys", [])
    ]

    if "interactiondate" not in partition_keys:
        raise ValueError(
            f"{SUMMARY_TABLE} must be partitioned by interactiondate. "
            f"Found partition keys: {partition_keys}"
        )

    date_index = partition_keys.index("interactiondate")
    summary_dates = set()

    paginator = glue.get_paginator("get_partitions")

    for page in paginator.paginate(
        DatabaseName=GLUE_DATABASE,
        TableName=SUMMARY_TABLE,
    ):
        for partition in page.get("Partitions", []):
            values = partition.get("Values", [])

            if len(values) > date_index:
                normalized = normalize_date(values[date_index])

                if normalized:
                    summary_dates.add(normalized)

    print(f"Registered summary dates: {sorted(summary_dates)}")

    return summary_dates


# ---------------------------------------------------------
# Find the earliest unprocessed date
# ---------------------------------------------------------

def fetch_missing_date(**context):
    """Discover source dates and compare them against summary partitions."""

    glue = get_aws_client("glue")

    source_dates = discover_source_dates()
    summary_dates = get_summary_dates(glue)

    missing_dates = source_dates - summary_dates

    print(f"Source date count: {len(source_dates)}")
    print(f"Processed date count: {len(summary_dates)}")
    print(f"Unprocessed dates: {sorted(missing_dates)}")

    if not missing_dates:
        print("No unprocessed interaction dates found.")
        return None

    selected_date = min(missing_dates)

    print(f"Selected interaction date: {selected_date}")

    return selected_date


# ---------------------------------------------------------
# Decide whether to run the summary job
# ---------------------------------------------------------

def select_interaction_date(**context):
    """Skip the summary job if all dates are already processed."""

    selected_date = context["ti"].xcom_pull(
        task_ids="fetch_missing_date"
    )

    if not selected_date:
        print(
            "No unprocessed interaction dates found. "
            "Skipping the CDE summary job."
        )
        return False

    print(f"Proceeding with interaction date: {selected_date}")
    return True


# ---------------------------------------------------------
# DAG definition
# ---------------------------------------------------------

with DAG(
    dag_id="callcenter_sync_prod_v1",
    description=(
        "Discover unprocessed Call Center dates from Parquet "
        "and run the existing CDE summary job."
    ),
    start_date=pendulum.datetime(2026, 6, 25, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["callcenter", "cde", "spark", "glue", "s3"],
) as dag:

    # Task 1: Discover source dates and identify the earliest missing date.
    fetch_missing = PythonOperator(
        task_id="fetch_missing_date",
        python_callable=fetch_missing_date,
    )

    # Task 2: Continue only when an unprocessed date is available.
    select_date = ShortCircuitOperator(
        task_id="select_interaction_date",
        python_callable=select_interaction_date,
    )

    # Task 3: Run the existing CDE summary job.
    run_summary = CdeRunJobOperator(
        task_id="run_callcenter_summary",
        job_name=SUMMARY_JOB_NAME,
        connection_id=CDE_CONN_ID,
        wait=True,
        overrides={
            "args": [
                "{{ ti.xcom_pull(task_ids='fetch_missing_date') }} "
                "--glue-database callcenter_data_s3 "
                "--source-table callcenter_interaction "
                "--summary-table callcenter_interaction_summary "
                "--aws-region us-west-1"
            ]
        },
    )

    fetch_missing >> select_date >> run_summary
