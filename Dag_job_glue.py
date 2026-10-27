"""Select the earliest unprocessed Call Center date and run the CDE Spark job."""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import (
    BranchPythonOperator,
    PythonOperator,
    ShortCircuitOperator,
)
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator


AWS_CONN_ID = "aws_glue"
CDE_CONN_ID = "awc-cde"
AWS_REGION = "us-west-1"

GLUE_DATABASE = "callcenter_data_s3"
SOURCE_TABLE = "callcenter_interaction"
SUMMARY_TABLE = "callcenter_interaction_summary"


def get_glue_client():
    """Create a Glue client using the Airflow AWS connection."""
    hook = AwsBaseHook(
        aws_conn_id=AWS_CONN_ID,
        client_type="glue",
        region_name=AWS_REGION,
    )
    return hook.get_client_type("glue")


def get_table_partitions(client, table_name):
    """Return registered interactiondate partitions."""
    paginator = client.get_paginator("get_partitions")

    dates = set()

    for page in paginator.paginate(
        DatabaseName=GLUE_DATABASE,
        TableName=table_name,
    ):
        for partition in page.get("Partitions", []):
            values = partition.get("Values", [])
            if values:
                # Assumes interactiondate is the first partition key.
                dates.add(values[0])

    return dates


def check_summary_table(**context):
    """Choose initial processing or missing-date discovery."""
    client = get_glue_client()

    try:
        client.get_table(
            DatabaseName=GLUE_DATABASE,
            Name=SUMMARY_TABLE,
        )
        return "fetch_missing_date"
    except client.exceptions.EntityNotFoundException:
        return "fetch_initial_date"


def fetch_initial_date(**context):
    """Select the earliest available source partition."""
    client = get_glue_client()

    source_dates = get_table_partitions(client, SOURCE_TABLE)

    return min(source_dates) if source_dates else None


def fetch_missing_date(**context):
    """Select the earliest source partition absent from the summary."""
    client = get_glue_client()

    source_dates = get_table_partitions(client, SOURCE_TABLE)
    summary_dates = get_table_partitions(client, SUMMARY_TABLE)

    missing_dates = source_dates - summary_dates

    return min(missing_dates) if missing_dates else None


def select_interaction_date(**context):
    """Read the selected date from whichever branch ran."""
    ti = context["ti"]

    selected_date = ti.xcom_pull(task_ids="fetch_missing_date")
    if selected_date is None:
        selected_date = ti.xcom_pull(task_ids="fetch_initial_date")

    if not selected_date:
        print("No unprocessed interaction dates found. Skipping CDE job.")
        return False

    print(f"Selected interaction date: {selected_date}")
    return selected_date


with DAG(
    dag_id="callcenter_sync_prod_v1",
    start_date=pendulum.datetime(2026, 6, 25, tz="UTC"),
    schedule=None,
    catchup=False,
    params={
        "aws_region": Param(default=AWS_REGION, type="string"),
    },
    tags=["callcenter", "cde", "spark", "glue", "s3"],
) as dag:

    check_table = BranchPythonOperator(
        task_id="check_summary_table",
        python_callable=check_summary_table,
    )

    fetch_missing = PythonOperator(
        task_id="fetch_missing_date",
        python_callable=fetch_missing_date,
    )

    fetch_initial = PythonOperator(
        task_id="fetch_initial_date",
        python_callable=fetch_initial_date,
    )

    select_date = ShortCircuitOperator(
        task_id="select_interaction_date",
        python_callable=select_interaction_date,
        trigger_rule="none_failed_min_one_success",
    )

    run_summary = CdeRunJobOperator(
        task_id="run_callcenter_summary",
        job_name="CallCenterSummary",
        connection_id=CDE_CONN_ID,
        wait=True,
        overrides={
            "args": [
                "{{ ti.xcom_pull(task_ids='select_interaction_date') }} "
                "--glue-database callcenter_data_s3 "
                "--source-table callcenter_interaction "
                "--summary-table callcenter_interaction_summary "
                "--aws-region us-west-1"
            ]
        },
    )

    check_table >> [fetch_missing, fetch_initial]
    [fetch_missing, fetch_initial] >> select_date
    select_date >> run_summary