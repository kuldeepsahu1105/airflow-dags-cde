"""Build Porsche Vehicle Health Iceberg analytics tables through Impala."""

from __future__ import annotations

from pathlib import Path

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

from pipeline_settings import (
    ANALYTICS_DATABASE,
    CDE_CONNECTION_ID,
    CDE_SPARK_JOB_NAME,
    CURATED_S3_ROOT,
    IMPALA_CONNECTION_ID,
    RAW_S3_ROOT,
    STAGING_DATABASE,
    TEMP_S3_URI,
)

SQL_DIR = Path(__file__).resolve().parent / "sql"
BUSINESS_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('business_date', macros.datetime.utcnow().strftime('%Y-%m-%d')) }}"
)
SPARK_JOB_ARGS_TEMPLATE = (
    "--input-s3-uri {{ params.raw_s3_root }}/vehicle_health_"
    "{{ dag_run.conf.get('business_date', macros.datetime.utcnow().strftime('%Y-%m-%d')) | replace('-', '_') }}.zip "
    "--output-s3-uri {{ params.curated_s3_root }} "
    "--business-date "
    + BUSINESS_DATE_TEMPLATE
    + " "
    "--temp-dir {{ params.temp_s3_uri }} "
    "--max-reject-rate 0.10 --write-format parquet"
)


with DAG(
    dag_id="porsche_vehicle_health_iceberg_analytics",
    start_date=pendulum.datetime(2026, 7, 9, tz="UTC"),
    schedule=None,
    catchup=False,
    template_searchpath=[str(SQL_DIR)],
    params={
        "analytics_database": Param(default=ANALYTICS_DATABASE, type="string"),
        "curated_s3_root": Param(default=CURATED_S3_ROOT, type="string"),
        "raw_s3_root": Param(default=RAW_S3_ROOT, type="string"),
        "staging_database": Param(default=STAGING_DATABASE, type="string"),
        "temp_s3_uri": Param(default=TEMP_S3_URI, type="string"),
    },
    tags=["porsche", "cde", "impala", "iceberg", "analytics"],
) as dag:
    create_iceberg_tables = SQLExecuteQueryOperator(
        task_id="create_iceberg_tables",
        conn_id=IMPALA_CONNECTION_ID,
        sql="create_iceberg_tables.sql",
        split_statements=True,
        return_last=False,
    )

    run_existing_spark_parquet_job = CdeRunJobOperator(
        task_id="run_existing_spark_parquet_job",
        connection_id=CDE_CONNECTION_ID,
        job_name=CDE_SPARK_JOB_NAME,
        wait=True,
        overrides={"args": [SPARK_JOB_ARGS_TEMPLATE]},
    )

    create_parquet_staging_tables = SQLExecuteQueryOperator(
        task_id="create_parquet_staging_tables",
        conn_id=IMPALA_CONNECTION_ID,
        sql="create_parquet_staging_tables.sql",
        split_statements=True,
        return_last=False,
    )

    load_iceberg_tables = SQLExecuteQueryOperator(
        task_id="load_iceberg_tables",
        conn_id=IMPALA_CONNECTION_ID,
        sql="load_iceberg_tables.sql",
        split_statements=True,
        return_last=False,
    )

    validate_iceberg_tables = SQLExecuteQueryOperator(
        task_id="validate_iceberg_tables",
        conn_id=IMPALA_CONNECTION_ID,
        sql="validate_iceberg_tables.sql",
        split_statements=True,
        return_last=False,
    )

    (
        create_iceberg_tables
        >> run_existing_spark_parquet_job
        >> create_parquet_staging_tables
        >> load_iceberg_tables
        >> validate_iceberg_tables
    )
