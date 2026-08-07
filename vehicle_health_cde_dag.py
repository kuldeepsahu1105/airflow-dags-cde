"""Trigger vehicle health Spark ETL on CDE, then register Glue metadata in Airflow."""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

from glue_registration import register_vehicle_health_glue_metadata
from pipeline_settings import (
    AWS_REGION,
    CURATED_S3_ROOT,
    GLUE_DATABASE,
    RAW_S3_ROOT,
    TEMP_S3_URI,
)

BUSINESS_DATE_TEMPLATE = (
    "{{ dag_run.conf.get('business_date', macros.datetime.utcnow().strftime('%Y-%m-%d')) }}"
)
INPUT_S3_URI_TEMPLATE = (
    "{{ dag_run.conf.get('input_s3_uri', "
    "params.raw_s3_root ~ '/vehicle_health_' ~ "
    "dag_run.conf.get('business_date', macros.datetime.utcnow().strftime('%Y-%m-%d')) | replace('-', '_') ~ '.zip') }}"
)
SPARK_JOB_ARGS_TEMPLATE = (
    "--input-s3-uri "
    + INPUT_S3_URI_TEMPLATE
    + " "
    "--output-s3-uri {{ params.curated_s3_root }} "
    "--business-date "
    + BUSINESS_DATE_TEMPLATE
    + " "
    "--temp-dir {{ params.temp_s3_uri }} "
    "--aws-region {{ params.aws_region }} "
    "--skip-glue-registration "
    "--max-reject-rate 0.10 --write-format parquet"
)


with DAG(
    dag_id="vehicle_health_cde_orchestration",
    start_date=pendulum.datetime(2026, 6, 25, tz="UTC"),
    schedule=None,
    catchup=False,
    params={
        "aws_region": Param(default=AWS_REGION, type="string"),
        "curated_s3_root": Param(default=CURATED_S3_ROOT, type="string"),
        "glue_database": Param(default=GLUE_DATABASE, type="string"),
        "raw_s3_root": Param(default=RAW_S3_ROOT, type="string"),
        "temp_s3_uri": Param(default=TEMP_S3_URI, type="string"),
    },
    tags=["vehicle-health", "cde", "spark", "glue", "trino"],
) as dag:
    run_vehicle_health_pipeline = CdeRunJobOperator(
        task_id="run_vehicle_health_pipeline",
        job_name="spark-etl-pipeline",
        connection_id="awc-cde",
        wait=True,
        overrides={"args": [SPARK_JOB_ARGS_TEMPLATE]},
    )

    register_glue_metadata = PythonOperator(
        task_id="register_glue_metadata",
        python_callable=register_vehicle_health_glue_metadata,
    )

    run_vehicle_health_pipeline >> register_glue_metadata
