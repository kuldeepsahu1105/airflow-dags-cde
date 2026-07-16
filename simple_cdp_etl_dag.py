"""Simple CDP Public Cloud ETL DAG.

This DAG expects a raw sales CSV to already exist in the configured data lake
path. It runs a CDE Spark job and then uses the Airflow SQL operator to run
Data Hub Impala SQL over the curated Parquet output.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

DAG_ID = "simple_cdp_sales_etl"
CDE_CONN_ID = "sstcwocde"
CDE_JOB_NAME = "simple-cdp-sales-etl"
IMPALA_CONN_ID = "datahub_impala"


default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def create_sales_summary_sql() -> str:
    sql = """
    CREATE DATABASE IF NOT EXISTS {{ params.impala_database }};

    CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.impala_database }}.clean_sales (
        order_id STRING,
        customer_id STRING,
        product_name STRING,
        quantity INT,
        unit_price DOUBLE,
        total_amount DOUBLE
    )
    PARTITIONED BY (order_date DATE)
    STORED AS PARQUET
    LOCATION '{{ params.clean_sales_path }}';

    INVALIDATE METADATA {{ params.impala_database }}.clean_sales;

    ALTER TABLE {{ params.impala_database }}.clean_sales RECOVER PARTITIONS;

    DROP VIEW IF EXISTS {{ params.impala_database }}.sales_revenue_by_order_date;

    CREATE VIEW {{ params.impala_database }}.sales_revenue_by_order_date AS
    SELECT
        order_date,
        COUNT(DISTINCT order_id) AS order_count,
        SUM(quantity) AS units_sold,
        ROUND(SUM(total_amount), 2) AS total_revenue
    FROM {{ params.impala_database }}.clean_sales
    GROUP BY order_date;
    """
    return sql


def validate_runtime_config(**context) -> None:
    params = context["params"]
    raw_sales_path = params["raw_sales_path"]
    clean_sales_path = params["clean_sales_path"]
    impala_database = params["impala_database"]

    if not raw_sales_path.startswith("s3a://"):
        raise ValueError("raw_sales_path must start with s3a://")
    if not clean_sales_path.startswith("s3a://"):
        raise ValueError("clean_sales_path must start with s3a://")
    if not impala_database.strip():
        raise ValueError("impala_database is required")

    print(f"raw_sales_path={raw_sales_path}")
    print(f"clean_sales_path={clean_sales_path}")
    print(f"impala_database={impala_database}")


dag = DAG(
    dag_id=DAG_ID,
    description="Simple CDP ETL using CDE and SQL operators.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    params={
        "raw_sales_path": Param(
            "s3a://porscheenv-buk-3b2308dc/data/raw/",
            type="string",
            description="S3/data lake folder containing the raw sales CSV file.",
        ),
        "clean_sales_path": Param(
            "s3a://porscheenv-buk-3b2308dc/data/curated/sales_clean",
            type="string",
            description="S3/data lake path where Spark writes clean Parquet output.",
        ),
        "impala_database": Param(
            "retail_demo",
            type="string",
            description="Data Hub Impala database used by the SQL step.",
        ),
    },
    tags=["cdp", "cde", "spark", "impala", "example"],
)

validate_config = PythonOperator(
    task_id="01_validate_runtime_config",
    python_callable=validate_runtime_config,
    dag=dag,
)

run_cde_spark_transform = CdeRunJobOperator(
    task_id="02_run_cde_spark_transform",
    connection_id=CDE_CONN_ID,
    job_name=CDE_JOB_NAME,
    variables={
        "raw_sales_path": "{{ params.raw_sales_path }}",
        "clean_sales_path": "{{ params.clean_sales_path }}",
    },
    wait=True,
    dag=dag,
)

run_datahub_impala_sql = SQLExecuteQueryOperator(
    task_id="03_run_datahub_impala_sql",
    conn_id=IMPALA_CONN_ID,
    sql=create_sales_summary_sql(),
    split_statements=True,
    return_last=False,
    show_return_value_in_logs=True,
    dag=dag,
)

validate_config >> run_cde_spark_transform >> run_datahub_impala_sql
