"""Trigger the  vehicle telemetry health CDE Spark job."""

from __future__ import annotations

import pendulum
from airflow import DAG
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator


with DAG(
    dag_id="vehicle_telemetry_cde_orchestration",
    start_date=pendulum.datetime(2026, 6, 25, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["cde", "spark", "vehicle-health"],
) as dag:
    run_vehicle_health_pipeline = CdeRunJobOperator(
        task_id="run_vehicle_health_pipeline",
        job_name="spark-etl-pipeline",
        connection_id="awc-cde",
        wait=True,
    )
