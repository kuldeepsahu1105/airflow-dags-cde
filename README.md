# airflow-dags-cde

Airflow DAGs for CDP/CDE pipelines focused on vehicle-health and sample ETL workflows.

## Repository structure

```text
.
├── glue_registration.py
├── pipeline_settings.py
├── porsche_vehicle_health_iceberg_analytics_dag.py
├── simple_cdp_etl_dag.py
├── vehicle_health_cde_dag.py
├── includes/
└── sql/
```

## Latest code structure

### `vehicle_health_cde_dag.py`
Main orchestration DAG for the current vehicle-health pipeline.

Flow:
1. Runs the CDE Spark ETL job `spark-etl-pipeline`
2. Passes runtime arguments for input S3 path, output S3 path, business date, region, temp directory, and Glue skip flag
3. Calls an Airflow Python task to register Glue metadata after Spark finishes

Key objects:
- `vehicle_health_cde_orchestration`
- `run_vehicle_health_pipeline`
- `register_glue_metadata`

### `glue_registration.py`
Glue registration helper invoked by the DAG.

Responsibilities:
- Loads helper modules from `includes/`
- Reads AWS credentials from the Airflow connection `aws_glue`
- Uses the configured region and curated S3 root
- Registers curated vehicle-health tables in AWS Glue

### `pipeline_settings.py`
Shared configuration for vehicle-health paths and AWS settings.

Contains:
- S3 bucket and region constants
- Raw, curated, and temp S3 locations
- Glue database name
- Airflow connection ID for AWS/Glue access
- `input_zip_uri(business_date)` helper for building the raw input file path

### `porsche_vehicle_health_iceberg_analytics_dag.py`
Repo-specific analytics DAG for Porsche vehicle-health data.

Flow:
1. Creates Iceberg tables in Impala
2. Runs the existing Spark ETL job
3. Creates Parquet staging tables
4. Loads Iceberg tables
5. Validates the final tables

Key objects:
- `porsche_vehicle_health_iceberg_analytics`
- `create_iceberg_tables`
- `run_existing_spark_parquet_job`
- `create_parquet_staging_tables`
- `load_iceberg_tables`
- `validate_iceberg_tables`

### `simple_cdp_etl_dag.py`
Standalone example ETL DAG.

Flow:
1. Validates runtime configuration
2. Runs a CDE Spark transform
3. Executes Impala SQL to create a summary view

Key objects:
- `simple_cdp_sales_etl`
- `validate_config`
- `run_cde_spark_transform`
- `run_datahub_impala_sql`

### `includes/`
Support modules used by Glue registration and related helpers.

### `sql/`
SQL files used by the analytics DAG, including table creation, loading, and validation scripts.

## High-level architecture

The repository currently centers on three patterns:

- **CDE Spark orchestration** for ETL processing
- **Airflow Python tasks** for metadata registration and validation
- **Impala SQL tasks** for warehouse/analytics table setup and checks

## Typical vehicle-health flow

The latest vehicle-health pipeline is:

1. Resolve input and output locations from `pipeline_settings.py`
2. Run the Spark job in CDE via Airflow
3. Register the curated output in AWS Glue
4. Make the data available for downstream query engines such as Trino

## Notes

- All code in this repository is Python.
- DAGs are configured with `schedule=None`, so they run manually or from external triggers.
- Runtime behavior is controlled through Airflow `params` and `dag_run.conf` values such as `business_date`.
