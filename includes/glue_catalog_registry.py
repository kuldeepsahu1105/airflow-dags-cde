"""Register curated vehicle-health Parquet datasets in AWS Glue Data Catalog for Trino."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from glue_aws_client import (
    AwsCredentials,
    GlueEntityNotFoundError,
    GlueRestClient,
    S3RestClient,
    resolve_default_aws_credentials,
)

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


@dataclass(frozen=True)
class GlueTableSpec:
    name: str
    columns_ddl: str
    partition_columns: Sequence[str]
    location_suffix: str


GLUE_DATABASE_DEFAULT = "vehicle_health_analytics"

CURATED_TABLE_SPECS: List[GlueTableSpec] = [
    GlueTableSpec(
        name="vehicle_telemetry_clean",
        columns_ddl="""
            source_file STRING,
            vin STRING,
            event_timestamp TIMESTAMP,
            event_date DATE,
            odometer_km DOUBLE,
            battery_voltage DOUBLE,
            engine_temp_c DOUBLE,
            oil_pressure_kpa DOUBLE,
            tire_pressure_fl DOUBLE,
            tire_pressure_fr DOUBLE,
            tire_pressure_rl DOUBLE,
            tire_pressure_rr DOUBLE,
            dtc_code STRING,
            severity STRING,
            source_zip STRING,
            pipeline_run_id STRING,
            ingested_at TIMESTAMP
        """,
        partition_columns=("business_date", "country"),
        location_suffix="vehicle_telemetry_clean",
    ),
    GlueTableSpec(
        name="service_events_clean",
        columns_ddl="""
            source_file STRING,
            service_id STRING,
            vin STRING,
            dealer_id STRING,
            service_open_timestamp TIMESTAMP,
            service_close_timestamp TIMESTAMP,
            service_date DATE,
            service_type STRING,
            warranty_flag BOOLEAN,
            labor_hours DOUBLE,
            parts_cost DECIMAL(12, 2),
            service_duration_hours DOUBLE,
            service_status STRING,
            source_zip STRING,
            pipeline_run_id STRING,
            ingested_at TIMESTAMP
        """,
        partition_columns=("business_date",),
        location_suffix="service_events_clean",
    ),
    GlueTableSpec(
        name="vehicle_health_enriched",
        columns_ddl="""
            source_file STRING,
            vin STRING,
            event_timestamp TIMESTAMP,
            event_date DATE,
            odometer_km DOUBLE,
            battery_voltage DOUBLE,
            engine_temp_c DOUBLE,
            oil_pressure_kpa DOUBLE,
            tire_pressure_fl DOUBLE,
            tire_pressure_fr DOUBLE,
            tire_pressure_rl DOUBLE,
            tire_pressure_rr DOUBLE,
            dtc_code STRING,
            severity STRING,
            country STRING,
            source_zip STRING,
            pipeline_run_id STRING,
            ingested_at TIMESTAMP,
            model_year INT,
            powertrain STRING,
            production_plant STRING,
            warranty_start_date DATE,
            customer_region STRING,
            dealer_id STRING,
            service_type STRING,
            service_status STRING,
            warranty_flag BOOLEAN,
            service_duration_hours DOUBLE,
            dealer_name STRING,
            region STRING,
            dealer_tier STRING,
            is_critical_event BOOLEAN,
            is_warning_event BOOLEAN
        """,
        partition_columns=("business_date", "model"),
        location_suffix="vehicle_health_enriched",
    ),
    GlueTableSpec(
        name="daily_vehicle_health_summary",
        columns_ddl="""
            event_date DATE,
            country STRING,
            customer_region STRING,
            model_year INT,
            powertrain STRING,
            severity STRING,
            telemetry_event_count BIGINT,
            affected_vehicle_count BIGINT,
            critical_dtc_count BIGINT,
            avg_odometer_km DOUBLE,
            max_odometer_km DOUBLE,
            avg_battery_voltage DOUBLE,
            avg_engine_temp_c DOUBLE,
            repeated_warning_vehicle_count BIGINT
        """,
        partition_columns=("business_date", "model"),
        location_suffix="daily_vehicle_health_summary",
    ),
    GlueTableSpec(
        name="service_kpi_summary",
        columns_ddl="""
            dealer_id STRING,
            dealer_name STRING,
            country STRING,
            dealer_tier STRING,
            model STRING,
            model_year INT,
            powertrain STRING,
            service_type STRING,
            service_event_count BIGINT,
            avg_service_duration_hours DOUBLE,
            avg_labor_hours DOUBLE,
            total_parts_cost DOUBLE,
            warranty_service_rate DOUBLE
        """,
        partition_columns=("business_date", "region"),
        location_suffix="service_kpi_summary",
    ),
    GlueTableSpec(
        name="rejects",
        columns_ddl="""
            pipeline_run_id STRING,
            reject_reason STRING,
            record_json STRING,
            rejected_at TIMESTAMP
        """,
        partition_columns=("business_date", "dataset_name"),
        location_suffix="rejects",
    ),
    GlueTableSpec(
        name="data_quality_report",
        columns_ddl="""
            dataset_name STRING,
            metric_name STRING,
            metric_value BIGINT,
            status STRING,
            pipeline_run_id STRING,
            reported_at TIMESTAMP
        """,
        partition_columns=("business_date",),
        location_suffix="data_quality_report",
    ),
]

GLUE_METASTORE_FACTORY = "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
PARQUET_STORAGE_DESCRIPTOR = {
    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    "SerdeInfo": {
        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
    },
}


def glue_spark_configs(region: str, warehouse_dir: str, catalog_id: Optional[str] = None) -> dict:
    """Spark/Hadoop configs to use AWS Glue Data Catalog as the Hive metastore."""
    configs = {
        "spark.hadoop.hive.metastore.client.factory.class": GLUE_METASTORE_FACTORY,
        "spark.hadoop.aws.region": region,
        "spark.sql.catalogImplementation": "hive",
        "spark.sql.warehouse.dir": warehouse_dir,
        "spark.hadoop.hive.metastore.warehouse.dir": warehouse_dir,
    }
    if catalog_id:
        configs["spark.hadoop.hive.metastore.glue.catalogid"] = catalog_id
    return configs


def _join_uri(base: str, suffix: str) -> str:
    return f"{base.rstrip('/')}/{suffix.lstrip('/')}"


def _column_lines(columns_ddl: str) -> List[str]:
    """Split DDL column lines on commas not nested inside parentheses."""
    lines: List[str] = []
    current: List[str] = []
    depth = 0
    for char in columns_ddl.strip():
        if char == "(":
            depth += 1
            current.append(char)
        elif char == ")":
            depth -= 1
            current.append(char)
        elif char == "," and depth == 0:
            line = "".join(current).strip()
            if line:
                lines.append(line)
            current = []
        else:
            current.append(char)
    line = "".join(current).strip()
    if line:
        lines.append(line)
    return lines


def _column_name(column_line: str) -> str:
    return column_line.split()[0].strip()


def _data_columns_ddl(columns_ddl: str, partition_columns: Sequence[str]) -> str:
    """Partition columns must appear only in PARTITIONED BY, not in the table body."""
    partition_set = set(partition_columns)
    kept = [
        line
        for line in _column_lines(columns_ddl)
        if _column_name(line) not in partition_set
    ]
    if not kept:
        raise ValueError("Table DDL has no data columns after excluding partition columns.")
    return ",\n    ".join(kept)


def _parse_hive_type(type_token: str) -> str:
    normalized = type_token.strip().upper()
    if normalized.startswith("DECIMAL"):
        match = re.match(r"DECIMAL\((\d+)\s*,\s*(\d+)\)", normalized)
        if match:
            return f"decimal({match.group(1)},{match.group(2)})"
    mapping = {
        "STRING": "string",
        "TIMESTAMP": "timestamp",
        "DATE": "date",
        "DOUBLE": "double",
        "BIGINT": "bigint",
        "INT": "int",
        "BOOLEAN": "boolean",
    }
    return mapping.get(normalized, "string")


def _glue_columns(columns_ddl: str, partition_columns: Sequence[str]) -> List[Dict[str, str]]:
    partition_set = set(partition_columns)
    columns: List[Dict[str, str]] = []
    for line in _column_lines(columns_ddl):
        name = _column_name(line)
        if name in partition_set:
            continue
        parts = line.split(None, 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid column DDL line: {line!r}")
        col_name, type_token = parts
        columns.append({"Name": col_name, "Type": _parse_hive_type(type_token)})
    if not columns:
        raise ValueError("Table DDL has no data columns after excluding partition columns.")
    return columns


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme not in {"s3", "s3a"}:
        raise ValueError(f"Expected s3/s3a URI, got: {uri}")
    bucket = parsed.netloc
    prefix = parsed.path.lstrip("/")
    if prefix and not prefix.endswith("/"):
        prefix += "/"
    return bucket, prefix


def _normalize_glue_location(uri: str) -> str:
    """Glue and Trino native S3 use s3://; Spark/Hadoop often use s3a://."""
    if uri.startswith("s3a://"):
        return "s3://" + uri[len("s3a://") :]
    return uri


def _resolve_env_credentials() -> Tuple[Optional[str], Optional[str], Optional[str]]:
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    session_token = os.environ.get("AWS_SESSION_TOKEN")
    return access_key, secret_key, session_token


def _build_aws_clients(
    region: str,
    aws_access_key: Optional[str] = None,
    aws_secret_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
) -> Tuple[GlueRestClient, S3RestClient]:
    if aws_access_key and aws_secret_key:
        credentials = AwsCredentials(
            access_key=aws_access_key,
            secret_key=aws_secret_key,
            region=region,
            session_token=aws_session_token,
        )
    else:
        credentials = resolve_default_aws_credentials(region)
    return GlueRestClient(credentials), S3RestClient(credentials)


def register_curated_tables_standalone(
    curated_s3_root: str,
    database: str = GLUE_DATABASE_DEFAULT,
    region: str = "us-east-1",
    aws_access_key: Optional[str] = None,
    aws_secret_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
) -> None:
    """Register external tables in AWS Glue (no Spark session required)."""
    glue_client, s3_client = _build_aws_clients(
        region,
        aws_access_key,
        aws_secret_key,
        aws_session_token,
    )
    warehouse_uri = _normalize_glue_location(_join_uri(curated_s3_root, "_warehouse") + "/")

    _ensure_glue_database(glue_client, database, warehouse_uri)

    for spec in CURATED_TABLE_SPECS:
        location = _normalize_glue_location(_join_uri(curated_s3_root, spec.location_suffix))
        print(f"Ensuring Glue table {database}.{spec.name} at {location}")
        _ensure_glue_table(glue_client, database, spec, location)
        _sync_partitions(glue_client, s3_client, database, spec, location)

    print(f"Glue Data Catalog registration completed via API for database {database}")


def _resolve_aws_credentials(
    spark: SparkSession,
    s3_uri: str,
    aws_access_key: Optional[str],
    aws_secret_key: Optional[str],
    aws_session_token: Optional[str],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if aws_access_key and aws_secret_key:
        return aws_access_key, aws_secret_key, aws_session_token

    bucket, _ = _parse_s3_uri(s3_uri)
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
    access_key = hadoop_conf.get(f"fs.s3a.bucket.{bucket}.access.key")
    secret_key = hadoop_conf.get(f"fs.s3a.bucket.{bucket}.secret.key")
    session_token = hadoop_conf.get(f"fs.s3a.bucket.{bucket}.session.token")
    if access_key and secret_key:
        return access_key, secret_key, session_token

    return None, None, None


def _ensure_glue_database(glue_client: GlueRestClient, database: str, location_uri: str) -> None:
    try:
        glue_client.get_database(database)
        print(f"Glue database already exists: {database}")
        return
    except GlueEntityNotFoundError:
        pass

    glue_client.create_database(
        {
            "Name": database,
            "Description": "Curated vehicle health analytics datasets",
            "LocationUri": location_uri,
        }
    )
    print(f"Created Glue database: {database}")


def _table_input(spec: GlueTableSpec, location: str) -> Dict[str, Any]:
    return {
        "Name": spec.name,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "EXTERNAL": "TRUE",
        },
        "StorageDescriptor": {
            **PARQUET_STORAGE_DESCRIPTOR,
            "Columns": _glue_columns(spec.columns_ddl, spec.partition_columns),
            "Location": _normalize_glue_location(location),
        },
        "PartitionKeys": [
            {"Name": column, "Type": "string"} for column in spec.partition_columns
        ],
    }


def _ensure_glue_table(
    glue_client: GlueRestClient,
    database: str,
    spec: GlueTableSpec,
    location: str,
) -> None:
    table_input = _table_input(spec, location)
    try:
        glue_client.get_table(database, spec.name)
        glue_client.update_table(database, table_input)
        print(f"Updated Glue table {database}.{spec.name}")
    except GlueEntityNotFoundError:
        glue_client.create_table(database, table_input)
        print(f"Created Glue table {database}.{spec.name}")


def _discover_partitions(
    s3_client: S3RestClient,
    bucket: str,
    prefix: str,
    partition_columns: Sequence[str],
) -> List[Dict[str, str]]:
    if not partition_columns:
        return []

    discovered: List[Dict[str, str]] = []

    def walk(current_prefix: str, depth: int, values: Dict[str, str]) -> None:
        if depth == len(partition_columns):
            discovered.append(dict(values))
            return

        column = partition_columns[depth]
        for common_prefix in s3_client.list_common_prefixes(bucket, current_prefix):
            segment = common_prefix[len(current_prefix) :].strip("/")
            if "=" not in segment:
                continue
            key, value = segment.split("=", 1)
            if key != column:
                continue
            walk(common_prefix, depth + 1, {**values, key: value})

    walk(prefix, 0, {})
    return discovered


def _existing_partition_values(glue_client: GlueRestClient, database: str, table: str) -> set:
    existing: set = set()
    next_token: Optional[str] = None
    while True:
        response = glue_client.get_partitions(database, table, next_token)
        for partition in response.get("Partitions", []):
            existing.add(tuple(partition["Values"]))
        next_token = response.get("NextToken")
        if not next_token:
            break
    return existing


def _sync_partitions(
    glue_client: GlueRestClient,
    s3_client: S3RestClient,
    database: str,
    spec: GlueTableSpec,
    location: str,
) -> None:
    bucket, table_prefix = _parse_s3_uri(location)
    discovered = _discover_partitions(s3_client, bucket, table_prefix, spec.partition_columns)
    if not discovered:
        print(f"No partitions discovered under {location}")
        return

    existing = _existing_partition_values(glue_client, database, spec.name)
    new_partitions: List[Dict[str, Any]] = []
    for values in discovered:
        ordered_values = tuple(values[column] for column in spec.partition_columns)
        if ordered_values in existing:
            continue
        partition_prefix = table_prefix + "/".join(
            f"{column}={values[column]}" for column in spec.partition_columns
        )
        if not partition_prefix.endswith("/"):
            partition_prefix += "/"
        new_partitions.append(
            {
                "Values": list(ordered_values),
                "StorageDescriptor": {
                    **PARQUET_STORAGE_DESCRIPTOR,
                    "Columns": _glue_columns(spec.columns_ddl, spec.partition_columns),
                    "Location": f"s3://{bucket}/{partition_prefix}",
                },
            }
        )

    if not new_partitions:
        print(f"Glue partitions already current for {database}.{spec.name}")
        return

    for index in range(0, len(new_partitions), 100):
        batch = new_partitions[index : index + 100]
        glue_client.batch_create_partition(database, spec.name, batch)
    print(f"Registered {len(new_partitions)} new partition(s) for {database}.{spec.name}")


def register_curated_tables_via_api(
    spark: SparkSession,
    curated_s3_root: str,
    database: str = GLUE_DATABASE_DEFAULT,
    region: str = "us-east-1",
    aws_access_key: Optional[str] = None,
    aws_secret_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
) -> None:
    """Register via Glue API using Spark Hadoop S3 config or platform credentials."""
    access_key, secret_key, session_token = _resolve_aws_credentials(
        spark,
        curated_s3_root,
        aws_access_key,
        aws_secret_key,
        aws_session_token,
    )
    if access_key and secret_key:
        credentials = AwsCredentials(
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            session_token=session_token,
        )
        glue_client = GlueRestClient(credentials)
        s3_client = S3RestClient(credentials)
        warehouse_uri = _join_uri(curated_s3_root, "_warehouse") + "/"
        _ensure_glue_database(glue_client, database, warehouse_uri)
        for spec in CURATED_TABLE_SPECS:
            location = _join_uri(curated_s3_root, spec.location_suffix)
            print(f"Ensuring Glue table {database}.{spec.name} at {location}")
            _ensure_glue_table(glue_client, database, spec, location)
            _sync_partitions(glue_client, s3_client, database, spec, location)
        print(f"Glue Data Catalog registration completed via API for database {database}")
        return

    register_curated_tables_standalone(curated_s3_root, database=database, region=region)


def _verify_glue_metastore(spark: SparkSession) -> None:
    """Fail fast when CDE falls back to embedded Derby instead of AWS Glue."""
    conf = spark.sparkContext._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
    factory = conf.get("hive.metastore.client.factory.class") or spark.conf.get(
        "spark.hadoop.hive.metastore.client.factory.class",
        "",
    )
    print(f"Hive metastore factory: {factory or '<not set>'}")
    if GLUE_METASTORE_FACTORY not in factory:
        raise RuntimeError(
            "AWS Glue Data Catalog is not active — Spark is using embedded Derby metastore. "
            "Use --glue-registration-mode api (default on CDE) or add AWS Glue catalog JARs. "
            "See trino/deploy/glue-setup.md."
        )


def _create_table_sql(database: str, spec: GlueTableSpec, location: str) -> str:
    partition_ddl = ", ".join(f"{column} STRING" for column in spec.partition_columns)
    data_columns = _data_columns_ddl(spec.columns_ddl, spec.partition_columns)
    return f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{spec.name} (
    {data_columns}
)
PARTITIONED BY ({partition_ddl})
STORED AS PARQUET
LOCATION '{location}'
""".strip()


def register_curated_tables_via_spark(
    spark: SparkSession,
    curated_s3_root: str,
    database: str = GLUE_DATABASE_DEFAULT,
) -> None:
    """Register external tables through Spark SQL + Hive Glue client (requires Glue JARs)."""
    _verify_glue_metastore(spark)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")

    for spec in CURATED_TABLE_SPECS:
        location = _join_uri(curated_s3_root, spec.location_suffix)
        ddl = _create_table_sql(database, spec, location)
        print(f"Ensuring Glue table {database}.{spec.name} at {location}")
        spark.sql(ddl)
        spark.sql(f"MSCK REPAIR TABLE {database}.{spec.name}")

    print(f"Glue Data Catalog registration completed via Spark for database {database}")


def register_curated_tables(
    spark: SparkSession,
    curated_s3_root: str,
    database: str = GLUE_DATABASE_DEFAULT,
    region: str = "us-east-1",
    registration_mode: str = "api",
    aws_access_key: Optional[str] = None,
    aws_secret_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
) -> None:
    """Register curated datasets in Glue using API (default) or Spark SQL."""
    mode = registration_mode.lower()
    if mode == "api":
        register_curated_tables_via_api(
            spark,
            curated_s3_root,
            database=database,
            region=region,
            aws_access_key=aws_access_key,
            aws_secret_key=aws_secret_key,
            aws_session_token=aws_session_token,
        )
        return
    if mode == "spark":
        register_curated_tables_via_spark(spark, curated_s3_root, database=database)
        return
    raise ValueError(f"Unsupported glue registration mode: {registration_mode}")
