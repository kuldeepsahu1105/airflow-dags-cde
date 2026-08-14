"""Ingest a zip archive from S3, transform vehicle health CSVs with Spark,
write curated Parquet to S3, and register tables in AWS Glue Data Catalog."""

from __future__ import annotations

import argparse
import os
import re
import shlex
import shutil
import sys
import tempfile
import uuid
import zipfile
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


GLUE_DATABASE_DEFAULT = "vehicle_health_analytics"


REQUIRED_FILES = {
    "telemetry": "vehicle_telemetry.csv",
    "service_events": "service_events.csv",
    "vehicle_master": "vehicle_master.csv",
    "dealer_master": "dealer_master.csv",
}

TELEMETRY_COLUMNS = [
    "vin",
    "event_timestamp",
    "odometer_km",
    "battery_voltage",
    "engine_temp_c",
    "oil_pressure_kpa",
    "tire_pressure_fl",
    "tire_pressure_fr",
    "tire_pressure_rl",
    "tire_pressure_rr",
    "dtc_code",
    "severity",
    "country",
]

SERVICE_COLUMNS = [
    "service_id",
    "vin",
    "dealer_id",
    "service_open_timestamp",
    "service_close_timestamp",
    "service_type",
    "warranty_flag",
    "labor_hours",
    "parts_cost",
    "service_status",
]

VEHICLE_MASTER_COLUMNS = [
    "vin",
    "model",
    "model_year",
    "powertrain",
    "production_plant",
    "warranty_start_date",
    "customer_region",
]

DEALER_MASTER_COLUMNS = [
    "dealer_id",
    "dealer_name",
    "country",
    "region",
    "dealer_tier",
]

VALID_SEVERITIES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
VALID_SERVICE_STATUSES = ["OPEN", "IN_PROGRESS", "CLOSED", "CANCELLED"]
VALID_POWERTRAINS = ["ICE", "HYBRID", "EV", "PHEV"]
NULL_MARKERS = ["", "NA", "N/A", "NULL", "NONE", "UNKNOWN"]

ENRICHED_WRITE_COLUMNS = [
    "source_file",
    "vin",
    "event_timestamp",
    "event_date",
    "odometer_km",
    "battery_voltage",
    "engine_temp_c",
    "oil_pressure_kpa",
    "tire_pressure_fl",
    "tire_pressure_fr",
    "tire_pressure_rl",
    "tire_pressure_rr",
    "dtc_code",
    "severity",
    "country",
    "business_date",
    "source_zip",
    "pipeline_run_id",
    "ingested_at",
    "model_year",
    "powertrain",
    "production_plant",
    "warranty_start_date",
    "customer_region",
    "dealer_id",
    "service_type",
    "service_status",
    "warranty_flag",
    "service_duration_hours",
    "dealer_name",
    "region",
    "dealer_tier",
    "is_critical_event",
    "is_warning_event",
    "model",
]

DAILY_SUMMARY_WRITE_COLUMNS = [
    "business_date",
    "event_date",
    "country",
    "customer_region",
    "model_year",
    "powertrain",
    "severity",
    "telemetry_event_count",
    "affected_vehicle_count",
    "critical_dtc_count",
    "avg_odometer_km",
    "max_odometer_km",
    "avg_battery_voltage",
    "avg_engine_temp_c",
    "repeated_warning_vehicle_count",
    "model",
]

SERVICE_KPI_WRITE_COLUMNS = [
    "business_date",
    "dealer_id",
    "dealer_name",
    "country",
    "dealer_tier",
    "model",
    "model_year",
    "powertrain",
    "service_type",
    "service_event_count",
    "avg_service_duration_hours",
    "avg_labor_hours",
    "total_parts_cost",
    "warranty_service_rate",
    "region",
]

QUALITY_REPORT_WRITE_COLUMNS = [
    "dataset_name",
    "metric_name",
    "metric_value",
    "status",
    "business_date",
    "pipeline_run_id",
    "reported_at",
]
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean and curate vehicle health CSV data from a zip archive."
    )
    parser.add_argument("--input-s3-uri", required=True, help="Input zip path, for example s3a://bucket/raw/file.zip.")
    parser.add_argument("--output-s3-uri", required=True, help="Curated output root, for example s3a://bucket/curated/.")
    parser.add_argument(
        "--business-date",
        default=None,
        help=(
            "Business date in YYYY-MM-DD format. "
            "If omitted, the job uses the UTC current date. "
            "Airflow should always pass this explicitly."
        ),
    )
    parser.add_argument(
        "--temp-dir",
        default=None,
        help="Distributed staging directory. Defaults to <output-s3-uri>/_staging/<run_id>.",
    )
    parser.add_argument(
        "--write-format",
        default="parquet",
        choices=["parquet", "csv"],
        help="Curated output format. Parquet is the production default; CSV remains available for demos.",
    )
    parser.add_argument("--write-mode", default="overwrite", choices=["overwrite", "append", "errorifexists", "ignore"])
    parser.add_argument("--csv-delimiter", default=",")
    parser.add_argument("--max-reject-rate", type=float, default=0.10, help="Fail if rejects exceed this ratio.")
    parser.add_argument("--keep-temp", action="store_true", help="Keep staged extracted CSV files for debugging.")
    parser.add_argument(
        "--aws-access-key-env",
        default="VEHICLE_HEALTH_AWS_ACCESS_KEY_ID",
        help="Environment variable containing the AWS access key for the target S3 bucket.",
    )
    parser.add_argument(
        "--aws-secret-key-env",
        default="VEHICLE_HEALTH_AWS_SECRET_ACCESS_KEY",
        help="Environment variable containing the AWS secret access key for the target S3 bucket.",
    )
    parser.add_argument(
        "--aws-session-token-env",
        default="VEHICLE_HEALTH_AWS_SESSION_TOKEN",
        help="Optional environment variable containing an AWS session token.",
    )
    parser.add_argument("--aws-region", default="us-east-1", help="AWS region for S3 and Glue Data Catalog.")
    parser.add_argument(
        "--glue-database",
        default=GLUE_DATABASE_DEFAULT,
        help="Glue database where curated external tables are registered.",
    )
    parser.add_argument(
        "--glue-catalog-id",
        default=None,
        help="Optional AWS account ID when using a cross-account Glue catalog.",
    )
    parser.add_argument(
        "--glue-registration-mode",
        default="api",
        choices=["api", "spark"],
        help="Register Glue tables via boto3 API (default, no Java JARs) or Spark SQL (needs Glue JARs).",
    )
    parser.add_argument(
        "--skip-glue-registration",
        action="store_true",
        help="Skip Glue Data Catalog registration (useful for local runs without AWS Glue).",
    )
    argv = sys.argv[1:]
    if len(argv) == 1 and argv[0].lstrip().startswith("--"):
        argv = shlex.split(argv[0])
    return parser.parse_args(argv)


def create_spark_session(args: argparse.Namespace) -> SparkSession:
    # CDE sets fs.s3a.committer.name=magic (MagicCommitter), which does not support
    # spark.sql.sources.partitionOverwriteMode=dynamic. Writes use replaceWhere instead.
    builder = (
        SparkSession.builder.appName("vehicle-health-cde-pipeline")
        .config("spark.sql.session.timeZone", "UTC")
    )
    if not args.skip_glue_registration and args.glue_registration_mode == "spark":
        from glue_catalog_registry import glue_spark_configs

        warehouse_dir = join_uri(args.output_s3_uri, "_warehouse")
        builder = builder.enableHiveSupport()
        for key, value in glue_spark_configs(args.aws_region, warehouse_dir, args.glue_catalog_id).items():
            builder = builder.config(key, value)
    return builder.getOrCreate()


def print_aws_execution_context(spark: SparkSession) -> None:
    """Print how this Spark driver authenticates to AWS (no boto3 required)."""
    print("=== AWS execution context (CDE Spark driver) ===")
    for env_name in (
        "AWS_ROLE_ARN",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_DEFAULT_REGION",
        "AWS_REGION",
        "AWS_SESSION_TOKEN",
        "VEHICLE_HEALTH_AWS_ACCESS_KEY_ID",
    ):
        value = os.environ.get(env_name)
        if value:
            if "TOKEN" in env_name or "VEHICLE_HEALTH" in env_name:
                print(f"{env_name} (env): <set, length={len(value)}>")
            else:
                print(f"{env_name}: {value}")

    for env_name in (
        "VEHICLE_HEALTH_AWS_ACCESS_KEY_ID",
        "VEHICLE_HEALTH_AWS_SECRET_ACCESS_KEY",
    ):
        value = _credential_from_spark_conf(spark, env_name)
        if value:
            print(f"{env_name} (spark conf): <set, length={len(value)}>")

    for bucket in ["aws-ccf-ixen-backup"]:
        if _bucket_s3a_credentials_configured(spark, bucket):
            print(f"S3A Hadoop config: fs.s3a.bucket.{bucket}.access.key is set")

    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    if access_key:
        print(f"AWS_ACCESS_KEY_ID prefix: {access_key[:4]}**** (length={len(access_key)})")
        print(
            "CDE injected static access keys (not AWS_ROLE_ARN). "
            "Find this key in IAM → Users → Security credentials → Access keys."
        )

    role_arn = os.environ.get("AWS_ROLE_ARN")
    if role_arn:
        print(f"Likely IAM role for S3/Glue: {role_arn}")

    try:
        import boto3

        identity = boto3.client("sts").get_caller_identity()
        print(f"STS caller identity: {identity}")
        print("=== end AWS execution context ===")
        return
    except Exception as exc:
        print(f"STS via boto3 unavailable: {exc}")

    try:
        jvm = spark._jvm  # type: ignore[attr-defined]
        builder = jvm.com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
        sts = builder.standard().build()
        request = jvm.com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest()
        identity = sts.getCallerIdentity(request)
        print(f"STS caller ARN: {identity.getArn()}")
        print(f"STS account: {identity.getAccount()}")
    except Exception as exc:
        print(f"STS via AWS Java SDK unavailable: {exc}")

    print("=== end AWS execution context ===")


def s3_bucket_name(uri: Optional[str]) -> Optional[str]:
    if not uri or not is_remote_uri(uri):
        return None
    parsed = urlparse(uri)
    if parsed.scheme not in {"s3", "s3a"}:
        return None
    return parsed.netloc


def _credential_from_spark_conf(spark: SparkSession, env_var_name: str) -> Optional[str]:
    conf = spark.sparkContext.getConf()
    for key in (
        f"spark.driverEnv.{env_var_name}",
        f"spark.executorEnv.{env_var_name}",
        env_var_name,
    ):
        value = conf.get(key, None)
        if value:
            return value
    return None


def _resolve_aws_credentials(
    spark: SparkSession,
    args: argparse.Namespace,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    access_key = os.environ.get(args.aws_access_key_env)
    secret_key = os.environ.get(args.aws_secret_key_env)
    session_token = os.environ.get(args.aws_session_token_env)

    if not access_key:
        access_key = _credential_from_spark_conf(spark, args.aws_access_key_env)
    if not secret_key:
        secret_key = _credential_from_spark_conf(spark, args.aws_secret_key_env)
    if not session_token:
        session_token = _credential_from_spark_conf(spark, args.aws_session_token_env)

    return access_key, secret_key, session_token


def _bucket_s3a_credentials_configured(spark: SparkSession, bucket: str) -> bool:
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
    return bool(hadoop_conf.get(f"fs.s3a.bucket.{bucket}.access.key"))


def configure_s3_credentials(spark: SparkSession, args: argparse.Namespace) -> None:
    """Configure bucket-specific S3A credentials from env vars, Spark conf, or pre-set Hadoop config."""
    buckets = sorted(
        {
            bucket
            for bucket in [
                s3_bucket_name(args.input_s3_uri),
                s3_bucket_name(args.output_s3_uri),
                s3_bucket_name(args.temp_dir),
            ]
            if bucket
        }
    )
    if not buckets:
        return

    preconfigured = [bucket for bucket in buckets if _bucket_s3a_credentials_configured(spark, bucket)]
    if preconfigured:
        print(
            "S3A bucket credentials already configured via Spark Hadoop config for: "
            + ", ".join(preconfigured)
        )
        return

    access_key, secret_key, session_token = _resolve_aws_credentials(spark, args)

    if not access_key or not secret_key:
        print(
            "No vehicle-health AWS credentials found in env vars or Spark conf; "
            "using the CDE runtime default S3 credentials."
        )
        print(
            "Tip: set spark.hadoop.fs.s3a.bucket.aws-ccf-ixen-backup.access.key and "
            ".secret.key in Spark Configurations, or VEHICLE_HEALTH_AWS_* env vars."
        )
        return

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
    provider = (
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
        if session_token
        else "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

    for bucket in buckets:
        prefix = f"fs.s3a.bucket.{bucket}."
        hadoop_conf.set(prefix + "access.key", access_key)
        hadoop_conf.set(prefix + "secret.key", secret_key)
        hadoop_conf.set(prefix + "aws.credentials.provider", provider)
        hadoop_conf.set(prefix + "endpoint", f"s3.{args.aws_region}.amazonaws.com")
        if session_token:
            hadoop_conf.set(prefix + "session.token", session_token)

    print(f"Configured explicit S3A credentials for buckets: {', '.join(buckets)}")


def schema_for(columns: Sequence[str]) -> StructType:
    # Read dirty raw values as strings, then clean and cast with Spark functions.
    return StructType([StructField(column, StringType(), True) for column in columns])


def is_remote_uri(uri: str) -> bool:
    return "://" in uri and not uri.startswith("file://")


def strip_file_scheme(uri: str) -> str:
    return uri[7:] if uri.startswith("file://") else uri


def join_uri(*parts: str) -> str:
    if not parts:
        return ""
    head = parts[0].rstrip("/")
    tail = [part.strip("/") for part in parts[1:] if part]
    return "/".join([head, *tail])


def resolve_business_date(business_date: Optional[str]) -> str:
    if business_date:
        return date.fromisoformat(business_date).isoformat()

    return datetime.now(timezone.utc).date().isoformat()


def hadoop_path(spark: SparkSession, uri: str):
    return spark._jvm.org.apache.hadoop.fs.Path(uri)  # type: ignore[attr-defined]


def hadoop_fs(spark: SparkSession, uri: str):
    jvm = spark._jvm  # type: ignore[attr-defined]
    conf = spark.sparkContext._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
    return jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(uri), conf)


def copy_input_zip_to_local(spark: SparkSession, input_uri: str, local_zip_path: str) -> None:
    if is_remote_uri(input_uri):
        fs = hadoop_fs(spark, input_uri)
        fs.copyToLocalFile(False, hadoop_path(spark, input_uri), hadoop_path(spark, local_zip_path), True)
        return

    shutil.copyfile(strip_file_scheme(input_uri), local_zip_path)


def copy_local_file_to_remote(spark: SparkSession, local_path: str, destination_uri: str) -> None:
    jvm = spark._jvm  # type: ignore[attr-defined]
    conf = spark.sparkContext._jsc.hadoopConfiguration()  # type: ignore[attr-defined]
    local_fs = jvm.org.apache.hadoop.fs.FileSystem.getLocal(conf)
    remote_fs = hadoop_fs(spark, destination_uri)
    input_stream = local_fs.open(hadoop_path(spark, local_path))
    output_stream = remote_fs.create(hadoop_path(spark, destination_uri), True)
    jvm.org.apache.hadoop.io.IOUtils.copyBytes(input_stream, output_stream, conf, True)


def extract_zip(local_zip_path: str, extract_dir: str) -> Dict[str, str]:
    with zipfile.ZipFile(local_zip_path) as archive:
        bad_member = archive.testzip()
        if bad_member:
            raise ValueError(f"Zip archive is corrupt at member: {bad_member}")

        csv_members = [member for member in archive.namelist() if member.lower().endswith(".csv")]
        if not csv_members:
            raise ValueError("Zip archive does not contain any CSV files.")

        archive.extractall(extract_dir, csv_members)

    discovered = {
        path.name: str(path)
        for path in Path(extract_dir).rglob("*.csv")
        if path.is_file()
    }
    missing = sorted(set(REQUIRED_FILES.values()) - set(discovered))
    if missing:
        raise ValueError(f"Zip archive is missing required CSV files: {missing}")
    return discovered


def upload_extracted_csvs(
    spark: SparkSession,
    discovered_files: Dict[str, str],
    staging_base_uri: str,
    pipeline_run_id: str,
) -> Tuple[Dict[str, str], str]:
    stage_uri = join_uri(staging_base_uri, pipeline_run_id)

    if is_remote_uri(stage_uri):
        fs = hadoop_fs(spark, stage_uri)
        fs.delete(hadoop_path(spark, stage_uri), True)
        for file_name, local_path in discovered_files.items():
            destination = join_uri(stage_uri, file_name)
            copy_local_file_to_remote(spark, local_path, destination)
        return {file_name: join_uri(stage_uri, file_name) for file_name in discovered_files}, stage_uri

    local_stage = Path(strip_file_scheme(stage_uri))
    if local_stage.exists():
        shutil.rmtree(local_stage)
    local_stage.mkdir(parents=True, exist_ok=True)
    staged_files = {}
    for file_name, local_path in discovered_files.items():
        destination = local_stage / file_name
        shutil.copyfile(local_path, destination)
        staged_files[file_name] = str(destination)
    return staged_files, str(local_stage)


def cleanup_staging(spark: SparkSession, stage_uri: str, keep_temp: bool) -> None:
    if keep_temp:
        return
    if is_remote_uri(stage_uri):
        fs = hadoop_fs(spark, stage_uri)
        fs.delete(hadoop_path(spark, stage_uri), True)
    else:
        shutil.rmtree(strip_file_scheme(stage_uri), ignore_errors=True)


def read_csv(
    spark: SparkSession,
    path: str,
    columns: Sequence[str],
    source_file_name: str,
    delimiter: str,
) -> DataFrame:
    return (
        spark.read.schema(schema_for(columns))
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("delimiter", delimiter)
        .csv(path)
        .withColumn("source_file", F.lit(source_file_name))
    )


def normalize_string_columns(df: DataFrame) -> DataFrame:
    result = df
    for name, dtype in result.dtypes:
        if dtype == "string":
            cleaned = F.trim(F.col(name))
            result = result.withColumn(
                name,
                F.when(F.upper(cleaned).isin(NULL_MARKERS), F.lit(None)).otherwise(cleaned),
            )
    return result


def normalized_vin(column_name: str) -> F.Column:
    return F.upper(F.regexp_replace(F.trim(F.col(column_name)), r"[^A-Za-z0-9]", ""))


def quote_identifier(column_name: str) -> str:
    return f"`{column_name.replace('`', '``')}`"


def clean_number(column_name: str, target_type: str = "double") -> F.Column:
    column = quote_identifier(column_name)
    return F.expr(f"try_cast(regexp_replace({column}, '[^0-9.\\\\-]', '') as {target_type})")


def parse_timestamp(column_name: str) -> F.Column:
    column = quote_identifier(column_name)
    return F.coalesce(
        F.expr(f"try_cast({column} as timestamp)"),
        F.expr(f"try_to_timestamp({column}, 'yyyy-MM-dd HH:mm:ss')"),
    )


def parse_date(column_name: str) -> F.Column:
    column = quote_identifier(column_name)
    return F.coalesce(
        F.expr(f"try_cast({column} as date)"),
        F.to_date(F.expr(f"try_to_timestamp({column}, 'yyyy-MM-dd')")),
    )


def add_reject_reasons(df: DataFrame, checks: Sequence[Tuple[str, F.Column]]) -> DataFrame:
    reason_array = F.array(*[F.when(condition, F.lit(reason)) for reason, condition in checks])
    return (
        df.withColumn("reject_reasons", F.array_except(reason_array, F.array(F.lit(None).cast("string"))))
        .withColumn("reject_reason", F.concat_ws("|", F.col("reject_reasons")))
    )


def split_valid_invalid(df: DataFrame, checks: Sequence[Tuple[str, F.Column]]) -> Tuple[DataFrame, DataFrame]:
    checked = add_reject_reasons(df, checks)
    valid = checked.filter(F.size("reject_reasons") == 0).drop("reject_reasons", "reject_reason")
    invalid = checked.filter(F.size("reject_reasons") > 0)
    return valid, invalid


def reject_output(df: DataFrame, dataset_name: str, pipeline_run_id: str, business_date: str) -> DataFrame:
    raw_columns = [column for column in df.columns if column not in {"reject_reasons"}]
    return df.select(
        F.lit(dataset_name).alias("dataset_name"),
        F.lit(business_date).alias("business_date"),
        F.lit(pipeline_run_id).alias("pipeline_run_id"),
        F.col("reject_reason"),
        F.to_json(F.struct(*[F.col(column) for column in raw_columns])).alias("record_json"),
        F.current_timestamp().alias("rejected_at"),
    )


def clean_telemetry(df: DataFrame, business_date: str, pipeline_run_id: str, source_zip: str) -> Tuple[DataFrame, DataFrame]:
    cleaned = (
        normalize_string_columns(df)
        .withColumn("vin", normalized_vin("vin"))
        .withColumn("event_timestamp", parse_timestamp("event_timestamp"))
        .withColumn("event_date", F.to_date("event_timestamp"))
        .withColumn("odometer_km", clean_number("odometer_km"))
        .withColumn("battery_voltage", clean_number("battery_voltage"))
        .withColumn("engine_temp_c", clean_number("engine_temp_c"))
        .withColumn("oil_pressure_kpa", clean_number("oil_pressure_kpa"))
        .withColumn("tire_pressure_fl", clean_number("tire_pressure_fl"))
        .withColumn("tire_pressure_fr", clean_number("tire_pressure_fr"))
        .withColumn("tire_pressure_rl", clean_number("tire_pressure_rl"))
        .withColumn("tire_pressure_rr", clean_number("tire_pressure_rr"))
        .withColumn("dtc_code", F.upper(F.col("dtc_code")))
        .withColumn("severity", F.upper(F.col("severity")))
        .withColumn("country", F.upper(F.col("country")))
        .withColumn("business_date", F.lit(business_date))
        .withColumn("source_zip", F.lit(source_zip))
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("ingested_at", F.current_timestamp())
        .dropDuplicates(["vin", "event_timestamp", "dtc_code"])
    )
    checks = [
        ("missing_vin", F.col("vin").isNull()),
        ("invalid_vin_length", F.length("vin") != 17),
        ("missing_event_timestamp", F.col("event_timestamp").isNull()),
        ("missing_odometer", F.col("odometer_km").isNull()),
        ("negative_odometer", F.col("odometer_km") < 0),
        ("unrealistic_odometer", F.col("odometer_km") > 500000),
        ("missing_battery_voltage", F.col("battery_voltage").isNull()),
        ("invalid_battery_voltage", ~F.col("battery_voltage").between(8, 16)),
        ("missing_engine_temperature", F.col("engine_temp_c").isNull()),
        ("invalid_engine_temperature", ~F.col("engine_temp_c").between(-40, 160)),
        ("missing_oil_pressure", F.col("oil_pressure_kpa").isNull()),
        ("invalid_oil_pressure", ~F.col("oil_pressure_kpa").between(0, 900)),
        ("missing_tire_pressure", F.col("tire_pressure_fl").isNull()),
        ("invalid_tire_pressure", ~F.col("tire_pressure_fl").between(15, 60)),
        ("missing_tire_pressure", F.col("tire_pressure_fr").isNull()),
        ("invalid_tire_pressure", ~F.col("tire_pressure_fr").between(15, 60)),
        ("missing_tire_pressure", F.col("tire_pressure_rl").isNull()),
        ("invalid_tire_pressure", ~F.col("tire_pressure_rl").between(15, 60)),
        ("missing_tire_pressure", F.col("tire_pressure_rr").isNull()),
        ("invalid_tire_pressure", ~F.col("tire_pressure_rr").between(15, 60)),
        ("missing_severity", F.col("severity").isNull()),
        ("invalid_severity", ~F.col("severity").isin(VALID_SEVERITIES)),
    ]
    return split_valid_invalid(cleaned, checks)


def clean_service_events(df: DataFrame, business_date: str, pipeline_run_id: str, source_zip: str) -> Tuple[DataFrame, DataFrame]:
    cleaned = (
        normalize_string_columns(df)
        .withColumn("vin", normalized_vin("vin"))
        .withColumn("dealer_id", F.upper(F.trim(F.col("dealer_id"))))
        .withColumn("service_open_timestamp", parse_timestamp("service_open_timestamp"))
        .withColumn("service_close_timestamp", parse_timestamp("service_close_timestamp"))
        .withColumn("service_date", F.to_date("service_open_timestamp"))
        .withColumn("service_type", F.upper(F.regexp_replace(F.col("service_type"), r"\s+", "_")))
        .withColumn("service_status", F.upper(F.regexp_replace(F.col("service_status"), r"\s+", "_")))
        .withColumn("warranty_flag", F.upper(F.col("warranty_flag")).isin("Y", "YES", "TRUE", "1"))
        .withColumn("labor_hours", clean_number("labor_hours"))
        .withColumn("parts_cost", clean_number("parts_cost", "decimal(12,2)"))
        .withColumn("service_duration_hours", (F.col("service_close_timestamp").cast("long") - F.col("service_open_timestamp").cast("long")) / 3600.0)
        .withColumn("business_date", F.lit(business_date))
        .withColumn("source_zip", F.lit(source_zip))
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("ingested_at", F.current_timestamp())
        .dropDuplicates(["service_id"])
    )
    checks = [
        ("missing_service_id", F.col("service_id").isNull()),
        ("missing_vin", F.col("vin").isNull()),
        ("invalid_vin_length", F.length("vin") != 17),
        ("missing_dealer_id", F.col("dealer_id").isNull()),
        ("missing_service_open_timestamp", F.col("service_open_timestamp").isNull()),
        ("invalid_service_status", ~F.col("service_status").isin(VALID_SERVICE_STATUSES)),
        ("missing_labor_hours", F.col("labor_hours").isNull()),
        ("negative_labor_hours", F.col("labor_hours") < 0),
        ("missing_parts_cost", F.col("parts_cost").isNull()),
        ("negative_parts_cost", F.col("parts_cost") < 0),
        ("closed_without_close_timestamp", (F.col("service_status") == "CLOSED") & F.col("service_close_timestamp").isNull()),
        ("close_before_open", F.col("service_close_timestamp") < F.col("service_open_timestamp")),
    ]
    return split_valid_invalid(cleaned, checks)


def clean_vehicle_master(df: DataFrame, business_date: str, pipeline_run_id: str, source_zip: str) -> Tuple[DataFrame, DataFrame]:
    current_year = date.today().year + 1
    cleaned = (
        normalize_string_columns(df)
        .withColumn("vin", normalized_vin("vin"))
        .withColumn("model", F.initcap(F.col("model")))
        .withColumn("model_year", clean_number("model_year", "int"))
        .withColumn("powertrain", F.upper(F.col("powertrain")))
        .withColumn("production_plant", F.upper(F.col("production_plant")))
        .withColumn("warranty_start_date", parse_date("warranty_start_date"))
        .withColumn("customer_region", F.upper(F.col("customer_region")))
        .withColumn("business_date", F.lit(business_date))
        .withColumn("source_zip", F.lit(source_zip))
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("ingested_at", F.current_timestamp())
        .dropDuplicates(["vin"])
    )
    checks = [
        ("missing_vin", F.col("vin").isNull()),
        ("invalid_vin_length", F.length("vin") != 17),
        ("missing_model", F.col("model").isNull()),
        ("invalid_model_year", ~F.col("model_year").between(1948, current_year)),
        ("invalid_powertrain", ~F.col("powertrain").isin(VALID_POWERTRAINS)),
        ("future_warranty_start_date", F.col("warranty_start_date") > F.current_date()),
    ]
    return split_valid_invalid(cleaned, checks)


def clean_dealer_master(df: DataFrame, business_date: str, pipeline_run_id: str, source_zip: str) -> Tuple[DataFrame, DataFrame]:
    cleaned = (
        normalize_string_columns(df)
        .withColumn("dealer_id", F.upper(F.trim(F.col("dealer_id"))))
        .withColumn("dealer_name", F.initcap(F.col("dealer_name")))
        .withColumn("country", F.upper(F.col("country")))
        .withColumn("region", F.upper(F.col("region")))
        .withColumn("dealer_tier", F.upper(F.col("dealer_tier")))
        .withColumn("business_date", F.lit(business_date))
        .withColumn("source_zip", F.lit(source_zip))
        .withColumn("pipeline_run_id", F.lit(pipeline_run_id))
        .withColumn("ingested_at", F.current_timestamp())
        .dropDuplicates(["dealer_id"])
    )
    checks = [
        ("missing_dealer_id", F.col("dealer_id").isNull()),
        ("missing_dealer_name", F.col("dealer_name").isNull()),
        ("missing_country", F.col("country").isNull()),
        ("missing_region", F.col("region").isNull()),
    ]
    return split_valid_invalid(cleaned, checks)


def reference_rejects(
    telemetry_df: DataFrame,
    vehicle_master_df: DataFrame,
    pipeline_run_id: str,
    business_date: str,
) -> DataFrame:
    return (
        telemetry_df.join(vehicle_master_df.select("vin").withColumn("known_vehicle", F.lit(True)), "vin", "left")
        .filter(F.col("known_vehicle").isNull())
        .withColumn("reject_reason", F.lit("unknown_vehicle_reference"))
        .transform(lambda df: reject_output(df, "vehicle_health_reference", pipeline_run_id, business_date))
    )


def build_enriched_vehicle_health(
    telemetry_df: DataFrame,
    service_df: DataFrame,
    vehicle_master_df: DataFrame,
    dealer_master_df: DataFrame,
) -> DataFrame:
    latest_service_window = Window.partitionBy("vin").orderBy(F.col("service_open_timestamp").desc_nulls_last())
    latest_service = (
        service_df.withColumn("service_rank", F.row_number().over(latest_service_window))
        .filter(F.col("service_rank") == 1)
        .drop("service_rank")
    )
    vehicle_reference = vehicle_master_df.select(
        "vin",
        "model",
        "model_year",
        "powertrain",
        "production_plant",
        "warranty_start_date",
        "customer_region",
    )

    return (
        telemetry_df.join(F.broadcast(vehicle_reference), "vin", "inner")
        .join(latest_service.select("vin", "dealer_id", "service_type", "service_status", "warranty_flag", "service_duration_hours"), "vin", "left")
        .join(F.broadcast(dealer_master_df.select("dealer_id", "dealer_name", "region", "dealer_tier")), "dealer_id", "left")
        .withColumn("is_critical_event", F.col("severity") == "CRITICAL")
        .withColumn("is_warning_event", F.col("severity").isin("HIGH", "CRITICAL"))
    )


def build_daily_health_summary(enriched_df: DataFrame) -> DataFrame:
    dimensions = ["business_date", "event_date", "country", "customer_region", "model", "model_year", "powertrain", "severity"]
    base_summary = enriched_df.groupBy(*dimensions).agg(
        F.count("*").alias("telemetry_event_count"),
        F.countDistinct("vin").alias("affected_vehicle_count"),
        F.sum(F.when(F.col("severity") == "CRITICAL", 1).otherwise(0)).alias("critical_dtc_count"),
        F.avg("odometer_km").alias("avg_odometer_km"),
        F.max("odometer_km").alias("max_odometer_km"),
        F.avg("battery_voltage").alias("avg_battery_voltage"),
        F.avg("engine_temp_c").alias("avg_engine_temp_c"),
    )

    repeated_warning = (
        enriched_df.filter(F.col("severity").isin("HIGH", "CRITICAL"))
        .groupBy(*dimensions, "vin")
        .agg(F.count("*").alias("warning_event_count"))
        .filter(F.col("warning_event_count") > 1)
        .groupBy(*dimensions)
        .agg(F.countDistinct("vin").alias("repeated_warning_vehicle_count"))
    )

    return base_summary.join(repeated_warning, dimensions, "left").fillna({"repeated_warning_vehicle_count": 0})


def build_service_kpi_summary(
    service_df: DataFrame,
    vehicle_master_df: DataFrame,
    dealer_master_df: DataFrame,
) -> DataFrame:
    service_enriched = (
        service_df.join(F.broadcast(vehicle_master_df.select("vin", "model", "model_year", "powertrain", "customer_region")), "vin", "left")
        .join(F.broadcast(dealer_master_df.select("dealer_id", "dealer_name", "country", "region", "dealer_tier")), "dealer_id", "left")
    )
    return service_enriched.groupBy(
        "business_date",
        "dealer_id",
        "dealer_name",
        "country",
        "region",
        "dealer_tier",
        "model",
        "model_year",
        "powertrain",
        "service_type",
    ).agg(
        F.count("*").alias("service_event_count"),
        F.avg("service_duration_hours").alias("avg_service_duration_hours"),
        F.avg("labor_hours").alias("avg_labor_hours"),
        F.sum("parts_cost").alias("total_parts_cost"),
        F.avg(F.col("warranty_flag").cast("double")).alias("warranty_service_rate"),
    )


def validate_curated_outputs(
    enriched_df: DataFrame,
    daily_summary_df: DataFrame,
    max_reject_rate: float,
    input_rows: int,
    reject_rows: int,
) -> List[Tuple[str, str, int, str]]:
    duplicate_events = (
        enriched_df.groupBy("vin", "event_timestamp", "dtc_code")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )
    checks = [
        ("vehicle_health_enriched", "null_vin", enriched_df.filter(F.col("vin").isNull()).count()),
        ("vehicle_health_enriched", "invalid_vin_length", enriched_df.filter(F.length("vin") != 17).count()),
        ("vehicle_health_enriched", "missing_model", enriched_df.filter(F.col("model").isNull()).count()),
        ("vehicle_health_enriched", "duplicate_event_key", duplicate_events),
        ("vehicle_health_enriched", "invalid_severity", enriched_df.filter(~F.col("severity").isin(VALID_SEVERITIES)).count()),
        ("vehicle_health_enriched", "negative_odometer", enriched_df.filter(F.col("odometer_km") < 0).count()),
    ]

    detail_count = enriched_df.count()
    summary_total = daily_summary_df.agg(F.sum("telemetry_event_count").alias("total")).collect()[0]["total"] or 0
    checks.append(("daily_vehicle_health_summary", "summary_reconciliation_delta", abs(detail_count - int(summary_total))))

    reject_rate = reject_rows / input_rows if input_rows else 0
    if reject_rate > max_reject_rate:
        checks.append(("pipeline", "reject_rate_threshold_exceeded", 1))

    failed = [(dataset, check, count) for dataset, check, count in checks if count > 0]
    if failed:
        raise ValueError(f"Curated validation failed: {failed}; reject_rate={reject_rate:.2%}")

    return [(dataset, check, count, "PASSED") for dataset, check, count in checks]


def quality_report(
    spark: SparkSession,
    checks: List[Tuple[str, str, int, str]],
    counts: Dict[str, int],
    pipeline_run_id: str,
    business_date: str,
) -> DataFrame:
    count_rows = [
        ("pipeline", metric_name, metric_value, "INFO")
        for metric_name, metric_value in counts.items()
    ]
    rows = [
        (dataset, metric_name, int(metric_value), status, business_date, pipeline_run_id)
        for dataset, metric_name, metric_value, status in [*checks, *count_rows]
    ]
    return spark.createDataFrame(
        rows,
        "dataset_name string, metric_name string, metric_value long, status string, business_date string, pipeline_run_id string",
    ).withColumn("reported_at", F.current_timestamp())


def write_dataset(
    df: DataFrame,
    path: str,
    mode: str,
    output_format: str,
    partition_by: Optional[Sequence[str]] = None,
    business_date: Optional[str] = None,
) -> None:
    writer = df.write.mode(mode).format(output_format)
    if output_format == "csv":
        writer = (
            writer.option("header", "true")
            .option("compression", "none")
            .option("emptyValue", "")
            .option("nullValue", "")
        )
    elif output_format == "parquet":
        writer = writer.option("compression", "snappy")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    if (
        mode == "overwrite"
        and business_date
        and partition_by
        and "business_date" in partition_by
    ):
        writer = writer.option("replaceWhere", f"business_date = '{business_date}'")
    writer.save(path)


def log_count(label: str, df: DataFrame) -> int:
    count = df.count()
    print(f"{label}: {count}")
    return count


def union_rejects(reject_dfs: Iterable[DataFrame]) -> Optional[DataFrame]:
    result = None
    for df in reject_dfs:
        result = df if result is None else result.unionByName(df, allowMissingColumns=True)
    return result


def run_pipeline(args: argparse.Namespace) -> None:
    business_date = resolve_business_date(args.business_date)

    pipeline_run_id = str(uuid.uuid4())
    source_zip = Path(strip_file_scheme(args.input_s3_uri)).name
    spark = create_spark_session(args)
    configure_s3_credentials(spark, args)
    print_aws_execution_context(spark)

    staging_base = args.temp_dir or join_uri(args.output_s3_uri, "_staging")

    with tempfile.TemporaryDirectory(prefix="vehicle_health_") as local_tmp:
        local_zip_path = os.path.join(local_tmp, source_zip)
        extract_dir = os.path.join(local_tmp, "extracted")
        os.makedirs(extract_dir, exist_ok=True)

        print(f"Pipeline run id: {pipeline_run_id}")
        print(f"Copying input zip from {args.input_s3_uri}")
        copy_input_zip_to_local(spark, args.input_s3_uri, local_zip_path)

        discovered_files = extract_zip(local_zip_path, extract_dir)
        staged_files, stage_uri = upload_extracted_csvs(spark, discovered_files, staging_base, pipeline_run_id)
        print(f"Staged extracted CSV files at {stage_uri}")

        telemetry_raw = read_csv(spark, staged_files[REQUIRED_FILES["telemetry"]], TELEMETRY_COLUMNS, REQUIRED_FILES["telemetry"], args.csv_delimiter)
        service_raw = read_csv(spark, staged_files[REQUIRED_FILES["service_events"]], SERVICE_COLUMNS, REQUIRED_FILES["service_events"], args.csv_delimiter)
        vehicle_master_raw = read_csv(spark, staged_files[REQUIRED_FILES["vehicle_master"]], VEHICLE_MASTER_COLUMNS, REQUIRED_FILES["vehicle_master"], args.csv_delimiter)
        dealer_master_raw = read_csv(spark, staged_files[REQUIRED_FILES["dealer_master"]], DEALER_MASTER_COLUMNS, REQUIRED_FILES["dealer_master"], args.csv_delimiter)

        telemetry_clean, telemetry_rejects = clean_telemetry(telemetry_raw, business_date, pipeline_run_id, source_zip)
        service_clean, service_rejects = clean_service_events(service_raw, business_date, pipeline_run_id, source_zip)
        vehicle_master_clean, vehicle_master_rejects = clean_vehicle_master(vehicle_master_raw, business_date, pipeline_run_id, source_zip)
        dealer_master_clean, dealer_master_rejects = clean_dealer_master(dealer_master_raw, business_date, pipeline_run_id, source_zip)

        telemetry_clean = telemetry_clean.cache()
        service_clean = service_clean.cache()
        vehicle_master_clean = vehicle_master_clean.cache()
        dealer_master_clean = dealer_master_clean.cache()

        vehicle_reference_rejects = reference_rejects(telemetry_clean, vehicle_master_clean, pipeline_run_id, business_date)
        enriched = build_enriched_vehicle_health(telemetry_clean, service_clean, vehicle_master_clean, dealer_master_clean).cache()
        daily_summary = build_daily_health_summary(enriched)
        service_summary = build_service_kpi_summary(service_clean, vehicle_master_clean, dealer_master_clean)

        rejects = union_rejects(
            [
                reject_output(telemetry_rejects, "vehicle_telemetry", pipeline_run_id, business_date),
                reject_output(service_rejects, "service_events", pipeline_run_id, business_date),
                reject_output(vehicle_master_rejects, "vehicle_master", pipeline_run_id, business_date),
                reject_output(dealer_master_rejects, "dealer_master", pipeline_run_id, business_date),
                vehicle_reference_rejects,
            ]
        )

        input_counts = {
            "telemetry_input_rows": log_count("telemetry_input_rows", telemetry_raw),
            "service_input_rows": log_count("service_input_rows", service_raw),
            "vehicle_master_input_rows": log_count("vehicle_master_input_rows", vehicle_master_raw),
            "dealer_master_input_rows": log_count("dealer_master_input_rows", dealer_master_raw),
            "vehicle_health_enriched_rows": log_count("vehicle_health_enriched_rows", enriched),
            "daily_summary_rows": log_count("daily_summary_rows", daily_summary),
            "service_summary_rows": log_count("service_summary_rows", service_summary),
        }
        input_rows = sum(
            input_counts[key]
            for key in [
                "telemetry_input_rows",
                "service_input_rows",
                "vehicle_master_input_rows",
                "dealer_master_input_rows",
            ]
        )
        reject_rows = log_count("reject_rows", rejects) if rejects is not None else 0
        input_counts["reject_rows"] = reject_rows

        validation_checks = validate_curated_outputs(
            enriched,
            daily_summary,
            args.max_reject_rate,
            input_rows,
            reject_rows,
        )
        report = quality_report(spark, validation_checks, input_counts, pipeline_run_id, business_date)

        curated_root = args.output_s3_uri.rstrip("/")
        write_dataset(
            telemetry_clean,
            join_uri(curated_root, "vehicle_telemetry_clean"),
            args.write_mode,
            args.write_format,
            ["business_date", "country"],
            business_date,
        )
        write_dataset(
            service_clean,
            join_uri(curated_root, "service_events_clean"),
            args.write_mode,
            args.write_format,
            ["business_date"],
            business_date,
        )
        write_dataset(
            enriched.select(*ENRICHED_WRITE_COLUMNS),
            join_uri(curated_root, "vehicle_health_enriched"),
            args.write_mode,
            args.write_format,
            ["business_date", "model"],
            business_date,
        )
        write_dataset(
            daily_summary.select(*DAILY_SUMMARY_WRITE_COLUMNS),
            join_uri(curated_root, "daily_vehicle_health_summary"),
            args.write_mode,
            args.write_format,
            ["business_date", "model"],
            business_date,
        )
        write_dataset(
            service_summary.select(*SERVICE_KPI_WRITE_COLUMNS),
            join_uri(curated_root, "service_kpi_summary"),
            args.write_mode,
            args.write_format,
            ["business_date", "region"],
            business_date,
        )
        if rejects is not None:
            write_dataset(
                rejects,
                join_uri(curated_root, "rejects"),
                args.write_mode,
                args.write_format,
                ["business_date", "dataset_name"],
                business_date,
            )
        write_dataset(
            report.select(*QUALITY_REPORT_WRITE_COLUMNS),
            join_uri(curated_root, "data_quality_report"),
            args.write_mode,
            args.write_format,
            ["business_date"],
            business_date,
        )

        if not args.skip_glue_registration:
            from glue_catalog_registry import register_curated_tables

            access_key, secret_key, session_token = _resolve_aws_credentials(spark, args)
            register_curated_tables(
                spark,
                curated_s3_root=curated_root,
                database=args.glue_database,
                region=args.aws_region,
                registration_mode=args.glue_registration_mode,
                aws_access_key=access_key,
                aws_secret_key=secret_key,
                aws_session_token=session_token,
            )
        else:
            print("Skipping Glue Data Catalog registration.")

        cleanup_staging(spark, stage_uri, args.keep_temp)
        print(f"Pipeline completed successfully. Curated output root: {curated_root}")


def main() -> None:
    args = parse_args()
    run_pipeline(args)


if __name__ == "__main__":
    main()
