"""Airflow task: register curated vehicle-health tables in AWS Glue after Spark completes."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from pipeline_settings import AWS_CONNECTION_ID, AWS_REGION, CURATED_S3_ROOT, GLUE_DATABASE


def _ensure_jobs_on_path() -> None:
    includes_dir = Path(__file__).resolve().parent / "includes"
    if not includes_dir.is_dir():
        raise RuntimeError(
            f"Missing Airflow includes directory: {includes_dir}. "
            "Upload glue_aws_client.py and glue_catalog_registry.py to dags/includes/."
        )
    path = str(includes_dir)
    if path not in sys.path:
        sys.path.insert(0, path)


def _connection_extra(conn) -> Dict[str, Any]:
    if not conn.extra:
        return {}
    if hasattr(conn, "extra_dejson"):
        return conn.extra_dejson or {}
    try:
        import json

        return json.loads(conn.extra)
    except Exception:
        return {}


def _credentials_from_airflow_connection(
    connection_id: str,
    default_region: str,
) -> Tuple[str, str, Optional[str], str]:
    from airflow.hooks.base import BaseHook

    conn = BaseHook.get_connection(connection_id)
    extra = _connection_extra(conn)

    access_key = (
        conn.login
        or extra.get("aws_access_key_id")
        or extra.get("access_key_id")
        or extra.get("access_key")
    )
    secret_key = (
        conn.password
        or extra.get("aws_secret_access_key")
        or extra.get("secret_access_key")
        or extra.get("secret_key")
    )
    session_token = extra.get("aws_session_token") or extra.get("session_token")
    region = (
        extra.get("region_name")
        or extra.get("region")
        or default_region
    )

    if not access_key or not secret_key:
        raise RuntimeError(
            f"Airflow connection '{connection_id}' (type={conn.conn_type!r}) has no AWS keys. "
            "Use either:\n"
            "  Generic: Login=access key ID, Password=secret access key\n"
            "  AWS type: Extra JSON "
            '{"aws_access_key_id":"AKIA...","aws_secret_access_key":"...","region_name":"us-east-1"}'
        )

    print(
        f"Using AWS credentials from Airflow connection '{connection_id}' "
        f"(type={conn.conn_type!r}, region={region})."
    )
    return access_key, secret_key, session_token, region


def register_vehicle_health_glue_metadata(**context: Any) -> None:
    """Register Glue tables/partitions for the curated S3 datasets."""
    print(f"glue_registration: loading includes and connection '{AWS_CONNECTION_ID}'")
    _ensure_jobs_on_path()
    from glue_catalog_registry import register_curated_tables_standalone

    params: Dict[str, Any] = context["params"]
    region = params.get("aws_region", AWS_REGION)

    access_key, secret_key, session_token, region = _credentials_from_airflow_connection(
        AWS_CONNECTION_ID,
        region,
    )

    register_curated_tables_standalone(
        curated_s3_root=params.get("curated_s3_root", CURATED_S3_ROOT),
        database=params.get("glue_database", GLUE_DATABASE),
        region=region,
        aws_access_key=access_key,
        aws_secret_key=secret_key,
        aws_session_token=session_token,
    )
