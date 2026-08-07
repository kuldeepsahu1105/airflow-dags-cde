"""Minimal AWS Glue + S3 clients using Python stdlib (no boto3 required on CDE)."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


class GlueEntityNotFoundError(Exception):
    """Raised when Glue returns EntityNotFoundException."""


@dataclass(frozen=True)
class AwsCredentials:
    access_key: str
    secret_key: str
    region: str
    session_token: Optional[str] = None


def resolve_default_aws_credentials(region: str) -> AwsCredentials:
    """Resolve AWS credentials from the runtime environment (no keys in code or DAG).

    Order:
    1. Platform-injected env vars (CDE/K8s: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, ...)
    2. IRSA / web identity (AWS_ROLE_ARN + AWS_WEB_IDENTITY_TOKEN_FILE)
    """
    access_key = os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    session_token = os.environ.get("AWS_SESSION_TOKEN")
    if access_key and secret_key:
        print("Using AWS credentials from runtime environment (AWS_ACCESS_KEY_ID).")
        return AwsCredentials(
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            session_token=session_token,
        )

    role_arn = os.environ.get("AWS_ROLE_ARN")
    token_file = os.environ.get("AWS_WEB_IDENTITY_TOKEN_FILE")
    if role_arn and token_file:
        print(f"Using AWS web identity credentials for role: {role_arn}")
        return _credentials_from_web_identity(role_arn, token_file, region)

    raise RuntimeError(
        "No AWS credentials found. Use one of: "
        "(1) Airflow connection aws_glue (Login=access key, Password=secret key), "
        "(2) IAM role on the Airflow worker (IRSA / instance profile), or "
        "(3) platform-injected AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars."
    )


def _credentials_from_web_identity(role_arn: str, token_file: str, region: str) -> AwsCredentials:
    with open(token_file, encoding="utf-8") as handle:
        web_token = handle.read().strip()

    params = urlencode(
        {
            "Action": "AssumeRoleWithWebIdentity",
            "Version": "2011-06-15",
            "RoleArn": role_arn,
            "RoleSessionName": "vehicle-health-glue-registration",
            "WebIdentityToken": web_token,
        }
    )
    request = Request(f"https://sts.{region}.amazonaws.com/?{params}", method="GET")
    try:
        with urlopen(request, timeout=60) as response:
            payload = response.read()
    except HTTPError as exc:
        raw = exc.read().decode("utf-8")
        raise RuntimeError(f"STS AssumeRoleWithWebIdentity failed ({exc.code}): {raw}") from exc

    root = ET.fromstring(payload)
    namespace = {"sts": "https://sts.amazonaws.com/doc/2011-06-15/"}
    access_key = root.findtext(".//sts:AccessKeyId", namespaces=namespace)
    secret_key = root.findtext(".//sts:SecretAccessKey", namespaces=namespace)
    session_token = root.findtext(".//sts:SessionToken", namespaces=namespace)
    if not access_key or not secret_key:
        raise RuntimeError("STS AssumeRoleWithWebIdentity did not return credentials.")
    return AwsCredentials(
        access_key=access_key,
        secret_key=secret_key,
        region=region,
        session_token=session_token,
    )


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_query_string(params: Dict[str, str]) -> str:
    """Build SigV4 canonical query string (sorted keys, RFC 3986 encoding)."""
    return "&".join(
        f"{quote(key, safe='-_.~')}={quote(value, safe='-_.~')}"
        for key, value in sorted(params.items())
    )


def _sign(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def _signature_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    key = _sign(f"AWS4{secret_key}".encode("utf-8"), date_stamp)
    key = _sign(key, region)
    key = _sign(key, service)
    return _sign(key, "aws4_request")


def _signed_headers(
    method: str,
    service: str,
    credentials: AwsCredentials,
    host: str,
    path: str,
    query: str,
    headers: Dict[str, str],
    payload: bytes,
) -> Dict[str, str]:
    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")

    payload_hash = _sha256_hex(payload)
    signed = dict(headers)
    signed["host"] = host
    signed["x-amz-date"] = amz_date
    signed["x-amz-content-sha256"] = payload_hash
    if credentials.session_token:
        signed["x-amz-security-token"] = credentials.session_token

    canonical_headers = "".join(
        f"{key.lower()}:{signed[key].strip()}\n" for key in sorted(signed, key=str.lower)
    )
    signed_header_names = ";".join(sorted(key.lower() for key in signed))
    canonical_request = "\n".join(
        [
            method,
            path,
            query,
            canonical_headers,
            signed_header_names,
            payload_hash,
        ]
    )
    credential_scope = f"{date_stamp}/{credentials.region}/{service}/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            _sha256_hex(canonical_request.encode("utf-8")),
        ]
    )
    signing_key = _signature_key(credentials.secret_key, date_stamp, credentials.region, service)
    signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
    signed["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={credentials.access_key}/{credential_scope}, "
        f"SignedHeaders={signed_header_names}, Signature={signature}"
    )
    return signed


def _request_json(
    credentials: AwsCredentials,
    service: str,
    host: str,
    target: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    headers = _signed_headers(
        "POST",
        service,
        credentials,
        host,
        "/",
        "",
        {
            "Content-Type": "application/x-amz-json-1.1",
            "X-Amz-Target": target,
        },
        body,
    )
    request = Request(f"https://{host}/", data=body, method="POST")
    for key, value in headers.items():
        request.add_header(key, value)

    try:
        with urlopen(request, timeout=120) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            error_body = json.loads(raw)
        except json.JSONDecodeError as decode_error:
            raise RuntimeError(f"AWS {service} request failed ({exc.code}): {raw}") from decode_error
        error_type = error_body.get("__type", "").split(".")[-1]
        if error_type == "EntityNotFoundException":
            raise GlueEntityNotFoundError(error_body.get("Message", "Entity not found"))
        raise RuntimeError(f"AWS {service} request failed: {error_body}") from exc


class GlueRestClient:
    def __init__(self, credentials: AwsCredentials) -> None:
        self.credentials = credentials
        self.host = f"glue.{credentials.region}.amazonaws.com"

    def _call(self, target: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return _request_json(self.credentials, "glue", self.host, target, payload)

    def get_database(self, name: str) -> Dict[str, Any]:
        return self._call("AWSGlue.GetDatabase", {"Name": name})

    def create_database(self, database_input: Dict[str, Any]) -> None:
        self._call("AWSGlue.CreateDatabase", {"DatabaseInput": database_input})

    def get_table(self, database_name: str, name: str) -> Dict[str, Any]:
        return self._call("AWSGlue.GetTable", {"DatabaseName": database_name, "Name": name})

    def create_table(self, database_name: str, table_input: Dict[str, Any]) -> None:
        self._call(
            "AWSGlue.CreateTable",
            {"DatabaseName": database_name, "TableInput": table_input},
        )

    def update_table(self, database_name: str, table_input: Dict[str, Any]) -> None:
        self._call(
            "AWSGlue.UpdateTable",
            {"DatabaseName": database_name, "TableInput": table_input},
        )

    def get_partitions(self, database_name: str, table_name: str, next_token: Optional[str] = None) -> Dict[str, Any]:
        payload: Dict[str, Any] = {"DatabaseName": database_name, "TableName": table_name}
        if next_token:
            payload["NextToken"] = next_token
        return self._call("AWSGlue.GetPartitions", payload)

    def batch_create_partition(
        self,
        database_name: str,
        table_name: str,
        partition_input_list: List[Dict[str, Any]],
    ) -> None:
        self._call(
            "AWSGlue.BatchCreatePartition",
            {
                "DatabaseName": database_name,
                "TableName": table_name,
                "PartitionInputList": partition_input_list,
            },
        )


class S3RestClient:
    def __init__(self, credentials: AwsCredentials) -> None:
        self.credentials = credentials

    def list_common_prefixes(self, bucket: str, prefix: str) -> List[str]:
        prefixes: List[str] = []
        continuation: Optional[str] = None
        host = f"{bucket}.s3.{self.credentials.region}.amazonaws.com"

        while True:
            params: Dict[str, str] = {
                "list-type": "2",
                "prefix": prefix,
                "delimiter": "/",
            }
            if continuation:
                params["continuation-token"] = continuation
            query = _canonical_query_string(params)
            headers = _signed_headers(
                "GET",
                "s3",
                self.credentials,
                host,
                "/",
                query,
                {},
                b"",
            )
            request = Request(f"https://{host}/?{query}", method="GET")
            for key, value in headers.items():
                request.add_header(key, value)

            try:
                with urlopen(request, timeout=120) as response:
                    payload = response.read()
            except HTTPError as exc:
                raw = exc.read().decode("utf-8")
                raise RuntimeError(f"S3 ListObjectsV2 failed ({exc.code}): {raw}") from exc

            root = ET.fromstring(payload)
            for element in root.findall("{*}CommonPrefixes/{*}Prefix"):
                if element.text:
                    prefixes.append(element.text)
            truncated = root.findtext("{*}IsTruncated", default="false")
            continuation = root.findtext("{*}NextContinuationToken")
            if truncated != "true" or not continuation:
                break

        return prefixes
