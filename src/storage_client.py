import io
import os
import gzip
import time
import pandas as pd
from typing import Optional
from boto3.session import Session
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig
from .config import settings
from .hashing import sha256_bytes, bytesio_copy
from .validate import basic_validate


class StorageClient:
    """Unified client for AWS S3 and MinIO with idempotent, atomic writes and presigning."""

    def __init__(self):
        addressing = "path" if int(settings.USE_PATH_STYLE) == 1 else "virtual"
        self._config = BotoConfig(
            retries={"max_attempts": 8, "mode": "standard"},
            s3={"addressing_style": addressing},
        )
        session_kwargs = {}
        if settings.AWS_PROFILE:
            session_kwargs["profile_name"] = settings.AWS_PROFILE
        if settings.AWS_REGION:
            session_kwargs["region_name"] = settings.AWS_REGION
        self._session = Session(**session_kwargs)
        self._client = self._session.client("s3", endpoint_url=settings.ENDPOINT_URL, config=self._config)
        self._bucket = settings.BUCKET_NAME
        self._transfer_cfg = TransferConfig(multipart_threshold=8 * 1024 * 1024, max_concurrency=8)

    def list(self, prefix: str) -> list[str]:
        """List object keys under a prefix."""
        keys = []
        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self._bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                keys.append(obj["Key"])
        return keys

    def exists(self, key: str) -> bool:
        """Return True if object key exists."""
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return False
            raise

    def _put_bytes_atomic(self, data: bytes, final_key: str, metadata: dict[str, str], tags: dict[str, str]) -> str:
        """Write bytes to staging, then copy to final key if not duplicate by content hash."""
        content_sha = sha256_bytes(data)
        try:
            head = self._client.head_object(Bucket=self._bucket, Key=final_key)
            existing = head.get("Metadata", {}).get("content_sha256")
            if existing == content_sha:
                return "skipped"
        except ClientError as e:
            status = e.response["ResponseMetadata"]["HTTPStatusCode"]
            if status != 404:
                raise
        staging_key = f"staging/{int(time.time()*1000)}-{os.path.basename(final_key)}"
        body = io.BytesIO(data)
        self._client.upload_fileobj(
            Fileobj=body,
            Bucket=self._bucket,
            Key=staging_key,
            ExtraArgs={"Metadata": {**metadata, "content_sha256": content_sha}},
            Config=self._transfer_cfg,
        )
        tag_str = "&".join(f"{k}={v}" for k, v in tags.items()) if tags else ""
        copy_src = {"Bucket": self._bucket, "Key": staging_key}
        self._client.copy_object(
            Bucket=self._bucket,
            Key=final_key,
            CopySource=copy_src,
            MetadataDirective="REPLACE",
            Metadata={**metadata, "content_sha256": content_sha},
            Tagging=tag_str,
        )
        self._client.delete_object(Bucket=self._bucket, Key=staging_key)
        return "uploaded"

    def put_csv(self, df: pd.DataFrame, key: str, compress: Optional[str] = "gzip",
                required_columns: Optional[list[str]] = None,
                tags: Optional[dict[str, str]] = None,
                metadata: Optional[dict[str, str]] = None) -> str:
        """Upload a DataFrame as CSV with optional gzip compression, atomically and idempotently."""
        basic_validate(df, required_columns or list(df.columns))
        csv_bytes = df.to_csv(index=False).encode("utf-8")
        if compress == "gzip":
            buf = io.BytesIO()
            gz = gzip.GzipFile(fileobj=buf, mode="wb")
            gz.write(csv_bytes)
            gz.close()
            data = buf.getvalue()
        else:
            data = csv_bytes
        result = self._put_bytes_atomic(
            data=data,
            final_key=key,
            metadata=metadata or {"schema_version": "1"},
            tags=tags or {},
        )
        return result

    def put_parquet(self, df: pd.DataFrame, key: str,
                    required_columns: Optional[list[str]] = None,
                    tags: Optional[dict[str, str]] = None,
                    metadata: Optional[dict[str, str]] = None) -> str:
        """Upload a DataFrame as Parquet with atomic commit and idempotency."""
        basic_validate(df, required_columns or list(df.columns))
        buf = io.BytesIO()
        df.to_parquet(buf, index=False)
        data = bytesio_copy(buf).read()
        result = self._put_bytes_atomic(
            data=data,
            final_key=key,
            metadata=metadata or {"schema_version": "1"},
            tags=tags or {},
        )
        return result

    def get_df(self, key: str, fmt: str = "csv") -> pd.DataFrame:
        """Download an object and parse into a DataFrame given a format."""
        obj = self._client.get_object(Bucket=self._bucket, Key=key)
        body = obj["Body"].read()
        if fmt == "csv":
            try:
                return pd.read_csv(io.BytesIO(body), compression="infer")
            except Exception:
                return pd.read_csv(io.BytesIO(body))
        if fmt == "parquet":
            return pd.read_parquet(io.BytesIO(body))
        raise ValueError("Unsupported format for get_df.")

    def presign_get(self, key: str, expires: Optional[int] = None) -> str:
        """Return a presigned GET URL for an object key."""
        ttl = expires or int(settings.PRESIGN_DEFAULT_EXPIRES)
        url = self._client.generate_presigned_url(
            "get_object", Params={"Bucket": self._bucket, "Key": key}, ExpiresIn=ttl
        )
        return url

    def presign_put(self, key: str, expires: Optional[int] = None) -> str:
        """Return a presigned PUT URL for an object key."""
        ttl = expires or int(settings.PRESIGN_DEFAULT_EXPIRES)
        url = self._client.generate_presigned_url(
            "put_object", Params={"Bucket": self._bucket, "Key": key}, ExpiresIn=ttl
        )
        return url