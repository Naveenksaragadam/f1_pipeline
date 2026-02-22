# src/f1_pipeline/minio/object_store.py
"""
Production-grade S3/MinIO object store client with connection pooling and error handling.
"""

import base64
import gzip
import hashlib
import io
import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import IO, Any

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

from ..config import MINIO_REGION

logger = logging.getLogger(__name__)


class F1ObjectStore:
    """
    S3-compatible object store client optimized for MinIO.

    Features:
    - Connection pooling for performance
    - Automatic bucket creation
    - Retry logic on transient failures
    - Type-safe serialization
    - Comprehensive error handling
    """

    def __init__(
        self,
        bucket_name: str,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        client: Any | None = None,
        max_pool_connections: int = 50,
    ) -> None:
        """
        Initialize the ObjectStore with a bucket and an S3 client.

        Args:
            bucket_name: Target S3 bucket name
            endpoint_url: MinIO/S3 endpoint URL
            access_key: AWS access key ID
            secret_key: AWS secret access key
            client: Optional pre-configured boto3 client (for connection sharing)
            max_pool_connections: Maximum connections in connection pool
        """
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        self.max_pool_connections = max_pool_connections

        # Initialize client if not provided
        self.client = client or self._create_client()

    def _create_client(self) -> Any:
        """
        Create boto3 S3 client with optimized configuration.

        Returns:
            Configured boto3 S3 client

        Raises:
            Exception: If client creation fails
        """
        return self.__class__.create_client(
            endpoint_url=self.endpoint_url,
            access_key=self.access_key,
            secret_key=self.secret_key,
            max_pool_connections=self.max_pool_connections,
        )

    @classmethod
    def create_client(
        cls,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        max_pool_connections: int = 50,
    ) -> Any:
        """
        Create a standalone boto3 S3 client without binding to a bucket.

        Use this when you need a shared client for multiple stores, without
        having to instantiate a dummy F1ObjectStore with a fake bucket name.

        Args:
            endpoint_url: MinIO/S3 endpoint URL
            access_key: AWS access key ID
            secret_key: AWS secret access key
            max_pool_connections: Maximum connections in connection pool

        Returns:
            Configured boto3 S3 client
        """
        try:
            config = Config(
                signature_version="s3v4",
                max_pool_connections=max_pool_connections,
                retries={"max_attempts": 3, "mode": "standard"},
            )
            client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=MINIO_REGION,
                config=config,
            )
            logger.info(f"✅ S3 client created (Region: {MINIO_REGION})")
            return client
        except Exception as e:
            logger.error(f"❌ Failed to create S3 client: {e}")
            raise

    def bucket_exists(self) -> bool:
        """
        Check if the configured bucket exists.

        Returns:
            True if bucket exists, False otherwise

        Raises:
            ClientError: On permission or configuration errors (not 404)
        """
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ["404", "NoSuchBucket"]:
                return False
            # For other errors (403 Forbidden, etc.), log and re-raise
            logger.error(f"❌ Error checking bucket {self.bucket_name}: {e}")
            raise

    def create_bucket_if_not_exists(self) -> None:
        """
        Idempotently create the bucket if it doesn't exist.

        Raises:
            ClientError: On permission or configuration errors
        """
        try:
            if not self.bucket_exists():
                self.client.create_bucket(Bucket=self.bucket_name)
                logger.info(f"✅ Bucket '{self.bucket_name}' created.")
            else:
                logger.info(f"ℹ️  Bucket '{self.bucket_name}' already exists.")
        except ClientError as e:
            logger.error(f"❌ Failed to create bucket {self.bucket_name}: {e}")
            raise

    def delete_bucket(self, force: bool = False) -> None:
        """
        Delete the current bucket.

        Args:
            force: If True, delete even if bucket contains objects

        Warning:
            This is a destructive operation. Use with caution.

        Raises:
            ClientError: On S3 errors
        """
        try:
            if force:
                # Delete all objects first
                self._empty_bucket()
            self.client.delete_bucket(Bucket=self.bucket_name)
            logger.warning(f"⚠️  Deleted bucket: {self.bucket_name}")
        except ClientError as e:
            logger.error(f"❌ Failed to delete bucket {self.bucket_name}: {e}")
            raise

    def _empty_bucket(self) -> None:
        """
        Permanently delete all objects in the bucket.

        Warning:
            This operation is **irreversible** and will permanently destroy all
            data in the bucket. It is only called internally by
            ``delete_bucket(force=True)``, which requires an explicit opt-in.
            Do NOT call this method directly — use ``delete_bucket(force=True)``
            instead so that the intent is explicit at the call site.
        """
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket_name):
            if "Contents" in page:
                objects = [{"Key": obj["Key"]} for obj in page["Contents"]]
                self.client.delete_objects(Bucket=self.bucket_name, Delete={"Objects": objects})
                logger.debug(f"Deleted {len(objects)} objects from {self.bucket_name}")

    def _serialize_body(self, body: dict | list | str | bytes) -> tuple[str | bytes, str]:
        """
        Serialize data for S3 upload.

        Args:
            body: Data to serialize

        Returns:
            Tuple of (serialized_body, content_type)
        """
        if isinstance(body, (dict, list)):
            return (json.dumps(body), "application/json")
        elif isinstance(body, bytes):
            return (body, "application/octet-stream")
        else:
            return (str(body), "text/plain")

    def put_object(
        self,
        key: str,
        body: dict | list | str | bytes | bytearray | memoryview | IO | io.BytesIO,
        metadata: dict[str, str] | None = None,
        compress: bool = True,
    ) -> None:
        """
        Upload object to S3/MinIO with Gzip compression and MD5 integrity checks.

        Args:
            key: S3 object key (path)
            body: Content to upload (supports dict, list, str, bytes, or file-like object)
            metadata: Optional S3 metadata tags
            compress: If True, compresses data with Gzip (Level 9)

        Raises:
            ClientError: On S3 errors
            BotoCoreError: On SDK/network errors
        """
        # Standardize input to bytes (In-memory processing for Bronze/JSON)
        data: bytes
        content_type = "application/octet-stream"

        if isinstance(body, (dict, list)):
            # Minify JSON (separators removes space) -> Encode to bytes
            data = json.dumps(body, separators=(",", ":")).encode("utf-8")
            content_type = "application/json"
        elif isinstance(body, str):
            data = body.encode("utf-8")
            content_type = "text/plain"
        elif isinstance(body, (bytes, bytearray, memoryview)):
            data = bytes(body)
        else:
            # File-like object path.
            # Fast-path: if compression is disabled, stream directly to S3 via
            # upload_fileobj — avoids loading large binaries (e.g. Parquet) into
            # memory. boto3 handles multipart upload and ETag integrity natively.
            if not compress and hasattr(body, "read"):
                try:
                    extra_args: dict[str, Any] = {"ContentType": content_type}
                    if metadata:
                        extra_args["Metadata"] = metadata
                    self.client.upload_fileobj(body, self.bucket_name, key, ExtraArgs=extra_args)
                    logger.debug(f"✅ Streamed upload: {key}")
                    return
                except (ClientError, BotoCoreError) as e:
                    logger.error(f"❌ Streaming upload failed for {key}: {e}")
                    raise

            # Slow-path: read into memory so we can apply gzip + MD5.
            read_method = getattr(body, "read", None)
            if callable(read_method):
                content = read_method()
                if isinstance(content, str):
                    data = content.encode("utf-8")
                elif isinstance(content, (bytes, bytearray, memoryview)):
                    data = bytes(content)
                else:
                    data = str(content).encode("utf-8")
            else:
                data = str(body).encode("utf-8")

        # Apply Gzip Compression (if requested)
        content_encoding = None
        if compress:
            # Level 9 = Maximum compression
            data = gzip.compress(data, compresslevel=9)
            content_encoding = "gzip"

        # Calculate MD5 Checksum (Data Integrity)
        # nosec: B303 - MD5 is used for S3/MinIO Content-MD5 integrity, not security.
        md5_hash = hashlib.new("md5", data, usedforsecurity=False).digest()  # nosec B303
        md5_b64 = base64.b64encode(md5_hash).decode("utf-8")

        try:
            # Prepare arguments for put_object
            put_params: dict[str, Any] = {
                "Bucket": self.bucket_name,
                "Key": key,
                "Body": data,
                "ContentType": content_type,
                "ContentMD5": md5_b64,
            }

            if content_encoding:
                put_params["ContentEncoding"] = content_encoding

            if metadata:
                put_params["Metadata"] = metadata

            # Upload using low-level put_object (supports ContentMD5)
            self.client.put_object(**put_params)

            # Logging
            logger.debug(f"✅ Uploaded {key} | Size: {len(data):,} bytes | Compressed: {compress}")

        except ClientError as e:
            error = e.response.get("Error", {})
            logger.error(
                f"❌ S3 ClientError - "
                f"Bucket: {self.bucket_name}, "
                f"Key: {key}, "
                f"Code: {error.get('Code')}, "
                f"Message: {error.get('Message')}"
            )
            raise

        except BotoCoreError as e:
            logger.error(
                f"❌ BotoCoreError - Bucket: {self.bucket_name}, Key: {key}, Error: {str(e)}"
            )
            raise

    def object_exists(self, key: str) -> bool:
        """
        Check if an object exists in the bucket.

        Args:
            key: S3 object key

        Returns:
            True if object exists, False otherwise

        Raises:
            ClientError: On permission or configuration errors (not 404)
        """
        try:
            self.client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code == "404":
                return False
            # Other errors (403 Forbidden, 500) should raise
            logger.error(f"❌ Error checking object {key}: {e}")
            raise

    def list_objects(self, prefix: str = "", max_keys: int | None = None) -> list[str]:
        """
        List objects in bucket with optional prefix filter.

        Args:
            prefix: Optional key prefix filter (default: "" - all objects)
            max_keys: Maximum number of keys to return

        Returns:
            List of object keys
        """
        try:
            paginator = self.client.get_paginator("list_objects_v2")
            pagination_config = {"PageSize": 1000}
            if max_keys:
                pagination_config["MaxItems"] = max_keys

            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=prefix,
                PaginationConfig=pagination_config,
            )

            keys: list[str] = []
            for page in pages:
                if "Contents" in page:
                    keys.extend(obj["Key"] for obj in page["Contents"])

            logger.debug(f"Listed {len(keys)} objects with prefix '{prefix}'")
            return keys
        except ClientError as e:
            logger.error(f"❌ Error listing objects with prefix '{prefix}': {e}")
            raise

    def get_object(self, key: str) -> bytes:
        """
        Download object from S3/MinIO.

        Args:
            key: S3 object key

        Returns:
            Raw bytes of object content

        Raises:
            ClientError: If object not found or access denied
        """
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            content = response["Body"].read()

            # 🔍 AUTO-DECOMPRESS: Check headers for gzip
            if response.get("ContentEncoding") == "gzip":
                content = gzip.decompress(content)

            from typing import cast

            logger.debug(f"✅ Downloaded {key} ({len(content):,} bytes)")
            return cast(bytes, content)
        except ClientError as e:
            logger.error(f"❌ Failed to get object {key}: {e}")
            raise

    @contextmanager
    def stream_object(self, key: str) -> Iterator[Any]:
        """
        Stream object content to avoid loading large files into memory.

        Args:
            key: S3 object key

        Yields:
            File-like object (StreamingBody) of the content

        Raises:
            ClientError: If object not found or access denied
        """
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            yield response["Body"]
        except ClientError as e:
            logger.error(f"❌ Failed to stream {key}: {e}")
            raise

    def get_json(self, key: str) -> dict[str, Any]:
        """
        Download and parse JSON object.

        Args:
            key: S3 object key

        Returns:
            Parsed JSON as dictionary

        Raises:
            ClientError: If object not found
            json.JSONDecodeError: If content is not valid JSON
        """
        try:
            content = self.get_object(key)
            data: dict[str, Any] = json.loads(content.decode("utf-8"))
            logger.debug(f"✅ Parsed JSON from {key}")
            return data
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in {key}: {e}")
            raise

    def delete_object(self, key: str) -> None:
        """
        Delete an object from the bucket.

        Args:
            key: S3 object key

        Raises:
            ClientError: On S3 errors
        """
        try:
            self.client.delete_object(Bucket=self.bucket_name, Key=key)
            logger.info(f"✅ Deleted object: {key}")
        except ClientError as e:
            logger.error(f"❌ Failed to delete {key}: {e}")
            raise

    def get_object_metadata(self, key: str) -> dict[str, Any]:
        """
        Get object metadata without downloading the object.

        Args:
            key: S3 object key

        Returns:
            Dictionary containing object metadata

        Raises:
            ClientError: If object not found
        """
        try:
            response = self.client.head_object(Bucket=self.bucket_name, Key=key)
            metadata = {
                "size": response.get("ContentLength", 0),
                "last_modified": response.get("LastModified"),
                "content_type": response.get("ContentType"),
                "etag": response.get("ETag"),
                "metadata": response.get("Metadata", {}),
            }
            return metadata
        except ClientError as e:
            logger.error(f"❌ Failed to get metadata for {key}: {e}")
            raise
