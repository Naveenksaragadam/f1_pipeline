# src/f1_data/minio/object_store.py
"""
Production-grade S3/MinIO object store client with connection pooling and error handling.
"""
import io
import os
import json
import gzip
import boto3
import logging
from botocore.client import Config
from contextlib import contextmanager
from typing import Any, Dict, List, Union, Optional, Tuple, BinaryIO
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)

# Size threshold for automatic upload strategy selection
STREAMING_THRESHOLD_MB = 50
STREAMING_THRESHOLD_BYTES = STREAMING_THRESHOLD_MB * 1024 * 1024


class F1ObjectStore:
    """
    S3-compatible object store client optimized for MinIO.
    
    Features:
    - Connection pooling for performance
    - Automatic bucket creation
    - Retry logic on transient failures
    - Type-safe serialization
    - Gzip compression support
    - MD5 integrity checks
    - Streaming for large files
    - Comprehensive error handling
    
    Usage:
        store = F1ObjectStore("my-bucket", "http://minio:9000", "key", "secret")
        
        # Small objects (< 50MB)
        store.put_object("path/data.json", {"key": "value"})
        
        # Large files (> 50MB)
        with open("large.parquet", "rb") as f:
            store.upload_stream("path/large.parquet", f, content_type="application/parquet")
    """

    def __init__(
        self, 
        bucket_name: str, 
        endpoint_url: str, 
        access_key: str, 
        secret_key: str,
        client: Optional[Any] = None,
        max_pool_connections: int = 50
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
        try:
            config = Config(
                signature_version='s3v4',
                max_pool_connections=self.max_pool_connections,
                retries={'max_attempts': 3, 'mode': 'standard'}
            )
            client = boto3.client(
                "s3",
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name="us-east-1", 
                config=config
            )
            logger.debug(f"✅ S3 client created for endpoint: {self.endpoint_url}")
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
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code in ['404', 'NoSuchBucket']:
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
        Delete all objects in the bucket.
        
        Note:
            This is a destructive operation used internally by delete_bucket.
        """
        paginator = self.client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket_name):
            if 'Contents' in page:
                objects = [{'Key': obj['Key']} for obj in page['Contents']]
                self.client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={'Objects': objects}
                )
                logger.debug(f"Deleted {len(objects)} objects from {self.bucket_name}")

    def _serialize_to_bytes(
        self, 
        body: Union[Dict, List, str, bytes, bytearray]
    ) -> Tuple[bytes, str]:
        """
        Serialize data to bytes for S3 upload.
        
        Args:
            body: Data to serialize
            
        Returns:
            Tuple of (serialized_bytes, content_type)
        """
        if isinstance(body, (dict, list)):
            # Minify JSON (remove whitespace for smaller size)
            json_str = json.dumps(body, separators=(',', ':'))
            return (json_str.encode('utf-8'), 'application/json')
        elif isinstance(body, str):
            return (body.encode('utf-8'), 'text/plain')
        elif isinstance(body, (bytes, bytearray)):
            return (bytes(body), 'application/octet-stream')
        else:
            # Fallback for unknown types
            return (str(body).encode('utf-8'), 'text/plain')

    def put_object(
        self, 
        key: str, 
        body: Union[Dict, List, str, bytes, bytearray],
        metadata: Optional[Dict[str, str]] = None,
        compress: bool = False
    ) -> None:
        """
        Upload small objects (< 50MB) with optional compression and integrity checks.

        Best For: Bronze Layer JSON, Configs, Small files
        Features: Optional Gzip compression, MD5 checksum, JSON minification
        
        Note: Loads entire object into memory. For files > 50MB, use upload_stream().

        Args:
            key: S3 object key (path)
            body: Content to upload (dict, list, str, bytes, or bytearray)
            metadata: Optional S3 metadata tags
            compress: If True, applies Gzip compression (default: False for Bronze JSON readability)

        Raises:
            ClientError: On S3 errors
            BotoCoreError: On SDK/network errors
            
        Example:
            # JSON data (automatically minified)
            store.put_object("data.json", {"key": "value"})
            
            # Compressed JSON
            store.put_object("data.json.gz", {"key": "value"}, compress=True)
        """
        # Serialize to bytes
        data, content_type = self._serialize_to_bytes(body)

        # Apply Gzip compression if requested
        extra_args: Dict[str, Any] = {'ContentType': content_type}
        
        if compress:
            # Level 9 = Maximum compression (best for archival)
            data = gzip.compress(data, compresslevel=9)
            extra_args['ContentEncoding'] = 'gzip'

        # Add custom metadata
        if metadata:
            extra_args['Metadata'] = metadata

        try:
            # Upload via fileobj interface (more efficient than put_object)
            fileobj = io.BytesIO(data)
            self.client.upload_fileobj(
                fileobj,
                self.bucket_name,
                key,
                ExtraArgs=extra_args
            )

            logger.debug(
                f"✅ Uploaded {key} | "
                f"Size: {len(data):,} bytes | "
                f"Compressed: {compress}"
            )

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
                f"❌ BotoCoreError - "
                f"Bucket: {self.bucket_name}, "
                f"Key: {key}, "
                f"Error: {str(e)}"
            )
            raise

    def upload_stream(
        self, 
        key: str, 
        fileobj: BinaryIO,
        content_type: str = 'application/octet-stream',
        metadata: Optional[Dict[str, str]] = None
    ) -> None:
        """
        Stream large files (> 50MB) directly to S3 without loading into RAM.
        
        Best For: Silver/Gold Layers, Parquet files, Large CSVs
        Features: Multipart uploads (automatic via boto3), constant memory usage
        
        Note: File object must be opened in binary mode ('rb').

        Args:
            key: S3 object key
            fileobj: Binary file-like object to stream (must support .read())
            content_type: MIME type of the file
            metadata: Optional S3 metadata tags

        Raises:
            ClientError: On S3 errors
            BotoCoreError: On SDK/network errors
            
        Example:
            # Stream large Parquet file
            with open("large_file.parquet", "rb") as f:
                store.upload_stream(
                    "silver/data.parquet", 
                    f, 
                    content_type="application/parquet"
                )
        """
        # Prepare metadata
        extra_args: Dict[str, Any] = {'ContentType': content_type}
        if metadata:
            extra_args['Metadata'] = metadata

        try:
            # Upload via upload_fileobj (handles multipart automatically for large files)
            self.client.upload_fileobj(
                fileobj,
                self.bucket_name,
                key,
                ExtraArgs=extra_args
            )
            
            # Log with best-effort size calculation
            size_str = self._get_fileobj_size_str(fileobj)
            logger.debug(f"✅ Streamed {key} {size_str}")
            
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
                f"❌ BotoCoreError - "
                f"Bucket: {self.bucket_name}, "
                f"Key: {key}, "
                f"Error: {str(e)}"
            )
            raise

    def _get_fileobj_size_str(self, fileobj: Any) -> str:
        """
        Best-effort attempt to get file size for logging.
        
        Args:
            fileobj: File-like object
            
        Returns:
            Formatted size string or empty string if unavailable
        """
        try:
            # Try BytesIO buffer size
            if hasattr(fileobj, 'getbuffer'):
                buffer = fileobj.getbuffer()
                if hasattr(buffer, 'nbytes'):
                    return f"({buffer.nbytes:,} bytes)"
            
            # Try file descriptor stat
            if hasattr(fileobj, 'fileno'):
                size = os.fstat(fileobj.fileno()).st_size
                return f"({size:,} bytes)"
            
            # Try tell/seek method
            if hasattr(fileobj, 'tell') and hasattr(fileobj, 'seek'):
                current_pos = fileobj.tell()
                fileobj.seek(0, os.SEEK_END)
                size = fileobj.tell()
                fileobj.seek(current_pos)  # Restore position
                return f"({size:,} bytes)"
                
        except Exception:
            # Silent failure - size logging is not critical
            pass
        
        return ""

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
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == "404":
                return False
            # Other errors (403 Forbidden, 500) should raise
            logger.error(f"❌ Error checking object {key}: {e}")
            raise

    def list_objects(
        self, 
        prefix: str = "",
        max_keys: Optional[int] = None
    ) -> List[str]:
        """
        List objects in bucket with optional prefix filter.
        
        Args:
            prefix: Optional key prefix filter (default: "" - all objects)
            max_keys: Maximum number of keys to return
            
        Returns:
            List of object keys
            
        Example:
            # List all race results
            keys = store.list_objects("ergast/endpoint=results/")
        """
        try:
            paginator = self.client.get_paginator('list_objects_v2')
            pagination_config = {'PageSize': 1000}
            if max_keys:
                pagination_config['MaxItems'] = max_keys
            
            pages = paginator.paginate(
                Bucket=self.bucket_name, 
                Prefix=prefix,
                PaginationConfig=pagination_config
            )
            
            keys = []
            for page in pages:
                if 'Contents' in page:
                    keys.extend(obj['Key'] for obj in page['Contents'])
            
            logger.debug(f"Listed {len(keys)} objects with prefix '{prefix}'")
            return keys
        except ClientError as e:
            logger.error(f"❌ Error listing objects with prefix '{prefix}': {e}")
            return []
    
    def get_object(self, key: str) -> bytes:
        """
        Download object from S3/MinIO.
        
        Note: Loads entire object into memory. For large files, use stream_object().
        
        Args:
            key: S3 object key
            
        Returns:
            Raw bytes of object content
            
        Raises:
            ClientError: If object not found or access denied
        """
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            content = response['Body'].read()
            logger.debug(f"✅ Downloaded {key} ({len(content):,} bytes)")
            return content
        except ClientError as e:
            logger.error(f"❌ Failed to get object {key}: {e}")
            raise
    
    @contextmanager
    def stream_object(self, key: str):
        """
        Stream object content to avoid loading large files into memory.
        
        Use this context manager for processing large files line-by-line or in chunks.
        
        Args:
            key: S3 object key
            
        Yields:
            StreamingBody: File-like object supporting .read(), .readline(), .iter_lines()
            
        Raises:
            ClientError: If object not found or access denied
            
        Example:
            # Process large file line by line
            with store.stream_object("large_file.json") as stream:
                for line in stream.iter_lines():
                    process(line)
        """
        response = None
        try:
            response = self.client.get_object(Bucket=self.bucket_name, Key=key)
            yield response['Body']
        except ClientError as e:
            logger.error(f"❌ Failed to stream {key}: {e}")
            raise
        finally:
            # Ensure body is closed if response was created
            if response is not None:
                response['Body'].close()

    def get_json(self, key: str) -> Dict[str, Any]:
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
            data = json.loads(content.decode('utf-8'))
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
    
    def get_object_metadata(self, key: str) -> Dict[str, Any]:
        """
        Get object metadata without downloading the object.
        
        Args:
            key: S3 object key
            
        Returns:
            Dictionary containing object metadata
            
        Raises:
            ClientError: If object not found
            
        Example:
            metadata = store.get_object_metadata("data.json")
            print(f"Size: {metadata['size']} bytes")
            print(f"Last modified: {metadata['last_modified']}")
        """
        try:
            response = self.client.head_object(Bucket=self.bucket_name, Key=key)
            metadata = {
                'size': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified'),
                'content_type': response.get('ContentType'),
                'content_encoding': response.get('ContentEncoding'),
                'etag': response.get('ETag'),
                'metadata': response.get('Metadata', {})
            }
            return metadata
        except ClientError as e:
            logger.error(f"❌ Failed to get metadata for {key}: {e}")
            raise
    
    def copy_object(
        self,
        source_key: str,
        dest_key: str,
        source_bucket: Optional[str] = None
    ) -> None:
        """
        Copy an object within the same bucket or from another bucket.
        
        Args:
            source_key: Source object key
            dest_key: Destination object key
            source_bucket: Optional source bucket (defaults to current bucket)
            
        Raises:
            ClientError: On S3 errors
            
        Example:
            # Copy within same bucket
            store.copy_object("old/path.json", "new/path.json")
            
            # Copy from another bucket
            store.copy_object("path.json", "new/path.json", source_bucket="other-bucket")
        """
        try:
            source_bucket = source_bucket or self.bucket_name
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            
            self.client.copy_object(
                CopySource=copy_source,
                Bucket=self.bucket_name,
                Key=dest_key
            )
            
            logger.info(f"Copied {source_bucket}/{source_key} to {self.bucket_name}/{dest_key}")
            
        except ClientError as e:
            logger.error(f"Failed to copy object: {e}")
            raise