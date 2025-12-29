#src/f1_data/minio/object_store.py
"""
Production-grade S3/MinIO object store client with connection pooling and error handling.
"""
import json
import boto3
import logging
from botocore.client import Config
from typing import Any, Dict, List, Union, Optional
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)

class F1ObjectStore:
    """
    S3-compatible object store client optimized for MinIO.
    
    Features:
    - Connection pooling
    - Automatic bucket creation
    - Retry logic on transient failures
    - Type-safe serialization
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
        Initializes the ObjectStore with a bucket and an S3 client.
        
        Args:
            bucket_name: Target S3 bucket name
            endpoint_url: MinIO/S3 endpoint URL
            access_key: AWS access key ID
            secret_key: AWS secret access key
            client: Optional pre-configured boto3 client
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
            return client
        except Exception as e:
            logger.error(f"❌ Failed to create S3 client: {e}")
            raise

    def bucket_exists(self) -> bool:
        """
        Check if the configured bucket exists.
        
        Returns:
            True if bucket exists, False otherwise
        """
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code in ['404', 'NoSuchBucket']:
                return False
            # For other errors (403 Forbidden, etc.), log and re-raise
            logger.error(f"Error checking bucket {self.bucket_name}: {e}")
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
                logger.info(f"ℹ️ Bucket '{self.bucket_name}' already exists.")
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
        """Delete all objects in the bucket."""
        paginator = self.client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=self.bucket_name):
            if 'Contents' in page:
                objects = [{'Key': obj['Key']} for obj in page['Contents']]
                self.client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={'Objects': objects}
                )

    def _serialize_body(
            self, 
            body: Union[Dict, List, str, bytes]
        ) -> tuple[Union[str, bytes], str]:
        """
        Serialize data for S3 upload.
        
        Args:
            body: Data to serialize
            
        Returns:
            Tuple of (serialized_body, content_type)
        """
        if isinstance(body, (dict, list)):
            return (json.dumps(body, indent=2), 'application/json')
        elif isinstance(body, bytes):
            return (body, 'application/octet-stream')
        else:
            return (str(body), 'text/plain') # type: ignore

    def put_object(
            self, 
            key: str, 
            body: Union[Dict, List, str, bytes],
            metadata: Optional[Dict[str, str]] = None
        ) -> None:
        """
        Upload object to S3/MinIO.
        
        Args:
            key: S3 object key (path)
            body: Content to upload
            metadata: Optional S3 metadata tags
            
        Raises:
            ClientError: On S3 errors
            BotoCoreError: On SDK/network errors
        """
        # 1. Serialize data using _serialize_body
        serialized_body, content_type = self._serialize_body(body)

        # 2. Call self.client.put_object using self.bucket_name
        try:
            put_params = {
                'Bucket': self.bucket_name,
                'Key': key,
                'Body': serialized_body,
                'ContentType': content_type,
            }
            
            if metadata:
                put_params['Metadata'] = metadata
            
            self.client.put_object(**put_params)

            # Calculate approximate size for logging
            size_bytes = len(serialized_body) if isinstance(serialized_body, (str, bytes)) else 0
            logger.debug(f"✅ Uploaded {key} ({size_bytes:,} bytes)")

        except ClientError as e:
            error = e.response.get("Error", {})
            logger.error("❌ S3 ClientError")
            logger.error(f"Bucket: {self.bucket_name}")
            logger.error(f"Key: {key}")
            logger.error(f"Code: {error.get('Code')}")
            logger.error(f"Message: {error.get('Message')}")
            raise  # keep failure visible during iteration

        except BotoCoreError as e:
            logger.error("❌ BotoCoreError (SDK / network / credentials)")
            logger.error(f"Bucket: {self.bucket_name}")
            logger.error(f"Key: {key}")
            logger.error(f"Error: {str(e)}")
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
            # 404 Not Found is the only "safe" error to return False for
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == "404":
                return False
            # Other errors (403 Forbidden, 500) should raise an exception
            logger.error(f"❌ Error checking object {key}: {e}")
            raise

    def list_objects(
            self, 
            prefix: str,
            max_keys: Optional[int] = None
        ) -> List[str]:
        """
        List objects in bucket with optional prefix filter.
        
        Args:
            prefix: Optional key prefix filter
            max_keys: Maximum number of keys to return
            
        Returns:
            List of object keys
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
            logger.error(f"Error listing objects with prefix '{prefix}': {e}")
            return []
    
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
            content = response['Body'].read()
            logger.debug(f"✅ Downloaded {key} ({len(content):,} bytes)")
            return content
        except ClientError as e:
            logger.error(f"❌ Failed to get object {key}: {e}")
            raise
        
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
        """
        try:
            self.client.delete_object(Bucket=self.bucket_name, Key=key)
            logger.info(f"✅ Deleted object: {key}")
        except ClientError as e:
            logger.error(f"❌ Failed to delete {key}: {e}")
            raise