
import json
import boto3
import logging
from typing import Any, Dict, List, Union, Optional
from botocore.exceptions import ClientError, BotoCoreError

logger = logging.getLogger(__name__)

class F1ObjectStore:
    def __init__(
        self, 
        bucket_name: str, 
        endpoint_url: str, 
        access_key: str, 
        secret_key: str,
        client: Optional[Any] = None
    ) -> None:
        """
        Initializes the ObjectStore with a bucket and an S3 client.
        
        Args:
            bucket_name: The name of the target S3 bucket.
            endpoint_url: URL for the MinIO/S3 service.
            access_key: AWS access key ID.
            secret_key: AWS secret access key.
            client: Optional pre-configured boto3 client (for testing/dependency injection).
        """
        self.bucket_name = bucket_name
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key
        
        # Initialize client if not provided
        self.client = client or self._create_client()

    def _create_client(self) -> Any:
        """Internal helper to create the low-level boto3 client."""
        s3_client = boto3.client(
        "s3",
        endpoint_url=self.endpoint_url,
        aws_access_key_id=self.access_key,
        aws_secret_access_key=self.secret_key,
        )
        return s3_client

    def _serialize_body(self, body: Union[Dict, List, str, bytes]) -> tuple[str, str]:
        """
        Helper to handle data serialization.
        
        Returns:
            A tuple containing (serialized_body, content_type)
        """
        if isinstance(body, (dict, list)):
            body = json.dumps(body, indent=2)
            content_type = 'application/json'
        elif isinstance(body, bytes):
             content_type = 'application/octet-stream'
        else:
            # Default for other types (strings, bytes)
            content_type = 'text/plain'
        return(body, content_type) # type: ignore

    def bucket_exists(self) -> bool:
        """Checks if the configured bucket exists in the store."""
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            return True
        except ClientError:
            return False

    def create_bucket_if_not_exists(self) -> None:
        """Creates the bucket if it does not already exist."""
        if not self.bucket_exists():
            self.client.create_bucket(Bucket=self.bucket_name)
            logger.info(f"Bucket '{self.bucket_name}' created.")
        else:
            logger.info(f"Bucket '{self.bucket_name}' already exists.")

    def put_object(self, key: str, body: Union[Dict, List, str]) -> None:
        """
        Uploads an object to the configured bucket.
        
        Args:
            key: The file path/name in S3 (e.g., 'f1/seasons.json').
            body: The content to upload (Dicts/Lists are auto-serialized to JSON).
        """
        # 1. Serialize data using _serialize_body
        body, content_type = self._serialize_body(body)

        # 2. Call self.client.put_object using self.bucket_name
        try:
            self.client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=body,
                ContentType=content_type,
            )

            logger.info(f"✅ Uploaded {key} to bucket {self.bucket_name}")

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
        

    def delete_bucket(self) -> None:
        """Deletes the current bucket."""
        self.client.delete_bucket(Bucket=self.bucket_name)
        logger.info(f"{self.bucket_name} deleted sucessfully.")