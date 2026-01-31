import os
import io
import json
import gzip
import pytest
from unittest.mock import MagicMock, patch, call
from f1_pipeline.minio.object_store import F1ObjectStore
from botocore.exceptions import ClientError

# Test Constants
PLACEHOLDER_VAL = "not_a_real_secret"

@pytest.fixture
def mock_s3_client():
    return MagicMock()

@pytest.fixture
def store(mock_s3_client):
    return F1ObjectStore(
        bucket_name="test-bucket",
        endpoint_url="http://mock-minio:9000",
        access_key=os.getenv("MINIO_ACCESS_KEY", PLACEHOLDER_VAL),
        secret_key=os.getenv("MINIO_SECRET_KEY", PLACEHOLDER_VAL),
        client=mock_s3_client
    )

def test_init_creates_client_with_region(mock_env):
    """Test that the client is initialized with the correct region from env."""
    with patch("boto3.client") as mock_boto:
        _ = F1ObjectStore(
            bucket_name="test",
            endpoint_url="http://url",
            access_key=os.getenv("TEST_KEY", PLACEHOLDER_VAL),
            secret_key=os.getenv("TEST_SECRET", PLACEHOLDER_VAL)
        )
        # Check if region_name was passed
        _, kwargs = mock_boto.call_args
        assert kwargs["region_name"] == "us-east-1"

def test_bucket_exists_true(store, mock_s3_client):
    """Test bucket exists returns true when head_bucket succeeds."""
    assert store.bucket_exists() is True
    mock_s3_client.head_bucket.assert_called_with(Bucket="test-bucket")

def test_bucket_exists_false(store, mock_s3_client):
    """Test bucket exists returns false on 404."""
    error_response = {'Error': {'Code': '404', 'Message': 'Not Found'}}
    mock_s3_client.head_bucket.side_effect = ClientError(error_response, 'HeadBucket')
    assert store.bucket_exists() is False

def test_create_bucket_if_not_exists(store, mock_s3_client):
    """Test bucket creation flow."""
    # Mock exists check to return False first
    with patch.object(store, 'bucket_exists', return_value=False):
        store.create_bucket_if_not_exists()
        mock_s3_client.create_bucket.assert_called_with(Bucket="test-bucket")

def test_put_object_compression(store, mock_s3_client):
    """Test that objects are compressed and MD5 checksums are calculated."""
    data = {"test": "data"}
    store.put_object("test.json", data, compress=True)
    
    # Verify call arguments
    call_args = mock_s3_client.put_object.call_args[1]
    assert call_args["Bucket"] == "test-bucket"
    assert call_args["Key"] == "test.json"
    assert call_args["ContentEncoding"] == "gzip"
    assert call_args["ContentType"] == "application/json"
    assert "ContentMD5" in call_args
    
    # Verify body is bytes (gzipped)
    assert isinstance(call_args["Body"], bytes)

def test_get_json_decompression(store, mock_s3_client):
    """Test that get_json automatically decompresses gzipped content."""
    # Prepare compressed mock data
    original_data = {"foo": "bar"}
    json_bytes = json.dumps(original_data).encode("utf-8")
    compressed_bytes = gzip.compress(json_bytes)
    
    # Mock response
    mock_body = MagicMock()
    mock_body.read.return_value = compressed_bytes
    
    mock_s3_client.get_object.return_value = {
        "Body": mock_body,
        "ContentEncoding": "gzip"
    }
    
    result = store.get_json("test.json")
    assert result == original_data
