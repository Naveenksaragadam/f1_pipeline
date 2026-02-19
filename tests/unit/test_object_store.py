import gzip
import json
import os
from typing import Any, cast
from unittest.mock import ANY, MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from f1_pipeline.minio.object_store import F1ObjectStore

# Test Constants
PLACEHOLDER_VAL = "not_a_real_secret"


@pytest.fixture
def mock_s3_client() -> MagicMock:
    return MagicMock()


@pytest.fixture
def store(mock_s3_client: MagicMock) -> F1ObjectStore:
    return F1ObjectStore(
        bucket_name="test-bucket",
        endpoint_url="http://mock-minio:9000",
        access_key=os.getenv("MINIO_ACCESS_KEY", PLACEHOLDER_VAL),
        secret_key=os.getenv("MINIO_SECRET_KEY", PLACEHOLDER_VAL),
        client=mock_s3_client,
    )


def test_init_creates_client_with_region(mock_env: Any) -> None:
    """Test that the client is initialized with the correct region from env."""
    with patch("boto3.client") as mock_boto:
        _ = F1ObjectStore(
            bucket_name="test",
            endpoint_url="http://url",
            access_key=os.getenv("TEST_KEY", PLACEHOLDER_VAL),
            secret_key=os.getenv("TEST_SECRET", PLACEHOLDER_VAL),
        )
        # Check if region_name was passed
        _, kwargs = mock_boto.call_args
        assert kwargs["region_name"] == "us-east-1"


def test_create_client_failure() -> None:
    """Test that client creation failures are raised."""
    with patch("boto3.client") as mock_boto:
        mock_boto.side_effect = Exception("Auth failure")
        with pytest.raises(Exception, match="Auth failure"):
            _ = F1ObjectStore(
                bucket_name="test",
                endpoint_url="http://url",
                access_key="key",
                secret_key="secret",
            )


def test_bucket_exists_true(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test bucket exists returns true when head_bucket succeeds."""
    assert store.bucket_exists() is True
    mock_s3_client.head_bucket.assert_called_with(Bucket="test-bucket")


def test_bucket_exists_false(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test bucket exists returns false on 404."""
    error_response: dict[str, Any] = {"Error": {"Code": "404", "Message": "Not Found"}}
    mock_s3_client.head_bucket.side_effect = ClientError(cast(Any, error_response), "HeadBucket")
    assert store.bucket_exists() is False


def test_bucket_exists_forbidden(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test bucket exists raises on other errors like 403."""
    error_response: dict[str, Any] = {"Error": {"Code": "403", "Message": "Forbidden"}}
    mock_s3_client.head_bucket.side_effect = ClientError(cast(Any, error_response), "HeadBucket")
    with pytest.raises(ClientError):
        store.bucket_exists()


def test_create_bucket_if_not_exists(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test bucket creation flow."""
    # Mock exists check to return False first
    with patch.object(store, "bucket_exists", return_value=False):
        store.create_bucket_if_not_exists()
        mock_s3_client.create_bucket.assert_called_with(Bucket="test-bucket")


def test_delete_bucket_standard(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test standard bucket deletion."""
    store.delete_bucket(force=False)
    mock_s3_client.delete_bucket.assert_called_with(Bucket="test-bucket")


def test_delete_bucket_force(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test force bucket deletion with emptying."""
    # Mock paginator for emptying
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{"Contents": [{"Key": "obj1"}, {"Key": "obj2"}]}]
    mock_s3_client.get_paginator.return_value = mock_paginator

    store.delete_bucket(force=True)

    # Should have called delete_objects
    mock_s3_client.delete_objects.assert_called()
    mock_s3_client.delete_bucket.assert_called_with(Bucket="test-bucket")


def test_delete_bucket_error(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test deletion error handling."""
    mock_s3_client.delete_bucket.side_effect = ClientError(
        {"Error": {"Code": "500", "Message": "Err"}}, "DeleteBucket"
    )
    with pytest.raises(ClientError):
        store.delete_bucket()


def test_put_object_compression(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
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


def test_get_json_decompression(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
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
        "ContentEncoding": "gzip",
    }

    result = store.get_json("test.json")
    assert result == original_data


def test_put_object_no_compression(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test that objects can be uploaded without compression."""
    data = {"test": "data"}
    store.put_object("test.json", data, compress=False)

    # Verify call arguments
    call_args = mock_s3_client.put_object.call_args[1]
    assert "ContentEncoding" not in call_args
    assert call_args["ContentType"] == "application/json"
    # Even if not gzipped, put_object encodes JSON to bytes
    assert isinstance(call_args["Body"], bytes)


def test_put_object_file_like(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test uploading a file-like object."""
    from io import BytesIO

    data = BytesIO(b"raw data")
    store.put_object("test.bin", data, compress=False)

    call_args = mock_s3_client.put_object.call_args[1]
    assert call_args["ContentType"] == "application/octet-stream"
    assert call_args["Body"] == b"raw data"


def test_put_object_metadata(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test custom metadata is passed."""
    store.put_object("test.json", {"d": 1}, metadata={"source": "jolpica"})
    call_args = mock_s3_client.put_object.call_args[1]
    assert call_args["Metadata"] == {"source": "jolpica"}


def test_put_object_md5_failure(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test MD5 mismatch error handling."""
    from botocore.exceptions import BotoCoreError

    mock_s3_client.put_object.side_effect = BotoCoreError()
    with pytest.raises(BotoCoreError):
        store.put_object("test.json", {"d": 1})


def test_object_exists_true(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test object_exists returns true."""
    mock_s3_client.head_object.return_value = {}
    assert store.object_exists("key") is True


def test_object_exists_false(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test object_exists returns false on 404."""
    mock_s3_client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
    assert store.object_exists("key") is False


def test_object_exists_error(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test object_exists raises on non-404 errors."""
    mock_s3_client.head_object.side_effect = ClientError({"Error": {"Code": "403"}}, "HeadObject")
    with pytest.raises(ClientError):
        store.object_exists("key")


def test_list_objects_empty(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test listing empty bucket."""
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [{}]
    mock_s3_client.get_paginator.return_value = mock_paginator
    assert store.list_objects() == []


def test_list_objects_pagination(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test listing with pagination."""
    mock_paginator = MagicMock()
    mock_paginator.paginate.return_value = [
        {"Contents": [{"Key": "o1"}]},
        {"Contents": [{"Key": "o2"}]},
    ]
    mock_s3_client.get_paginator.return_value = mock_paginator
    keys = store.list_objects()
    assert keys == ["o1", "o2"]


def test_list_objects_max_keys(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test listing with max_keys limit. Note: paginator handles truncation."""
    mock_paginator = MagicMock()
    # In reality, MaxItems in PaginationConfig would limit this
    mock_paginator.paginate.return_value = [{"Contents": [{"Key": "o1"}, {"Key": "o2"}]}]
    mock_s3_client.get_paginator.return_value = mock_paginator
    keys = store.list_objects(max_keys=2)
    assert keys == ["o1", "o2"]


def test_get_object_no_compression(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test retrieving non-compressed object."""
    mock_body = MagicMock()
    mock_body.read.return_value = b"raw bytes"
    mock_s3_client.get_object.return_value = {"Body": mock_body}

    assert store.get_object("key") == b"raw bytes"


def test_get_object_error(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test get_object error handling."""
    mock_s3_client.get_object.side_effect = ClientError({"Error": {"Code": "404"}}, "GetObject")
    with pytest.raises(ClientError):
        store.get_object("key")


def test_stream_object(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test streaming object."""
    mock_body = MagicMock()
    mock_s3_client.get_object.return_value = {"Body": mock_body}

    with store.stream_object("key") as body:
        assert body == mock_body


def test_stream_object_error(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test streaming error handling."""
    mock_s3_client.get_object.side_effect = ClientError({"Error": {"Code": "404"}}, "GetObject")
    with pytest.raises(ClientError):
        with store.stream_object("key"):
            pass


def test_get_json_decode_error(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test handling of invalid JSON."""
    mock_body = MagicMock()
    mock_body.read.return_value = b"invalid json"
    mock_s3_client.get_object.return_value = {"Body": mock_body}

    with pytest.raises(json.JSONDecodeError):
        store.get_json("key")


def test_delete_object(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test successful object deletion."""
    store.delete_object("key")
    mock_s3_client.delete_object.assert_called_with(Bucket="test-bucket", Key="key")


def test_delete_object_error(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test delete_object error handling."""
    mock_s3_client.delete_object.side_effect = ClientError(
        {"Error": {"Code": "403"}}, "DeleteObject"
    )
    with pytest.raises(ClientError):
        store.delete_object("key")


def test_get_object_metadata(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test metadata retrieval."""
    mock_s3_client.head_object.return_value = {
        "Metadata": {"v": "1"},
        "ContentType": "application/json",
    }
    result = store.get_object_metadata("key")
    assert result["metadata"] == {"v": "1"}
    assert result["content_type"] == "application/json"


def test_get_object_metadata_error(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test metadata retrieval error."""
    mock_s3_client.head_object.side_effect = ClientError({"Error": {"Code": "404"}}, "HeadObject")
    with pytest.raises(ClientError):
        store.get_object_metadata("key")


def test_create_bucket_already_exists(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test create_bucket when it already exists."""
    with patch.object(store, "bucket_exists", return_value=True):
        store.create_bucket_if_not_exists()
        mock_s3_client.create_bucket.assert_not_called()


def test_create_bucket_client_error(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test create_bucket error handling."""
    with patch.object(store, "bucket_exists", return_value=False):
        mock_s3_client.create_bucket.side_effect = ClientError(
            {"Error": {"Code": "500"}}, "CreateBucket"
        )
        with pytest.raises(ClientError):
            store.create_bucket_if_not_exists()


def test_serialize_body_all_types(store: F1ObjectStore) -> None:
    """Test the internal _serialize_body method (coverage)."""
    # dict
    body, ct = store._serialize_body({"a": 1})
    assert ct == "application/json"
    assert "a" in body
    # list
    body, ct = store._serialize_body([1, 2])
    assert ct == "application/json"
    # bytes
    body, ct = store._serialize_body(b"bytes")
    assert ct == "application/octet-stream"
    assert body == b"bytes"
    # other/str
    body, ct = store._serialize_body(123)  # type: ignore[arg-type]
    assert ct == "text/plain"
    assert body == "123"


def test_put_object_variations(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test put_object with string, bytes, and unsupported types."""
    # string
    store.put_object("key", "hello", compress=False)
    mock_s3_client.put_object.assert_called_with(
        Bucket=ANY, Key="key", Body=b"hello", ContentType="text/plain", ContentMD5=ANY
    )
    # bytes
    store.put_object("key", b"bytes", compress=False)
    mock_s3_client.put_object.assert_called_with(
        Bucket=ANY, Key="key", Body=b"bytes", ContentType="application/octet-stream", ContentMD5=ANY
    )
    # unsupported (int) -> converted to str
    store.put_object("key", 123, compress=False)  # type: ignore[arg-type]
    mock_s3_client.put_object.assert_called_with(
        Bucket=ANY, Key="key", Body=b"123", ContentType="application/octet-stream", ContentMD5=ANY
    )


def test_put_object_file_like_variations(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test put_object with file-like objects returning different types."""
    # returning str
    obj = MagicMock()
    obj.read.return_value = "content"
    store.put_object("key", obj, compress=False)
    mock_s3_client.put_object.assert_called_with(
        Bucket=ANY,
        Key="key",
        Body=b"content",
        ContentType="application/octet-stream",
        ContentMD5=ANY,
    )
    # returning something else
    obj.read.return_value = 123
    store.put_object("key", obj, compress=False)
    mock_s3_client.put_object.assert_called_with(
        Bucket=ANY, Key="key", Body=b"123", ContentType="application/octet-stream", ContentMD5=ANY
    )


def test_put_object_client_error(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test put_object S3 ClientError."""
    mock_s3_client.put_object.side_effect = ClientError(
        {"Error": {"Code": "403", "Message": "Forbidden"}}, "PutObject"
    )
    with pytest.raises(ClientError):
        store.put_object("key", {"a": 1})


def test_list_objects_client_error(store: F1ObjectStore, mock_s3_client: MagicMock) -> None:
    """Test list_objects S3 ClientError (returns empty list)."""
    # Paginator error
    mock_paginator = MagicMock()
    mock_paginator.paginate.side_effect = ClientError({"Error": {"Code": "500"}}, "ListObjectsV2")
    mock_s3_client.get_paginator.return_value = mock_paginator

    assert store.list_objects() == []
