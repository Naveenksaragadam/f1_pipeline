import boto3
from botocore.exceptions import ClientError

minio_endpoint = "http://localhost:9000"
access_key = "minioadmin"
secret_key = "password"

s3_client = boto3.client(
    "s3",
    endpoint_url=minio_endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
)


bucket_name = "f1-pipeline"

# Check if bucket exists
def bucket_exists(bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return True
    except ClientError:
        return False

# Create bucket if it does not exist
if not bucket_exists(bucket_name):
    s3_client.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' created.")
else:
    print(f"Bucket '{bucket_name}' already exists.")

buckets = s3_client.list_buckets()
for b in buckets['Buckets']:
    print(b['Name'])

s3_client.delete_bucket(Bucket=bucket_name)
