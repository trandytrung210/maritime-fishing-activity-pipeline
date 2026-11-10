import os

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

print("Access Key:", bool(os.getenv("AWS_ACCESS_KEY_ID")))
print("Secret Key:", bool(os.getenv("AWS_SECRET_ACCESS_KEY")))
print("Endpoint:", os.getenv("S3_ENDPOINT"))

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT", "http://127.0.0.1:8333"),
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name="us-east-1",
)
buckets = [
    "fishing-bronze",
    "fishing-silver",
    "fishing-gold",
]
for bucket in buckets:
    try:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket created: {bucket}")
    except ClientError as error:
        code = error.response["Error"]["Code"]
        if code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"Bucket already exists: {bucket}")
        else:
            raise

response = s3.list_buckets()
print("\nAvailable buckets:")
for bucket in response["Buckets"]:
    print("-", bucket["Name"])
