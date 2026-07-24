import os
from typing import TYPE_CHECKING

from boto3.session import Session
from botocore.config import Config

if TYPE_CHECKING:
    from boto3 import Session as Boto3Session
    from botocore.client import BaseClient
else:
    Boto3Session = object
    BaseClient = object

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")


def S3Session() -> "Boto3Session":
    """Return a boto3 Session pointed at MinIO."""

    return Session(aws_access_key_id=AWS_KEY, aws_secret_access_key=AWS_SECRET)


def S3Client(endpoint_url: str = MINIO_ENDPOINT) -> BaseClient:
    """Return a boto3 S3 client pointed at MinIO."""

    session = S3Session()

    return session.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        config=Config(signature_version="s3v4"),
        region_name="eu-central-1",
    )
