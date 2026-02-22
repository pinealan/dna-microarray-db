"""
S3-compatible storage helpers (targets DigitalOcean Spaces but works with any S3).
"""

from pathlib import Path

import boto3
from botocore.client import Config

from miqa import config


def _client():
    return boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT_URL,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
    )


def upload_file(local_path: str | Path, s3_key: str) -> str:
    """Upload a local file to S3. Returns the s3_key on success."""
    _client().upload_file(str(local_path), config.S3_BUCKET, s3_key)
    return s3_key


def delete_file(s3_key: str) -> None:
    """Delete an object from S3."""
    _client().delete_object(Bucket=config.S3_BUCKET, Key=s3_key)
