"""MinIO resource - S3-compatible object storage for Bronze/Silver/Gold."""

from dagster import ConfigurableResource
from pydantic import Field
import boto3
from typing import Optional


class MinIOResource(ConfigurableResource):
    """Dagster resource for MinIO / S3-compatible storage."""

    endpoint_url: str = Field(default="http://minio:9000")
    access_key: str = Field(default="minioadmin")
    secret_key: str = Field(default="minioadmin")
    bucket: str = Field(default="tourism")
    region_name: str = Field(default="us-east-1")

    def get_client(self):
        """Create boto3 S3 client pointing to MinIO."""
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region_name,
        )

    def upload_file(self, local_path: str, s3_key: str) -> str:
        """Upload a file to MinIO."""
        client = self.get_client()
        client.upload_file(local_path, self.bucket, s3_key)
        return f"s3a://{self.bucket}/{s3_key}"

    def list_objects(self, prefix: str = "") -> list[str]:
        """List objects in bucket with given prefix."""
        client = self.get_client()
        response = client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]

    def object_exists(self, s3_key: str) -> bool:
        """Check if an object exists in MinIO."""
        client = self.get_client()
        try:
            client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except Exception:
            return False


minio_resource = MinIOResource()
