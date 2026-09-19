"""Spark resource - submit PySpark jobs to Spark Master."""

from dagster import ConfigurableResource
from pydantic import Field
import subprocess
import logging

logger = logging.getLogger(__name__)


class SparkResource(ConfigurableResource):
    """Dagster resource for submitting Spark jobs."""

    master_url: str = Field(default="spark://spark-master:7077")
    deploy_mode: str = Field(default="client")
    driver_bind_address: str = Field(default="0.0.0.0")
    s3_endpoint: str = Field(default="minio:9000")
    s3_access_key: str = Field(default="minioadmin")
    s3_secret_key: str = Field(default="minioadmin")

    def submit_job(
        self,
        job_path: str,
        additional_args: list[str] | None = None,
        timeout: int = 600,
    ) -> dict:
        """Submit a PySpark job to the Spark cluster.

        Args:
            job_path: Path to the .py file to submit.
            additional_args: Extra spark-submit arguments.
            timeout: Maximum seconds to wait for completion.

        Returns:
            Dict with returncode, stdout, stderr.
        """
        cmd = [
            "spark-submit",
            "--master", self.master_url,
            "--deploy-mode", self.deploy_mode,
            "--conf", f"spark.driver.bindAddress={self.driver_bind_address}",
            "--conf", f"fs.s3a.endpoint={self.s3_endpoint}",
            "--conf", f"fs.s3a.access.key={self.s3_access_key}",
            "--conf", f"fs.s3a.secret.key={self.s3_secret_key}",
            "--conf", "fs.s3a.path.style.access=true",
            "--conf", "fs.s3a.connection.ssl.enabled=false",
        ]

        if additional_args:
            cmd.extend(additional_args)

        cmd.append(job_path)

        logger.info("Submitting Spark job: %s", " ".join(cmd))

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
        )

        return {
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "success": result.returncode == 0,
        }


spark_resource = SparkResource()
