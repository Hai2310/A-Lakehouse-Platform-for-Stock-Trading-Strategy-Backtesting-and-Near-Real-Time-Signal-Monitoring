"""Helpers for reading and writing lakehouse objects in Google Cloud Storage.

Authentication uses Application Default Credentials (ADC), so local development
can use ``gcloud auth application-default login`` and deployed workloads can use
their attached workload identity or service account.
"""

from __future__ import annotations

import os
from io import BytesIO
from pathlib import Path
from typing import TypeAlias

import pandas as pd
from dotenv import load_dotenv
from google.auth.exceptions import DefaultCredentialsError
from google.cloud import storage


PathLike: TypeAlias = str | os.PathLike[str]


class GCSStore:
    """Access a configured GCS bucket using Google Application Default Credentials."""

    def __init__(self, bucket_name: str | None = None, project: str | None = None) -> None:
        load_dotenv()
        self.bucket_name = bucket_name or os.getenv("GCS_BUCKET")
        if not self.bucket_name:
            raise ValueError("Set GCS_BUCKET in .env or pass bucket_name explicitly.")

        project_id = project or os.getenv("GOOGLE_CLOUD_PROJECT") or None
        try:
            self.client = storage.Client(project=project_id)
        except DefaultCredentialsError as exc:
            raise RuntimeError(
                "Google credentials are not configured. Run "
                "`gcloud auth application-default login` and try again."
            ) from exc

        self.bucket = self.client.bucket(self.bucket_name)

    def upload_file(
        self,
        source_path: PathLike,
        object_name: str,
        *,
        content_type: str | None = None,
    ) -> str:
        """Upload a local file and return its ``gs://`` URI."""
        blob = self.bucket.blob(object_name)
        blob.upload_from_filename(str(source_path), content_type=content_type)
        return f"gs://{self.bucket_name}/{object_name}"

    def download_file(self, object_name: str, destination_path: PathLike) -> Path:
        """Download an object, creating the destination directory if needed."""
        destination = Path(destination_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.bucket.blob(object_name).download_to_filename(str(destination))
        return destination

    def upload_dataframe_parquet(
        self,
        dataframe: pd.DataFrame,
        object_name: str,
        *,
        index: bool = False,
    ) -> str:
        """Serialize a DataFrame as Parquet in memory and upload it to GCS."""
        buffer = BytesIO()
        dataframe.to_parquet(buffer, index=index)
        self.bucket.blob(object_name).upload_from_string(
            buffer.getvalue(), content_type="application/vnd.apache.parquet"
        )
        return f"gs://{self.bucket_name}/{object_name}"

    def list_objects(self, prefix: str = "") -> list[str]:
        """List object names under an optional prefix."""
        return [blob.name for blob in self.client.list_blobs(self.bucket, prefix=prefix)]
