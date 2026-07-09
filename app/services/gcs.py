import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from google.cloud import storage
from app.config import settings

logger = logging.getLogger("appoptimize.gcs")


class GCSService:
    """Service for exporting report data to Google Cloud Storage."""

    def __init__(self, storage_client: Optional[storage.Client] = None):
        self._client = storage_client

    def _get_client(self) -> storage.Client:
        if not self._client:
            self._client = storage.Client()
        return self._client

    async def export_report(
        self,
        report_id: str,
        data: str,
        file_name: Optional[str] = None,
        bucket_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Uploads report data string to a GCS bucket asynchronously."""
        target_bucket = bucket_name or settings.reports_bucket
        if not target_bucket or target_bucket == "YOUR_BUCKET_NAME":
            return {
                "success": False,
                "error": "GCS bucket name not configured. Please set REPORTS_BUCKET env var.",
                "gcs_uri": None,
            }

        if not file_name:
            prefix = "appoptimizev2-utilization" if "utilization" in data.lower() else "appoptimizev2-costs"
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
            file_name = f"{prefix}-{report_id}-{timestamp}.json"

        def _upload():
            client = self._get_client()
            bucket = client.bucket(target_bucket)
            blob = bucket.blob(file_name)
            blob.upload_from_string(data, content_type="application/json")
            return f"gs://{target_bucket}/{file_name}"

        try:
            gcs_uri = await asyncio.to_thread(_upload)
            logger.info(f"Successfully exported report {report_id} to {gcs_uri}")
            return {
                "success": True,
                "error": None,
                "gcs_uri": gcs_uri,
            }
        except Exception as e:
            logger.error(f"Error exporting report {report_id} to GCS: {e}")
            return {
                "success": False,
                "error": f"GCS Upload Error: {str(e)}",
                "gcs_uri": None,
            }
