import asyncio
import unittest
from unittest.mock import MagicMock, patch
import tests.mock_deps

from app.services.gcs import GCSService


class TestGCSService(unittest.TestCase):
    def test_export_report_no_bucket_configured(self):
        service = GCSService()
        with patch("app.services.gcs.settings") as mock_settings:
            mock_settings.reports_bucket = ""
            res = asyncio.run(
                service.export_report(report_id="rep1", data='{"cost": 10}')
            )
            self.assertFalse(res["success"])
            self.assertIn("not configured", res["error"])

    def test_export_report_success(self):
        mock_blob = MagicMock()
        mock_bucket = MagicMock()
        mock_bucket.blob.return_value = mock_blob
        mock_storage_client = MagicMock()
        mock_storage_client.bucket.return_value = mock_bucket

        service = GCSService(storage_client=mock_storage_client)

        with patch("app.services.gcs.settings") as mock_settings:
            mock_settings.reports_bucket = "my-export-bucket"

            res = asyncio.run(
                service.export_report(
                    report_id="rep123",
                    data='{"cost": 100}',
                    file_name="custom_report.json",
                )
            )

            self.assertTrue(res["success"])
            self.assertEqual(res["gcs_uri"], "gs://my-export-bucket/custom_report.json")
            mock_storage_client.bucket.assert_called_with("my-export-bucket")
            mock_bucket.blob.assert_called_with("custom_report.json")
            mock_blob.upload_from_string.assert_called_once()


if __name__ == "__main__":
    unittest.main()
