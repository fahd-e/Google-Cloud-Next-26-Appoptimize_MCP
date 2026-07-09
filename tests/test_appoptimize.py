import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import tests.mock_deps

from app.services.appoptimize import AppOptimizeService


class TestAppOptimizeService(unittest.TestCase):
    def setUp(self):
        self.mock_client = MagicMock()
        self.mock_client.is_closed = False
        self.service = AppOptimizeService(client=self.mock_client)

    @patch("app.services.appoptimize.get_access_token", new_callable=AsyncMock)
    def test_create_report(self, mock_get_token):
        mock_get_token.return_value = "fake-token"
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = '{"name": "reports/rep1"}'
        self.mock_client.post = AsyncMock(return_value=mock_resp)

        with patch("app.services.appoptimize.settings") as mock_settings:
            mock_settings.get_project_id.return_value = "test-project"
            mock_settings.base_url = "https://appoptimize.googleapis.com/v1beta"

            res = asyncio.run(
                self.service.create_report(
                    report_id="rep1",
                    dimensions=["project"],
                    metrics=["cost"],
                )
            )

            self.assertTrue(res["success"])
            self.assertEqual(res["status_code"], 200)
            self.mock_client.post.assert_called_once()
            call_args = self.mock_client.post.call_args
            self.assertIn("projects/test-project/locations/global/reports?reportId=rep1", call_args[0][0])

    @patch("app.services.appoptimize.get_access_token", new_callable=AsyncMock)
    def test_get_report(self, mock_get_token):
        mock_get_token.return_value = "fake-token"
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.is_success = True
        mock_resp.text = '{"state": "COMPLETED"}'
        self.mock_client.get = AsyncMock(return_value=mock_resp)

        with patch("app.services.appoptimize.settings") as mock_settings:
            mock_settings.get_project_id.return_value = "test-project"
            mock_settings.base_url = "https://appoptimize.googleapis.com/v1beta"

            res = asyncio.run(self.service.get_report(report_id="rep1"))
            self.assertTrue(res["success"])

    @patch("app.services.appoptimize.get_access_token", new_callable=AsyncMock)
    def test_poll_until_ready_success(self, mock_get_token):
        mock_get_token.return_value = "fake-token"
        mock_resp1 = MagicMock()
        mock_resp1.status_code = 400
        mock_resp1.is_success = False
        mock_resp1.text = '{"error": "Report is not ready yet"}'

        mock_resp2 = MagicMock()
        mock_resp2.status_code = 200
        mock_resp2.is_success = True
        mock_resp2.text = '{"rows": [{"cost": 100}]}'

        self.mock_client.post = AsyncMock(side_effect=[mock_resp1, mock_resp2])

        with patch("app.services.appoptimize.settings") as mock_settings:
            mock_settings.get_project_id.return_value = "test-project"
            mock_settings.base_url = "https://appoptimize.googleapis.com/v1beta"

            res = asyncio.run(
                self.service.poll_until_ready(
                    report_id="rep1",
                    initial_backoff=0.01,
                    timeout_seconds=5.0,
                )
            )

            self.assertTrue(res["ready"])
            self.assertEqual(res["data"], '{"rows": [{"cost": 100}]}')
            self.assertEqual(self.mock_client.post.call_count, 2)


if __name__ == "__main__":
    unittest.main()
