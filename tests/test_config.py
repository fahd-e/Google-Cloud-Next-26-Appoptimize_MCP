import os
import sys
import unittest
from unittest.mock import MagicMock, patch

# Mock external GCP and FastAPI dependencies before importing app modules
mock_google = MagicMock()
mock_google_auth = MagicMock()
mock_google.auth = mock_google_auth
sys.modules["google"] = mock_google
sys.modules["google.auth"] = mock_google_auth
sys.modules["google.auth.transport"] = MagicMock()
sys.modules["google.auth.transport.requests"] = MagicMock()
sys.modules["google.cloud"] = MagicMock()
sys.modules["google.cloud.storage"] = MagicMock()
sys.modules["google.cloud.bigquery"] = MagicMock()
sys.modules["httpx"] = MagicMock()
sys.modules["fastapi"] = MagicMock()
sys.modules["fastapi.responses"] = MagicMock()
sys.modules["mcp"] = MagicMock()
sys.modules["mcp.server"] = MagicMock()
sys.modules["mcp.server.sse"] = MagicMock()

from app.config import Settings


class TestConfig(unittest.TestCase):
    def test_settings_default_values(self):
        with patch.dict(os.environ, {}, clear=True):
            s = Settings()
            self.assertEqual(s.host, "0.0.0.0")
            self.assertEqual(s.port, 8080)
            self.assertEqual(s.log_level, "INFO")
            self.assertEqual(s.bigquery_dataset, "appoptimize_demo")

    def test_settings_env_overrides(self):
        env = {
            "HOST": "127.0.0.1",
            "PORT": "9090",
            "LOG_LEVEL": "DEBUG",
            "PROJECT_ID": "my-test-proj",
            "REPORTS_BUCKET": "my-test-bucket",
            "BIGQUERY_DATASET": "custom_dataset",
        }
        with patch.dict(os.environ, env, clear=True):
            s = Settings()
            self.assertEqual(s.host, "127.0.0.1")
            self.assertEqual(s.port, 9090)
            self.assertEqual(s.log_level, "DEBUG")
            self.assertEqual(s.project_id, "my-test-proj")
            self.assertEqual(s.reports_bucket, "my-test-bucket")
            self.assertEqual(s.bigquery_dataset, "custom_dataset")

    def test_get_project_id_missing(self):
        with patch.dict(os.environ, {}, clear=True):
            s = Settings()
            s._auth_initialized = True  # Prevent ADC call in test
            s._default_project = None
            with self.assertRaises(ValueError):
                s.get_project_id()


if __name__ == "__main__":
    unittest.main()
