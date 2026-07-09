import unittest
from unittest.mock import MagicMock, patch
import tests.mock_deps  # Ensures dependencies are mocked

import asyncio
from app.auth import get_access_token


class TestAuth(unittest.TestCase):
    def test_get_access_token_cached(self):
        mock_creds = MagicMock()
        mock_creds.valid = True
        mock_creds.expired = False
        mock_creds.token = "cached_token_xyz"

        with patch("app.auth.settings") as mock_settings:
            mock_settings.credentials = mock_creds

            token = asyncio.run(get_access_token())
            self.assertEqual(token, "cached_token_xyz")
            mock_creds.refresh.assert_not_called()

    def test_get_access_token_refresh(self):
        mock_creds = MagicMock()
        mock_creds.valid = False
        mock_creds.expired = True
        mock_creds.token = "new_token_abc"

        with patch("app.auth.settings") as mock_settings:
            mock_settings.credentials = mock_creds

            token = asyncio.run(get_access_token())
            self.assertEqual(token, "new_token_abc")
            self.assertTrue(mock_creds.refresh.called)

    def test_get_access_token_no_credentials(self):
        with patch("app.auth.settings") as mock_settings:
            mock_settings.credentials = None
            with self.assertRaises(RuntimeError):
                asyncio.run(get_access_token())


if __name__ == "__main__":
    unittest.main()
