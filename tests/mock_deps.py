import sys
from unittest.mock import MagicMock

def setup_mocks():
    """Sets up sys.modules mocks for external packages if they are not installed."""
    for mod_name in [
        "google",
        "google.auth",
        "google.auth.transport",
        "google.auth.transport.requests",
        "google.cloud",
        "google.cloud.storage",
        "google.cloud.bigquery",
        "httpx",
        "fastapi",
        "fastapi.responses",
        "mcp",
        "mcp.server",
        "mcp.server.sse",
    ]:
        if mod_name not in sys.modules:
            sys.modules[mod_name] = MagicMock()

setup_mocks()
