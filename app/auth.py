import asyncio
import logging
import google.auth.transport.requests
from app.config import settings

logger = logging.getLogger("appoptimize.auth")
_token_lock = asyncio.Lock()

async def get_access_token() -> str:
    """Retrieves a valid GCP OAuth access token, refreshing only when expired."""
    creds = settings.credentials
    if not creds:
        raise RuntimeError(
            "No Google credentials available. Please set up ADC or service account."
        )

    async with _token_lock:
        if creds.valid and creds.token and not creds.expired:
            return creds.token

        logger.debug("Refreshing Google access token...")
        loop = asyncio.get_running_loop()
        def _refresh():
            auth_req = google.auth.transport.requests.Request()
            creds.refresh(auth_req)

        await loop.run_in_executor(None, _refresh)
        return creds.token
