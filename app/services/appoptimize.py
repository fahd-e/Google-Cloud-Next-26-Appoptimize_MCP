import asyncio
import logging
from typing import Any, Dict, List, Optional
import httpx
from app.auth import get_access_token
from app.config import settings

logger = logging.getLogger("appoptimize.service")


class AppOptimizeService:
    """Service for interacting with the Google AppOptimize REST API."""

    def __init__(self, client: Optional[httpx.AsyncClient] = None):
        self._client = client

    def _get_client(self) -> httpx.AsyncClient:
        if self._client and not self._client.is_closed:
            return self._client
        return httpx.AsyncClient(timeout=600.0)

    async def _get_headers(self) -> Dict[str, str]:
        token = await get_access_token()
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    async def create_report(
        self,
        report_id: str,
        dimensions: List[str],
        metrics: List[str],
        project_id: Optional[str] = None,
        location: str = "global",
        scopes: Optional[List[Dict[str, str]]] = None,
        filter_expr: Optional[str] = None,
    ) -> Dict[str, Any]:
        project = settings.get_project_id(project_id)
        url = f"{settings.base_url}/projects/{project}/locations/{location}/reports?reportId={report_id}"

        payload: Dict[str, Any] = {
            "dimensions": dimensions,
            "metrics": metrics,
        }
        if scopes:
            payload["scopes"] = scopes
        if filter_expr:
            payload["filter"] = filter_expr

        client = self._get_client()
        headers = await self._get_headers()
        try:
            resp = await client.post(url, json=payload, headers=headers)
            return {
                "status_code": resp.status_code,
                "text": resp.text,
                "success": resp.status_code in (200, 201),
            }
        finally:
            if not self._client:
                await client.aclose()

    async def get_report(
        self,
        report_id: str,
        project_id: Optional[str] = None,
        location: str = "global",
    ) -> Dict[str, Any]:
        project = settings.get_project_id(project_id)
        url = f"{settings.base_url}/projects/{project}/locations/{location}/reports/{report_id}"

        client = self._get_client()
        headers = await self._get_headers()
        try:
            resp = await client.get(url, headers=headers)
            return {
                "status_code": resp.status_code,
                "text": resp.text,
                "success": resp.is_success,
            }
        finally:
            if not self._client:
                await client.aclose()

    async def read_report(
        self,
        report_id: str,
        project_id: Optional[str] = None,
        location: str = "global",
        page_size: Optional[int] = None,
        page_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        project = settings.get_project_id(project_id)
        url = f"{settings.base_url}/projects/{project}/locations/{location}/reports/{report_id}:read"

        payload: Dict[str, Any] = {}
        if page_size:
            payload["pageSize"] = page_size
        if page_token:
            payload["pageToken"] = page_token

        client = self._get_client()
        headers = await self._get_headers()
        try:
            resp = await client.post(url, json=payload, headers=headers)
            return {
                "status_code": resp.status_code,
                "text": resp.text,
                "success": resp.is_success,
            }
        finally:
            if not self._client:
                await client.aclose()

    async def list_reports(
        self,
        project_id: Optional[str] = None,
        location: str = "global",
    ) -> Dict[str, Any]:
        project = settings.get_project_id(project_id)
        url = f"{settings.base_url}/projects/{project}/locations/{location}/reports"

        client = self._get_client()
        headers = await self._get_headers()
        try:
            resp = await client.get(url, headers=headers)
            return {
                "status_code": resp.status_code,
                "text": resp.text,
                "success": resp.is_success,
            }
        finally:
            if not self._client:
                await client.aclose()

    async def delete_report(
        self,
        report_id: str,
        project_id: Optional[str] = None,
        location: str = "global",
    ) -> Dict[str, Any]:
        project = settings.get_project_id(project_id)
        url = f"{settings.base_url}/projects/{project}/locations/{location}/reports/{report_id}"

        client = self._get_client()
        headers = await self._get_headers()
        try:
            resp = await client.delete(url, headers=headers)
            return {
                "status_code": resp.status_code,
                "text": resp.text,
                "success": resp.is_success,
            }
        finally:
            if not self._client:
                await client.aclose()

    async def poll_until_ready(
        self,
        report_id: str,
        project_id: Optional[str] = None,
        location: str = "global",
        timeout_seconds: float = 900.0,
        initial_backoff: float = 3.0,
        max_backoff: float = 15.0,
    ) -> Dict[str, Any]:
        """Polls the report read endpoint with exponential backoff until data is ready."""
        project = settings.get_project_id(project_id)
        start_time = asyncio.get_event_loop().time()
        current_backoff = initial_backoff

        while True:
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout_seconds:
                logger.error(f"Timed out waiting for report {report_id} after {elapsed:.1f}s")
                return {
                    "ready": False,
                    "error": f"Timed out waiting for report {report_id} to be ready after {elapsed:.1f}s",
                    "data": None,
                }

            result = await self.read_report(
                report_id=report_id, project_id=project, location=location
            )

            if result["success"]:
                logger.info(f"Report {report_id} is ready.")
                return {
                    "ready": True,
                    "error": None,
                    "data": result["text"],
                }

            status_code = result["status_code"]
            text = result["text"]

            is_not_ready = status_code == 400 and "not ready" in text.lower()
            is_generating = status_code == 404

            if is_not_ready or is_generating:
                logger.info(
                    f"Report {report_id} not ready yet (Status: {status_code}). "
                    f"Retrying in {current_backoff:.1f}s..."
                )
                await asyncio.sleep(current_backoff)
                current_backoff = min(current_backoff * 1.5, max_backoff)
            else:
                logger.error(f"Failed to read report {report_id}: Status {status_code}, Response: {text}")
                return {
                    "ready": False,
                    "error": f"Error reading report: Status {status_code}, Response: {text}",
                    "data": None,
                }
