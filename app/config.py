import os
import logging
from typing import Optional, Tuple
import google.auth

logger = logging.getLogger("appoptimize.config")

class Settings:
    def __init__(self):
        self.host: str = os.environ.get("HOST", "0.0.0.0")
        self.port: int = int(os.environ.get("PORT", "8080"))
        self.log_level: str = os.environ.get("LOG_LEVEL", "INFO").upper()
        self.base_url: str = os.environ.get(
            "APPOPTIMIZE_BASE_URL", "https://appoptimize.googleapis.com/v1beta"
        )
        self.reports_bucket: str = os.environ.get("REPORTS_BUCKET", "")
        self.bigquery_dataset: str = os.environ.get(
            "BIGQUERY_DATASET", "appoptimize_demo"
        )

        self._credentials = None
        self._default_project = None
        self._auth_initialized = False

    def _init_auth(self):
        if self._auth_initialized:
            return
        self._auth_initialized = True
        try:
            creds, project = google.auth.default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )
            self._credentials = creds
            self._default_project = project
            logger.info(f"Loaded GCP credentials. Default project: {project}")
        except Exception as e:
            logger.warning(f"Could not load Google default credentials: {e}")

    @property
    def credentials(self):
        self._init_auth()
        return self._credentials

    @property
    def project_id(self) -> Optional[str]:
        project = os.environ.get("PROJECT_ID")
        if project:
            return project
        self._init_auth()
        return self._default_project

    def get_project_id(self, override: Optional[str] = None) -> str:
        res = override or self.project_id
        if not res:
            raise ValueError(
                "Google Cloud Project ID not specified and could not be detected from environment or ADC."
            )
        return res

settings = Settings()
