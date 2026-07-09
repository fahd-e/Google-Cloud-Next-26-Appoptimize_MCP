import asyncio
import decimal
import json
import logging
from datetime import datetime, date, time, timezone
from typing import Any, Dict, List, Optional
from google.cloud import bigquery
from app.config import settings

logger = logging.getLogger("appoptimize.bigquery")


def serialize_bq_cell(value: Any) -> Any:
    """Recursively serializes BigQuery cell values to JSON-compatible Python types."""
    if value is None:
        return None
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, dict):
        return {k: serialize_bq_cell(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [serialize_bq_cell(v) for v in value]
    return value


class BigQueryService:
    """Service for BigQuery interactions: executing SQL and inserting report data."""

    def __init__(self, bq_client: Optional[bigquery.Client] = None):
        self._client = bq_client

    def _get_client(self, project_id: Optional[str] = None) -> bigquery.Client:
        target_project = settings.get_project_id(project_id)
        if not self._client or self._client.project != target_project:
            self._client = bigquery.Client(project=target_project)
        return self._client

    async def execute_sql(
        self, query: str, project_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Executes a BigQuery SQL query asynchronously and returns serialized rows."""
        target_project = settings.get_project_id(project_id)
        logger.info(f"Executing BigQuery SQL query in project '{target_project}'")

        def _run():
            client = self._get_client(target_project)
            query_job = client.query(query)
            results = query_job.result()

            rows = []
            for row in results:
                row_dict = {}
                for key, value in row.items():
                    row_dict[key] = serialize_bq_cell(value)
                rows.append(row_dict)
            return rows

        return await asyncio.to_thread(_run)

    async def insert_report_data(
        self,
        report_id: str,
        data_str: str,
        dataset_id: Optional[str] = None,
        project_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Inserts report JSON payload into the appropriate BigQuery table."""
        target_project = settings.get_project_id(project_id)
        target_dataset = dataset_id or settings.bigquery_dataset

        try:
            parsed_data = json.loads(data_str) if isinstance(data_str, str) else data_str
        except Exception as e:
            logger.error(f"Failed to parse report data as JSON for BQ insertion: {e}")
            return {
                "success": False,
                "error": f"Invalid JSON report data: {str(e)}",
            }

        if isinstance(parsed_data, dict) and "error" in parsed_data:
            logger.warning(f"Skipping BQ insertion because report contains error: {parsed_data['error']}")
            return {
                "success": False,
                "error": f"Report data contains error: {parsed_data['error']}",
            }

        table_id = "utilization_reports" if "utilization" in str(data_str).lower() else "cost_reports"

        def _insert():
            client = self._get_client(target_project)
            table_ref = client.dataset(target_dataset).table(table_id)

            rows_to_insert = [
                {
                    "report_id": report_id,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "data": json.dumps(parsed_data) if not isinstance(data_str, str) else data_str,
                }
            ]

            errors = client.insert_rows_json(table_ref, rows_to_insert)
            if errors:
                raise RuntimeError(f"BigQuery streaming insert errors: {errors}")
            return f"{target_project}.{target_dataset}.{table_id}"

        try:
            target_table = await asyncio.to_thread(_insert)
            logger.info(f"Inserted report {report_id} into BigQuery table {target_table}")
            return {
                "success": True,
                "target_table": target_table,
                "error": None,
            }
        except Exception as e:
            logger.error(f"Failed to insert report {report_id} into BigQuery: {e}")
            return {
                "success": False,
                "target_table": None,
                "error": f"BigQuery Insertion Error: {str(e)}",
            }
