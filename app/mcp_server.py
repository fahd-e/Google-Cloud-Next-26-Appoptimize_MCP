import logging
import json
from typing import Any, Dict, List, Optional
import httpx
from mcp.server import Server

from app.services.appoptimize import AppOptimizeService
from app.services.gcs import GCSService
from app.services.bigquery import BigQueryService

logger = logging.getLogger("appoptimize.mcp")

mcp_server = Server("appoptimize-mcp")


def get_tool_definitions() -> List[Dict[str, Any]]:
    """Returns the list of MCP tool definitions."""
    return [
        {
            "name": "create_report",
            "description": "Creates a new cost and utilization report in AppOptimize API (asynchronous operation).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "report_id": {"type": "string", "description": "Unique ID for the report."},
                    "dimensions": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of dimensions (e.g., project, application).",
                    },
                    "metrics": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of metrics (e.g., cost, cpu_mean_utilization).",
                    },
                    "project_id": {
                        "type": "string",
                        "description": "Google Cloud Project ID. Defaults to environment/ADC config.",
                    },
                    "location": {
                        "type": "string",
                        "description": "Location for the report. Defaults to 'global'.",
                    },
                    "filter": {"type": "string", "description": "CEL filter expression."},
                    "scopes": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "project": {"type": "string", "description": "Format: projects/{project_id}"},
                                "application": {
                                    "type": "string",
                                    "description": "Format: projects/{project_id}/locations/{location}/applications/{app_id}",
                                },
                            },
                        },
                        "description": "Resource containers to fetch data from.",
                    },
                },
                "required": ["report_id", "dimensions", "metrics"],
            },
        },
        {
            "name": "get_report",
            "description": "Gets metadata for an existing report, including its state and expiration time.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "report_id": {"type": "string", "description": "Unique ID for the report."},
                    "project_id": {"type": "string"},
                    "location": {"type": "string"},
                },
                "required": ["report_id"],
            },
        },
        {
            "name": "read_report",
            "description": "Reads the tabular data of a completed report.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "report_id": {"type": "string", "description": "Unique ID for the report."},
                    "project_id": {"type": "string"},
                    "location": {"type": "string"},
                    "page_size": {"type": "integer", "description": "Max rows to return."},
                    "page_token": {"type": "string", "description": "Token for next page."},
                },
                "required": ["report_id"],
            },
        },
        {
            "name": "list_reports",
            "description": "Lists reports in a specific project and location.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "project_id": {"type": "string"},
                    "location": {"type": "string"},
                },
            },
        },
        {
            "name": "delete_report",
            "description": "Deletes a report by ID.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "report_id": {"type": "string"},
                    "project_id": {"type": "string"},
                    "location": {"type": "string"},
                },
                "required": ["report_id"],
            },
        },
        {
            "name": "export_report_to_gcs",
            "description": "Reads a report (polling if generating) and exports it to a GCS bucket.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "report_id": {"type": "string"},
                    "project_id": {"type": "string"},
                    "location": {"type": "string"},
                    "file_name": {"type": "string", "description": "Optional override for filename in GCS."},
                    "bucket_name": {"type": "string", "description": "Optional GCS bucket override."},
                },
                "required": ["report_id"],
            },
        },
        {
            "name": "create_and_export_report",
            "description": "Creates a report, polls until ready, exports to GCS, and streams into BigQuery.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "report_id": {"type": "string"},
                    "dimensions": {"type": "array", "items": {"type": "string"}},
                    "metrics": {"type": "array", "items": {"type": "string"}},
                    "project_id": {"type": "string"},
                    "location": {"type": "string"},
                    "filter": {"type": "string"},
                    "scopes": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "project": {"type": "string"},
                                "application": {"type": "string"},
                            },
                        },
                    },
                    "file_name": {"type": "string", "description": "Optional override for GCS filename."},
                    "export_to_gcs": {"type": "boolean", "description": "Whether to export to GCS. Default: true."},
                    "export_to_bigquery": {
                        "type": "boolean",
                        "description": "Whether to export to BigQuery. Default: true.",
                    },
                },
                "required": ["report_id", "dimensions", "metrics"],
            },
        },
        {
            "name": "execute_sql",
            "description": "Executes a SQL query on BigQuery.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The SQL query to execute."},
                    "projectId": {"type": "string", "description": "Google Cloud Project ID override."},
                },
                "required": ["query"],
            },
        },
    ]


@mcp_server.list_tools()
async def handle_list_tools():
    return get_tool_definitions()


class MCPToolHandler:
    """Dispatches tool calls to appropriate service handlers."""

    def __init__(self, http_client: Optional[httpx.AsyncClient] = None):
        self.appoptimize_service = AppOptimizeService(client=http_client)
        self.gcs_service = GCSService()
        self.bigquery_service = BigQueryService()

    async def execute(self, name: str, arguments: Dict[str, Any]) -> List[Dict[str, str]]:
        logger.info(f"Executing tool '{name}' with arguments: {arguments}")
        try:
            if name == "create_report":
                res = await self.appoptimize_service.create_report(
                    report_id=arguments["report_id"],
                    dimensions=arguments["dimensions"],
                    metrics=arguments["metrics"],
                    project_id=arguments.get("project_id"),
                    location=arguments.get("location", "global"),
                    scopes=arguments.get("scopes"),
                    filter_expr=arguments.get("filter"),
                )
                return [{"type": "text", "text": f"Status: {res['status_code']}\nResponse: {res['text']}"}]

            elif name == "get_report":
                res = await self.appoptimize_service.get_report(
                    report_id=arguments["report_id"],
                    project_id=arguments.get("project_id"),
                    location=arguments.get("location", "global"),
                )
                return [{"type": "text", "text": f"Status: {res['status_code']}\nResponse: {res['text']}"}]

            elif name == "read_report":
                res = await self.appoptimize_service.read_report(
                    report_id=arguments["report_id"],
                    project_id=arguments.get("project_id"),
                    location=arguments.get("location", "global"),
                    page_size=arguments.get("page_size"),
                    page_token=arguments.get("page_token"),
                )
                return [{"type": "text", "text": f"Status: {res['status_code']}\nResponse: {res['text']}"}]

            elif name == "list_reports":
                res = await self.appoptimize_service.list_reports(
                    project_id=arguments.get("project_id"),
                    location=arguments.get("location", "global"),
                )
                return [{"type": "text", "text": f"Status: {res['status_code']}\nResponse: {res['text']}"}]

            elif name == "delete_report":
                res = await self.appoptimize_service.delete_report(
                    report_id=arguments["report_id"],
                    project_id=arguments.get("project_id"),
                    location=arguments.get("location", "global"),
                )
                return [{"type": "text", "text": f"Status: {res['status_code']}\nResponse: {res['text']}"}]

            elif name == "export_report_to_gcs":
                report_id = arguments["report_id"]
                project_id = arguments.get("project_id")
                location = arguments.get("location", "global")

                poll_res = await self.appoptimize_service.poll_until_ready(
                    report_id=report_id, project_id=project_id, location=location
                )
                if not poll_res["ready"]:
                    return [{"type": "text", "text": poll_res["error"]}]

                export_res = await self.gcs_service.export_report(
                    report_id=report_id,
                    data=poll_res["data"],
                    file_name=arguments.get("file_name"),
                    bucket_name=arguments.get("bucket_name"),
                )

                if export_res["success"]:
                    return [
                        {
                            "type": "text",
                            "text": f"Report {report_id} exported successfully to {export_res['gcs_uri']}",
                        }
                    ]
                else:
                    return [{"type": "text", "text": f"GCS Export Failed: {export_res['error']}"}]

            elif name == "create_and_export_report":
                report_id = arguments["report_id"]
                project_id = arguments.get("project_id")
                location = arguments.get("location", "global")

                # 1. Create report
                create_res = await self.appoptimize_service.create_report(
                    report_id=report_id,
                    dimensions=arguments["dimensions"],
                    metrics=arguments["metrics"],
                    project_id=project_id,
                    location=location,
                    scopes=arguments.get("scopes"),
                    filter_expr=arguments.get("filter"),
                )
                if not create_res["success"]:
                    return [
                        {
                            "type": "text",
                            "text": f"Error creating report: Status {create_res['status_code']}, Response: {create_res['text']}",
                        }
                    ]

                # 2. Poll until ready
                poll_res = await self.appoptimize_service.poll_until_ready(
                    report_id=report_id, project_id=project_id, location=location
                )
                if not poll_res["ready"]:
                    return [{"type": "text", "text": poll_res["error"]}]

                report_data = poll_res["data"]
                results_summary = [f"Report {report_id} created and ready."]

                # 3. Export to GCS if requested (default true)
                if arguments.get("export_to_gcs", True):
                    gcs_res = await self.gcs_service.export_report(
                        report_id=report_id,
                        data=report_data,
                        file_name=arguments.get("file_name"),
                    )
                    if gcs_res["success"]:
                        results_summary.append(f"GCS Export: {gcs_res['gcs_uri']}")
                    else:
                        results_summary.append(f"GCS Export: Failed ({gcs_res['error']})")

                # 4. Export to BigQuery if requested (default true)
                if arguments.get("export_to_bigquery", True):
                    bq_res = await self.bigquery_service.insert_report_data(
                        report_id=report_id,
                        data_str=report_data,
                        project_id=project_id,
                    )
                    if bq_res["success"]:
                        results_summary.append(f"BigQuery Insert: Success ({bq_res['target_table']})")
                    else:
                        results_summary.append(f"BigQuery Insert: Failed ({bq_res['error']})")

                return [{"type": "text", "text": "\n".join(results_summary)}]

            elif name == "execute_sql":
                query = arguments["query"]
                project_id = arguments.get("projectId") or arguments.get("project_id")
                rows = await self.bigquery_service.execute_sql(query=query, project_id=project_id)
                return [{"type": "text", "text": json.dumps(rows)}]

            else:
                return [{"type": "text", "text": f"Unknown tool: {name}"}]

        except Exception as e:
            logger.exception(f"Error handling tool call '{name}'")
            return [{"type": "text", "text": f"Error executing tool '{name}': {str(e)}"}]


@mcp_server.call_tool()
async def handle_call_tool(name: str, arguments: dict):
    handler = MCPToolHandler()
    return await handler.execute(name, arguments)
