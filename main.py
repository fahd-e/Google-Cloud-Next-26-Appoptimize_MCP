import os
import asyncio
import httpx
import google.auth
import google.auth.transport.requests
from fastapi import FastAPI, Request
from mcp.server import Server
from mcp.server.sse import SseServerTransport

app = FastAPI()
mcp_server = Server("appoptimize-mcp-v2")
sse = SseServerTransport("/messages")

# Get default project
try:
    credentials, DEFAULT_PROJECT = google.auth.default(scopes=['https://www.googleapis.com/auth/cloud-platform'])
except Exception as e:
    print(f"Warning: Could not load default credentials: {e}")
    credentials = None
    DEFAULT_PROJECT = None

if not DEFAULT_PROJECT:
    DEFAULT_PROJECT = os.environ.get("PROJECT_ID")

BASE_URL = "https://appoptimize.googleapis.com/v1beta"

async def get_token():
    if not credentials:
        raise Exception("No credentials available. Please set up ADC or service account.")
    auth_req = google.auth.transport.requests.Request()
    credentials.refresh(auth_req)
    return credentials.token

@mcp_server.list_tools()
async def handle_list_tools():
    return [
        {
            "name": "create_report",
            "description": "Creates a new cost and utilization report. This is an asynchronous operation.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "report_id": {"type": "string", "description": "Unique ID for the report."},
                    "dimensions": {"type": "array", "items": {"type": "string"}, "description": "List of dimensions (e.g., project, application)."},
                    "metrics": {"type": "array", "items": {"type": "string"}, "description": "List of metrics (e.g., cost, cpu_mean_utilization)."},
                    "project_id": {"type": "string", "description": "Google Cloud Project ID. Defaults to active config project."},
                    "location": {"type": "string", "description": "Location for the report. Defaults to 'global'."},
                    "filter": {"type": "string", "description": "CEL filter expression."},
                    "scopes": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "project": {"type": "string", "description": "Format: projects/{project_id}"},
                                "application": {"type": "string", "description": "Format: projects/{project_id}/locations/{location}/applications/{app_id}"}
                            }
                        },
                        "description": "Resource containers to fetch data from."
                    }
                },
                "required": ["report_id", "dimensions", "metrics"]
            }
        },
        {
            "name": "get_report",
            "description": "Gets metadata for a report, including its state and expiration time.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "report_id": {"type": "string"},
                    "project_id": {"type": "string"},
                    "location": {"type": "string"}
                },
                "required": ["report_id"]
            }
        },
        {
            "name": "read_report",
            "description": "Reads the tabular data of a completed report.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "report_id": {"type": "string"},
                    "project_id": {"type": "string"},
                    "location": {"type": "string"},
                    "page_size": {"type": "integer", "description": "Max rows to return."},
                    "page_token": {"type": "string", "description": "Token for next page."}
                },
                "required": ["report_id"]
            }
        },
        {
            "name": "list_reports",
            "description": "Lists reports in a specific project and location.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "project_id": {"type": "string"},
                    "location": {"type": "string"}
                }
            }
        },
        {
            "name": "delete_report",
            "description": "Deletes a report.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "report_id": {"type": "string"},
                    "project_id": {"type": "string"},
                    "location": {"type": "string"}
                },
                "required": ["report_id"]
            }
        },
        {
            "name": "export_report_to_gcs",
            "description": "Reads a report and exports it to a GCS bucket.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "report_id": {"type": "string"},
                    "project_id": {"type": "string"},
                    "location": {"type": "string"},
                    "file_name": {"type": "string", "description": "Optional override for filename."}
                },
                "required": ["report_id"]
            }
        },
        {
            "name": "create_and_export_report",
            "description": "Creates a new report, waits for it to be ready, and exports it to GCS.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "report_id": {"type": "string", "description": "Unique ID for the report."},
                    "dimensions": {"type": "array", "items": {"type": "string"}, "description": "List of dimensions."},
                    "metrics": {"type": "array", "items": {"type": "string"}, "description": "List of metrics."},
                    "project_id": {"type": "string"},
                    "location": {"type": "string"},
                    "filter": {"type": "string"},
                    "scopes": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "project": {"type": "string"},
                                "application": {"type": "string"}
                            }
                        }
                    },
                    "file_name": {"type": "string", "description": "Optional override for filename."}
                },
                "required": ["report_id", "dimensions", "metrics"]
            }
        },
        {
            "name": "execute_sql",
            "description": "Executes a SQL query on BigQuery.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The SQL query to execute."},
                    "projectId": {"type": "string", "description": "Google Cloud Project ID."}
                },
                "required": ["query"]
            }
        }
    ]

@mcp_server.call_tool()
async def handle_call_tool(name: str, arguments: dict):
    token = await get_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    project = arguments.get("project_id") or DEFAULT_PROJECT
    if not project:
        return [{"type": "text", "text": "Error: Project ID not specified and could not be detected."}]
    
    location = arguments.get("location", "global")
    
    async with httpx.AsyncClient(timeout=600.0) as client:
        if name == "create_report":
            report_id = arguments["report_id"]
            payload = {
                "dimensions": arguments["dimensions"],
                "metrics": arguments["metrics"]
            }
            if "scopes" in arguments:
                payload["scopes"] = arguments["scopes"]
            if "filter" in arguments:
                payload["filter"] = arguments["filter"]
                
            url = f"{BASE_URL}/projects/{project}/locations/{location}/reports?reportId={report_id}"
            resp = await client.post(url, json=payload, headers=headers)
            return [{"type": "text", "text": f"Status: {resp.status_code}\nResponse: {resp.text}"}]
            
        elif name == "get_report":
            report_id = arguments["report_id"]
            url = f"{BASE_URL}/projects/{project}/locations/{location}/reports/{report_id}"
            resp = await client.get(url, headers=headers)
            return [{"type": "text", "text": f"Status: {resp.status_code}\nResponse: {resp.text}"}]
            
        elif name == "read_report":
            report_id = arguments["report_id"]
            payload = {}
            if "page_size" in arguments:
                payload["pageSize"] = arguments["page_size"]
            if "page_token" in arguments:
                payload["pageToken"] = arguments["page_token"]
                
            url = f"{BASE_URL}/projects/{project}/locations/{location}/reports/{report_id}:read"
            resp = await client.post(url, json=payload, headers=headers)
            return [{"type": "text", "text": f"Status: {resp.status_code}\nResponse: {resp.text}"}]
            
        elif name == "list_reports":
            url = f"{BASE_URL}/projects/{project}/locations/{location}/reports"
            resp = await client.get(url, headers=headers)
            return [{"type": "text", "text": f"Status: {resp.status_code}\nResponse: {resp.text}"}]
            
        elif name == "delete_report":
            report_id = arguments["report_id"]
            url = f"{BASE_URL}/projects/{project}/locations/{location}/reports/{report_id}"
            resp = await client.delete(url, headers=headers)
            return [{"type": "text", "text": f"Status: {resp.status_code}\nResponse: {resp.text}"}]
            
        elif name == "export_report_to_gcs":
            from google.cloud import storage
            from datetime import datetime
            
            report_id = arguments["report_id"]
            # 1. Read report data (with polling if not ready)
            url = f"{BASE_URL}/projects/{project}/locations/{location}/reports/{report_id}:read"
            data = ""
            for _ in range(30): # Poll for up to 5 minutes
                resp = await client.post(url, json={}, headers=headers)
                if resp.status_code == 200:
                    data = resp.text
                    break
                elif resp.status_code == 400 and "Report is not ready" in resp.text:
                    logging.info(f"Report {report_id} is not ready yet, waiting 10 seconds...")
                    await asyncio.sleep(10)
                elif resp.status_code == 404:
                    logging.info(f"Report {report_id} not found yet (might be generating), waiting 10 seconds...")
                    await asyncio.sleep(10)
                else:
                    return [{"type": "text", "text": f"Error reading report: Status {resp.status_code}, Response: {resp.text}"}]
            
            if not data:
                return [{"type": "text", "text": f"Timed out waiting for report {report_id} to be ready."}]
            
            # 2. Determine type
            prefix = "appoptimizev2-costs"
            if "utilization" in data:
                prefix = "appoptimizev2-utilization"
                
            # 3. Naming convention
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            file_name = arguments.get("file_name", f"{prefix}-{timestamp}.json")
            
            # 4. Get bucket from env or fallback
            bucket_name = os.environ.get("REPORTS_BUCKET", "YOUR_BUCKET_NAME")
            
            try:
                storage_client = storage.Client()
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(file_name)
                
                await asyncio.to_thread(blob.upload_from_string, data, content_type='application/json')
                return [{"type": "text", "text": f"Report {report_id} exported to gs://{bucket_name}/{file_name}"}]
            except Exception as e:
                return [{"type": "text", "text": f"Error uploading to GCS: {str(e)}"}]
            
        elif name == "create_and_export_report":
            # 1. Create report
            report_id = arguments["report_id"]
            payload = {
                "dimensions": arguments["dimensions"],
                "metrics": arguments["metrics"]
            }
            if "scopes" in arguments:
                payload["scopes"] = arguments["scopes"]
            if "filter" in arguments:
                payload["filter"] = arguments["filter"]
                
            url = f"{BASE_URL}/projects/{project}/locations/{location}/reports?reportId={report_id}"
            resp = await client.post(url, json=payload, headers=headers)
            
            if resp.status_code not in [200, 201]:
                return [{"type": "text", "text": f"Error creating report: Status {resp.status_code}, Response: {resp.text}"}]
            
            # 2. Wait for report data (with polling if not ready)
            url = f"{BASE_URL}/projects/{project}/locations/{location}/reports/{report_id}:read"
            data = ""
            for _ in range(90): # Poll for up to 15 minutes
                resp = await client.post(url, json={}, headers=headers)
                if resp.status_code == 200:
                    data = resp.text
                    break
                elif resp.status_code == 400 and "Report is not ready" in resp.text:
                    await asyncio.sleep(10)
                elif resp.status_code == 404:
                    await asyncio.sleep(10)
                else:
                    return [{"type": "text", "text": f"Error reading report: Status {resp.status_code}, Response: {resp.text}"}]
            
            if not data:
                return [{"type": "text", "text": f"Timed out waiting for report {report_id} to be ready."}]
                
            # 3. Determine type
            prefix = "appoptimizev2-costs"
            if "utilization" in data:
                prefix = "appoptimizev2-utilization"
                
            # 4. Naming convention
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            file_name = arguments.get("file_name", f"{prefix}-{timestamp}.json")
            
            # 5. Get bucket from env or fallback
            bucket_name = os.environ.get("REPORTS_BUCKET", "YOUR_BUCKET_NAME")
            
            # 5. Insert into BigQuery
            bq_status = "Skipped"
            try:
                from google.cloud import bigquery
                import json
                
                bq_client = bigquery.Client(project=project)
                dataset_id = "appoptimize_demo"
                table_id = "utilization_reports" if "utilization" in data else "cost_reports"
                table_ref = bq_client.dataset(dataset_id).table(table_id)
                
                parsed_data = json.loads(data)
                
                if isinstance(parsed_data, dict) and "error" in parsed_data:
                    print(f"DEBUG: Skipping BQ insertion because data contains error: {parsed_data['error']}")
                    bq_status = "Skipped (Error in data)"
                else:
                    rows_to_insert = [
                        {
                            "report_id": report_id,
                            "timestamp": datetime.utcnow().isoformat(),
                            "data": json.dumps(parsed_data)
                        }
                    ]
                    
                    errors = bq_client.insert_rows_json(table_ref, rows_to_insert)
                    if errors:
                        bq_status = f"Failed (Errors: {errors})"
                    else:
                        bq_status = "Success"
            except Exception as e:
                bq_status = f"Failed (Exception: {str(e)})"

            return [{"type": "text", "text": f"Report {report_id} created and pushed to BigQuery. BQ Insert: {bq_status}"}]

        elif name == "execute_sql":
            from google.cloud import bigquery
            from datetime import datetime
            import json
            import decimal
            
            query = arguments["query"]
            print(f"DEBUG MCP: Executing SQL query: {query}")
            project_id = arguments.get("projectId") or project
            
            try:
                bq_client = bigquery.Client(project=project_id)
                query_job = bq_client.query(query)
                results = query_job.result()
                
                rows = []
                for row in results:
                    # Convert row to dict, handling datetime and decimal objects
                    row_dict = {}
                    for key, value in row.items():
                        if isinstance(value, datetime):
                            row_dict[key] = value.isoformat()
                        elif isinstance(value, decimal.Decimal):
                            row_dict[key] = float(value)
                        else:
                            row_dict[key] = value
                    rows.append(row_dict)
                    
                return [{"type": "text", "text": json.dumps(rows)}]
            except Exception as e:
                return [{"type": "text", "text": f"Error executing SQL: {str(e)}"}]
            
        else:
            return [{"type": "text", "text": f"Unknown tool: {name}"}]

# SSE Endpoints
@app.get("/sse")
async def sse_endpoint():
    async with sse.connect_sse() as (read_stream, write_stream):
        await mcp_server.run(read_stream, write_stream, mcp_server.create_initialization_options())

@app.post("/messages")
async def messages_endpoint(request: Request):
    await sse.handle_post_message(request)

@app.post("/call/{name}")
async def call_tool_direct(name: str, arguments: dict = None):
    try:
        result = await handle_call_tool(name, arguments or {})
        return result
    except Exception as e:
        return {"error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
