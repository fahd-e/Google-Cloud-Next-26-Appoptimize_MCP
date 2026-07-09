# AppOptimize MCP Server

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A production-ready **Model Context Protocol (MCP)** server for interacting with the Google Cloud **AppOptimize API**, Google Cloud Storage (GCS), and **Google BigQuery**.

This server allows Large Language Models (LLMs) and MCP-compliant clients to generate, manage, read, and export cost & utilization reports, as well as execute arbitrary SQL queries on BigQuery.

Developed for Google Cloud Next '26.

---

## 🏗️ Architecture & Project Structure

The project has been refactored into a modular, testable, and high-performance Python package (`app/`):

```
.
├── app/
│   ├── __init__.py         # Package metadata
│   ├── config.py           # Centralized configuration & environment loader
│   ├── auth.py             # Smart OAuth token caching and auto-refresh
│   ├── main.py             # FastAPI server with lifespan HTTP connection pool
│   ├── mcp_server.py       # MCP tool definitions and request dispatcher
│   └── services/
│       ├── __init__.py
│       ├── appoptimize.py  # AppOptimize REST API client with exponential backoff
│       ├── gcs.py          # GCS report export service
│       └── bigquery.py     # BigQuery SQL executor & streaming table inserter
├── tests/                  # Unit test suite (100% offline-runnable with mocks)
│   ├── test_config.py
│   ├── test_auth.py
│   ├── test_appoptimize.py
│   ├── test_gcs.py
│   ├── test_bigquery.py
│   └── test_mcp_server.py
├── Dockerfile              # Optimized non-root container image with health check
├── pyproject.toml          # Modern Python packaging configuration
├── requirements.txt        # Runtime dependencies
├── README.md               # Documentation
└── main.py                 # Application entrypoint (backwards-compatible)
```

---

## ✨ Features & Capabilities

- **Modular Services**: Separate, isolated services for AppOptimize API, GCS, and BigQuery.
- **Connection Pooling**: Reuses an `httpx.AsyncClient` across requests for low latency and high throughput.
- **Smart OAuth Token Caching**: Caches GCP OAuth access tokens and refreshes them only when expired.
- **Exponential Backoff Polling**: Intelligent polling with backoff for asynchronous report generation.
- **Robust BigQuery Serialization**: Handles Datetime, Date, Decimal, Bytes, and JSON types smoothly.
- **Dual Transport Support**: Exposes both standard MCP SSE transport (`/sse`, `/messages`) and direct REST endpoint (`/call/{tool_name}`).
- **Built-in Health Checks**: `/health` and `/livez` endpoints for Cloud Run & Kubernetes probers.

---

## 🛠️ MCP Tools Exposed

| Tool Name | Description | Key Arguments |
| :--- | :--- | :--- |
| `create_report` | Creates a new cost or utilization report (asynchronous operation). | `report_id`, `dimensions`, `metrics`, `scopes`, `filter` |
| `get_report` | Fetches metadata for an existing report (state, creation/expiration time). | `report_id`, `project_id`, `location` |
| `read_report` | Reads tabular row data from a completed report (with pagination support). | `report_id`, `page_size`, `page_token` |
| `list_reports` | Lists all reports in a specified project and location. | `project_id`, `location` |
| `delete_report` | Deletes a report by ID. | `report_id`, `project_id`, `location` |
| `export_report_to_gcs` | Polls until a report is ready and exports its payload to a GCS bucket. | `report_id`, `file_name`, `bucket_name` |
| `create_and_export_report` | Creates a report, waits for completion, and exports to GCS & BigQuery. | `report_id`, `dimensions`, `metrics`, `export_to_gcs`, `export_to_bigquery` |
| `execute_sql` | Executes arbitrary SQL queries on BigQuery with custom type serialization. | `query`, `projectId` |

---

## ⚙️ Configuration

The server reads configuration from environment variables (or falls back to Application Default Credentials):

| Variable | Description | Default |
| :--- | :--- | :--- |
| `PROJECT_ID` | Google Cloud Project ID. | Detected from ADC / gcloud |
| `REPORTS_BUCKET` | Target GCS bucket for report exports. | None |
| `BIGQUERY_DATASET` | Target BigQuery dataset for report streaming insert. | `appoptimize_demo` |
| `PORT` | HTTP server port. | `8080` |
| `HOST` | HTTP server binding host. | `0.0.0.0` |
| `LOG_LEVEL` | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`). | `INFO` |

---

## 🚀 Running the Server

### 1. Local Python

```bash
# Install dependencies
pip install -r requirements.txt

# Run the server
python main.py
```

The server starts at `http://0.0.0.0:8080`.

### 2. Docker

Build and run the container locally:

```bash
docker build -t appoptimize-mcp .
docker run -p 8080:8080 \
  -e PROJECT_ID="your-gcp-project" \
  -e REPORTS_BUCKET="your-gcs-bucket" \
  appoptimize-mcp
```

### 3. Deploying to Google Cloud Run

```bash
gcloud run deploy appoptimize-mcp \
  --source . \
  --region us-central1 \
  --set-env-vars PROJECT_ID="your-gcp-project",REPORTS_BUCKET="your-gcs-bucket" \
  --allow-unauthenticated
```

---

## 🧪 Running Unit Tests

Run the full, offline-compatible unit test suite:

```bash
python3 -m unittest discover -s tests
```

---

## 📄 License

Apache License 2.0. See [LICENSE](LICENSE) for details.
