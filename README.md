# AppOptimize MCP Server

A Model Context Protocol (MCP) server for interacting with the AppOptimize API and Google BigQuery. This server allows Large Language Models (LLMs) to generate, manage, and read cost and utilization reports, as well as execute arbitrary SQL queries on BigQuery.

This project was developed for a demo at Google Cloud Next '26.

## About AppOptimize API

The AppOptimize API offers a programmatic way to fetch detailed cost and utilization data for Google Cloud resources. Key characteristics include:
- **Tabular Data**: Returns data in a structured, rows-and-columns format, easy to parse.
- **Asynchronous Processing**: Report generation takes time; the API uses a 3-stage workflow: Request, Wait/Poll, and Retrieve.
- **Auto-Deletion**: Generated reports are automatically deleted after 24 hours.
- **Gross Costs**: Costs shown are generally gross costs before credits (like sustained use discounts).

This MCP server simplifies interaction with this API by handling the polling and data extraction, making it easy for LLMs to use.

## Features

-   **Create Reports**: Create new cost and utilization reports with specific dimensions and metrics.
-   **Read Reports**: Fetch tabular data from completed reports.
-   **List Reports**: List all existing reports in a project.
-   **Export to GCS**: Export report data to a Google Cloud Storage bucket.
-   **Execute SQL**: Run SQL queries on BigQuery to analyze historical data.
-   **Combined Workflow**: Create a report, wait for completion, and export to BigQuery in one operation (`create_and_export_report`).

## Prerequisites

-   Python 3.11+
-   Google Cloud Project with AppOptimize API and BigQuery enabled.
-   Application Default Credentials (ADC) configured or service account key.

## Installation

```bash
pip install -r requirements.txt
```

## Running the Server

You can run the server directly using Python:

```bash
python main.py
```

The server starts on port `8080` by default and exposes SSE endpoints for MCP communication.

### Docker Support

You can also build and run the server as a container:

```bash
docker build -t appoptimize-mcp .
docker run -p 8080:8080 -e PROJECT_ID=your-project-id appoptimize-mcp
```

## Configuration

The server uses the following environment variables:

-   `PROJECT_ID`: Your Google Cloud Project ID. Used for both AppOptimize API and BigQuery.
-   `REPORTS_BUCKET`: The GCS bucket to export reports to.

### BigQuery Requirements (Optional)
BigQuery integration is optional. Only the `create_and_export_report` and `execute_sql` tools require it. If you wish to use these features, the server expects a BigQuery dataset named `appoptimize_demo` to exist in the specified project, with tables named `cost_reports` and `utilization_reports` containing a `data` column (STRING or JSON) to store the report payload.

## MCP Tools Exposed

-   `create_report`: Creates a new report.
-   `get_report`: Gets metadata for a report.
-   `read_report`: Reads the tabular data of a completed report.
-   `list_reports`: Lists reports in a specific project and location.
-   `delete_report`: Deletes a report.
-   `export_report_to_gcs`: Reads a report and exports it to GCS.
-   `create_and_export_report`: Creates a report, waits for it to be ready, and inserts data into BigQuery.
-   `execute_sql`: Executes a SQL query on BigQuery (supports custom handling for datetime and decimal types).

## License

[Specify License here, e.g., Apache 2.0 or MIT]
