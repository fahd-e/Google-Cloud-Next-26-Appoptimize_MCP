import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
import tests.mock_deps

from app.mcp_server import get_tool_definitions, MCPToolHandler


class TestMCPServer(unittest.TestCase):
    def test_tool_definitions_schema(self):
        tools = get_tool_definitions()
        tool_names = [t["name"] for t in tools]
        expected_tools = [
            "create_report",
            "get_report",
            "read_report",
            "list_reports",
            "delete_report",
            "export_report_to_gcs",
            "create_and_export_report",
            "execute_sql",
        ]
        for expected in expected_tools:
            self.assertIn(expected, tool_names)

        for t in tools:
            self.assertIn("name", t)
            self.assertIn("description", t)
            self.assertIn("inputSchema", t)

    def test_execute_create_report(self):
        handler = MCPToolHandler()
        handler.appoptimize_service.create_report = AsyncMock(
            return_value={"status_code": 200, "text": "OK", "success": True}
        )

        res = asyncio.run(
            handler.execute(
                "create_report",
                {
                    "report_id": "r1",
                    "dimensions": ["project"],
                    "metrics": ["cost"],
                },
            )
        )
        self.assertEqual(len(res), 1)
        self.assertIn("Status: 200", res[0]["text"])

    def test_execute_create_and_export_report(self):
        handler = MCPToolHandler()
        handler.appoptimize_service.create_report = AsyncMock(
            return_value={"status_code": 200, "text": "OK", "success": True}
        )
        handler.appoptimize_service.poll_until_ready = AsyncMock(
            return_value={"ready": True, "data": '{"cost": 10}', "error": None}
        )
        handler.gcs_service.export_report = AsyncMock(
            return_value={"success": True, "gcs_uri": "gs://bkt/r1.json", "error": None}
        )
        handler.bigquery_service.insert_report_data = AsyncMock(
            return_value={"success": True, "target_table": "p.d.t", "error": None}
        )

        res = asyncio.run(
            handler.execute(
                "create_and_export_report",
                {
                    "report_id": "r1",
                    "dimensions": ["project"],
                    "metrics": ["cost"],
                },
            )
        )
        self.assertEqual(len(res), 1)
        text = res[0]["text"]
        self.assertIn("gs://bkt/r1.json", text)
        self.assertIn("Success (p.d.t)", text)


if __name__ == "__main__":
    unittest.main()
