import asyncio
import decimal
from datetime import datetime, date
import unittest
from unittest.mock import MagicMock, patch
import tests.mock_deps

from app.services.bigquery import BigQueryService, serialize_bq_cell


class TestBigQueryService(unittest.TestCase):
    def test_serialize_bq_cell(self):
        now = datetime(2026, 7, 9, 12, 0, 0)
        d = date(2026, 7, 9)
        dec = decimal.Decimal("123.45")

        self.assertEqual(serialize_bq_cell(now), "2026-07-09T12:00:00")
        self.assertEqual(serialize_bq_cell(d), "2026-07-09")
        self.assertEqual(serialize_bq_cell(dec), 123.45)
        self.assertEqual(serialize_bq_cell(b"hello"), "68656c6c6f")
        self.assertEqual(serialize_bq_cell({"a": dec}), {"a": 123.45})
        self.assertEqual(serialize_bq_cell([now, d]), ["2026-07-09T12:00:00", "2026-07-09"])

    def test_execute_sql_success(self):
        mock_bq_client = MagicMock()
        mock_bq_client.project = "my-proj"
        mock_job = MagicMock()
        row1 = {"col_time": datetime(2026, 7, 9, 10, 0, 0), "col_dec": decimal.Decimal("99.9")}
        mock_job.result.return_value = [row1]
        mock_bq_client.query.return_value = mock_job

        service = BigQueryService(bq_client=mock_bq_client)

        with patch("app.services.bigquery.settings") as mock_settings:
            mock_settings.get_project_id.return_value = "my-proj"
            rows = asyncio.run(service.execute_sql(query="SELECT * FROM table", project_id="my-proj"))

            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["col_time"], "2026-07-09T10:00:00")
            self.assertEqual(rows[0]["col_dec"], 99.9)

    def test_insert_report_data_success(self):
        mock_bq_client = MagicMock()
        mock_bq_client.project = "my-proj"
        mock_dataset = MagicMock()
        mock_table = MagicMock()
        mock_dataset.table.return_value = mock_table
        mock_bq_client.dataset.return_value = mock_dataset
        mock_bq_client.insert_rows_json.return_value = []  # No errors

        service = BigQueryService(bq_client=mock_bq_client)

        with patch("app.services.bigquery.settings") as mock_settings:
            mock_settings.get_project_id.return_value = "my-proj"
            mock_settings.bigquery_dataset = "appoptimize_demo"

            res = asyncio.run(
                service.insert_report_data(
                    report_id="rep1",
                    data_str='{"cost": 50}',
                    project_id="my-proj",
                )
            )

            self.assertTrue(res["success"])
            self.assertIn("my-proj.appoptimize_demo.cost_reports", res["target_table"])
            mock_bq_client.insert_rows_json.assert_called_once()


if __name__ == "__main__":
    unittest.main()
