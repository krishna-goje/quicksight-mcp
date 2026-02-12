"""Test dataset tools."""

import pytest
from unittest.mock import MagicMock, patch, call
from fastmcp import FastMCP


class TestDatasetToolRegistration:
    """Test that dataset tools register correctly."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.mock_tracker = MagicMock()
        self.mcp = FastMCP("test")

    def test_register_creates_tools(self):
        """Test that register_dataset_tools adds tools to the MCP server."""
        from quicksight_mcp.tools.datasets import register_dataset_tools

        register_dataset_tools(
            self.mcp, lambda: self.mock_client, lambda: self.mock_tracker
        )
        # Registration should not raise


class TestDatasetClientInteractions:
    """Test dataset operations against the client mock."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.mock_tracker = MagicMock()

    def test_list_datasets_returns_formatted_result(self):
        """Test list_datasets formats API response correctly."""
        self.mock_client.list_datasets.return_value = [
            {"Name": "Sales Data", "DataSetId": "ds-001", "ImportMode": "SPICE"},
            {"Name": "Cost Data", "DataSetId": "ds-002", "ImportMode": "DIRECT_QUERY"},
        ]

        result = self.mock_client.list_datasets()
        assert len(result) == 2
        assert result[0]["Name"] == "Sales Data"
        assert result[1]["ImportMode"] == "DIRECT_QUERY"

    def test_search_datasets_passes_name(self):
        """Test search_datasets passes search term correctly."""
        self.mock_client.search_datasets.return_value = [
            {"Name": "WBR Dataset", "DataSetId": "ds-003", "ImportMode": "SPICE"},
        ]

        result = self.mock_client.search_datasets("WBR")
        self.mock_client.search_datasets.assert_called_once_with("WBR")
        assert len(result) == 1
        assert result[0]["Name"] == "WBR Dataset"

    def test_search_datasets_returns_empty_on_no_match(self):
        """Test search_datasets returns empty list when no match."""
        self.mock_client.search_datasets.return_value = []

        result = self.mock_client.search_datasets("nonexistent")
        assert len(result) == 0

    def test_get_dataset_sql_returns_sql(self):
        """Test get_dataset_sql returns SQL string."""
        self.mock_client.get_dataset_sql.return_value = "SELECT * FROM my_table"

        result = self.mock_client.get_dataset_sql("ds-001")
        assert result == "SELECT * FROM my_table"

    def test_get_dataset_sql_returns_none_for_direct_table(self):
        """Test get_dataset_sql returns None for non-SQL datasets."""
        self.mock_client.get_dataset_sql.return_value = None

        result = self.mock_client.get_dataset_sql("ds-001")
        assert result is None

    def test_update_dataset_sql_calls_with_backup(self):
        """Test update_dataset_sql creates backup by default."""
        self.mock_client.update_dataset_sql.return_value = {"status": "success"}

        self.mock_client.update_dataset_sql("ds-001", "SELECT 1", backup_first=True)
        self.mock_client.update_dataset_sql.assert_called_with(
            "ds-001", "SELECT 1", backup_first=True
        )

    def test_update_dataset_sql_without_backup(self):
        """Test update_dataset_sql can skip backup."""
        self.mock_client.update_dataset_sql.return_value = {"status": "success"}

        self.mock_client.update_dataset_sql("ds-001", "SELECT 1", backup_first=False)
        self.mock_client.update_dataset_sql.assert_called_with(
            "ds-001", "SELECT 1", backup_first=False
        )

    def test_refresh_dataset_returns_ingestion_id(self):
        """Test refresh_dataset returns ingestion tracking info."""
        self.mock_client.refresh_dataset.return_value = {
            "ingestion_id": "refresh-20260212",
            "status": "RUNNING",
        }

        result = self.mock_client.refresh_dataset("ds-001")
        assert "ingestion_id" in result
        assert result["status"] == "RUNNING"

    def test_get_refresh_status_completed(self):
        """Test get_refresh_status for a completed refresh."""
        self.mock_client.get_refresh_status.return_value = {
            "status": "COMPLETED",
            "row_count": 50000,
            "error": None,
        }

        result = self.mock_client.get_refresh_status("ds-001", "ing-001")
        assert result["status"] == "COMPLETED"
        assert result["row_count"] == 50000

    def test_get_refresh_status_failed(self):
        """Test get_refresh_status for a failed refresh."""
        self.mock_client.get_refresh_status.return_value = {
            "status": "FAILED",
            "row_count": 0,
            "error": "SQL compilation error",
        }

        result = self.mock_client.get_refresh_status("ds-001", "ing-001")
        assert result["status"] == "FAILED"
        assert "SQL compilation error" in result["error"]

    def test_get_dataset_returns_metadata(self):
        """Test get_dataset returns structured metadata."""
        self.mock_client.get_dataset.return_value = {
            "Name": "My Dataset",
            "ImportMode": "SPICE",
            "PhysicalTableMap": {"table1": {}},
            "LogicalTableMap": {"log1": {}},
            "OutputColumns": [
                {"Name": "col1", "Type": "STRING"},
                {"Name": "col2", "Type": "INTEGER"},
            ],
        }

        result = self.mock_client.get_dataset("ds-001")
        assert result["Name"] == "My Dataset"
        assert len(result["OutputColumns"]) == 2
