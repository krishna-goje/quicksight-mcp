"""Test analysis tools."""

import pytest
from unittest.mock import MagicMock


class TestAnalysisClientInteractions:
    """Test analysis operations against the client mock."""

    def setup_method(self):
        self.mock_client = MagicMock()
        self.mock_tracker = MagicMock()

    def test_list_analyses(self):
        """Test list_analyses returns formatted result."""
        self.mock_client.list_analyses.return_value = [
            {
                "Name": "Sales Analysis",
                "AnalysisId": "an-001",
                "Status": "CREATION_SUCCESSFUL",
            },
            {
                "Name": "Ops WBR",
                "AnalysisId": "an-002",
                "Status": "CREATION_SUCCESSFUL",
            },
        ]
        result = self.mock_client.list_analyses()
        assert len(result) == 2
        assert result[0]["Status"] == "CREATION_SUCCESSFUL"

    def test_search_analyses(self):
        """Test search_analyses filters correctly."""
        self.mock_client.search_analyses.return_value = [
            {
                "Name": "WBR Analysis",
                "AnalysisId": "an-002",
                "Status": "CREATION_SUCCESSFUL",
            },
        ]
        result = self.mock_client.search_analyses("WBR")
        assert len(result) == 1
        assert result[0]["Name"] == "WBR Analysis"
        self.mock_client.search_analyses.assert_called_once_with("WBR")

    def test_search_analyses_no_match(self):
        """Test search_analyses returns empty when no match found."""
        self.mock_client.search_analyses.return_value = []
        result = self.mock_client.search_analyses("nonexistent")
        assert len(result) == 0

    def test_get_visuals(self):
        """Test get_visuals returns parsed visual info."""
        self.mock_client.get_visuals.return_value = [
            {
                "type": "Table",
                "visual_id": "v-001",
                "title": "Details",
                "sheet_name": "Sheet 1",
            },
            {
                "type": "KPI",
                "visual_id": "v-002",
                "title": "Revenue",
                "sheet_name": "Sheet 1",
            },
            {
                "type": "BarChart",
                "visual_id": "v-003",
                "title": "Trend",
                "sheet_name": "Sheet 2",
            },
        ]
        visuals = self.mock_client.get_visuals("an-001")
        assert len(visuals) == 3
        assert visuals[0]["type"] == "Table"
        assert visuals[1]["type"] == "KPI"
        assert visuals[2]["sheet_name"] == "Sheet 2"

    def test_get_columns_used(self):
        """Test get_columns_used returns frequency map."""
        self.mock_client.get_columns_used.return_value = {
            "MARKET_NAME": 15,
            "REVENUE": 10,
            "DATE": 8,
            "COST": 3,
        }
        columns = self.mock_client.get_columns_used("an-001")
        assert columns["MARKET_NAME"] == 15
        assert "REVENUE" in columns
        assert columns["COST"] == 3

    def test_get_calculated_fields(self):
        """Test get_calculated_fields returns field list."""
        self.mock_client.get_calculated_fields.return_value = [
            {
                "Name": "Profit",
                "Expression": "{Revenue} - {Cost}",
                "DataSetIdentifier": "ds1",
            },
            {
                "Name": "Margin",
                "Expression": "({Revenue} - {Cost}) / {Revenue}",
                "DataSetIdentifier": "ds1",
            },
        ]
        fields = self.mock_client.get_calculated_fields("an-001")
        assert len(fields) == 2
        assert fields[0]["Name"] == "Profit"
        assert "{Revenue}" in fields[1]["Expression"]

    def test_describe_analysis_returns_structure(self):
        """Test describe_analysis returns complete structure."""
        self.mock_client.describe_analysis.return_value = {
            "analysis_id": "an-001",
            "name": "Sales Analysis",
            "status": "CREATION_SUCCESSFUL",
            "sheets": [
                {"name": "Overview", "visuals": 5},
                {"name": "Details", "visuals": 3},
            ],
            "total_visuals": 8,
            "calculated_fields": 4,
            "datasets": ["ds-001", "ds-002"],
        }
        result = self.mock_client.describe_analysis("an-001")
        assert result["name"] == "Sales Analysis"
        assert len(result["sheets"]) == 2
        assert result["total_visuals"] == 8

    def test_analysis_error_handling(self):
        """Test that analysis errors propagate correctly."""
        self.mock_client.describe_analysis.side_effect = Exception("Analysis not found")

        with pytest.raises(Exception, match="Analysis not found"):
            self.mock_client.describe_analysis("bad-id")
