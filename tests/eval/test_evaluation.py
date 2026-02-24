"""Evaluation suite — 10 multi-hop scenarios testing end-to-end workflows.

These tests use mocked AWS responses to verify the complete tool → service
→ AWS pipeline works correctly. They test the *integration* between layers,
not individual units.

For live integration tests, see tests/integration_sprint*.py.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from quicksight_mcp.config import Settings
from quicksight_mcp.core.cache import TTLCache
from quicksight_mcp.safety.exceptions import (
    DestructiveChangeError,
)


# =========================================================================
# Helpers
# =========================================================================

def _make_mock_aws():
    """Create a mock AwsClient with common responses."""
    aws = MagicMock()
    aws.account_id = "123456789012"
    aws.ensure_account_id.return_value = "123456789012"
    return aws


def _make_dataset_response(name="Test Dataset", sql="SELECT 1"):
    return {
        "DataSet": {
            "DataSetId": "ds-001",
            "Name": name,
            "ImportMode": "SPICE",
            "PhysicalTableMap": {
                "pt-1": {
                    "CustomSql": {
                        "SqlQuery": sql,
                        "DataSourceArn": "arn:aws:quicksight:us-east-1:123:datasource/src",
                        "Name": name,
                        "Columns": [{"Name": "Col1", "Type": "STRING"}],
                    }
                }
            },
            "LogicalTableMap": {
                "lt-1": {"Alias": name, "Source": {"PhysicalTableId": "pt-1"}}
            },
            "OutputColumns": [{"Name": "Col1", "Type": "STRING"}],
        }
    }


def _make_analysis_response(status="UPDATE_SUCCESSFUL"):
    return {
        "Analysis": {
            "AnalysisId": "an-001",
            "Name": "Test Analysis",
            "Status": status,
            "Arn": "arn:aws:quicksight:us-east-1:123:analysis/an-001",
            "LastUpdatedTime": "2026-01-01T00:00:00Z",
        }
    }


def _make_definition(sheets=1, visuals_per_sheet=2, calc_fields=1):
    sheets_list = []
    for i in range(sheets):
        visuals = []
        layout_elements = []
        for j in range(visuals_per_sheet):
            vid = f"v-{i}-{j}"
            visuals.append({
                "KPIVisual": {
                    "VisualId": vid,
                    "Title": {"FormatText": {"PlainText": f"Visual {j}"}},
                    "ChartConfiguration": {"FieldWells": {"Values": []}},
                }
            })
            layout_elements.append({
                "ElementId": vid,
                "ElementType": "VISUAL",
                "ColumnIndex": 0,
                "ColumnSpan": 36,
                "RowIndex": j * 12,
                "RowSpan": 12,
            })
        sheets_list.append({
            "SheetId": f"sheet-{i}",
            "Name": f"Sheet {i}",
            "ContentType": "INTERACTIVE",
            "Visuals": visuals,
            "Layouts": [{"Configuration": {"GridLayout": {"Elements": layout_elements}}}],
        })

    calc = []
    for k in range(calc_fields):
        calc.append({
            "DataSetIdentifier": "ds1",
            "Name": f"CalcField_{k}",
            "Expression": f"sum({{col{k}}})",
        })

    return {
        "Definition": {
            "DataSetIdentifierDeclarations": [
                {"Identifier": "ds1", "DataSetArn": "arn:aws:quicksight:us-east-1:123:dataset/ds-001"}
            ],
            "Sheets": sheets_list,
            "CalculatedFields": calc,
            "ParameterDeclarations": [],
            "FilterGroups": [],
        }
    }


# =========================================================================
# E1: Dataset workflow (search → get SQL → modify → verify)
# =========================================================================

class TestE1DatasetWorkflow:
    """End-to-end dataset SQL modification workflow."""

    def test_search_get_modify_verify(self):
        from quicksight_mcp.services.datasets import DatasetService

        aws = _make_mock_aws()
        cache = TTLCache()
        settings = Settings()

        # Mock responses
        # search uses aws.call("search_data_sets", ...) first
        aws.call.side_effect = [
            # search_data_sets (server-side search)
            {"DataSetSummaries": [
                {"Name": "WBR Weekly", "DataSetId": "ds-001", "ImportMode": "SPICE"}
            ]},
            # describe_data_set for get_sql
            _make_dataset_response("WBR Weekly", "SELECT * FROM old_table"),
            # describe_data_set for update_sql (to get current dataset)
            _make_dataset_response("WBR Weekly", "SELECT * FROM old_table"),
            # update_data_set
            {"Status": 200},
        ]

        svc = DatasetService(aws, cache, settings)

        # Step 1: Search
        results = svc.search("WBR")
        assert len(results) == 1
        assert results[0]["Name"] == "WBR Weekly"

        # Step 2: Get SQL
        sql = svc.get_sql("ds-001")
        assert "old_table" in sql

        # Step 3: Modify
        svc.update_sql(
            "ds-001",
            "SELECT * FROM new_table",
            backup_first=False,
            verify=False,
        )

        # Verify update_data_set was called
        assert any(
            "update_data_set" in str(c) for c in aws.call.call_args_list
        )


# =========================================================================
# E5: Destructive change blocked
# =========================================================================

class TestE5DestructiveChangeBlocked:
    """Destructive guard blocks deletion of all sheets."""

    def test_blocks_removal_of_all_sheets(self):
        from quicksight_mcp.safety.destructive_guard import (
            validate_definition_not_destructive,
        )

        current = _make_definition(sheets=3, visuals_per_sheet=5)["Definition"]
        new_def = {
            "Sheets": [],  # Removing ALL sheets
            "CalculatedFields": current.get("CalculatedFields", []),
        }

        with pytest.raises(DestructiveChangeError) as exc_info:
            validate_definition_not_destructive(current, new_def, "an-001")

        assert "DELETE ALL" in str(exc_info.value)
        assert exc_info.value.error_type == "destructive_blocked"


# =========================================================================
# E6: Visual/layout alignment health check
# =========================================================================

class TestE6HealthCheck:
    """Health check catches visuals without layout elements."""

    def test_healthy_analysis(self):
        from quicksight_mcp.services.analyses import AnalysisService

        aws = _make_mock_aws()
        cache = TTLCache()
        settings = Settings()

        aws.call.side_effect = [
            _make_analysis_response(),
            _make_definition(sheets=2, visuals_per_sheet=3),
        ]

        svc = AnalysisService(aws, cache, settings)
        # Clear cache to force fresh fetch
        cache.clear()

        result = svc.verify_health("an-001")
        assert result["healthy"] is True


# =========================================================================
# E10: Memory learns from operations
# =========================================================================

class TestE10MemoryLearning:
    """Memory system records and surfaces insights from usage."""

    def test_memory_learns_from_calls(self, tmp_path):
        from quicksight_mcp.memory.manager import MemoryManager

        mgr = MemoryManager(str(tmp_path))

        # Simulate 5 search_datasets calls
        for i in range(5):
            mgr.record_call("search_datasets", {"name": f"test{i}"}, 50.0, True)

        # Simulate workflow pattern
        mgr.record_call("search_datasets", {}, 50.0, True)
        mgr.record_call("get_dataset_sql", {}, 30.0, True)
        mgr.record_call("update_dataset_sql", {}, 200.0, True)

        insights = mgr.usage.get_insights()
        assert insights["total_calls"] == 8
        assert any(
            t["tool"] == "search_datasets" for t in insights["most_used_tools"]
        )

    def test_error_recovery_suggestions(self, tmp_path):
        from quicksight_mcp.memory.manager import MemoryManager

        mgr = MemoryManager(str(tmp_path))

        # Record error with recovery
        mgr.errors.record_error(
            "an-001", "update_failed", "Analysis update failed",
            recovery_used="restore from backup", recovery_worked=True,
        )

        # Get suggestions
        suggestions = mgr.get_recovery_suggestions("an-001", "update_failed")
        assert len(suggestions) > 0
        assert "restore from backup" in suggestions[0]


# =========================================================================
# Pydantic validation
# =========================================================================

class TestPydanticValidation:
    """Input validation catches bad inputs before they hit AWS."""

    def test_sql_must_start_with_select(self):
        from quicksight_mcp.tools._models import UpdateDatasetSqlInput

        with pytest.raises(Exception):
            UpdateDatasetSqlInput(
                dataset_id="ds-001",
                new_sql="DROP TABLE foo",
                backup_first=True,
            )

    def test_layout_bounds_validated(self):
        from quicksight_mcp.tools._models import SetVisualLayoutInput

        with pytest.raises(Exception):
            SetVisualLayoutInput(
                analysis_id="a1",
                visual_id="v1",
                column_index=36,  # Max is 35
                column_span=1,
                row_index=0,
                row_span=1,
            )

    def test_extra_fields_rejected(self):
        from quicksight_mcp.tools._models import DatasetIdInput

        with pytest.raises(Exception):
            DatasetIdInput(dataset_id="ds-001", extra_field="bad")

    def test_valid_input_passes(self):
        from quicksight_mcp.tools._models import CreateKpiInput

        m = CreateKpiInput(
            analysis_id="an-001",
            sheet_id="sh-001",
            title="Total Revenue",
            column="REVENUE",
            aggregation="SUM",
            dataset_identifier="ds1",
        )
        assert m.aggregation == "SUM"
