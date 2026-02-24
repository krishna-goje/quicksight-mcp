"""Pydantic input models for MCP tool validation.

Every tool gets a model with Field() constraints and descriptions.
Models are organized by domain section.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


# =========================================================================
# Base
# =========================================================================


class StrictModel(BaseModel):
    """Base model that rejects extra fields."""

    model_config = ConfigDict(extra="forbid")


# =========================================================================
# Datasets
# =========================================================================


class SearchInput(StrictModel):
    """Input for search_datasets / search_analyses / search_dashboards."""

    name: str = Field(..., min_length=1, max_length=256, description="Search string")


class DatasetIdInput(StrictModel):
    """Input requiring a dataset_id."""

    dataset_id: str = Field(..., min_length=1, max_length=256)


class UpdateDatasetSqlInput(StrictModel):
    """Input for update_dataset_sql."""

    dataset_id: str = Field(..., min_length=1, max_length=256)
    new_sql: str = Field(..., min_length=1, description="Must contain SELECT or WITH")
    backup_first: bool = Field(True, description="Create backup before updating")

    @field_validator("new_sql")
    @classmethod
    def sql_must_be_valid(cls, v: str) -> str:
        upper = v.strip().upper()
        if not (upper.startswith("SELECT") or upper.startswith("WITH")):
            raise ValueError("SQL must start with SELECT or WITH")
        return v


class ModifyDatasetSqlInput(StrictModel):
    """Input for modify_dataset_sql (find/replace)."""

    dataset_id: str = Field(..., min_length=1, max_length=256)
    find: str = Field(..., min_length=1, description="Exact text to find")
    replace: str = Field(..., description="Replacement text")


class CreateDatasetInput(StrictModel):
    """Input for create_dataset."""

    name: str = Field(..., min_length=1, max_length=256)
    sql: str = Field(..., min_length=1)
    data_source_arn: str = Field(..., min_length=1)
    import_mode: Literal["SPICE", "DIRECT_QUERY"] = "SPICE"


class UpdateDatasetDefinitionInput(StrictModel):
    """Input for update_dataset_definition."""

    dataset_id: str = Field(..., min_length=1, max_length=256)
    definition_json: str = Field(..., min_length=2, description="JSON string")


class RefreshStatusInput(StrictModel):
    """Input for get_refresh_status."""

    dataset_id: str = Field(..., min_length=1, max_length=256)
    ingestion_id: str = Field(..., min_length=1)


class CancelRefreshInput(StrictModel):
    """Input for cancel_refresh."""

    dataset_id: str = Field(..., min_length=1, max_length=256)
    ingestion_id: str = Field(..., min_length=1)


class ListRefreshesInput(StrictModel):
    """Input for list_recent_refreshes."""

    dataset_id: str = Field(..., min_length=1, max_length=256)
    limit: int = Field(5, ge=1, le=100)


# =========================================================================
# Analyses
# =========================================================================


class AnalysisIdInput(StrictModel):
    """Input requiring an analysis_id."""

    analysis_id: str = Field(..., min_length=1, max_length=256)


# =========================================================================
# Calculated Fields
# =========================================================================


class AddCalcFieldInput(StrictModel):
    """Input for add_calculated_field."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    name: str = Field(..., min_length=1, max_length=256)
    expression: str = Field(..., min_length=1)
    dataset_identifier: str = Field(..., min_length=1)


class UpdateCalcFieldInput(StrictModel):
    """Input for update_calculated_field."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    name: str = Field(..., min_length=1, max_length=256)
    new_expression: str = Field(..., min_length=1)


class DeleteCalcFieldInput(StrictModel):
    """Input for delete_calculated_field."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    name: str = Field(..., min_length=1, max_length=256)


class GetCalcFieldInput(StrictModel):
    """Input for get_calculated_field."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    name: str = Field(..., min_length=1, max_length=256)


# =========================================================================
# Dashboards
# =========================================================================


class DashboardIdInput(StrictModel):
    """Input requiring a dashboard_id."""

    dashboard_id: str = Field(..., min_length=1, max_length=256)


class DashboardVersionsInput(StrictModel):
    """Input for get_dashboard_versions."""

    dashboard_id: str = Field(..., min_length=1, max_length=256)
    limit: int = Field(10, ge=1, le=100)


class PublishDashboardInput(StrictModel):
    """Input for publish_dashboard."""

    dashboard_id: str = Field(..., min_length=1, max_length=256)
    source_analysis_id: str = Field(..., min_length=1, max_length=256)
    version_description: str = ""


class RollbackDashboardInput(StrictModel):
    """Input for rollback_dashboard."""

    dashboard_id: str = Field(..., min_length=1, max_length=256)
    version_number: int = Field(..., ge=1)


# =========================================================================
# Sheets
# =========================================================================


class AddSheetInput(StrictModel):
    """Input for add_sheet."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    name: str = Field(..., min_length=1, max_length=256)


class DeleteSheetInput(StrictModel):
    """Input for delete_sheet."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    sheet_id: str = Field(..., min_length=1, max_length=256)


class RenameSheetInput(StrictModel):
    """Input for rename_sheet."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    sheet_id: str = Field(..., min_length=1, max_length=256)
    new_name: str = Field(..., min_length=1, max_length=256)


class ReplicateSheetInput(StrictModel):
    """Input for replicate_sheet."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    source_sheet_id: str = Field(..., min_length=1, max_length=256)
    target_sheet_name: str = Field(..., min_length=1, max_length=256)


class DeleteEmptySheetsInput(StrictModel):
    """Input for delete_empty_sheets."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    name_contains: str = ""


class ListSheetVisualsInput(StrictModel):
    """Input for list_sheet_visuals."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    sheet_id: str = Field(..., min_length=1, max_length=256)


# =========================================================================
# Visuals
# =========================================================================


class GetVisualDefInput(StrictModel):
    """Input for get_visual_definition."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    visual_id: str = Field(..., min_length=1, max_length=256)


class AddVisualInput(StrictModel):
    """Input for add_visual."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    sheet_id: str = Field(..., min_length=1, max_length=256)
    visual_definition: str = Field(..., min_length=2, description="JSON string")


class DeleteVisualInput(StrictModel):
    """Input for delete_visual."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    visual_id: str = Field(..., min_length=1, max_length=256)


class SetVisualTitleInput(StrictModel):
    """Input for set_visual_title."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    visual_id: str = Field(..., min_length=1, max_length=256)
    title: str = Field(..., min_length=1)


class SetVisualLayoutInput(StrictModel):
    """Input for set_visual_layout."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    visual_id: str = Field(..., min_length=1, max_length=256)
    column_index: int = Field(..., ge=0, le=35)
    column_span: int = Field(..., ge=1, le=36)
    row_index: int = Field(..., ge=0)
    row_span: int = Field(..., ge=1)


# =========================================================================
# Parameters & Filters
# =========================================================================


class AddParameterInput(StrictModel):
    """Input for add_parameter."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    parameter_definition: str = Field(..., min_length=2, description="JSON string")


class DeleteParameterInput(StrictModel):
    """Input for delete_parameter."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    parameter_name: str = Field(..., min_length=1, max_length=256)


class AddFilterGroupInput(StrictModel):
    """Input for add_filter_group."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    filter_group_definition: str = Field(
        ..., min_length=2, description="JSON string"
    )


class DeleteFilterGroupInput(StrictModel):
    """Input for delete_filter_group."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    filter_group_id: str = Field(..., min_length=1, max_length=256)


# =========================================================================
# Backup & Restore
# =========================================================================


class BackupInput(StrictModel):
    """Input for backup_analysis / backup_dataset."""

    analysis_id: str = Field(
        default="", min_length=0, max_length=256,
        description="Analysis ID (for backup_analysis)",
    )
    dataset_id: str = Field(
        default="", min_length=0, max_length=256,
        description="Dataset ID (for backup_dataset)",
    )


class RestoreInput(StrictModel):
    """Input for restore_analysis."""

    backup_file: str = Field(..., min_length=1)
    analysis_id: str = Field(default="", max_length=256)


class CloneAnalysisInput(StrictModel):
    """Input for clone_analysis."""

    source_analysis_id: str = Field(..., min_length=1, max_length=256)
    new_name: str = Field(..., min_length=1, max_length=256)


# =========================================================================
# Snapshots
# =========================================================================


class SnapshotInput(StrictModel):
    """Input for snapshot_analysis."""

    analysis_id: str = Field(..., min_length=1, max_length=256)


class DiffInput(StrictModel):
    """Input for diff_analysis."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    snapshot_id: str = Field(..., min_length=1)


# =========================================================================
# Chart Builders
# =========================================================================


class CreateKpiInput(StrictModel):
    """Input for create_kpi."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    sheet_id: str = Field(..., min_length=1, max_length=256)
    title: str = Field(..., min_length=1)
    column: str = Field(..., min_length=1)
    aggregation: Literal[
        "SUM", "COUNT", "AVG", "MIN", "MAX", "DISTINCT_COUNT"
    ]
    dataset_identifier: str = Field(..., min_length=1)
    format_string: str = ""
    conditional_format: str = ""


class CreateBarChartInput(StrictModel):
    """Input for create_bar_chart."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    sheet_id: str = Field(..., min_length=1, max_length=256)
    title: str = Field(..., min_length=1)
    category_column: str = Field(..., min_length=1)
    value_column: str = Field(..., min_length=1)
    value_aggregation: Literal[
        "SUM", "COUNT", "AVG", "MIN", "MAX", "DISTINCT_COUNT"
    ]
    dataset_identifier: str = Field(..., min_length=1)
    orientation: Literal["VERTICAL", "HORIZONTAL"] = "VERTICAL"
    format_string: str = ""
    show_data_labels: bool = False


class CreateLineChartInput(StrictModel):
    """Input for create_line_chart."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    sheet_id: str = Field(..., min_length=1, max_length=256)
    title: str = Field(..., min_length=1)
    date_column: str = Field(..., min_length=1)
    value_column: str = Field(..., min_length=1)
    value_aggregation: Literal[
        "SUM", "COUNT", "AVG", "MIN", "MAX", "DISTINCT_COUNT"
    ]
    dataset_identifier: str = Field(..., min_length=1)
    date_granularity: Literal[
        "DAY", "WEEK", "MONTH", "QUARTER", "YEAR"
    ] = "WEEK"
    format_string: str = ""
    show_data_labels: bool = False


class CreatePivotTableInput(StrictModel):
    """Input for create_pivot_table."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    sheet_id: str = Field(..., min_length=1, max_length=256)
    title: str = Field(..., min_length=1)
    row_columns: str = Field(
        ..., min_length=1,
        description="Comma-separated dimension columns",
    )
    value_columns: str = Field(
        ..., min_length=1,
        description="Comma-separated measure columns",
    )
    value_aggregations: str = Field(
        ..., min_length=1,
        description="Comma-separated aggregations (one per value column)",
    )
    dataset_identifier: str = Field(..., min_length=1)


class CreateTableInput(StrictModel):
    """Input for create_table."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    sheet_id: str = Field(..., min_length=1, max_length=256)
    title: str = Field(..., min_length=1)
    columns: str = Field(
        ..., min_length=1,
        description="Comma-separated column names",
    )
    dataset_identifier: str = Field(..., min_length=1)


class CreateComboChartInput(StrictModel):
    """Input for create_combo_chart."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    sheet_id: str = Field(..., min_length=1, max_length=256)
    title: str = Field(..., min_length=1)
    category_column: str = Field(..., min_length=1)
    bar_column: str = Field(..., min_length=1)
    bar_aggregation: Literal[
        "SUM", "COUNT", "AVG", "MIN", "MAX", "DISTINCT_COUNT"
    ]
    line_column: str = Field(..., min_length=1)
    line_aggregation: Literal[
        "SUM", "COUNT", "AVG", "MIN", "MAX", "DISTINCT_COUNT"
    ]
    dataset_identifier: str = Field(..., min_length=1)
    bar_format_string: str = ""
    line_format_string: str = ""
    show_data_labels: bool = False


class CreatePieChartInput(StrictModel):
    """Input for create_pie_chart."""

    analysis_id: str = Field(..., min_length=1, max_length=256)
    sheet_id: str = Field(..., min_length=1, max_length=256)
    title: str = Field(..., min_length=1)
    group_column: str = Field(..., min_length=1)
    value_column: str = Field(..., min_length=1)
    value_aggregation: Literal[
        "SUM", "COUNT", "AVG", "MIN", "MAX", "DISTINCT_COUNT"
    ]
    dataset_identifier: str = Field(..., min_length=1)
    format_string: str = ""
