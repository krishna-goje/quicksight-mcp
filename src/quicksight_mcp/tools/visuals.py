"""Visual management MCP tools for QuickSight.

Provides tools for inspecting, adding, deleting visuals and
managing their layout and titles within analyses.
"""

import json
import logging
from typing import Callable

from fastmcp import FastMCP

from quicksight_mcp.tools._decorator import qs_tool

logger = logging.getLogger(__name__)


def register_visual_tools(mcp: FastMCP, get_client: Callable, get_tracker: Callable, get_memory=None):
    """Register all visual-related MCP tools."""

    @qs_tool(mcp, get_memory, read_only=True)
    def get_visual_definition(analysis_id: str, visual_id: str) -> dict:
        """Get the full raw definition of a specific visual.

        Use this to inspect a visual's complete configuration including
        field mappings, aggregations, formatting, and chart configuration.
        The returned definition can be modified and passed to add_visual
        to create a copy.

        Args:
            analysis_id: The QuickSight analysis ID.
            visual_id: The visual ID to inspect.

        Returns the complete visual definition dict, or indicates not found.
        """
        client = get_client()
        visual_def = client.get_visual_definition(analysis_id, visual_id)
        if visual_def is None:
            return {
                "analysis_id": analysis_id,
                "visual_id": visual_id,
                "found": False,
                "note": "Visual not found. Use list_visuals to see available visuals.",
            }
        return {
            "analysis_id": analysis_id,
            "visual_id": visual_id,
            "found": True,
            "definition": visual_def,
        }

    @qs_tool(mcp, get_memory, idempotent=True)
    def add_visual(
        analysis_id: str, sheet_id: str, visual_definition: str
    ) -> dict:
        """Add a visual to a sheet in a QuickSight analysis.

        WARNING: This modifies the analysis definition. A backup is
        automatically created before making changes.

        Args:
            analysis_id: The QuickSight analysis ID.
            sheet_id: The target sheet ID to add the visual to.
            visual_definition: JSON string of the visual definition.
                Must be a dict with one key being the visual type
                (e.g., {"KPIVisual": {...}}, {"BarChartVisual": {...}}).
                Get examples from get_visual_definition on existing visuals.

        Returns confirmation with the visual ID.
        """
        client = get_client()
        parsed_def = json.loads(visual_definition) if isinstance(visual_definition, str) else visual_definition
        result = client.add_visual_to_sheet(
            analysis_id, sheet_id, parsed_def
        )
        return {
            "status": "success",
            "analysis_id": analysis_id,
            "sheet_id": sheet_id,
            "visual_id": result.get("visual_id"),
            "note": (
                "Visual added. Use set_visual_layout to position it. "
                "Use set_visual_title to set the display title."
            ),
        }

    @qs_tool(mcp, get_memory, destructive=True)
    def delete_visual(analysis_id: str, visual_id: str) -> dict:
        """Delete a visual from a QuickSight analysis.

        WARNING: This is destructive. The visual and its layout element
        will be removed. A backup is automatically created before deletion.

        Args:
            analysis_id: The QuickSight analysis ID.
            visual_id: The ID of the visual to delete.
        """
        client = get_client()
        client.delete_visual(analysis_id, visual_id)
        return {
            "status": "success",
            "analysis_id": analysis_id,
            "deleted_visual_id": visual_id,
            "note": "Visual deleted. Use backup_analysis to restore if needed.",
        }

    @qs_tool(mcp, get_memory, idempotent=True)
    def set_visual_title(analysis_id: str, visual_id: str, title: str) -> dict:
        """Set or update the title of a visual.

        WARNING: This modifies the analysis definition. A backup is
        automatically created before making changes.

        Args:
            analysis_id: The QuickSight analysis ID.
            visual_id: The visual ID to update.
            title: The new display title for the visual.
        """
        client = get_client()
        client.set_visual_title(analysis_id, visual_id, title)
        return {
            "status": "success",
            "analysis_id": analysis_id,
            "visual_id": visual_id,
            "title": title,
        }

    @qs_tool(mcp, get_memory, idempotent=True)
    def set_visual_layout(
        analysis_id: str,
        visual_id: str,
        column_index: int,
        column_span: int,
        row_index: int,
        row_span: int,
    ) -> dict:
        """Set the position and size of a visual in the grid layout.

        QuickSight uses a 36-column grid. Common patterns:
        - Full width: column_index=0, column_span=36
        - Half width: column_span=18
        - Third width: column_span=12
        - Row height: typically 8-16 rows per visual

        WARNING: This modifies the analysis definition. A backup is
        automatically created before making changes.

        Args:
            analysis_id: The QuickSight analysis ID.
            visual_id: The visual ID to position.
            column_index: Column position (0-35).
            column_span: Width in columns (1-36).
            row_index: Row position (0-based).
            row_span: Height in rows.
        """
        client = get_client()
        client.set_visual_layout(
            analysis_id, visual_id,
            column_index=column_index,
            column_span=column_span,
            row_index=row_index,
            row_span=row_span,
        )
        return {
            "status": "success",
            "analysis_id": analysis_id,
            "visual_id": visual_id,
            "layout": {
                "column_index": column_index,
                "column_span": column_span,
                "row_index": row_index,
                "row_span": row_span,
            },
        }

    # ------------------------------------------------------------------
    # Chart Builder Tools (simple-parameter visual creation)
    # ------------------------------------------------------------------

    @qs_tool(mcp, get_memory, idempotent=True)
    def create_kpi(
        analysis_id: str, sheet_id: str, title: str,
        column: str, aggregation: str, dataset_identifier: str,
        format_string: str = "", conditional_format: str = "",
    ) -> dict:
        """Create a KPI visual from simple parameters.

        Args:
            analysis_id: The QuickSight analysis ID.
            sheet_id: The sheet to add the KPI to.
            title: Display title (e.g., "Total Contracts").
            column: Column name (e.g., "FLIP_TOKEN").
            aggregation: SUM, COUNT, AVG, MIN, MAX, or DISTINCT_COUNT.
            dataset_identifier: The dataset identifier.
            format_string: Display format (e.g., "#,##0", "$#,##0.00", "0.0%").
                Leave empty for default formatting.
            conditional_format: JSON string of color rules. Example:
                '[{"condition": ">= 100", "color": "#2CAF4A"},
                  {"condition": "< 50", "color": "#DE3B00"}]'
                Leave empty for no conditional formatting.
        """
        client = get_client()
        cf = json.loads(conditional_format) if conditional_format else None
        result = client.create_kpi(
            analysis_id, sheet_id, title, column, aggregation, dataset_identifier,
            format_string=format_string or None,
            conditional_format=cf,
        )
        return {
            "status": "success",
            "visual_id": result.get("visual_id"),
            "title": title,
            "note": "KPI created. Use set_visual_layout to reposition.",
        }

    @qs_tool(mcp, get_memory, idempotent=True)
    def create_bar_chart(
        analysis_id: str, sheet_id: str, title: str,
        category_column: str, value_column: str, value_aggregation: str,
        dataset_identifier: str, orientation: str = "VERTICAL",
        format_string: str = "", show_data_labels: bool = False,
    ) -> dict:
        """Create a bar chart from simple parameters.

        Args:
            analysis_id: The QuickSight analysis ID.
            sheet_id: The sheet to add the chart to.
            title: Display title.
            category_column: Dimension column for X-axis (e.g., "MARKET_NAME").
            value_column: Measure column for Y-axis (e.g., "FLIP_TOKEN").
            value_aggregation: SUM, COUNT, AVG, etc.
            dataset_identifier: The dataset identifier.
            orientation: VERTICAL (default) or HORIZONTAL.
        """
        client = get_client()
        result = client.create_bar_chart(
            analysis_id, sheet_id, title, category_column,
            value_column, value_aggregation, dataset_identifier, orientation,
            format_string=format_string or None,
            show_data_labels=show_data_labels,
        )
        return {
            "status": "success",
            "visual_id": result.get("visual_id"),
            "title": title,
            "note": "Bar chart created. Use set_visual_layout to reposition.",
        }

    @qs_tool(mcp, get_memory, idempotent=True)
    def create_line_chart(
        analysis_id: str, sheet_id: str, title: str,
        date_column: str, value_column: str, value_aggregation: str,
        dataset_identifier: str, date_granularity: str = "WEEK",
        format_string: str = "", show_data_labels: bool = False,
    ) -> dict:
        """Create a line chart from simple parameters.

        Args:
            analysis_id: The QuickSight analysis ID.
            sheet_id: The sheet to add the chart to.
            title: Display title.
            date_column: Date column for X-axis.
            value_column: Measure column for Y-axis.
            value_aggregation: SUM, COUNT, AVG, etc.
            dataset_identifier: The dataset identifier.
            date_granularity: DAY, WEEK, MONTH, QUARTER, or YEAR.
        """
        client = get_client()
        result = client.create_line_chart(
            analysis_id, sheet_id, title, date_column,
            value_column, value_aggregation, dataset_identifier, date_granularity,
            format_string=format_string or None,
            show_data_labels=show_data_labels,
        )
        return {
            "status": "success",
            "visual_id": result.get("visual_id"),
            "title": title,
            "note": "Line chart created. Use set_visual_layout to reposition.",
        }

    @qs_tool(mcp, get_memory, idempotent=True)
    def create_pivot_table(
        analysis_id: str, sheet_id: str, title: str,
        row_columns: str, value_columns: str, value_aggregations: str,
        dataset_identifier: str,
    ) -> dict:
        """Create a pivot table from simple parameters.

        Args:
            analysis_id: The QuickSight analysis ID.
            sheet_id: The sheet to add the table to.
            title: Display title.
            row_columns: Comma-separated dimension columns for rows
                (e.g., "MARKET_NAME,ASSESSMENT_TYPE").
            value_columns: Comma-separated measure columns for values
                (e.g., "FLIP_TOKEN,REVENUE").
            value_aggregations: Comma-separated aggregations, one per value
                (e.g., "COUNT,SUM").
            dataset_identifier: The dataset identifier.
        """
        client = get_client()
        rows = [c.strip() for c in row_columns.split(',')]
        vals = [c.strip() for c in value_columns.split(',')]
        aggs = [a.strip() for a in value_aggregations.split(',')]
        result = client.create_pivot_table(
            analysis_id, sheet_id, title, rows, vals, aggs, dataset_identifier
        )
        return {
            "status": "success",
            "visual_id": result.get("visual_id"),
            "title": title,
            "note": "Pivot table created. Use set_visual_layout to reposition.",
        }

    @qs_tool(mcp, get_memory, idempotent=True)
    def create_table(
        analysis_id: str, sheet_id: str, title: str,
        columns: str, dataset_identifier: str,
    ) -> dict:
        """Create a flat table visual from simple parameters.

        Args:
            analysis_id: The QuickSight analysis ID.
            sheet_id: The sheet to add the table to.
            title: Display title.
            columns: Comma-separated column names to display
                (e.g., "FLIP_TOKEN,MARKET_NAME,PURCHASE_AGREEMENT_COMPLETED_AT").
            dataset_identifier: The dataset identifier.
        """
        client = get_client()
        cols = [c.strip() for c in columns.split(',')]
        result = client.create_table(
            analysis_id, sheet_id, title, cols, dataset_identifier
        )
        return {
            "status": "success",
            "visual_id": result.get("visual_id"),
            "title": title,
            "note": "Table created. Use set_visual_layout to reposition.",
        }

    @qs_tool(mcp, get_memory, idempotent=True)
    def create_combo_chart(
        analysis_id: str, sheet_id: str, title: str,
        category_column: str, bar_column: str, bar_aggregation: str,
        line_column: str, line_aggregation: str, dataset_identifier: str,
        bar_format_string: str = "", line_format_string: str = "",
        show_data_labels: bool = False,
    ) -> dict:
        """Create a combo chart (bars + line on same chart) from simple parameters.

        A combo chart overlays bar values and line values sharing a category
        axis.  For example, count bars with a percentage line overlay.

        Args:
            analysis_id: The QuickSight analysis ID.
            sheet_id: The sheet to add the chart to.
            title: Display title.
            category_column: Dimension column for the shared X-axis
                (e.g., "MARKET_NAME" or "WEEK_DATE").
            bar_column: Measure column rendered as bars (e.g., "FLIP_TOKEN").
            bar_aggregation: Aggregation for bar values (SUM, COUNT, AVG, etc.).
            line_column: Measure column rendered as a line (e.g., "CONVERSION_RATE").
            line_aggregation: Aggregation for line values (SUM, COUNT, AVG, etc.).
            dataset_identifier: The dataset identifier.
            bar_format_string: Display format for bar values (e.g., "#,##0").
                Leave empty for default formatting.
            line_format_string: Display format for line values (e.g., "0.0%").
                Leave empty for default formatting.
            show_data_labels: Show value labels on bars and line points.
        """
        client = get_client()
        result = client.create_combo_chart(
            analysis_id, sheet_id, title, category_column,
            bar_column, bar_aggregation,
            line_column, line_aggregation,
            dataset_identifier,
            bar_format_string=bar_format_string or None,
            line_format_string=line_format_string or None,
            show_data_labels=show_data_labels,
        )
        return {
            "status": "success",
            "visual_id": result.get("visual_id"),
            "title": title,
            "note": "Combo chart created. Use set_visual_layout to reposition.",
        }

    @qs_tool(mcp, get_memory, idempotent=True)
    def create_pie_chart(
        analysis_id: str, sheet_id: str, title: str,
        group_column: str, value_column: str, value_aggregation: str,
        dataset_identifier: str, format_string: str = "",
    ) -> dict:
        """Create a pie chart from simple parameters.

        Args:
            analysis_id: The QuickSight analysis ID.
            sheet_id: The sheet to add the chart to.
            title: Display title.
            group_column: Dimension column for pie slices (e.g., "MARKET_NAME").
            value_column: Measure column for slice sizes (e.g., "REVENUE").
            value_aggregation: SUM, COUNT, AVG, MIN, MAX, or DISTINCT_COUNT.
            dataset_identifier: The dataset identifier.
            format_string: Display format (e.g., "#,##0", "$#,##0.00", "0.0%").
                Leave empty for default formatting.
        """
        client = get_client()
        result = client.create_pie_chart(
            analysis_id, sheet_id, title,
            group_column, value_column, value_aggregation,
            dataset_identifier,
            format_string=format_string or None,
        )
        return {
            "status": "success",
            "visual_id": result.get("visual_id"),
            "title": title,
            "note": "Pie chart created. Use set_visual_layout to reposition.",
        }
