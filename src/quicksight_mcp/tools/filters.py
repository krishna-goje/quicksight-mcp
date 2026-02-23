"""Filter group management MCP tools for QuickSight.

Provides tools for adding and deleting filter groups within
QuickSight analyses. Filter groups control what data is shown
and can be scoped to specific sheets or visuals.
"""

import json
import logging
from typing import Callable

from fastmcp import FastMCP

from quicksight_mcp.tools._decorator import qs_tool

logger = logging.getLogger(__name__)


def register_filter_tools(
    mcp: FastMCP, get_client: Callable, get_tracker: Callable, get_memory=None
):
    """Register all filter-related MCP tools."""

    @qs_tool(mcp, get_memory, idempotent=True)
    def add_filter_group(analysis_id: str, filter_group_definition: str) -> dict:
        """Add a filter group to a QuickSight analysis.

        WARNING: This modifies the analysis definition. A backup is
        automatically created before making changes.

        Args:
            analysis_id: The QuickSight analysis ID.
            filter_group_definition: JSON string of the filter group.
                Must include FilterGroupId, Filters array, CrossDataset
                setting, and ScopeConfiguration.

                Example:
                {"FilterGroupId": "my-filter-group",
                 "Filters": [{"CategoryFilter": {...}}],
                 "CrossDataset": "SINGLE_DATASET",
                 "ScopeConfiguration": {
                     "SelectedSheets": {
                         "SheetVisualScopingConfigurations": [
                             {"SheetId": "...", "Scope": "ALL_VISUALS"}
                         ]
                     }
                 },
                 "Status": "ENABLED"}

        Returns confirmation with the filter group ID.
        """
        client = get_client()
        parsed_def = json.loads(filter_group_definition) if isinstance(filter_group_definition, str) else filter_group_definition
        result = client.add_filter_group(analysis_id, parsed_def)
        return {
            "status": "success",
            "analysis_id": analysis_id,
            "filter_group_id": result.get("filter_group_id"),
            "note": (
                "Filter group added. It will apply to the configured "
                "scope (sheets/visuals) immediately."
            ),
        }

    @qs_tool(mcp, get_memory, destructive=True)
    def delete_filter_group(analysis_id: str, filter_group_id: str) -> dict:
        """Delete a filter group from a QuickSight analysis.

        WARNING: This is destructive. Removing a filter group may change
        what data is displayed in affected visuals. A backup is
        automatically created before deletion.

        Args:
            analysis_id: The QuickSight analysis ID.
            filter_group_id: The ID of the filter group to delete.
        """
        client = get_client()
        client.delete_filter_group(analysis_id, filter_group_id)
        return {
            "status": "success",
            "analysis_id": analysis_id,
            "deleted_filter_group_id": filter_group_id,
            "note": "Filter group deleted. Use backup_analysis to restore if needed.",
        }
