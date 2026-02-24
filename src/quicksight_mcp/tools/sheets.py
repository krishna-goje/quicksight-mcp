"""Sheet management MCP tools for QuickSight.

Provides tools for adding, deleting, renaming sheets and listing
visuals within a specific sheet.
"""

import logging
from typing import Callable

from fastmcp import FastMCP

from quicksight_mcp.tools._decorator import qs_tool

logger = logging.getLogger(__name__)


def register_sheet_tools(mcp: FastMCP, get_client: Callable, get_tracker: Callable, get_memory=None):
    """Register all sheet-related MCP tools."""

    @qs_tool(mcp, get_memory)
    def add_sheet(analysis_id: str, name: str) -> dict:
        """Add a new sheet to a QuickSight analysis.

        WARNING: This modifies the analysis definition. A backup is
        automatically created before making changes.

        Args:
            analysis_id: The QuickSight analysis ID.
            name: Display name for the new sheet.

        Returns the new sheet ID and confirmation.
        """
        client = get_client()
        result = client.add_sheet(analysis_id, name)
        return {
            "status": "success",
            "analysis_id": analysis_id,
            "sheet_id": result.get("sheet_id"),
            "sheet_name": name,
            "note": (
                "Sheet created. Use add_visual to populate it. "
                "Use set_visual_layout to arrange visuals."
            ),
        }

    @qs_tool(mcp, get_memory, destructive=True)
    def delete_sheet(analysis_id: str, sheet_id: str) -> dict:
        """Delete a sheet from a QuickSight analysis.

        WARNING: This is destructive. All visuals on the sheet will be
        removed. A backup is automatically created before deletion.

        Args:
            analysis_id: The QuickSight analysis ID.
            sheet_id: The ID of the sheet to delete.
        """
        client = get_client()
        client.delete_sheet(analysis_id, sheet_id)
        return {
            "status": "success",
            "analysis_id": analysis_id,
            "deleted_sheet_id": sheet_id,
            "note": "Sheet deleted. Use backup_analysis to restore if needed.",
        }

    @qs_tool(mcp, get_memory, idempotent=True)
    def rename_sheet(analysis_id: str, sheet_id: str, new_name: str) -> dict:
        """Rename an existing sheet in a QuickSight analysis.

        WARNING: This modifies the analysis definition. A backup is
        automatically created before making changes.

        Args:
            analysis_id: The QuickSight analysis ID.
            sheet_id: The ID of the sheet to rename.
            new_name: The new display name for the sheet.
        """
        client = get_client()
        client.rename_sheet(analysis_id, sheet_id, new_name)
        return {
            "status": "success",
            "analysis_id": analysis_id,
            "sheet_id": sheet_id,
            "new_name": new_name,
        }

    @qs_tool(mcp, get_memory, read_only=True)
    def list_sheet_visuals(analysis_id: str, sheet_id: str) -> dict:
        """List all visuals in a specific sheet of a QuickSight analysis.

        Args:
            analysis_id: The QuickSight analysis ID.
            sheet_id: The sheet ID to list visuals for.

        Returns visual IDs, types, and titles for every visual on the sheet.
        """
        client = get_client()
        visuals = client.list_sheet_visuals(analysis_id, sheet_id)
        return {
            "analysis_id": analysis_id,
            "sheet_id": sheet_id,
            "count": len(visuals),
            "visuals": visuals,
        }

    @qs_tool(mcp, get_memory)
    def replicate_sheet(
        analysis_id: str, source_sheet_id: str, target_sheet_name: str
    ) -> dict:
        """Copy all visuals from one sheet to a new sheet in the same analysis.

        This is the recommended way to duplicate a sheet. It copies all
        visuals with their layouts in a single API call, which is much
        more reliable than adding visuals one at a time.

        Visual IDs are automatically prefixed with 'rc_' to avoid conflicts.

        WARNING: This modifies the analysis definition. A backup is
        automatically created before making changes.

        Args:
            analysis_id: The QuickSight analysis ID.
            source_sheet_id: The sheet ID to copy visuals from.
                Use describe_analysis to find sheet IDs.
            target_sheet_name: Display name for the new sheet.

        Returns the new sheet ID, visual count, and visual types.
        """
        client = get_client()
        result = client.replicate_sheet(
            analysis_id, source_sheet_id, target_sheet_name
        )
        return {
            "status": "success",
            **result,
            "note": (
                f"Sheet replicated with {result['visual_count']} visuals. "
                "Visual IDs are prefixed with 'rc_'. "
                "Use set_visual_title or set_visual_layout to customize."
            ),
        }

    @qs_tool(mcp, get_memory, destructive=True)
    def delete_empty_sheets(analysis_id: str, name_contains: str = "") -> dict:
        """Delete all empty sheets (0 visuals) from an analysis.

        Use this to clean up orphan sheets left by failed operations.
        Automatically removes filter groups scoped to deleted sheets.

        WARNING: This is destructive. A backup is automatically created.

        Args:
            analysis_id: The QuickSight analysis ID.
            name_contains: If set, only delete empty sheets whose name
                contains this text (case-insensitive). Leave empty to
                delete ALL empty sheets.
        """
        client = get_client()
        result = client.delete_empty_sheets(
            analysis_id,
            name_contains=name_contains or None,
        )
        if not result['deleted_sheets']:
            return {
                "status": "no_change",
                "analysis_id": analysis_id,
                "note": "No empty sheets found matching criteria.",
            }
        return {
            "status": "success",
            "analysis_id": analysis_id,
            **result,
            "note": f"Deleted {len(result['deleted_sheets'])} empty sheets.",
        }
