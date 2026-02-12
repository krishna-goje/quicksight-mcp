"""Sheet management MCP tools for QuickSight.

Provides tools for adding, deleting, renaming sheets and listing
visuals within a specific sheet.
"""

import time
import logging
from typing import Callable

from fastmcp import FastMCP

logger = logging.getLogger(__name__)


def register_sheet_tools(mcp: FastMCP, get_client: Callable, get_tracker: Callable):
    """Register all sheet-related MCP tools."""

    @mcp.tool
    def add_sheet(analysis_id: str, name: str) -> dict:
        """Add a new sheet to a QuickSight analysis.

        WARNING: This modifies the analysis definition. A backup is
        automatically created before making changes.

        Args:
            analysis_id: The QuickSight analysis ID.
            name: Display name for the new sheet.

        Returns the new sheet ID and confirmation.
        """
        start = time.time()
        client = get_client()
        try:
            result = client.add_sheet(analysis_id, name)
            get_tracker().record_call(
                "add_sheet",
                {"analysis_id": analysis_id, "name": name},
                (time.time() - start) * 1000,
                True,
            )
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
        except Exception as e:
            get_tracker().record_call(
                "add_sheet",
                {"analysis_id": analysis_id, "name": name},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def delete_sheet(analysis_id: str, sheet_id: str) -> dict:
        """Delete a sheet from a QuickSight analysis.

        WARNING: This is destructive. All visuals on the sheet will be
        removed. A backup is automatically created before deletion.

        Args:
            analysis_id: The QuickSight analysis ID.
            sheet_id: The ID of the sheet to delete.
        """
        start = time.time()
        client = get_client()
        try:
            result = client.delete_sheet(analysis_id, sheet_id)
            get_tracker().record_call(
                "delete_sheet",
                {"analysis_id": analysis_id, "sheet_id": sheet_id},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "status": "success",
                "analysis_id": analysis_id,
                "deleted_sheet_id": sheet_id,
                "note": "Sheet deleted. Use backup_analysis to restore if needed.",
            }
        except Exception as e:
            get_tracker().record_call(
                "delete_sheet",
                {"analysis_id": analysis_id, "sheet_id": sheet_id},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def rename_sheet(analysis_id: str, sheet_id: str, new_name: str) -> dict:
        """Rename an existing sheet in a QuickSight analysis.

        WARNING: This modifies the analysis definition. A backup is
        automatically created before making changes.

        Args:
            analysis_id: The QuickSight analysis ID.
            sheet_id: The ID of the sheet to rename.
            new_name: The new display name for the sheet.
        """
        start = time.time()
        client = get_client()
        try:
            result = client.rename_sheet(analysis_id, sheet_id, new_name)
            get_tracker().record_call(
                "rename_sheet",
                {"analysis_id": analysis_id, "sheet_id": sheet_id, "new_name": new_name},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "status": "success",
                "analysis_id": analysis_id,
                "sheet_id": sheet_id,
                "new_name": new_name,
            }
        except Exception as e:
            get_tracker().record_call(
                "rename_sheet",
                {"analysis_id": analysis_id, "sheet_id": sheet_id, "new_name": new_name},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def list_sheet_visuals(analysis_id: str, sheet_id: str) -> dict:
        """List all visuals in a specific sheet of a QuickSight analysis.

        Args:
            analysis_id: The QuickSight analysis ID.
            sheet_id: The sheet ID to list visuals for.

        Returns visual IDs, types, and titles for every visual on the sheet.
        """
        start = time.time()
        client = get_client()
        try:
            visuals = client.list_sheet_visuals(analysis_id, sheet_id)
            get_tracker().record_call(
                "list_sheet_visuals",
                {"analysis_id": analysis_id, "sheet_id": sheet_id},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "analysis_id": analysis_id,
                "sheet_id": sheet_id,
                "count": len(visuals),
                "visuals": visuals,
            }
        except Exception as e:
            get_tracker().record_call(
                "list_sheet_visuals",
                {"analysis_id": analysis_id, "sheet_id": sheet_id},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
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
        start = time.time()
        client = get_client()
        try:
            result = client.replicate_sheet(
                analysis_id, source_sheet_id, target_sheet_name
            )
            get_tracker().record_call(
                "replicate_sheet",
                {
                    "analysis_id": analysis_id,
                    "source_sheet_id": source_sheet_id,
                    "target_sheet_name": target_sheet_name,
                },
                (time.time() - start) * 1000,
                True,
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
        except Exception as e:
            get_tracker().record_call(
                "replicate_sheet",
                {
                    "analysis_id": analysis_id,
                    "source_sheet_id": source_sheet_id,
                },
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}
