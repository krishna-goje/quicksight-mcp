"""Visual management MCP tools for QuickSight.

Provides tools for inspecting, adding, deleting visuals and
managing their layout and titles within analyses.
"""

import json
import time
import logging
from typing import Callable

from fastmcp import FastMCP

logger = logging.getLogger(__name__)


def register_visual_tools(mcp: FastMCP, get_client: Callable, get_tracker: Callable):
    """Register all visual-related MCP tools."""

    @mcp.tool
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
        start = time.time()
        client = get_client()
        try:
            visual_def = client.get_visual_definition(analysis_id, visual_id)
            get_tracker().record_call(
                "get_visual_definition",
                {"analysis_id": analysis_id, "visual_id": visual_id},
                (time.time() - start) * 1000,
                True,
            )
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
        except Exception as e:
            get_tracker().record_call(
                "get_visual_definition",
                {"analysis_id": analysis_id, "visual_id": visual_id},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
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
        start = time.time()
        client = get_client()
        try:
            try:
                parsed_def = json.loads(visual_definition) if isinstance(visual_definition, str) else visual_definition
            except json.JSONDecodeError as je:
                return {"error": f"Invalid JSON in visual_definition: {je}"}
            result = client.add_visual_to_sheet(
                analysis_id, sheet_id, parsed_def
            )
            get_tracker().record_call(
                "add_visual",
                {"analysis_id": analysis_id, "sheet_id": sheet_id},
                (time.time() - start) * 1000,
                True,
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
        except Exception as e:
            get_tracker().record_call(
                "add_visual",
                {"analysis_id": analysis_id, "sheet_id": sheet_id},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def delete_visual(analysis_id: str, visual_id: str) -> dict:
        """Delete a visual from a QuickSight analysis.

        WARNING: This is destructive. The visual and its layout element
        will be removed. A backup is automatically created before deletion.

        Args:
            analysis_id: The QuickSight analysis ID.
            visual_id: The ID of the visual to delete.
        """
        start = time.time()
        client = get_client()
        try:
            result = client.delete_visual(analysis_id, visual_id)
            get_tracker().record_call(
                "delete_visual",
                {"analysis_id": analysis_id, "visual_id": visual_id},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "status": "success",
                "analysis_id": analysis_id,
                "deleted_visual_id": visual_id,
                "note": "Visual deleted. Use backup_analysis to restore if needed.",
            }
        except Exception as e:
            get_tracker().record_call(
                "delete_visual",
                {"analysis_id": analysis_id, "visual_id": visual_id},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def set_visual_title(analysis_id: str, visual_id: str, title: str) -> dict:
        """Set or update the title of a visual.

        WARNING: This modifies the analysis definition. A backup is
        automatically created before making changes.

        Args:
            analysis_id: The QuickSight analysis ID.
            visual_id: The visual ID to update.
            title: The new display title for the visual.
        """
        start = time.time()
        client = get_client()
        try:
            result = client.set_visual_title(analysis_id, visual_id, title)
            get_tracker().record_call(
                "set_visual_title",
                {"analysis_id": analysis_id, "visual_id": visual_id, "title": title},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "status": "success",
                "analysis_id": analysis_id,
                "visual_id": visual_id,
                "title": title,
            }
        except Exception as e:
            get_tracker().record_call(
                "set_visual_title",
                {"analysis_id": analysis_id, "visual_id": visual_id, "title": title},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
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
        start = time.time()
        client = get_client()
        try:
            result = client.set_visual_layout(
                analysis_id, visual_id,
                column_index=column_index,
                column_span=column_span,
                row_index=row_index,
                row_span=row_span,
            )
            get_tracker().record_call(
                "set_visual_layout",
                {
                    "analysis_id": analysis_id,
                    "visual_id": visual_id,
                    "column_index": column_index,
                    "column_span": column_span,
                    "row_index": row_index,
                    "row_span": row_span,
                },
                (time.time() - start) * 1000,
                True,
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
        except Exception as e:
            get_tracker().record_call(
                "set_visual_layout",
                {"analysis_id": analysis_id, "visual_id": visual_id},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}
