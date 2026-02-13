"""Calculated field MCP tools for QuickSight.

Provides tools for creating, reading, updating, and deleting calculated
fields within QuickSight analyses.
"""

import time
import logging
from typing import Callable

from fastmcp import FastMCP

logger = logging.getLogger(__name__)


def register_calculated_field_tools(
    mcp: FastMCP, get_client: Callable, get_tracker: Callable
):
    """Register all calculated-field-related MCP tools."""

    @mcp.tool
    def get_calculated_field(analysis_id: str, name: str) -> dict:
        """Get details of a specific calculated field in an analysis.

        Args:
            analysis_id: The QuickSight analysis ID.
            name: Exact name of the calculated field.

        Returns the field's expression, dataset identifier, and name,
        or indicates that the field was not found.
        """
        start = time.time()
        client = get_client()
        try:
            field = client.get_calculated_field(analysis_id, name)
            get_tracker().record_call(
                "get_calculated_field",
                {"analysis_id": analysis_id, "name": name},
                (time.time() - start) * 1000,
                True,
            )
            if field is None:
                return {
                    "analysis_id": analysis_id,
                    "name": name,
                    "found": False,
                    "note": (
                        f"No calculated field named '{name}' found. "
                        "Use list_calculated_fields to see available fields."
                    ),
                }
            return {
                "analysis_id": analysis_id,
                "found": True,
                **field,
            }
        except Exception as e:
            get_tracker().record_call(
                "get_calculated_field",
                {"analysis_id": analysis_id, "name": name},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def add_calculated_field(
        analysis_id: str,
        name: str,
        expression: str,
        dataset_identifier: str,
    ) -> dict:
        """Add a new calculated field to a QuickSight analysis.

        WARNING: This modifies the analysis definition. A backup is
        automatically created before making changes.

        Args:
            analysis_id: The QuickSight analysis ID.
            name: Name for the new calculated field. Must be unique
                  within the analysis.
            expression: QuickSight expression using curly-brace field
                        references. Examples:
                        - sum({Revenue})
                        - {Price} * {Quantity}
                        - ifelse({Status} = 'Active', 1, 0)
                        - dateDiff({Start}, {End}, "DAY")
            dataset_identifier: The dataset identifier this field belongs
                                to. Find available identifiers using
                                describe_analysis (look at dataset_identifiers).

        Returns confirmation with the created field details.
        """
        start = time.time()
        client = get_client()
        try:
            client.add_calculated_field(
                analysis_id, name, expression, dataset_identifier
            )
            get_tracker().record_call(
                "add_calculated_field",
                {
                    "analysis_id": analysis_id,
                    "name": name,
                    "dataset_identifier": dataset_identifier,
                },
                (time.time() - start) * 1000,
                True,
            )
            return {
                "status": "success",
                "analysis_id": analysis_id,
                "field_name": name,
                "expression": expression,
                "dataset_identifier": dataset_identifier,
                "note": (
                    "Field added. You can now use it in visuals. "
                    "If publishing to a dashboard, call publish_dashboard."
                ),
            }
        except Exception as e:
            get_tracker().record_call(
                "add_calculated_field",
                {
                    "analysis_id": analysis_id,
                    "name": name,
                    "dataset_identifier": dataset_identifier,
                },
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def update_calculated_field(
        analysis_id: str, name: str, new_expression: str
    ) -> dict:
        """Update the expression of an existing calculated field.

        WARNING: This modifies the analysis definition. A backup is
        automatically created before making changes. Visuals using this
        field will reflect the new expression immediately.

        Args:
            analysis_id: The QuickSight analysis ID.
            name: Exact name of the calculated field to update.
            new_expression: The new QuickSight expression. Uses the same
                            syntax as add_calculated_field.

        Returns confirmation with the updated expression.
        """
        start = time.time()
        client = get_client()
        try:
            client.update_calculated_field(
                analysis_id, name, new_expression
            )
            get_tracker().record_call(
                "update_calculated_field",
                {"analysis_id": analysis_id, "name": name},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "status": "success",
                "analysis_id": analysis_id,
                "field_name": name,
                "new_expression": new_expression,
                "note": (
                    "Expression updated. All visuals using this field "
                    "will reflect the change. Publish dashboard to "
                    "propagate to viewers."
                ),
            }
        except Exception as e:
            get_tracker().record_call(
                "update_calculated_field",
                {"analysis_id": analysis_id, "name": name},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def delete_calculated_field(analysis_id: str, name: str) -> dict:
        """Delete a calculated field from a QuickSight analysis.

        WARNING: This is destructive. If the field is used by any visuals
        or other calculated fields, those references will break. Check
        get_columns_used first to understand the impact.

        A backup is automatically created before deletion.

        Args:
            analysis_id: The QuickSight analysis ID.
            name: Exact name of the calculated field to delete.
        """
        start = time.time()
        client = get_client()
        try:
            client.delete_calculated_field(analysis_id, name)
            get_tracker().record_call(
                "delete_calculated_field",
                {"analysis_id": analysis_id, "name": name},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "status": "success",
                "analysis_id": analysis_id,
                "deleted_field": name,
                "note": (
                    "Field deleted. Check that no visuals were broken. "
                    "Use backup_analysis to restore if needed."
                ),
            }
        except Exception as e:
            get_tracker().record_call(
                "delete_calculated_field",
                {"analysis_id": analysis_id, "name": name},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}
