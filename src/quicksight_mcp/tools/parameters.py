"""Parameter management MCP tools for QuickSight.

Provides tools for adding and deleting parameter declarations
within QuickSight analyses. Parameters drive dynamic filters
and controls in dashboards.
"""

import json
import logging
from typing import Callable

from fastmcp import FastMCP

from quicksight_mcp.tools._decorator import qs_tool

logger = logging.getLogger(__name__)


def register_parameter_tools(
    mcp: FastMCP, get_client: Callable, get_tracker: Callable, get_memory=None
):
    """Register all parameter-related MCP tools."""

    @qs_tool(mcp, get_memory, idempotent=True)
    def add_parameter(analysis_id: str, parameter_definition: str) -> dict:
        """Add a parameter to a QuickSight analysis.

        WARNING: This modifies the analysis definition. A backup is
        automatically created before making changes.

        Args:
            analysis_id: The QuickSight analysis ID.
            parameter_definition: JSON string of the parameter declaration.
                Must contain one of: StringParameterDeclaration,
                IntegerParameterDeclaration, DecimalParameterDeclaration,
                or DateTimeParameterDeclaration.

                Example for a string parameter:
                {"StringParameterDeclaration": {
                    "ParameterValueType": "SINGLE_VALUED",
                    "Name": "market",
                    "DefaultValues": {"StaticValues": ["All"]},
                    "ValueWhenUnset": {"ValueWhenUnsetOption": "RECOMMENDED_VALUE"}
                }}

        Returns confirmation with the parameter name.
        """
        client = get_client()
        parsed_def = json.loads(parameter_definition) if isinstance(parameter_definition, str) else parameter_definition
        result = client.add_parameter(analysis_id, parsed_def)
        return {
            "status": "success",
            "analysis_id": analysis_id,
            "parameter_name": result.get("parameter_name"),
            "note": (
                "Parameter added. You can now reference it with "
                "${paramName} in calculated fields and filters."
            ),
        }

    @qs_tool(mcp, get_memory, destructive=True)
    def delete_parameter(analysis_id: str, parameter_name: str) -> dict:
        """Delete a parameter from a QuickSight analysis.

        WARNING: This is destructive. If the parameter is used by filters,
        calculated fields, or controls, those references will break.
        Check get_parameters first to understand dependencies.

        A backup is automatically created before deletion.

        Args:
            analysis_id: The QuickSight analysis ID.
            parameter_name: Exact name of the parameter to delete.
        """
        client = get_client()
        client.delete_parameter(analysis_id, parameter_name)
        return {
            "status": "success",
            "analysis_id": analysis_id,
            "deleted_parameter": parameter_name,
            "note": (
                "Parameter deleted. Check that no filters or calc fields "
                "were broken. Use backup_analysis to restore if needed."
            ),
        }
