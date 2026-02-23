"""Analysis MCP tools for QuickSight.

Provides tools for listing, searching, and inspecting QuickSight analyses --
including their sheets, visuals, calculated fields, parameters, and filters.
"""

import logging
from typing import Callable

from fastmcp import FastMCP

from quicksight_mcp.tools._decorator import qs_tool

logger = logging.getLogger(__name__)


def register_analysis_tools(mcp: FastMCP, get_client: Callable, get_tracker: Callable, get_memory=None):
    """Register all analysis-related MCP tools."""

    @qs_tool(mcp, get_memory, read_only=True)
    def list_analyses() -> dict:
        """List all QuickSight analyses with their names, IDs, and status.

        Returns every analysis in the account. Results are cached for
        5 minutes. Use this to discover analyses before inspecting them.

        Each entry includes:
        - name: Human-readable analysis name
        - id: Analysis ID (use this for other analysis operations)
        - status: CREATION_SUCCESSFUL, UPDATE_SUCCESSFUL, etc.
        """
        client = get_client()
        analyses = client.list_analyses()
        return {
            "count": len(analyses),
            "analyses": [
                {
                    "name": a.get("Name"),
                    "id": a.get("AnalysisId"),
                    "status": a.get("Status"),
                }
                for a in analyses
            ],
        }

    @qs_tool(mcp, get_memory, read_only=True)
    def search_analyses(name: str) -> dict:
        """Search QuickSight analyses by name (case-insensitive partial match).

        Args:
            name: Search string to match against analysis names.
                  Example: "WBR" matches "Ops-WBR", "WBR Weekly", etc.

        Returns matching analyses with their IDs and statuses.
        """
        client = get_client()
        results = client.search_analyses(name)
        return {
            "query": name,
            "count": len(results),
            "analyses": [
                {
                    "name": a.get("Name"),
                    "id": a.get("AnalysisId"),
                    "status": a.get("Status"),
                }
                for a in results
            ],
        }

    @qs_tool(mcp, get_memory, read_only=True)
    def describe_analysis(analysis_id: str) -> dict:
        """Get a structured summary of a QuickSight analysis.

        Returns an overview of the analysis structure without the full raw
        definition -- ideal for understanding what an analysis contains
        before making changes.

        Args:
            analysis_id: The QuickSight analysis ID.

        Returns:
            - name, status, ARN
            - sheets with their names, IDs, and visual counts
            - total counts of calculated fields, parameters, and filter groups
            - dataset identifiers used by the analysis
        """
        client = get_client()
        definition = client.get_analysis_definition(analysis_id)
        analysis = client.get_analysis(analysis_id)

        # Extract sheets summary
        sheets_raw = definition.get("Sheets", [])
        sheets = []
        for s in sheets_raw:
            visuals = s.get("Visuals", [])
            sheets.append(
                {
                    "name": s.get("Name", ""),
                    "id": s.get("SheetId", ""),
                    "visual_count": len(visuals),
                }
            )

        calc_fields = definition.get("CalculatedFields", [])
        params = definition.get("ParameterDeclarations", [])
        filter_groups = definition.get("FilterGroups", [])
        ds_id_decls = definition.get("DataSetIdentifierDeclarations", [])

        return {
            "analysis_id": analysis_id,
            "name": analysis.get("Name", ""),
            "status": analysis.get("Status", ""),
            "sheets_count": len(sheets),
            "sheets": sheets,
            "calculated_fields_count": len(calc_fields),
            "parameters_count": len(params),
            "filter_groups_count": len(filter_groups),
            "dataset_identifiers": [
                {
                    "identifier": d.get("Identifier"),
                    "dataset_arn": d.get("DataSetArn"),
                }
                for d in ds_id_decls
            ],
        }

    @qs_tool(mcp, get_memory, read_only=True)
    def list_visuals(analysis_id: str) -> dict:
        """List all visuals in a QuickSight analysis with type and location info.

        Args:
            analysis_id: The QuickSight analysis ID.

        Returns every visual across all sheets, including:
        - visual_id: Unique visual identifier
        - type: Visual type (e.g., TableVisual, BarChartVisual, KPIVisual)
        - title: Visual title (if set)
        - sheet_name: Which sheet the visual is on
        """
        client = get_client()
        visuals = client.get_visuals(analysis_id)
        return {
            "analysis_id": analysis_id,
            "count": len(visuals),
            "visuals": visuals,
        }

    @qs_tool(mcp, get_memory, read_only=True)
    def list_calculated_fields(analysis_id: str) -> dict:
        """List all calculated fields in a QuickSight analysis.

        Args:
            analysis_id: The QuickSight analysis ID.

        Returns each calculated field with:
        - name: The field name
        - expression: The QuickSight expression (e.g., sum({Revenue}))
        - dataset_identifier: Which dataset the field belongs to
        """
        client = get_client()
        fields = client.get_calculated_fields(analysis_id)
        return {
            "analysis_id": analysis_id,
            "count": len(fields),
            "calculated_fields": fields,
        }

    @qs_tool(mcp, get_memory, read_only=True)
    def get_columns_used(analysis_id: str) -> dict:
        """Get a frequency map of columns used across an analysis.

        Args:
            analysis_id: The QuickSight analysis ID.

        Returns a dict mapping column names to the number of times they
        appear in visuals, calculated fields, filters, etc. Useful for
        understanding which columns are most important and for impact
        analysis before modifying a dataset.
        """
        client = get_client()
        usage = client.get_columns_used(analysis_id)
        # Sort by frequency descending
        sorted_usage = dict(
            sorted(usage.items(), key=lambda x: x[1], reverse=True)
        )
        return {
            "analysis_id": analysis_id,
            "unique_columns": len(sorted_usage),
            "columns": sorted_usage,
        }

    @qs_tool(mcp, get_memory, read_only=True)
    def get_parameters(analysis_id: str) -> dict:
        """List all parameters defined in a QuickSight analysis.

        Args:
            analysis_id: The QuickSight analysis ID.

        Returns parameter declarations with names, types, and default values.
        Parameters drive dynamic filters and controls in dashboards.
        """
        client = get_client()
        params = client.get_parameters(analysis_id)
        return {
            "analysis_id": analysis_id,
            "count": len(params),
            "parameters": params,
        }

    @qs_tool(mcp, get_memory, read_only=True)
    def get_analysis_raw(analysis_id: str) -> dict:
        """Get the complete raw analysis definition for inspection.

        Returns the full Definition dict exactly as stored by AWS.
        This is useful for debugging, manual inspection, or extracting
        complex structures (visual definitions, filter groups, etc.)
        that can be passed to other tools.

        WARNING: The output can be very large for complex analyses.

        Args:
            analysis_id: The QuickSight analysis ID.
        """
        client = get_client()
        raw = client.get_analysis_raw(analysis_id)
        return {
            "analysis_id": analysis_id,
            "definition": raw,
        }

    @qs_tool(mcp, get_memory, read_only=True)
    def get_filters(analysis_id: str) -> dict:
        """List all filter groups defined in a QuickSight analysis.

        Args:
            analysis_id: The QuickSight analysis ID.

        Returns filter group definitions including scope (which sheets/visuals
        they apply to) and the filter conditions.
        """
        client = get_client()
        filters = client.get_filters(analysis_id)
        return {
            "analysis_id": analysis_id,
            "count": len(filters),
            "filter_groups": filters,
        }

    @qs_tool(mcp, get_memory, read_only=True)
    def verify_analysis_health(analysis_id: str) -> dict:
        """Run a comprehensive health check on a QuickSight analysis.

        Use this AFTER any write operation to verify the analysis is healthy.
        This is the "reviewer" that ensures changes actually took effect and
        nothing was silently broken.

        Checks performed:
        - Analysis status is SUCCESSFUL (not FAILED or IN_PROGRESS)
        - Sheet count is within QuickSight limits (<=20)
        - All visuals have corresponding layout elements
        - All calculated fields reference valid dataset identifiers

        Args:
            analysis_id: The QuickSight analysis ID to check.

        Returns a health report with pass/fail for each check and a list
        of any issues found.
        """
        client = get_client()
        return client.verify_analysis_health(analysis_id)

    @qs_tool(mcp, get_memory, read_only=True)
    def snapshot_analysis(analysis_id: str) -> dict:
        """Take a snapshot of the current analysis state for QA comparison.

        Use this BEFORE making changes. After changes, use diff_analysis
        to compare and verify exactly what changed.

        Args:
            analysis_id: The QuickSight analysis ID.

        Returns the snapshot with a snapshot_id to use with diff_analysis.
        """
        client = get_client()
        return client.snapshot_analysis(analysis_id)

    @qs_tool(mcp, get_memory, read_only=True)
    def diff_analysis(analysis_id: str, snapshot_id: str) -> dict:
        """Compare current analysis state against a previous snapshot.

        Use AFTER making changes to see what was added, removed, or
        modified. This is the QA reviewer -- ensures changes had the
        intended effect and nothing unexpected broke.

        Args:
            analysis_id: The QuickSight analysis ID.
            snapshot_id: The snapshot_id from a previous snapshot_analysis call.

        Returns a detailed diff: added/removed/changed sheets, visuals,
        and calculated fields.
        """
        client = get_client()
        return client.diff_analysis(analysis_id, snapshot_id)
