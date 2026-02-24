"""Dashboard MCP tools for QuickSight.

Provides tools for listing, searching, versioning, publishing, and
rolling back QuickSight dashboards.
"""

import logging
from typing import Callable

from fastmcp import FastMCP

from quicksight_mcp.tools._decorator import qs_tool

logger = logging.getLogger(__name__)


def register_dashboard_tools(
    mcp: FastMCP, get_client: Callable, get_tracker: Callable, get_memory=None
):
    """Register all dashboard-related MCP tools."""

    @qs_tool(mcp, get_memory, read_only=True)
    def list_dashboards() -> dict:
        """List all QuickSight dashboards with their names, IDs, and publish status.

        Returns every dashboard in the account. Results are cached for
        5 minutes. Dashboards are the published, viewer-facing version
        of analyses.

        Each entry includes:
        - name: Dashboard display name
        - id: Dashboard ID (use this for other dashboard operations)
        - published_version: Current published version number
        """
        client = get_client()
        dashboards = client.list_dashboards()
        return {
            "count": len(dashboards),
            "dashboards": [
                {
                    "name": d.get("Name"),
                    "id": d.get("DashboardId"),
                    "published_version": d.get("Version", {}).get(
                        "VersionNumber"
                    )
                    if isinstance(d.get("Version"), dict)
                    else None,
                }
                for d in dashboards
            ],
        }

    @qs_tool(mcp, get_memory, read_only=True)
    def search_dashboards(name: str) -> dict:
        """Search QuickSight dashboards by name (case-insensitive partial match).

        Args:
            name: Search string to match against dashboard names.
                  Example: "Sales" matches "T&O Sales", "Sales KPIs", etc.

        Returns matching dashboards with their IDs.
        """
        client = get_client()
        results = client.search_dashboards(name)
        return {
            "query": name,
            "count": len(results),
            "dashboards": [
                {
                    "name": d.get("Name"),
                    "id": d.get("DashboardId"),
                }
                for d in results
            ],
        }

    @qs_tool(mcp, get_memory, read_only=True)
    def get_dashboard_versions(dashboard_id: str, limit: int = 10) -> dict:
        """List the version history of a QuickSight dashboard.

        Args:
            dashboard_id: The QuickSight dashboard ID.
            limit: Maximum number of versions to return (default 10).

        Returns version entries with number, description, creation time,
        and source analysis ARN. Useful for auditing changes and finding
        a version number to rollback to.
        """
        client = get_client()
        versions = client.get_dashboard_versions(dashboard_id, limit=limit)
        current = client.get_current_dashboard_version(dashboard_id)
        return {
            "dashboard_id": dashboard_id,
            "current_version": current.get("VersionNumber"),
            "version_count": len(versions),
            "versions": versions,
        }

    @qs_tool(mcp, get_memory, destructive=True)
    def publish_dashboard(
        dashboard_id: str,
        source_analysis_id: str,
        version_description: str = "",
    ) -> dict:
        """Publish a QuickSight analysis to an existing dashboard.

        WARNING: This is a DESTRUCTIVE operation that replaces the current
        dashboard content with the analysis content. All viewers will
        immediately see the new version. Make sure you have tested the
        analysis thoroughly before publishing.

        Best practice:
        1. Clone the analysis first (clone_analysis) and test
        2. Back up the dashboard (backup_analysis on the source)
        3. Publish with a descriptive version_description
        4. If something goes wrong, use rollback_dashboard

        Args:
            dashboard_id: The target dashboard ID to publish to.
            source_analysis_id: The analysis ID to publish from.
            version_description: Optional description for this version
                                 (e.g., "Added revenue breakdown chart").
        """
        client = get_client()
        client.publish_dashboard(
            dashboard_id,
            source_analysis_id,
            version_description=version_description or None,
        )
        return {
            "status": "published",
            "dashboard_id": dashboard_id,
            "source_analysis_id": source_analysis_id,
            "version_description": version_description,
            "note": (
                "Dashboard updated. All viewers will see the new version. "
                "Use rollback_dashboard if you need to revert."
            ),
        }

    @qs_tool(mcp, get_memory, destructive=True)
    def rollback_dashboard(dashboard_id: str, version_number: int) -> dict:
        """Rollback a QuickSight dashboard to a previous version.

        WARNING: This is a DESTRUCTIVE operation. The current dashboard
        content will be replaced with the specified previous version.
        All viewers will immediately see the rolled-back version.

        Use get_dashboard_versions first to find the version number
        you want to restore.

        Args:
            dashboard_id: The QuickSight dashboard ID.
            version_number: The version number to rollback to.
                            Use get_dashboard_versions to find valid numbers.
        """
        client = get_client()
        client.rollback_dashboard(dashboard_id, version_number)
        return {
            "status": "rolled_back",
            "dashboard_id": dashboard_id,
            "restored_version": version_number,
            "note": (
                "Dashboard rolled back successfully. "
                "All viewers now see the restored version."
            ),
        }
