"""Dashboard MCP tools for QuickSight.

Provides tools for listing, searching, versioning, publishing, and
rolling back QuickSight dashboards.
"""

import time
import logging
from typing import Callable

from fastmcp import FastMCP

logger = logging.getLogger(__name__)


def register_dashboard_tools(
    mcp: FastMCP, get_client: Callable, get_tracker: Callable
):
    """Register all dashboard-related MCP tools."""

    @mcp.tool
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
        start = time.time()
        client = get_client()
        try:
            dashboards = client.list_dashboards()
            result = {
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
            get_tracker().record_call(
                "list_dashboards", {}, (time.time() - start) * 1000, True
            )
            return result
        except Exception as e:
            get_tracker().record_call(
                "list_dashboards", {}, (time.time() - start) * 1000, False, str(e)
            )
            return {"error": str(e)}

    @mcp.tool
    def search_dashboards(name: str) -> dict:
        """Search QuickSight dashboards by name (case-insensitive partial match).

        Args:
            name: Search string to match against dashboard names.
                  Example: "Sales" matches "T&O Sales", "Sales KPIs", etc.

        Returns matching dashboards with their IDs.
        """
        start = time.time()
        client = get_client()
        try:
            results = client.search_dashboards(name)
            result = {
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
            get_tracker().record_call(
                "search_dashboards",
                {"name": name},
                (time.time() - start) * 1000,
                True,
            )
            return result
        except Exception as e:
            get_tracker().record_call(
                "search_dashboards",
                {"name": name},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def get_dashboard_versions(dashboard_id: str, limit: int = 10) -> dict:
        """List the version history of a QuickSight dashboard.

        Args:
            dashboard_id: The QuickSight dashboard ID.
            limit: Maximum number of versions to return (default 10).

        Returns version entries with number, description, creation time,
        and source analysis ARN. Useful for auditing changes and finding
        a version number to rollback to.
        """
        start = time.time()
        client = get_client()
        try:
            versions = client.get_dashboard_versions(dashboard_id, limit=limit)
            current = client.get_current_dashboard_version(dashboard_id)
            get_tracker().record_call(
                "get_dashboard_versions",
                {"dashboard_id": dashboard_id, "limit": limit},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "dashboard_id": dashboard_id,
                "current_version": current.get("VersionNumber"),
                "version_count": len(versions),
                "versions": versions,
            }
        except Exception as e:
            get_tracker().record_call(
                "get_dashboard_versions",
                {"dashboard_id": dashboard_id, "limit": limit},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
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
        start = time.time()
        client = get_client()
        try:
            result = client.publish_dashboard(
                dashboard_id,
                source_analysis_id,
                version_description=version_description or None,
            )
            get_tracker().record_call(
                "publish_dashboard",
                {
                    "dashboard_id": dashboard_id,
                    "source_analysis_id": source_analysis_id,
                },
                (time.time() - start) * 1000,
                True,
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
        except Exception as e:
            get_tracker().record_call(
                "publish_dashboard",
                {
                    "dashboard_id": dashboard_id,
                    "source_analysis_id": source_analysis_id,
                },
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
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
        start = time.time()
        client = get_client()
        try:
            result = client.rollback_dashboard(dashboard_id, version_number)
            get_tracker().record_call(
                "rollback_dashboard",
                {
                    "dashboard_id": dashboard_id,
                    "version_number": version_number,
                },
                (time.time() - start) * 1000,
                True,
            )
            return {
                "status": "rolled_back",
                "dashboard_id": dashboard_id,
                "restored_version": version_number,
                "note": (
                    "Dashboard rolled back successfully. "
                    "All viewers now see the restored version."
                ),
            }
        except Exception as e:
            get_tracker().record_call(
                "rollback_dashboard",
                {
                    "dashboard_id": dashboard_id,
                    "version_number": version_number,
                },
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}
