"""Dashboard operations: list, search, publish, rollback, etc.

Extracted from the monolithic ``QuickSightClient`` into a focused service
that depends only on ``AwsClient`` (for API calls) and ``TTLCache`` (for
list caching).
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional

from quicksight_mcp.core.aws_client import AwsClient
from quicksight_mcp.core.cache import TTLCache

logger = logging.getLogger(__name__)


class DashboardService:
    """Service for QuickSight dashboard operations.

    Args:
        aws: Low-level AWS client with auto-retry and credential refresh.
        cache: TTL cache instance (shared or dedicated).
    """

    def __init__(self, aws: AwsClient, cache: TTLCache) -> None:
        self._aws = aws
        self._cache = cache

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def list_all(self, use_cache: bool = True) -> List[Dict]:
        """List all dashboards with TTL-based caching.

        Args:
            use_cache: Use the cache (default ``True``).

        Returns:
            List of dashboard summary dicts.
        """
        if use_cache:
            cached = self._cache.get("dashboards")
            if cached is not None:
                return cached

        dashboards = self._aws.paginate("list_dashboards", "DashboardSummaryList")

        self._cache.set("dashboards", dashboards)
        logger.debug("Dashboard cache refreshed (%d dashboards)", len(dashboards))
        return dashboards

    def search(self, name_contains: str) -> List[Dict]:
        """Search dashboards by name (client-side filter on cached list).

        Args:
            name_contains: Substring to search for in dashboard names.
        """
        all_dashboards = self.list_all()
        needle = name_contains.lower()
        return [d for d in all_dashboards if needle in d.get("Name", "").lower()]

    def get(self, dashboard_id: str) -> Dict:
        """Get dashboard details (describe_dashboard).

        Args:
            dashboard_id: QuickSight dashboard ID.

        Returns:
            Dashboard dict.
        """
        self._aws.ensure_account_id()
        response = self._aws.call(
            "describe_dashboard",
            AwsAccountId=self._aws.account_id,
            DashboardId=dashboard_id,
        )
        return response.get("Dashboard", {})

    def get_definition(self, dashboard_id: str) -> Dict:
        """Get full dashboard definition (describe_dashboard_definition).

        Args:
            dashboard_id: QuickSight dashboard ID.

        Returns:
            Dashboard definition dict.
        """
        self._aws.ensure_account_id()
        response = self._aws.call(
            "describe_dashboard_definition",
            AwsAccountId=self._aws.account_id,
            DashboardId=dashboard_id,
        )
        return response.get("Definition", {})

    def get_versions(self, dashboard_id: str, limit: int = 10) -> List[Dict]:
        """Get dashboard version history, newest first.

        Args:
            dashboard_id: QuickSight dashboard ID.
            limit: Maximum number of versions to return (default 10).
        """
        self._aws.ensure_account_id()
        response = self._aws.call(
            "list_dashboard_versions",
            AwsAccountId=self._aws.account_id,
            DashboardId=dashboard_id,
        )
        versions = response.get("DashboardVersionSummaryList", [])
        versions.sort(key=lambda x: x.get("VersionNumber", 0), reverse=True)
        return versions[:limit]

    def get_current_version(self, dashboard_id: str) -> Dict:
        """Get the currently published dashboard version metadata.

        Args:
            dashboard_id: QuickSight dashboard ID.

        Returns:
            dict with ``version_number``, ``status``, ``created_time``,
            ``description``.
        """
        dashboard = self.get(dashboard_id)
        version = dashboard.get("Version", {})
        return {
            "version_number": version.get("VersionNumber"),
            "status": version.get("Status"),
            "created_time": version.get("CreatedTime"),
            "description": version.get("Description"),
        }

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def publish(
        self,
        dashboard_id: str,
        source_analysis_arn: str,
        dashboard_name: str,
        dataset_references: List[Dict],
        version_description: Optional[str] = None,
    ) -> Dict:
        """Publish/update a dashboard from an analysis.

        Creates a new dashboard version from the source analysis definition
        and immediately publishes it (makes it live for viewers).

        Args:
            dashboard_id: Target dashboard ID.
            source_analysis_arn: ARN of the source analysis.
            dashboard_name: Current dashboard name (used in the update call).
            dataset_references: List of ``{'DataSetPlaceholder': ...,
                'DataSetArn': ...}`` dicts describing dataset bindings.
                Obtain these from the analysis definition's
                ``DataSetIdentifierDeclarations``.
            version_description: Optional description for this version.

        Returns:
            dict with ``dashboard_id``, ``version_arn``, ``version_number``,
            ``status``.
        """
        self._aws.ensure_account_id()

        response = self._aws.call(
            "update_dashboard",
            AwsAccountId=self._aws.account_id,
            DashboardId=dashboard_id,
            Name=dashboard_name,
            SourceEntity={
                "SourceAnalysis": {
                    "Arn": source_analysis_arn,
                    "DataSetReferences": dataset_references,
                }
            },
            VersionDescription=(
                version_description or "Published from analysis"
            ),
        )

        # Extract the new version number and publish it
        # update_dashboard creates a DRAFT -- must call
        # update_dashboard_published_version to make it live for viewers
        version_arn = response.get("VersionArn", "")
        new_version = None
        if version_arn:
            # VersionArn format: .../dashboard/<id>/version/<number>
            parts = version_arn.rsplit("/", 1)
            if len(parts) == 2 and parts[-1].isdigit():
                new_version = int(parts[-1])

        if new_version:
            self._aws.call(
                "update_dashboard_published_version",
                AwsAccountId=self._aws.account_id,
                DashboardId=dashboard_id,
                VersionNumber=new_version,
            )
            logger.info(
                "Dashboard %s published version %d", dashboard_id, new_version
            )

        return {
            "dashboard_id": dashboard_id,
            "version_arn": version_arn,
            "version_number": new_version,
            "status": response.get("CreationStatus"),
        }

    def rollback(self, dashboard_id: str, version_number: int) -> Dict:
        """Rollback dashboard to a previous version.

        Args:
            dashboard_id: Dashboard ID.
            version_number: Version number to publish.

        Returns:
            dict with ``dashboard_id`` and ``status``.
        """
        self._aws.ensure_account_id()
        response = self._aws.call(
            "update_dashboard_published_version",
            AwsAccountId=self._aws.account_id,
            DashboardId=dashboard_id,
            VersionNumber=version_number,
        )
        return {
            "dashboard_id": response.get("DashboardId"),
            "status": f"Published version updated to {version_number}",
        }

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Clear the dashboard list cache."""
        self._cache.invalidate("dashboards")
