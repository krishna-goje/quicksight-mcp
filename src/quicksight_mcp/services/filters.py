"""Filter group management service for QuickSight analyses.

Handles adding and deleting filter groups.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, Optional

from quicksight_mcp.core.cache import TTLCache
from quicksight_mcp.safety.exceptions import ChangeVerificationError

if TYPE_CHECKING:
    from quicksight_mcp.core.aws_client import AwsClient
    from quicksight_mcp.services.analyses import AnalysisService

logger = logging.getLogger(__name__)


class FilterService:
    """Manage filter groups within QuickSight analyses.

    Args:
        aws: Low-level AWS client.
        cache: TTL cache instance.
        analyses: Reference to the AnalysisService for definition access and updates.
    """

    def __init__(
        self,
        aws: AwsClient,
        cache: TTLCache,
        analyses: AnalysisService,
    ) -> None:
        self._aws = aws
        self._cache = cache
        self._analyses = analyses

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def add(
        self,
        analysis_id: str,
        filter_group_definition: Dict,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Add a filter group to an analysis.

        Args:
            analysis_id: Analysis ID.
            filter_group_definition: Full filter group dict with
                FilterGroupId, Filters, ScopeConfiguration, etc.
            backup_first: Back up before writing.
            use_optimistic_locking: Override default optimistic locking.
            verify: Override default post-write verification.

        Returns:
            dict with update status and ``filter_group_id``.

        Raises:
            ValueError: If a filter group with the same ID already exists.
        """
        definition, last_updated = self._analyses.get_definition_with_version(
            analysis_id
        )
        filter_groups = definition.setdefault("FilterGroups", [])

        new_id = filter_group_definition.get("FilterGroupId")
        if new_id and any(
            fg.get("FilterGroupId") == new_id for fg in filter_groups
        ):
            raise ValueError(f"Filter group '{new_id}' already exists")

        filter_groups.append(filter_group_definition)

        result = self._analyses.update_analysis(
            analysis_id,
            definition,
            backup_first=backup_first,
            expected_last_updated=(
                last_updated
                if self._analyses.should_lock(use_optimistic_locking)
                else None
            ),
        )

        if new_id and self._analyses.should_verify(verify):
            self._verify_filter_group_exists(analysis_id, new_id)

        result["filter_group_id"] = new_id
        return result

    def delete(
        self,
        analysis_id: str,
        filter_group_id: str,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Delete a filter group by ID.

        Raises:
            ValueError: If the filter group is not found.
        """
        definition, last_updated = self._analyses.get_definition_with_version(
            analysis_id
        )
        fgs = definition.get("FilterGroups", [])
        original_count = len(fgs)

        definition["FilterGroups"] = [
            fg
            for fg in fgs
            if fg.get("FilterGroupId") != filter_group_id
        ]
        if len(definition["FilterGroups"]) == original_count:
            raise ValueError(
                f"Filter group '{filter_group_id}' not found"
            )

        result = self._analyses.update_analysis(
            analysis_id,
            definition,
            backup_first=backup_first,
            expected_last_updated=(
                last_updated
                if self._analyses.should_lock(use_optimistic_locking)
                else None
            ),
        )

        if self._analyses.should_verify(verify):
            self._verify_filter_group_deleted(analysis_id, filter_group_id)

        return result

    # ------------------------------------------------------------------
    # Verification helpers
    # ------------------------------------------------------------------

    def _verify_filter_group_exists(
        self, analysis_id: str, filter_group_id: str
    ) -> bool:
        """Verify a filter group exists after creation."""
        self._analyses.clear_definition_cache(analysis_id)
        definition = self._analyses.get_definition(analysis_id)
        for fg in definition.get("FilterGroups", []):
            if fg.get("FilterGroupId") == filter_group_id:
                return True
        raise ChangeVerificationError(
            "add_filter_group",
            analysis_id,
            f"Filter group '{filter_group_id}' not found after update.",
        )

    def _verify_filter_group_deleted(
        self, analysis_id: str, filter_group_id: str
    ) -> bool:
        """Verify a filter group was actually deleted."""
        self._analyses.clear_definition_cache(analysis_id)
        definition = self._analyses.get_definition(analysis_id)
        for fg in definition.get("FilterGroups", []):
            if fg.get("FilterGroupId") == filter_group_id:
                raise ChangeVerificationError(
                    "delete_filter_group",
                    analysis_id,
                    f"Filter group '{filter_group_id}' still exists "
                    f"after deletion.",
                )
        return True
