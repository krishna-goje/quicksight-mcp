"""Parameter management service for QuickSight analyses.

Handles adding and deleting parameter declarations.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, Optional

from quicksight_mcp.core.cache import TTLCache
from quicksight_mcp.core.types import PARAMETER_TYPES
from quicksight_mcp.safety.exceptions import ChangeVerificationError

if TYPE_CHECKING:
    from quicksight_mcp.core.aws_client import AwsClient
    from quicksight_mcp.services.analyses import AnalysisService

logger = logging.getLogger(__name__)


class ParameterService:
    """Manage parameter declarations within QuickSight analyses.

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
        parameter_definition: Dict,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Add a parameter to an analysis.

        Args:
            analysis_id: Analysis ID.
            parameter_definition: Full parameter declaration dict
                (e.g., ``{"StringParameterDeclaration": {"Name": ..., ...}}``).
            backup_first: Back up before writing.
            use_optimistic_locking: Override default optimistic locking.
            verify: Override default post-write verification.

        Returns:
            dict with update status and ``parameter_name``.

        Raises:
            ValueError: If a parameter with the same name already exists.
        """
        definition, last_updated = self._analyses.get_definition_with_version(
            analysis_id
        )
        params = definition.setdefault("ParameterDeclarations", [])

        # Extract name from any parameter type
        new_name = self._extract_parameter_name(parameter_definition)

        if new_name:
            for p in params:
                existing_name = self._extract_parameter_name(p)
                if existing_name == new_name:
                    raise ValueError(
                        f"Parameter '{new_name}' already exists"
                    )

        params.append(parameter_definition)

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

        if new_name and self._analyses.should_verify(verify):
            self._verify_parameter_exists(analysis_id, new_name)

        result["parameter_name"] = new_name
        return result

    def delete(
        self,
        analysis_id: str,
        parameter_name: str,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Delete a parameter by name.

        Raises:
            ValueError: If the parameter is not found.
        """
        definition, last_updated = self._analyses.get_definition_with_version(
            analysis_id
        )
        params = definition.get("ParameterDeclarations", [])
        original_count = len(params)

        def _matches(p: Dict) -> bool:
            for ptype in PARAMETER_TYPES:
                if ptype in p and p[ptype].get("Name") == parameter_name:
                    return True
            return False

        definition["ParameterDeclarations"] = [
            p for p in params if not _matches(p)
        ]
        if len(definition["ParameterDeclarations"]) == original_count:
            raise ValueError(f"Parameter '{parameter_name}' not found")

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
            self._verify_parameter_deleted(analysis_id, parameter_name)

        return result

    # ------------------------------------------------------------------
    # Verification helpers
    # ------------------------------------------------------------------

    def _verify_parameter_exists(
        self, analysis_id: str, param_name: str
    ) -> bool:
        """Verify a parameter exists after creation."""
        self._analyses.clear_definition_cache(analysis_id)
        definition = self._analyses.get_definition(analysis_id)
        for p in definition.get("ParameterDeclarations", []):
            for ptype in PARAMETER_TYPES:
                if ptype in p and p[ptype].get("Name") == param_name:
                    return True
        raise ChangeVerificationError(
            "add_parameter",
            analysis_id,
            f"Parameter '{param_name}' not found after update.",
        )

    def _verify_parameter_deleted(
        self, analysis_id: str, param_name: str
    ) -> bool:
        """Verify a parameter was actually deleted."""
        self._analyses.clear_definition_cache(analysis_id)
        definition = self._analyses.get_definition(analysis_id)
        for p in definition.get("ParameterDeclarations", []):
            for ptype in PARAMETER_TYPES:
                if ptype in p and p[ptype].get("Name") == param_name:
                    raise ChangeVerificationError(
                        "delete_parameter",
                        analysis_id,
                        f"Parameter '{param_name}' still exists after deletion.",
                    )
        return True

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_parameter_name(parameter_definition: Dict) -> Optional[str]:
        """Extract the Name from a parameter declaration of any type."""
        for ptype in PARAMETER_TYPES:
            if ptype in parameter_definition:
                return parameter_definition[ptype].get("Name")
        return None
