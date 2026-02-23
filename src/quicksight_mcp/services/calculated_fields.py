"""Calculated-field service — add, update, delete, and inspect calc fields.

All mutations flow through ``AnalysisService.update_analysis``, ensuring
backup, optimistic locking, destructive guard, and completion polling
are applied uniformly.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from quicksight_mcp.safety.exceptions import ChangeVerificationError

logger = logging.getLogger(__name__)


class CalculatedFieldService:
    """Service for QuickSight calculated-field operations.

    Depends on ``AnalysisService`` for reading definitions and writing
    updates through the central ``update_analysis`` gateway.

    Args:
        analyses: The ``AnalysisService`` instance (provides
            ``get_definition_with_version``, ``update_analysis``,
            ``clear_def_cache``, ``get_definition``).
    """

    def __init__(self, analyses: Any) -> None:
        # Use Any to avoid circular import; runtime type is AnalysisService
        self._analyses = analyses

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def list_all(self, analysis_id: str) -> List[Dict]:
        """Get all calculated fields in an analysis."""
        definition = self._analyses.get_definition(analysis_id)
        return definition.get("CalculatedFields", [])

    def get(self, analysis_id: str, name: str) -> Optional[Dict]:
        """Get a specific calculated field by name, or ``None``."""
        for f in self.list_all(analysis_id):
            if f.get("Name") == name:
                return f
        return None

    # ------------------------------------------------------------------
    # Write (all go through AnalysisService.update_analysis)
    # ------------------------------------------------------------------

    def add(
        self,
        analysis_id: str,
        name: str,
        expression: str,
        data_set_identifier: str,
        *,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Add a calculated field to an analysis.

        Args:
            analysis_id: Target analysis.
            name: Name for the new calculated field.
            expression: QuickSight expression string.
            data_set_identifier: Dataset identifier the field belongs to.
            backup_first: Create a backup before writing.
            use_optimistic_locking: Use optimistic locking (default from settings).
            verify: Verify the field was created (default from settings).

        Raises:
            ValueError: If a field with the same name already exists.
            ChangeVerificationError: If verification is enabled and the field
                was not created.
        """
        definition, last_updated = (
            self._analyses.get_definition_with_version(analysis_id)
        )

        new_field = {
            "DataSetIdentifier": data_set_identifier,
            "Name": name,
            "Expression": expression,
        }

        calc_fields = definition.setdefault("CalculatedFields", [])
        if any(f.get("Name") == name for f in calc_fields):
            raise ValueError(
                f"Calculated field '{name}' already exists. "
                f"Use update instead."
            )

        calc_fields.append(new_field)
        result = self._analyses.update_analysis(
            analysis_id,
            definition,
            backup_first=backup_first,
            expected_last_updated=(
                last_updated
                if self._analyses._should_lock(use_optimistic_locking)
                else None
            ),
        )

        if self._analyses._should_verify(verify):
            self._verify_calculated_field_exists(
                analysis_id, name, expression
            )

        return result

    def update(
        self,
        analysis_id: str,
        name: str,
        new_expression: str,
        *,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Update an existing calculated field's expression.

        Args:
            analysis_id: Target analysis.
            name: Name of the field to update.
            new_expression: New QuickSight expression string.
            backup_first: Create a backup before writing.
            use_optimistic_locking: Use optimistic locking (default from settings).
            verify: Verify the expression was updated (default from settings).

        Raises:
            ValueError: If the field is not found.
            ChangeVerificationError: If verification is enabled and the
                expression was not updated.
        """
        definition, last_updated = (
            self._analyses.get_definition_with_version(analysis_id)
        )

        found = False
        for field in definition.get("CalculatedFields", []):
            if field.get("Name") == name:
                field["Expression"] = new_expression
                found = True
                break

        if not found:
            raise ValueError(f"Calculated field '{name}' not found")

        result = self._analyses.update_analysis(
            analysis_id,
            definition,
            backup_first=backup_first,
            expected_last_updated=(
                last_updated
                if self._analyses._should_lock(use_optimistic_locking)
                else None
            ),
        )

        if self._analyses._should_verify(verify):
            self._verify_calculated_field_exists(
                analysis_id, name, new_expression
            )

        return result

    def delete(
        self,
        analysis_id: str,
        name: str,
        *,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Delete a calculated field from an analysis.

        Args:
            analysis_id: Target analysis.
            name: Name of the field to delete.
            backup_first: Create a backup before writing.
            use_optimistic_locking: Use optimistic locking (default from settings).
            verify: Verify the field was deleted (default from settings).

        Raises:
            ValueError: If the field is not found.
            ChangeVerificationError: If verification is enabled and the field
                still exists after deletion.
        """
        definition, last_updated = (
            self._analyses.get_definition_with_version(analysis_id)
        )

        original_count = len(definition.get("CalculatedFields", []))
        definition["CalculatedFields"] = [
            f
            for f in definition.get("CalculatedFields", [])
            if f.get("Name") != name
        ]

        if len(definition.get("CalculatedFields", [])) == original_count:
            raise ValueError(f"Calculated field '{name}' not found")

        result = self._analyses.update_analysis(
            analysis_id,
            definition,
            backup_first=backup_first,
            expected_last_updated=(
                last_updated
                if self._analyses._should_lock(use_optimistic_locking)
                else None
            ),
        )

        if self._analyses._should_verify(verify):
            self._verify_calculated_field_deleted(analysis_id, name)

        return result

    # ------------------------------------------------------------------
    # Verification helpers
    # ------------------------------------------------------------------

    def _verify_calculated_field_exists(
        self,
        analysis_id: str,
        name: str,
        expected_expression: Optional[str] = None,
    ) -> bool:
        """Verify a calculated field exists (and optionally matches expression).

        Raises:
            ChangeVerificationError: If the field is missing or expression mismatches.
        """
        self._analyses.clear_def_cache(analysis_id)
        for f in self.list_all(analysis_id):
            if f.get("Name") == name:
                if (
                    expected_expression
                    and f.get("Expression") != expected_expression
                ):
                    raise ChangeVerificationError(
                        "add_calculated_field",
                        analysis_id,
                        f"Field '{name}' exists but expression does not match.",
                    )
                return True
        raise ChangeVerificationError(
            "add_calculated_field",
            analysis_id,
            f"Field '{name}' not found after update.",
        )

    def _verify_calculated_field_deleted(
        self, analysis_id: str, name: str
    ) -> bool:
        """Verify a calculated field was successfully deleted.

        Raises:
            ChangeVerificationError: If the field still exists.
        """
        self._analyses.clear_def_cache(analysis_id)
        for f in self.list_all(analysis_id):
            if f.get("Name") == name:
                raise ChangeVerificationError(
                    "delete_calculated_field",
                    analysis_id,
                    f"Field '{name}' still exists after deletion.",
                )
        return True
