"""Sheet management service for QuickSight analyses.

Handles adding, deleting, renaming, replicating, and cleaning up sheets.
"""

from __future__ import annotations

import copy as _copy
import logging
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from quicksight_mcp.core.cache import TTLCache
from quicksight_mcp.core.types import VISUAL_TYPES
from quicksight_mcp.safety.exceptions import ChangeVerificationError

if TYPE_CHECKING:
    from quicksight_mcp.core.aws_client import AwsClient
    from quicksight_mcp.services.analyses import AnalysisService

logger = logging.getLogger(__name__)


class SheetService:
    """Manage sheets within QuickSight analyses.

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
        name: str,
        sheet_id: Optional[str] = None,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Add a new empty sheet to an analysis.

        Args:
            analysis_id: Analysis ID.
            name: Display name for the new sheet.
            sheet_id: Optional sheet ID (auto-generated if omitted).
            backup_first: Back up before writing.
            use_optimistic_locking: Override default optimistic locking.
            verify: Override default post-write verification.

        Returns:
            dict with ``status``, ``analysis_id``, ``sheet_id``, ``sheet_name``.

        Raises:
            ValueError: If a sheet with the given ID already exists.
        """
        definition, last_updated = self._analyses.get_definition_with_version(
            analysis_id
        )
        new_sheet_id = sheet_id or str(uuid.uuid4())

        sheets = definition.setdefault("Sheets", [])
        if any(s.get("SheetId") == new_sheet_id for s in sheets):
            raise ValueError(f"Sheet '{new_sheet_id}' already exists")

        new_sheet: Dict[str, Any] = {
            "SheetId": new_sheet_id,
            "Name": name,
            "ContentType": "INTERACTIVE",
            "Visuals": [],
            "Layouts": [
                {
                    "Configuration": {
                        "GridLayout": {
                            "Elements": [],
                        }
                    }
                }
            ],
        }
        sheets.append(new_sheet)

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
            self._verify_sheet_exists(analysis_id, new_sheet_id, name)

        result["sheet_id"] = new_sheet_id
        result["sheet_name"] = name
        return result

    def delete(
        self,
        analysis_id: str,
        sheet_id: str,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Delete a sheet from an analysis.

        Automatically removes filter groups scoped to the sheet.

        Raises:
            ValueError: If the sheet is not found.
        """
        definition, last_updated = self._analyses.get_definition_with_version(
            analysis_id
        )
        sheets = definition.get("Sheets", [])
        original_count = len(sheets)

        definition["Sheets"] = [
            s for s in sheets if s.get("SheetId") != sheet_id
        ]
        if len(definition["Sheets"]) == original_count:
            raise ValueError(f"Sheet '{sheet_id}' not found")

        fg_removed = self._remove_sheet_filter_scopes(definition, {sheet_id})

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
            self._verify_sheet_deleted(analysis_id, sheet_id)

        if fg_removed:
            logger.info(
                "Removed %d filter groups with no remaining scope after "
                "sheet %s deletion",
                fg_removed,
                sheet_id,
            )
        return result

    def rename(
        self,
        analysis_id: str,
        sheet_id: str,
        new_name: str,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Rename an existing sheet.

        Raises:
            ValueError: If the sheet is not found.
        """
        definition, last_updated = self._analyses.get_definition_with_version(
            analysis_id
        )
        found = False
        for s in definition.get("Sheets", []):
            if s.get("SheetId") == sheet_id:
                s["Name"] = new_name
                found = True
                break

        if not found:
            raise ValueError(f"Sheet '{sheet_id}' not found")

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
            self._verify_sheet_exists(analysis_id, sheet_id, new_name)

        return result

    def replicate(
        self,
        analysis_id: str,
        source_sheet_id: str,
        target_sheet_name: str,
        target_sheet_id: Optional[str] = None,
        id_prefix: str = "rc_",
        backup_first: bool = True,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Copy all visuals from one sheet to a new sheet.

        Performs a batch copy in a single API call. Visual IDs are prefixed
        to avoid conflicts. Layout positions are preserved from the source.

        Args:
            analysis_id: Analysis ID.
            source_sheet_id: Sheet ID to copy visuals from.
            target_sheet_name: Name for the new sheet.
            target_sheet_id: Optional ID for the new sheet (auto-generated).
            id_prefix: Prefix for new visual IDs (default ``'rc_'``).
            backup_first: Back up before writing.
            verify: Override default post-write verification.

        Returns:
            dict with ``analysis_id``, ``sheet_id``, ``sheet_name``,
            ``visual_count``, ``visual_types``.

        Raises:
            ValueError: If the source sheet is not found or sheet limit exceeded.
        """
        definition, last_updated = self._analyses.get_definition_with_version(
            analysis_id
        )

        # Check sheet limit (QuickSight max is 20 sheets per analysis)
        current_sheets = definition.get("Sheets", [])
        if len(current_sheets) >= 20:
            raise ValueError(
                f"Cannot add sheet: analysis already has {len(current_sheets)} "
                f"sheets (QuickSight max is 20). Delete a sheet first."
            )

        # Find source sheet
        source_sheet = None
        for s in current_sheets:
            if s.get("SheetId") == source_sheet_id:
                source_sheet = s
                break
        if not source_sheet:
            raise ValueError(f"Source sheet '{source_sheet_id}' not found")

        # Build layout map from source
        src_layouts = (
            source_sheet.get("Layouts", [{}])[0]
            .get("Configuration", {})
            .get("GridLayout", {})
            .get("Elements", [])
        )
        layout_map = {
            le["ElementId"]: le for le in src_layouts if "ElementId" in le
        }

        # Create new sheet
        new_sheet_id = target_sheet_id or str(uuid.uuid4())
        new_visuals: List[Dict] = []
        new_layout_elements: List[Dict] = []
        type_counts: Dict[str, int] = {}

        for v in source_sheet.get("Visuals", []):
            visual_type = None
            old_id = None
            for vtype in VISUAL_TYPES:
                if vtype in v:
                    visual_type = vtype
                    old_id = v[vtype].get("VisualId", "")
                    break
            if not visual_type:
                continue

            new_id = f"{id_prefix}{old_id}"
            new_visual = _copy.deepcopy(v)
            new_visual[visual_type]["VisualId"] = new_id
            new_visuals.append(new_visual)
            type_counts[visual_type] = type_counts.get(visual_type, 0) + 1

            # Copy layout
            if old_id in layout_map:
                le = _copy.deepcopy(layout_map[old_id])
                le["ElementId"] = new_id
                new_layout_elements.append(le)
            else:
                new_layout_elements.append(
                    {
                        "ElementId": new_id,
                        "ElementType": "VISUAL",
                        "ColumnIndex": 0,
                        "ColumnSpan": 36,
                        "RowIndex": len(new_layout_elements) * 12,
                        "RowSpan": 12,
                    }
                )

        new_sheet: Dict[str, Any] = {
            "SheetId": new_sheet_id,
            "Name": target_sheet_name,
            "ContentType": "INTERACTIVE",
            "Visuals": new_visuals,
            "Layouts": [
                {
                    "Configuration": {
                        "GridLayout": {
                            "Elements": new_layout_elements,
                        }
                    }
                }
            ],
        }
        definition.setdefault("Sheets", []).append(new_sheet)

        self._analyses.update_analysis(
            analysis_id,
            definition,
            backup_first=backup_first,
            expected_last_updated=(
                last_updated
                if self._analyses.should_lock(None)
                else None
            ),
        )

        if self._analyses.should_verify(verify):
            self._verify_sheet_exists(
                analysis_id, new_sheet_id, target_sheet_name
            )
            self._verify_sheet_visual_count(
                analysis_id, new_sheet_id, len(new_visuals)
            )

        logger.info(
            "Replicated sheet %s -> %s (%d visuals)",
            source_sheet_id,
            new_sheet_id,
            len(new_visuals),
        )
        return {
            "analysis_id": analysis_id,
            "sheet_id": new_sheet_id,
            "sheet_name": target_sheet_name,
            "visual_count": len(new_visuals),
            "visual_types": type_counts,
        }

    def delete_empty(
        self,
        analysis_id: str,
        name_contains: Optional[str] = None,
        backup_first: bool = True,
    ) -> Dict:
        """Delete all empty sheets (0 visuals) from an analysis.

        Optionally filter by name substring. Automatically removes
        scoped filter groups for deleted sheets.

        Args:
            analysis_id: Analysis ID.
            name_contains: If set, only delete empty sheets whose name
                contains this substring (case-insensitive).
            backup_first: Back up before writing.

        Returns:
            dict with ``deleted_sheets``, ``filter_groups_removed``,
            ``sheet_count_after``.
        """
        definition, last_updated = self._analyses.get_definition_with_version(
            analysis_id
        )
        sheets = definition.get("Sheets", [])

        to_delete: set[str] = set()
        for s in sheets:
            if len(s.get("Visuals", [])) == 0:
                if (
                    name_contains is None
                    or name_contains.lower() in s.get("Name", "").lower()
                ):
                    to_delete.add(s["SheetId"])

        if not to_delete:
            return {"deleted_sheets": [], "filter_groups_removed": 0}

        definition["Sheets"] = [
            s for s in sheets if s["SheetId"] not in to_delete
        ]

        fg_removed = self._remove_sheet_filter_scopes(definition, to_delete)

        self._analyses.update_analysis(
            analysis_id,
            definition,
            backup_first=backup_first,
            expected_last_updated=(
                last_updated
                if self._analyses.should_lock(None)
                else None
            ),
        )

        deleted_names = [
            s.get("Name") for s in sheets if s["SheetId"] in to_delete
        ]
        logger.info(
            "Deleted %d empty sheets from %s: %s",
            len(to_delete),
            analysis_id,
            deleted_names,
        )
        return {
            "deleted_sheets": deleted_names,
            "filter_groups_removed": fg_removed,
            "sheet_count_after": len(definition["Sheets"]),
        }

    # ------------------------------------------------------------------
    # Verification helpers
    # ------------------------------------------------------------------

    def _verify_sheet_exists(
        self,
        analysis_id: str,
        sheet_id: str,
        expected_name: Optional[str] = None,
    ) -> bool:
        """Verify a sheet exists after creation/rename."""
        self._analyses.clear_definition_cache(analysis_id)
        definition = self._analyses.get_definition(analysis_id)
        for s in definition.get("Sheets", []):
            if s.get("SheetId") == sheet_id:
                if expected_name and s.get("Name") != expected_name:
                    raise ChangeVerificationError(
                        "sheet",
                        analysis_id,
                        f"Sheet '{sheet_id}' exists but name is "
                        f"'{s.get('Name')}', expected '{expected_name}'.",
                    )
                return True
        raise ChangeVerificationError(
            "sheet",
            analysis_id,
            f"Sheet '{sheet_id}' not found after update.",
        )

    def _verify_sheet_deleted(
        self, analysis_id: str, sheet_id: str
    ) -> bool:
        """Verify a sheet was actually deleted."""
        self._analyses.clear_definition_cache(analysis_id)
        definition = self._analyses.get_definition(analysis_id)
        for s in definition.get("Sheets", []):
            if s.get("SheetId") == sheet_id:
                raise ChangeVerificationError(
                    "delete_sheet",
                    analysis_id,
                    f"Sheet '{sheet_id}' still exists after deletion.",
                )
        return True

    def _verify_sheet_visual_count(
        self,
        analysis_id: str,
        sheet_id: str,
        expected_count: int,
    ) -> bool:
        """Verify a sheet has the expected number of visuals."""
        self._analyses.clear_definition_cache(analysis_id)
        definition = self._analyses.get_definition(analysis_id)
        for s in definition.get("Sheets", []):
            if s.get("SheetId") == sheet_id:
                actual_count = len(s.get("Visuals", []))
                if actual_count != expected_count:
                    raise ChangeVerificationError(
                        "replicate_sheet",
                        analysis_id,
                        f"Sheet has {actual_count} visuals, "
                        f"expected {expected_count}.",
                    )
                return True
        raise ChangeVerificationError(
            "replicate_sheet",
            analysis_id,
            f"Sheet '{sheet_id}' not found after replication.",
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _remove_sheet_filter_scopes(
        definition: Dict, sheet_ids: set[str]
    ) -> int:
        """Remove filter-group scoping entries for deleted sheets.

        Removes entire filter groups that end up with zero remaining scopes
        (unless they use AllSheets). Returns the number of filter groups removed.
        """
        for fg in definition.get("FilterGroups", []):
            scopes = (
                fg.get("ScopeConfiguration", {})
                .get("SelectedSheets", {})
                .get("SheetVisualScopingConfigurations", [])
            )
            filtered = [
                s for s in scopes if s.get("SheetId") not in sheet_ids
            ]
            if len(filtered) < len(scopes):
                fg["ScopeConfiguration"]["SelectedSheets"][
                    "SheetVisualScopingConfigurations"
                ] = filtered

        fg_before = len(definition.get("FilterGroups", []))
        definition["FilterGroups"] = [
            fg
            for fg in definition.get("FilterGroups", [])
            if len(
                fg.get("ScopeConfiguration", {})
                .get("SelectedSheets", {})
                .get("SheetVisualScopingConfigurations", [])
            )
            > 0
            or "AllSheets" in fg.get("ScopeConfiguration", {})
        ]
        return fg_before - len(definition.get("FilterGroups", []))
