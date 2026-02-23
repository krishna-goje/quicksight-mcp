"""Visual management service for QuickSight analyses.

Handles getting, adding, deleting, and configuring visuals within sheets.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Dict, List, Optional

from quicksight_mcp.core.cache import TTLCache
from quicksight_mcp.core.types import VISUAL_TYPES, extract_visual_id, parse_visual
from quicksight_mcp.safety.exceptions import ChangeVerificationError

if TYPE_CHECKING:
    from quicksight_mcp.core.aws_client import AwsClient
    from quicksight_mcp.services.analyses import AnalysisService

logger = logging.getLogger(__name__)


class VisualService:
    """Manage visuals within QuickSight analysis sheets.

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

    def get_definition(
        self, analysis_id: str, visual_id: str
    ) -> Optional[Dict]:
        """Get the full raw definition of a specific visual.

        Searches all sheets in the analysis for the given visual ID.

        Returns:
            The visual dict as stored in the analysis definition,
            or ``None`` if not found.
        """
        definition = self._analyses.get_definition(analysis_id)
        for sheet in definition.get("Sheets", []):
            for v in sheet.get("Visuals", []):
                for vtype in VISUAL_TYPES:
                    if vtype in v and v[vtype].get("VisualId") == visual_id:
                        return v
        return None

    def add(
        self,
        analysis_id: str,
        sheet_id: str,
        visual_definition: Dict,
        layout: Optional[Dict] = None,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Add a visual to a sheet.

        Args:
            analysis_id: Analysis ID.
            sheet_id: Target sheet ID.
            visual_definition: Full visual definition dict
                (e.g., ``{"KPIVisual": {...}}``).
            layout: Optional layout element for grid placement
                (``{"ElementId": ..., "ColumnIndex": ...}``).
            backup_first: Back up before writing.
            use_optimistic_locking: Override default optimistic locking.
            verify: Override default post-write verification.

        Returns:
            dict with update status and ``visual_id``.

        Raises:
            ValueError: If the sheet is not found.
        """
        definition, last_updated = self._analyses.get_definition_with_version(
            analysis_id
        )

        target_sheet = None
        for s in definition.get("Sheets", []):
            if s.get("SheetId") == sheet_id:
                target_sheet = s
                break

        if target_sheet is None:
            raise ValueError(f"Sheet '{sheet_id}' not found")

        # Extract visual ID for layout
        visual_id = extract_visual_id(visual_definition)

        target_sheet.setdefault("Visuals", []).append(visual_definition)

        # Add layout element if provided or auto-generate one
        if layout or visual_id:
            layouts = target_sheet.setdefault("Layouts", [])
            if not layouts:
                layouts.append(
                    {"Configuration": {"GridLayout": {"Elements": []}}}
                )
            elements: List[Dict] = (
                layouts[0]
                .setdefault("Configuration", {})
                .setdefault("GridLayout", {})
                .setdefault("Elements", [])
            )
            if layout:
                elements.append(layout)
            elif visual_id:
                # Default: full-width, 12 rows high, appended below existing
                max_row = max(
                    (
                        e.get("RowIndex", 0) + e.get("RowSpan", 0)
                        for e in elements
                    ),
                    default=0,
                )
                elements.append(
                    {
                        "ElementId": visual_id,
                        "ElementType": "VISUAL",
                        "ColumnIndex": 0,
                        "ColumnSpan": 36,
                        "RowIndex": max_row,
                        "RowSpan": 12,
                    }
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

        if visual_id and self._analyses.should_verify(verify):
            self._verify_visual_exists(analysis_id, visual_id)

        result["visual_id"] = visual_id
        return result

    def delete(
        self,
        analysis_id: str,
        visual_id: str,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Delete a visual from an analysis.

        Also removes the corresponding layout element.

        Raises:
            ValueError: If the visual is not found.
        """
        definition, last_updated = self._analyses.get_definition_with_version(
            analysis_id
        )

        found = False
        for sheet in definition.get("Sheets", []):
            original_len = len(sheet.get("Visuals", []))
            sheet["Visuals"] = [
                v
                for v in sheet.get("Visuals", [])
                if not any(
                    vtype in v and v[vtype].get("VisualId") == visual_id
                    for vtype in VISUAL_TYPES
                )
            ]
            if len(sheet["Visuals"]) < original_len:
                found = True
                # Remove layout element
                for layout_obj in sheet.get("Layouts", []):
                    grid = (
                        layout_obj.get("Configuration", {}).get(
                            "GridLayout", {}
                        )
                    )
                    grid["Elements"] = [
                        e
                        for e in grid.get("Elements", [])
                        if e.get("ElementId") != visual_id
                    ]
                break

        if not found:
            raise ValueError(f"Visual '{visual_id}' not found")

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
            self._verify_visual_deleted(analysis_id, visual_id)

        return result

    def set_title(
        self,
        analysis_id: str,
        visual_id: str,
        title: str,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Set or update the title of a visual.

        Raises:
            ValueError: If the visual is not found.
        """
        definition, last_updated = self._analyses.get_definition_with_version(
            analysis_id
        )

        found = False
        for sheet in definition.get("Sheets", []):
            for v in sheet.get("Visuals", []):
                for vtype in VISUAL_TYPES:
                    if (
                        vtype in v
                        and v[vtype].get("VisualId") == visual_id
                    ):
                        v[vtype].setdefault("Title", {})["FormatText"] = {
                            "PlainText": title,
                        }
                        v[vtype]["Title"]["Visibility"] = "VISIBLE"
                        found = True
                        break
                if found:
                    break
            if found:
                break

        if not found:
            raise ValueError(f"Visual '{visual_id}' not found")

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
            self._verify_visual_title(analysis_id, visual_id, title)

        return result

    def get_layout(
        self, analysis_id: str, visual_id: str
    ) -> Optional[Dict]:
        """Get the layout (position/size) for a visual.

        Returns:
            The layout element dict, or ``None`` if not found.
        """
        definition = self._analyses.get_definition(analysis_id)
        for sheet in definition.get("Sheets", []):
            for layout_obj in sheet.get("Layouts", []):
                for elem in (
                    layout_obj.get("Configuration", {})
                    .get("GridLayout", {})
                    .get("Elements", [])
                ):
                    if elem.get("ElementId") == visual_id:
                        return elem
        return None

    def set_layout(
        self,
        analysis_id: str,
        visual_id: str,
        column_index: Optional[int] = None,
        column_span: Optional[int] = None,
        row_index: Optional[int] = None,
        row_span: Optional[int] = None,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
    ) -> Dict:
        """Set position and size for a visual in the grid layout.

        Only the provided dimensions are updated; others remain unchanged.

        Raises:
            ValueError: If the visual layout element is not found.
        """
        definition, last_updated = self._analyses.get_definition_with_version(
            analysis_id
        )

        found = False
        for sheet in definition.get("Sheets", []):
            for layout_obj in sheet.get("Layouts", []):
                for elem in (
                    layout_obj.get("Configuration", {})
                    .get("GridLayout", {})
                    .get("Elements", [])
                ):
                    if elem.get("ElementId") == visual_id:
                        if column_index is not None:
                            elem["ColumnIndex"] = column_index
                        if column_span is not None:
                            elem["ColumnSpan"] = column_span
                        if row_index is not None:
                            elem["RowIndex"] = row_index
                        if row_span is not None:
                            elem["RowSpan"] = row_span
                        found = True
                        break
                if found:
                    break
            if found:
                break

        if not found:
            raise ValueError(
                f"Layout element for visual '{visual_id}' not found"
            )

        return self._analyses.update_analysis(
            analysis_id,
            definition,
            backup_first=backup_first,
            expected_last_updated=(
                last_updated
                if self._analyses.should_lock(use_optimistic_locking)
                else None
            ),
        )

    # ------------------------------------------------------------------
    # Verification helpers
    # ------------------------------------------------------------------

    def _verify_visual_exists(
        self, analysis_id: str, visual_id: str
    ) -> bool:
        """Verify a visual exists after creation."""
        self._analyses.clear_definition_cache(analysis_id)
        if self.get_definition(analysis_id, visual_id) is not None:
            return True
        raise ChangeVerificationError(
            "visual",
            analysis_id,
            f"Visual '{visual_id}' not found after update.",
        )

    def _verify_visual_deleted(
        self, analysis_id: str, visual_id: str
    ) -> bool:
        """Verify a visual was actually deleted."""
        self._analyses.clear_definition_cache(analysis_id)
        if self.get_definition(analysis_id, visual_id) is not None:
            raise ChangeVerificationError(
                "delete_visual",
                analysis_id,
                f"Visual '{visual_id}' still exists after deletion.",
            )
        return True

    def _verify_visual_title(
        self, analysis_id: str, visual_id: str, expected_title: str
    ) -> bool:
        """Verify a visual's title matches expected value."""
        self._analyses.clear_definition_cache(analysis_id)
        vdef = self.get_definition(analysis_id, visual_id)
        if vdef is None:
            raise ChangeVerificationError(
                "set_visual_title",
                analysis_id,
                f"Visual '{visual_id}' not found after title update.",
            )
        parsed = parse_visual(vdef)
        actual_title = parsed.get("title", "")
        if actual_title != expected_title:
            raise ChangeVerificationError(
                "set_visual_title",
                analysis_id,
                f"Visual '{visual_id}' title is '{actual_title}', "
                f"expected '{expected_title}'.",
            )
        return True
