"""Lightweight snapshot and diff service for QuickSight analysis QA.

Captures a structural snapshot (sheets, visuals, calc fields) and compares
current state against a saved snapshot to detect changes.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Set, TYPE_CHECKING

from quicksight_mcp.core.cache import TTLCache
from quicksight_mcp.core.types import parse_visual

if TYPE_CHECKING:
    from quicksight_mcp.core.aws_client import AwsClient
    from quicksight_mcp.services.analyses import AnalysisService

logger = logging.getLogger(__name__)


class SnapshotService:
    """Captures and compares lightweight structural snapshots of analyses.

    A snapshot records sheet names, visual IDs/types/titles, and calculated
    field names/expressions -- enough to detect structural drift without
    storing the entire (large) definition.

    Args:
        aws: Low-level AWS client.
        cache: TTL cache instance.
        analysis_service: AnalysisService for read operations.
    """

    def __init__(
        self,
        aws: AwsClient,
        cache: TTLCache,
        analysis_service: AnalysisService,
    ) -> None:
        self._aws = aws
        self._cache = cache
        self._analysis = analysis_service

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _snapshot_dir(self) -> str:
        """Return (and create) the snapshot directory."""
        # Snapshots sit alongside backups: ~/.quicksight-mcp/snapshots/
        backup_dir = self._analysis._settings.backup_dir  # noqa: SLF001 (private access OK within same package)
        snap_dir = str(Path(backup_dir).parent / "snapshots")
        os.makedirs(snap_dir, mode=0o700, exist_ok=True)
        return snap_dir

    # ------------------------------------------------------------------
    # Snapshot
    # ------------------------------------------------------------------

    def snapshot(self, analysis_id: str) -> Dict[str, Any]:
        """Capture a lightweight snapshot of the current analysis state.

        Clears the definition cache first to ensure a fresh read.

        Returns:
            dict with ``snapshot_id``, ``analysis_id``, ``sheets``,
            ``visuals``, ``calc_fields``, ``parameter_count``,
            ``filter_group_count``, and ``snapshot_file``.
        """
        self._analysis.clear_def_cache(analysis_id)
        analysis = self._analysis.get(analysis_id)
        definition = self._analysis.get_definition(analysis_id)

        now = datetime.now()
        snapshot_id = f"snap_{now.strftime('%Y%m%d_%H%M%S')}"

        sheets: List[Dict] = []
        visuals: List[Dict] = []
        for s in definition.get("Sheets", []):
            sheet_visuals: List[Dict] = []
            for v in s.get("Visuals", []):
                parsed = parse_visual(v)
                parsed["sheet_id"] = s.get("SheetId", "")
                visuals.append(parsed)
                sheet_visuals.append(parsed)
            sheets.append({
                "id": s.get("SheetId", ""),
                "name": s.get("Name", ""),
                "visual_count": len(sheet_visuals),
            })

        calc_fields = [
            {
                "name": f.get("Name", ""),
                "dataset": f.get("DataSetIdentifier", ""),
                "expression": f.get("Expression", ""),
            }
            for f in definition.get("CalculatedFields", [])
        ]

        snapshot: Dict[str, Any] = {
            "snapshot_id": snapshot_id,
            "analysis_id": analysis_id,
            "analysis_name": analysis.get("Name", ""),
            "timestamp": now.isoformat(),
            "status": analysis.get("Status", ""),
            "sheets": sheets,
            "visuals": visuals,
            "calc_fields": calc_fields,
            "parameter_count": len(definition.get("ParameterDeclarations", [])),
            "filter_group_count": len(definition.get("FilterGroups", [])),
        }

        # Persist to disk
        snap_dir = self._snapshot_dir()
        snap_file = os.path.join(snap_dir, f"{snapshot_id}.json")
        with open(snap_file, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, indent=2, default=str)

        snapshot["snapshot_file"] = snap_file
        return snapshot

    # ------------------------------------------------------------------
    # Diff
    # ------------------------------------------------------------------

    def diff(self, analysis_id: str, snapshot_id: str) -> Dict[str, Any]:
        """Compare current analysis state against a saved snapshot.

        Takes a fresh snapshot internally and computes structural diffs
        for sheets, visuals, and calculated fields.

        Args:
            analysis_id: Analysis ID to inspect.
            snapshot_id: Snapshot ID from a previous ``snapshot()`` call.

        Returns:
            dict with ``has_changes`` and per-category added/removed/changed lists.

        Raises:
            ValueError: If the snapshot file is not found.
        """
        # Load saved snapshot
        snap_dir = self._snapshot_dir()
        snap_file = os.path.join(snap_dir, f"{snapshot_id}.json")
        if not os.path.isfile(snap_file):
            raise ValueError(f"Snapshot '{snapshot_id}' not found at {snap_file}")

        with open(snap_file, encoding="utf-8") as f:
            old_snapshot = json.load(f)

        # Capture current state
        current = self.snapshot(analysis_id)

        # --- Diff sheets ---
        old_sheets = {s["id"]: s for s in old_snapshot.get("sheets", [])}
        new_sheets = {s["id"]: s for s in current.get("sheets", [])}

        sheets_added = [s for sid, s in new_sheets.items() if sid not in old_sheets]
        sheets_removed = [s for sid, s in old_sheets.items() if sid not in new_sheets]

        # --- Diff visuals ---
        old_visuals = {v["visual_id"]: v for v in old_snapshot.get("visuals", [])}
        new_visuals = {v["visual_id"]: v for v in current.get("visuals", [])}

        visuals_added = [v for vid, v in new_visuals.items() if vid not in old_visuals]
        visuals_removed = [v for vid, v in old_visuals.items() if vid not in new_visuals]

        visual_changes: List[Dict] = []
        common_visual_ids: Set[str] = set(old_visuals) & set(new_visuals)
        for vid in common_visual_ids:
            old_v, new_v = old_visuals[vid], new_visuals[vid]
            if old_v.get("title") != new_v.get("title"):
                visual_changes.append({
                    "visual_id": vid,
                    "field": "title",
                    "old": old_v.get("title"),
                    "new": new_v.get("title"),
                })
            if old_v.get("type") != new_v.get("type"):
                visual_changes.append({
                    "visual_id": vid,
                    "field": "type",
                    "old": old_v.get("type"),
                    "new": new_v.get("type"),
                })

        # --- Diff calc fields ---
        old_cfs = {f["name"]: f for f in old_snapshot.get("calc_fields", [])}
        new_cfs = {f["name"]: f for f in current.get("calc_fields", [])}

        calc_fields_added = [f for name, f in new_cfs.items() if name not in old_cfs]
        calc_fields_removed = [f for name, f in old_cfs.items() if name not in new_cfs]
        calc_fields_changed: List[Dict] = []
        for name in set(old_cfs) & set(new_cfs):
            if old_cfs[name].get("expression") != new_cfs[name].get("expression"):
                calc_fields_changed.append({
                    "name": name,
                    "old_expression": old_cfs[name].get("expression"),
                    "new_expression": new_cfs[name].get("expression"),
                })

        has_changes = any([
            sheets_added,
            sheets_removed,
            visuals_added,
            visuals_removed,
            visual_changes,
            calc_fields_added,
            calc_fields_removed,
            calc_fields_changed,
        ])

        return {
            "analysis_id": analysis_id,
            "snapshot_id": snapshot_id,
            "has_changes": has_changes,
            "sheets_added": sheets_added,
            "sheets_removed": sheets_removed,
            "visuals_added": visuals_added,
            "visuals_removed": visuals_removed,
            "visual_changes": visual_changes,
            "calc_fields_added": calc_fields_added,
            "calc_fields_removed": calc_fields_removed,
            "calc_fields_changed": calc_fields_changed,
            "summary": {
                "old_visual_count": len(old_visuals),
                "new_visual_count": len(new_visuals),
                "old_calc_field_count": len(old_cfs),
                "new_calc_field_count": len(new_cfs),
            },
        }
