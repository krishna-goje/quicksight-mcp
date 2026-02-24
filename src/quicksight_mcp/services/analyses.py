"""Analysis service — read, write, backup, and health-check QuickSight analyses.

``AnalysisService.update_analysis`` is THE central write gateway:
every mutation to an analysis flows through it, ensuring backup,
optimistic locking, destructive-change guard, cache invalidation,
completion polling, and optional health verification.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from quicksight_mcp.config import Settings
from quicksight_mcp.core.aws_client import AwsClient
from quicksight_mcp.core.cache import TTLCache
from quicksight_mcp.core.types import VISUAL_TYPES, parse_visual
from quicksight_mcp.safety.exceptions import (
    ConcurrentModificationError,
    DestructiveChangeError,
)

logger = logging.getLogger(__name__)


class AnalysisService:
    """Service for QuickSight analysis operations.

    Args:
        aws: Low-level AWS client.
        cache: TTL cache instance (shared or dedicated).
        settings: Server-wide configuration.
    """

    def __init__(
        self,
        aws: AwsClient,
        cache: TTLCache,
        settings: Settings,
    ) -> None:
        self._aws = aws
        self._cache = cache
        self._settings = settings

    # ------------------------------------------------------------------
    # List / Search
    # ------------------------------------------------------------------

    def list_all(self, use_cache: bool = True) -> List[Dict]:
        """List all analyses with TTL-based caching."""
        if use_cache:
            cached = self._cache.get("analyses")
            if cached is not None:
                return cached

        self._aws.ensure_account_id()
        analyses = self._aws.paginate("list_analyses", "AnalysisSummaryList")
        self._cache.set("analyses", analyses)
        logger.debug("Analysis cache refreshed (%d analyses)", len(analyses))
        return analyses

    def search(self, name_contains: str) -> List[Dict]:
        """Search analyses by name (client-side filter on cached list)."""
        all_analyses = self.list_all()
        needle = name_contains.lower()
        return [
            a for a in all_analyses if needle in a.get("Name", "").lower()
        ]

    # ------------------------------------------------------------------
    # Single-analysis reads
    # ------------------------------------------------------------------

    def get(self, analysis_id: str) -> Dict:
        """Get analysis summary (describe_analysis)."""
        self._aws.ensure_account_id()
        response = self._aws.call(
            "describe_analysis",
            AwsAccountId=self._aws.account_id,
            AnalysisId=analysis_id,
        )
        return response.get("Analysis", {})

    def get_definition(
        self, analysis_id: str, use_cache: bool = True
    ) -> Dict:
        """Get full analysis definition (sheets, visuals, calculated fields).

        Cached by analysis ID so repeated lookups within a session are fast.
        """
        cache_key = f"def:{analysis_id}"
        if use_cache:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached

        self._aws.ensure_account_id()
        response = self._aws.call(
            "describe_analysis_definition",
            AwsAccountId=self._aws.account_id,
            AnalysisId=analysis_id,
        )
        definition = response.get("Definition", {})
        self._cache.set(cache_key, definition)
        return definition

    def get_definition_with_version(
        self, analysis_id: str
    ) -> Tuple[Dict, Any]:
        """Get analysis definition together with version info for optimistic locking.

        Returns:
            Tuple of ``(definition, last_updated_time)``.
        """
        analysis = self.get(analysis_id)
        definition = self.get_definition(analysis_id)
        return definition, analysis.get("LastUpdatedTime")

    def get_permissions(self, analysis_id: str) -> List[Dict]:
        """Get analysis permissions (for cloning)."""
        self._aws.ensure_account_id()
        response = self._aws.call(
            "describe_analysis_permissions",
            AwsAccountId=self._aws.account_id,
            AnalysisId=analysis_id,
        )
        return response.get("Permissions", [])

    def clear_def_cache(self, analysis_id: Optional[str] = None) -> None:
        """Clear cached analysis definition(s).

        Args:
            analysis_id: Clear a specific definition cache entry.
                If ``None``, clears the analysis list cache and all
                definition caches (full cache wipe).
        """
        if analysis_id:
            self._cache.invalidate(f"def:{analysis_id}")
        else:
            self._cache.clear()

    # ------------------------------------------------------------------
    # Definition sub-reads
    # ------------------------------------------------------------------

    def get_sheets(self, analysis_id: str) -> List[Dict]:
        """Get all sheets in an analysis."""
        return self.get_definition(analysis_id).get("Sheets", [])

    def get_visuals(self, analysis_id: str) -> List[Dict]:
        """Get all visuals across all sheets (parsed into summary dicts)."""
        sheets = self.get_sheets(analysis_id)
        visuals: List[Dict] = []
        for sheet in sheets:
            sheet_name = sheet.get("Name", "Unknown")
            sheet_id = sheet.get("SheetId", "")
            for visual in sheet.get("Visuals", []):
                info = parse_visual(visual)
                info["sheet_name"] = sheet_name
                info["sheet_id"] = sheet_id
                visuals.append(info)
        return visuals

    def get_parameters(self, analysis_id: str) -> List[Dict]:
        """Get all parameter declarations in an analysis."""
        return self.get_definition(analysis_id).get(
            "ParameterDeclarations", []
        )

    def get_filters(self, analysis_id: str) -> List[Dict]:
        """Get all filter groups in an analysis."""
        return self.get_definition(analysis_id).get("FilterGroups", [])

    def get_columns_used(self, analysis_id: str) -> Dict[str, int]:
        """Get usage counts for every ColumnName referenced in the analysis."""
        definition = self.get_definition(analysis_id)
        columns: Dict[str, int] = {}

        def _walk(obj: Any) -> None:
            if isinstance(obj, dict):
                if "ColumnName" in obj:
                    col = obj["ColumnName"]
                    columns[col] = columns.get(col, 0) + 1
                for v in obj.values():
                    _walk(v)
            elif isinstance(obj, list):
                for item in obj:
                    _walk(item)

        _walk(definition)
        return dict(sorted(columns.items(), key=lambda x: -x[1]))

    def get_raw(self, analysis_id: str) -> Dict:
        """Return the complete raw analysis definition (cache-busting)."""
        return self.get_definition(analysis_id, use_cache=False)

    def get_sheet(self, analysis_id: str, sheet_id: str) -> Optional[Dict]:
        """Get a specific sheet by ID, or ``None``."""
        for s in self.get_sheets(analysis_id):
            if s.get("SheetId") == sheet_id:
                return s
        return None

    def list_sheet_visuals(
        self, analysis_id: str, sheet_id: str
    ) -> List[Dict]:
        """Get all visuals in a specific sheet."""
        sheet = self.get_sheet(analysis_id, sheet_id)
        if not sheet:
            raise ValueError(f"Sheet '{sheet_id}' not found")
        visuals: List[Dict] = []
        for v in sheet.get("Visuals", []):
            info = parse_visual(v)
            info["sheet_id"] = sheet_id
            info["sheet_name"] = sheet.get("Name", "")
            visuals.append(info)
        return visuals

    # ------------------------------------------------------------------
    # Central write gateway
    # ------------------------------------------------------------------

    def update_analysis(
        self,
        analysis_id: str,
        definition: Dict,
        *,
        backup_first: bool = True,
        wait_for_completion: bool = True,
        timeout_seconds: Optional[int] = None,
        expected_last_updated: Any = None,
        allow_destructive: bool = False,
    ) -> Dict:
        """Update an analysis with a new definition.

        This is THE central write method.  Every service that mutates an
        analysis must call this.  It enforces:

        1. **Backup** — automatic pre-write backup (unless opted out).
        2. **Optimistic locking** — ``expected_last_updated`` check.
        3. **Destructive guard** — blocks updates that would wipe sheets/visuals.
        4. **Cache invalidation** — clears stale definitions before the API call.
        5. **Completion polling** — waits for CREATION_SUCCESSFUL / UPDATE_SUCCESSFUL.
        6. **Post-update cache clear** — ensures the next read gets fresh data.

        Args:
            analysis_id: Analysis to update.
            definition: Full analysis Definition dict.
            backup_first: Create a backup before writing (default ``True``).
            wait_for_completion: Poll until the update completes (default ``True``).
            timeout_seconds: Max seconds to wait (default from settings).
            expected_last_updated: Optimistic-lock timestamp.  If set,
                raises ``ConcurrentModificationError`` when the analysis was
                modified since this timestamp.
            allow_destructive: If ``False`` (default), blocks updates that would
                delete all sheets, >50 % of visuals, or >50 % of calc fields.

        Raises:
            ConcurrentModificationError: On optimistic-locking conflict.
            DestructiveChangeError: On blocked destructive update.
            RuntimeError: On update failure, timeout, or FAILED analysis status.
        """
        timeout = timeout_seconds or self._settings.update_timeout_seconds

        # Step 1: Backup
        if backup_first:
            self.backup(analysis_id)

        # Step 2: Fetch current state
        analysis = self.get(analysis_id)

        # Refuse to update a FAILED analysis
        status = analysis.get("Status", "")
        if "FAILED" in status:
            raise RuntimeError(
                f"Cannot update analysis: current status is {status}. "
                f"Restore from backup first using restore_analysis."
            )

        # Step 3: Optimistic locking check
        if expected_last_updated is not None:
            actual = analysis.get("LastUpdatedTime")
            if actual and actual != expected_last_updated:
                raise ConcurrentModificationError(
                    analysis_id, expected_last_updated, actual
                )

        # Step 4: Destructive-change guard
        if not allow_destructive:
            self._validate_definition_not_destructive(
                analysis_id, definition
            )

        # Step 5: Clear cache BEFORE update (crash leaves no stale data)
        self.clear_def_cache(analysis_id)

        # Step 6: API call
        self._aws.ensure_account_id()
        response = self._aws.call(
            "update_analysis",
            AwsAccountId=self._aws.account_id,
            AnalysisId=analysis_id,
            Name=analysis["Name"],
            Definition=definition,
        )

        if not wait_for_completion:
            return response

        # Step 7: Poll for completion
        poll_interval = self._settings.update_poll_interval_seconds
        start = time.time()
        while time.time() - start < timeout:
            time.sleep(poll_interval)
            refreshed = self.get(analysis_id)
            status = refreshed.get("Status", "")

            if "SUCCESSFUL" in status:
                logger.info(
                    "Analysis %s update completed successfully", analysis_id
                )
                self.clear_def_cache(analysis_id)
                return {
                    "status": status,
                    "analysis_id": analysis_id,
                    "errors": None,
                }

            if "FAILED" in status:
                errors = refreshed.get("Errors", [])
                msgs = [
                    f"{e.get('Type')}: {e.get('Message')}" for e in errors
                ]
                raise RuntimeError(
                    f"Analysis update failed: {'; '.join(msgs)}"
                )

        raise RuntimeError(
            f"Analysis update timed out after {timeout}s"
        )

    # ------------------------------------------------------------------
    # Backup
    # ------------------------------------------------------------------

    def backup(
        self, analysis_id: str, backup_dir: Optional[str] = None
    ) -> str:
        """Backup analysis + definition to a timestamped JSON file.

        Returns:
            Path to the backup file.
        """
        bdir = backup_dir or self._settings.backup_dir
        Path(bdir).mkdir(parents=True, exist_ok=True, mode=0o700)

        analysis = self.get(analysis_id)
        definition = self.get_definition(analysis_id)

        name = (
            analysis.get("Name", analysis_id)
            .replace(" ", "_")
            .replace("/", "_")
        )
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{bdir}/analysis_{name}_{ts}.json"

        backup_data = {
            "analysis": analysis,
            "definition": definition,
        }

        with open(
            os.open(
                filename,
                os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                0o600,
            ),
            "w",
        ) as f:
            json.dump(backup_data, f, indent=2, default=str)

        logger.info("Backed up analysis to: %s", filename)
        return filename

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    def verify_health(self, analysis_id: str) -> Dict:
        """Run a comprehensive health check on an analysis.

        Checks:
        - Analysis status is SUCCESSFUL (not FAILED or IN_PROGRESS).
        - Sheet count within QuickSight limits (<= 20).
        - All visuals have corresponding layout elements.
        - All calculated fields reference valid dataset identifiers.

        Returns:
            dict with ``healthy`` (bool), ``checks`` (list), ``issues`` (list),
            and ``summary`` (counts).
        """
        self.clear_def_cache(analysis_id)
        analysis = self.get(analysis_id)
        definition = self.get_definition(analysis_id)

        checks: List[Dict] = []
        issues: List[str] = []

        # Check 1: Analysis status
        status = analysis.get("Status", "")
        ok = "SUCCESSFUL" in status
        checks.append(
            {"check": "analysis_status", "status": status, "ok": ok}
        )
        if not ok:
            errors = analysis.get("Errors", [])
            issues.append(
                f"Analysis status: {status}. "
                f"Errors: {[e.get('Message', '') for e in errors]}"
            )

        sheets = definition.get("Sheets", [])

        # Check 2: Sheet count within limits
        ok = len(sheets) <= 20
        checks.append(
            {
                "check": "sheet_count",
                "count": len(sheets),
                "limit": 20,
                "ok": ok,
            }
        )
        if not ok:
            issues.append(
                f"Sheet count {len(sheets)} exceeds QuickSight max of 20"
            )

        # Check 3: Visual/layout alignment per sheet
        total_visuals = 0
        total_layout_elements = 0
        for s in sheets:
            sheet_name = s.get("Name", "")
            visuals = s.get("Visuals", [])
            total_visuals += len(visuals)

            # Collect visual IDs
            visual_ids: set[str] = set()
            for v in visuals:
                for vtype in VISUAL_TYPES:
                    if vtype in v:
                        visual_ids.add(v[vtype].get("VisualId", ""))
                        break

            # Collect layout element IDs
            layout_ids: set[str] = set()
            for layout in s.get("Layouts", []):
                for elem in (
                    layout.get("Configuration", {})
                    .get("GridLayout", {})
                    .get("Elements", [])
                ):
                    layout_ids.add(elem.get("ElementId", ""))
                    total_layout_elements += 1

            # Visuals without layout
            orphan_visuals = visual_ids - layout_ids
            if orphan_visuals:
                issues.append(
                    f"Sheet '{sheet_name}': "
                    f"{len(orphan_visuals)} visuals without layout: "
                    f"{list(orphan_visuals)[:3]}..."
                )

        checks.append(
            {
                "check": "visual_layout_alignment",
                "total_visuals": total_visuals,
                "total_layout_elements": total_layout_elements,
                "ok": len(
                    [i for i in issues if "without layout" in i]
                )
                == 0,
            }
        )

        # Check 4: Calculated fields reference valid dataset identifiers
        valid_ds_ids = {
            d.get("Identifier")
            for d in definition.get("DataSetIdentifierDeclarations", [])
        }
        invalid_refs: List[str] = []
        for f in definition.get("CalculatedFields", []):
            ds_id = f.get("DataSetIdentifier", "")
            if ds_id and ds_id not in valid_ds_ids:
                invalid_refs.append(f"{f.get('Name')} -> {ds_id}")

        ok = len(invalid_refs) == 0
        checks.append(
            {
                "check": "calc_field_dataset_refs",
                "valid_datasets": len(valid_ds_ids),
                "invalid_refs": len(invalid_refs),
                "ok": ok,
            }
        )
        if not ok:
            issues.append(
                f"Calc fields with invalid dataset refs: "
                f"{invalid_refs[:5]}"
            )

        healthy = len(issues) == 0
        return {
            "analysis_id": analysis_id,
            "healthy": healthy,
            "checks": checks,
            "issues": issues,
            "summary": {
                "sheets": len(sheets),
                "visuals": total_visuals,
                "calc_fields": len(
                    definition.get("CalculatedFields", [])
                ),
                "parameters": len(
                    definition.get("ParameterDeclarations", [])
                ),
                "filter_groups": len(
                    definition.get("FilterGroups", [])
                ),
            },
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _should_verify(self, verify: Optional[bool]) -> bool:
        """Resolve verify flag against default setting."""
        return verify if verify is not None else self._settings.verify_by_default

    def _should_lock(self, lock: Optional[bool]) -> bool:
        """Resolve optimistic-locking flag against default setting."""
        return (
            lock
            if lock is not None
            else self._settings.optimistic_locking_by_default
        )

    def _validate_definition_not_destructive(
        self,
        analysis_id: str,
        new_definition: Dict,
    ) -> bool:
        """Block updates that would delete all sheets or >50% of visuals/calc fields.

        Raises:
            DestructiveChangeError: When the update is considered destructive.
        """
        current_def = self.get_definition(analysis_id, use_cache=False)

        current_sheets = current_def.get("Sheets", [])
        cur_sheet_cnt = len(current_sheets)
        cur_visual_cnt = sum(
            len(s.get("Visuals", [])) for s in current_sheets
        )
        cur_calc_cnt = len(current_def.get("CalculatedFields", []))

        new_sheets = new_definition.get("Sheets", [])
        new_sheet_cnt = len(new_sheets)
        new_visual_cnt = sum(
            len(s.get("Visuals", [])) for s in new_sheets
        )
        new_calc_cnt = len(new_definition.get("CalculatedFields", []))

        current_counts = {
            "sheets": cur_sheet_cnt,
            "visuals": cur_visual_cnt,
            "calculated_fields": cur_calc_cnt,
        }
        new_counts = {
            "sheets": new_sheet_cnt,
            "visuals": new_visual_cnt,
            "calculated_fields": new_calc_cnt,
        }

        issues: List[str] = []

        if cur_sheet_cnt > 0 and new_sheet_cnt == 0:
            issues.append(f"Would DELETE ALL {cur_sheet_cnt} SHEETS")

        if cur_visual_cnt > 0:
            loss_pct = (
                (cur_visual_cnt - new_visual_cnt) / cur_visual_cnt * 100
            )
            if loss_pct > 50:
                issues.append(
                    f"Would delete {loss_pct:.0f}% of visuals "
                    f"({cur_visual_cnt} -> {new_visual_cnt})"
                )

        if cur_calc_cnt > 0:
            loss_pct = (
                (cur_calc_cnt - new_calc_cnt) / cur_calc_cnt * 100
            )
            if loss_pct > 50:
                issues.append(
                    f"Would delete {loss_pct:.0f}% of calculated fields "
                    f"({cur_calc_cnt} -> {new_calc_cnt})"
                )

        if issues:
            raise DestructiveChangeError(
                analysis_id,
                "; ".join(issues),
                current_counts,
                new_counts,
            )
        return True
