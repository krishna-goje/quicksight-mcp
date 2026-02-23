"""Backup, restore, and clone operations for QuickSight assets.

Provides atomic file writes, path-traversal protection, and automatic
pre-restore safety backups.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from quicksight_mcp.config import Settings
from quicksight_mcp.core.cache import TTLCache

if TYPE_CHECKING:
    from quicksight_mcp.core.aws_client import AwsClient
    from quicksight_mcp.services.analyses import AnalysisService

logger = logging.getLogger(__name__)


class BackupService:
    """Manages backups, restores, and cloning for datasets and analyses.

    All backup files are written atomically (tempfile + rename) and
    with restrictive permissions (0o600 for files, 0o700 for dirs).

    Args:
        aws: Low-level AWS client for direct API calls during restore.
        cache: TTL cache (used for definition cache invalidation).
        settings: Server-wide configuration (provides backup_dir).
        analysis_service: AnalysisService for fetching analysis state.
    """

    def __init__(
        self,
        aws: AwsClient,
        cache: TTLCache,
        settings: Settings,
        analysis_service: AnalysisService,
    ) -> None:
        self._aws = aws
        self._cache = cache
        self._settings = settings
        self._analysis = analysis_service
        self._backup_dir = settings.backup_dir

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _ensure_backup_dir(self, path: Optional[str] = None) -> str:
        """Create the backup directory if it doesn't exist (mode 0o700)."""
        d = path or self._backup_dir
        os.makedirs(d, mode=0o700, exist_ok=True)
        return d

    @staticmethod
    def _sanitize_name(name: str) -> str:
        """Replace characters that are unsafe in file names."""
        return name.replace(" ", "_").replace("/", "_").replace("\\", "_")

    def _atomic_write_json(self, filepath: str, data: Any) -> None:
        """Write JSON atomically: tempfile in the same dir, then rename."""
        directory = os.path.dirname(filepath)
        fd = None
        tmp_path = None
        try:
            fd, tmp_path = tempfile.mkstemp(
                dir=directory, suffix=".tmp", prefix=".backup_"
            )
            with os.fdopen(fd, "w") as f:
                fd = None  # os.fdopen takes ownership
                json.dump(data, f, indent=2, default=str)
            os.chmod(tmp_path, 0o600)
            os.rename(tmp_path, filepath)
            tmp_path = None  # rename succeeded
        finally:
            if fd is not None:
                os.close(fd)
            if tmp_path is not None:
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass

    def _allowed_restore_dirs(self) -> List[str]:
        """Return the list of directories from which restores are allowed."""
        base = os.path.realpath(self._backup_dir)
        snap = os.path.realpath(str(Path(self._backup_dir).parent / "snapshots"))
        return [base, snap, "/tmp/qs_backup"]

    def _validate_restore_path(self, path: str) -> str:
        """Validate that *path* is inside an allowed directory.

        Returns the real path on success.

        Raises:
            ValueError: On path-traversal attempt.
        """
        real = os.path.realpath(path)
        for allowed in self._allowed_restore_dirs():
            if real.startswith(allowed + os.sep) or real == allowed:
                return real
        raise ValueError(
            f"Backup file must be within the backup directory. Got: {path}"
        )

    # ------------------------------------------------------------------
    # Backup
    # ------------------------------------------------------------------

    def backup_dataset(
        self, dataset_id: str, backup_dir: Optional[str] = None
    ) -> str:
        """Backup a dataset definition to a timestamped JSON file.

        Args:
            dataset_id: QuickSight dataset ID.
            backup_dir: Override directory (uses settings.backup_dir by default).

        Returns:
            Absolute path to the backup file.
        """
        bdir = self._ensure_backup_dir(backup_dir)

        acct = self._aws.ensure_account_id()
        dataset = self._aws.call(
            "describe_data_set",
            AwsAccountId=acct,
            DataSetId=dataset_id,
        ).get("DataSet", {})

        name = self._sanitize_name(dataset.get("Name", dataset_id))
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(bdir, f"dataset_{name}_{ts}.json")

        self._atomic_write_json(filepath, dataset)
        logger.info("Backed up dataset to: %s", filepath)
        return filepath

    def backup_analysis(
        self, analysis_id: str, backup_dir: Optional[str] = None
    ) -> str:
        """Backup an analysis (summary + full definition) to a timestamped JSON file.

        Args:
            analysis_id: QuickSight analysis ID.
            backup_dir: Override directory (uses settings.backup_dir by default).

        Returns:
            Absolute path to the backup file.
        """
        bdir = self._ensure_backup_dir(backup_dir)

        analysis = self._analysis.get_analysis(analysis_id)
        definition = self._analysis.get_definition(analysis_id)

        name = self._sanitize_name(analysis.get("Name", analysis_id))
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filepath = os.path.join(bdir, f"analysis_{name}_{ts}.json")

        backup_data = {"analysis": analysis, "definition": definition}
        self._atomic_write_json(filepath, backup_data)
        logger.info("Backed up analysis to: %s", filepath)
        return filepath

    # ------------------------------------------------------------------
    # Restore
    # ------------------------------------------------------------------

    def restore_analysis(
        self,
        backup_file: str,
        analysis_id: Optional[str] = None,
    ) -> Dict:
        """Restore an analysis from a backup JSON file.

        Handles FAILED-status analyses by calling the AWS API directly
        (bypassing the normal status guard). A pre-restore backup is
        created when possible.

        Args:
            backup_file: Path to the backup JSON file.
            analysis_id: Target analysis ID (uses the ID from the backup if omitted).

        Returns:
            dict with ``status``, ``analysis_id``.

        Raises:
            ValueError: If the file is outside allowed dirs or has no definition.
        """
        real_path = self._validate_restore_path(backup_file)

        with open(real_path) as f:
            backup_data = json.load(f)

        target_id = analysis_id or backup_data.get("analysis", {}).get("AnalysisId")
        if not target_id:
            raise ValueError("No analysis ID provided and none found in backup")

        return self.restore_from_backup(real_path, target_id)

    def restore_dataset(
        self,
        backup_file: str,
        dataset_id: Optional[str] = None,
    ) -> Dict:
        """Restore a dataset's SQL from a backup JSON file.

        A pre-restore backup of the current state is created automatically.

        Args:
            backup_file: Path to the backup JSON file.
            dataset_id: Target dataset ID (uses the ID from the backup if omitted).

        Returns:
            dict with ``status``, ``dataset_id``.

        Raises:
            ValueError: If the backup contains no CustomSql.
        """
        real_path = self._validate_restore_path(backup_file)

        with open(real_path) as f:
            backup_data = json.load(f)

        target_id = dataset_id or backup_data.get("DataSetId")
        if not target_id:
            raise ValueError("No dataset ID provided and none found in backup")

        # Pre-restore safety backup
        self.backup_dataset(target_id)

        # Extract and restore SQL
        for _table_id, table_def in backup_data.get("PhysicalTableMap", {}).items():
            if "CustomSql" in table_def:
                sql = table_def["CustomSql"].get("SqlQuery")
                if sql:
                    acct = self._aws.ensure_account_id()
                    dataset = self._aws.call(
                        "describe_data_set",
                        AwsAccountId=acct,
                        DataSetId=target_id,
                    ).get("DataSet", {})

                    physical_map = dataset.get("PhysicalTableMap", {})
                    for _tid, tdef in physical_map.items():
                        if "CustomSql" in tdef:
                            tdef["CustomSql"]["SqlQuery"] = sql
                            break

                    update_params: Dict[str, Any] = {
                        "AwsAccountId": acct,
                        "DataSetId": target_id,
                        "Name": dataset["Name"],
                        "PhysicalTableMap": physical_map,
                        "LogicalTableMap": dataset.get("LogicalTableMap", {}),
                        "ImportMode": dataset.get("ImportMode", "SPICE"),
                    }
                    self._aws.call("update_data_set", **update_params)
                    return {"status": "restored", "dataset_id": target_id}

        raise ValueError("No CustomSql found in backup file")

    def restore_from_backup(
        self,
        backup_file: str,
        analysis_id: str,
    ) -> Dict:
        """Restore an analysis from a JSON backup file (core implementation).

        Reads the backup, creates a pre-restore backup (best-effort),
        then updates the analysis with the backed-up definition.
        This is the recommended way to recover from a FAILED analysis state.

        Args:
            backup_file: Path to the backup JSON file.
            analysis_id: Analysis ID to restore.

        Returns:
            dict with ``status``, ``analysis_id``.

        Raises:
            ValueError: On path-traversal or missing definition.
            RuntimeError: On restore failure or timeout.
        """
        real_path = self._validate_restore_path(backup_file)

        with open(real_path) as f:
            backup_data = json.load(f)

        # Handle both Definition (capital) and definition (lower) key casing
        definition = backup_data.get("Definition", backup_data.get("definition", {}))
        if not definition:
            raise ValueError(f"No Definition found in backup file: {backup_file}")

        # Pre-restore backup (best-effort; may fail for FAILED analyses)
        try:
            self.backup_analysis(analysis_id)
        except Exception:
            logger.warning(
                "Could not create pre-restore backup (analysis may be in FAILED state)"
            )

        # Force update — calls AWS directly to bypass FAILED status check
        analysis = self._analysis.get_analysis(analysis_id)
        acct = self._aws.ensure_account_id()
        self._aws.call(
            "update_analysis",
            AwsAccountId=acct,
            AnalysisId=analysis_id,
            Name=analysis["Name"],
            Definition=definition,
        )

        # Poll for completion
        poll_interval = self._settings.update_poll_interval_seconds
        timeout = self._settings.update_timeout_seconds
        start = time.time()
        while time.time() - start < timeout:
            time.sleep(poll_interval)
            refreshed = self._analysis.get_analysis(analysis_id)
            status = refreshed.get("Status", "")
            if "SUCCESSFUL" in status:
                self._analysis.clear_def_cache(analysis_id)
                logger.info(
                    "Analysis %s restored from %s", analysis_id, backup_file
                )
                return {"status": status, "analysis_id": analysis_id}
            if "FAILED" in status:
                errors = refreshed.get("Errors", [])
                raise RuntimeError(
                    f"Restore failed: {[e.get('Message', '') for e in errors]}"
                )

        raise RuntimeError(f"Restore timed out after {timeout}s")

    # ------------------------------------------------------------------
    # Clone
    # ------------------------------------------------------------------

    def clone_analysis(
        self,
        source_analysis_id: str,
        new_name: str,
        new_analysis_id: Optional[str] = None,
    ) -> Dict:
        """Clone an analysis to create a copy (e.g., for testing).

        Args:
            source_analysis_id: Source analysis ID.
            new_name: Name for the new analysis.
            new_analysis_id: Optional ID (auto-generated UUID if omitted).

        Returns:
            dict with ``analysis_id``, ``name``, ``arn``, ``status``.
        """
        definition = self._analysis.get_definition(source_analysis_id)
        new_id = new_analysis_id or str(uuid.uuid4())

        # Copy permissions from source
        permissions = self._analysis.get_permissions(source_analysis_id)

        acct = self._aws.ensure_account_id()
        response = self._aws.call(
            "create_analysis",
            AwsAccountId=acct,
            AnalysisId=new_id,
            Name=new_name,
            Definition=definition,
            Permissions=permissions,
        )

        logger.info(
            "Cloned analysis %s -> %s (%s)",
            source_analysis_id,
            new_id,
            new_name,
        )
        return {
            "analysis_id": new_id,
            "name": new_name,
            "arn": response.get("Arn"),
            "status": response.get("CreationStatus"),
        }

    # ------------------------------------------------------------------
    # List backups
    # ------------------------------------------------------------------

    def list_backups(
        self,
        resource_type: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict]:
        """List backup files in the backup directory.

        Args:
            resource_type: Filter by ``'analysis'`` or ``'dataset'`` (None = all).
            limit: Maximum number of entries to return (most recent first).

        Returns:
            List of dicts with ``path``, ``filename``, ``resource_type``,
            ``resource_name``, ``timestamp``, ``size_bytes``.
        """
        bdir = self._backup_dir
        if not os.path.isdir(bdir):
            return []

        entries: List[Dict] = []
        for fname in os.listdir(bdir):
            if not fname.endswith(".json"):
                continue

            # Filter by resource type prefix
            if resource_type:
                prefix = resource_type.lower() + "_"
                if not fname.startswith(prefix):
                    continue

            fpath = os.path.join(bdir, fname)
            if not os.path.isfile(fpath):
                continue

            stat = os.stat(fpath)

            # Parse filename: <type>_<name>_<YYYYMMDD_HHMMSS>.json
            parts = fname.rsplit("_", 2)
            if len(parts) >= 3:
                rtype = parts[0].split("_")[0]  # "analysis" or "dataset"
                # Name is everything between type prefix and last two timestamp parts
                name_part = fname[len(rtype) + 1 : -(len(parts[-1]) + len(parts[-2]) + 2)]
                timestamp_str = f"{parts[-2]}_{parts[-1].replace('.json', '')}"
            else:
                rtype = "unknown"
                name_part = fname.replace(".json", "")
                timestamp_str = ""

            entries.append({
                "path": fpath,
                "filename": fname,
                "resource_type": rtype,
                "resource_name": name_part,
                "timestamp": timestamp_str,
                "size_bytes": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            })

        # Sort by modification time, most recent first
        entries.sort(key=lambda e: e["modified"], reverse=True)
        return entries[:limit]
