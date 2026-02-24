"""Backup and restore MCP tools for QuickSight.

Provides tools for backing up, restoring, and cloning QuickSight
analyses and datasets. Backups are saved as JSON files to
~/.quicksight-mcp/backups/ by default.
"""

import os
import logging
from typing import Callable

from fastmcp import FastMCP

from quicksight_mcp.tools._decorator import qs_tool

logger = logging.getLogger(__name__)

DEFAULT_BACKUP_DIR = os.path.expanduser("~/.quicksight-mcp/backups")


def register_backup_tools(
    mcp: FastMCP, get_client: Callable, get_tracker: Callable, get_memory=None
):
    """Register all backup-related MCP tools."""

    @qs_tool(mcp, get_memory, idempotent=True)
    def backup_analysis(analysis_id: str) -> dict:
        """Save a full backup of a QuickSight analysis definition to disk.

        Creates a timestamped JSON file containing the complete analysis
        definition (sheets, visuals, calculated fields, parameters,
        filters, etc.). Use restore_analysis to restore from a backup.

        Backups are saved to ~/.quicksight-mcp/backups/.

        Args:
            analysis_id: The QuickSight analysis ID to back up.

        Returns the file path of the created backup.
        """
        client = get_client()
        os.makedirs(DEFAULT_BACKUP_DIR, exist_ok=True)
        filepath = client.backup_analysis(analysis_id, DEFAULT_BACKUP_DIR)
        return {
            "status": "success",
            "analysis_id": analysis_id,
            "backup_file": filepath,
            "note": (
                "Backup saved. Use restore_analysis with this file "
                "path to restore if needed."
            ),
        }

    @qs_tool(mcp, get_memory, idempotent=True)
    def backup_dataset(dataset_id: str) -> dict:
        """Save a full backup of a QuickSight dataset configuration to disk.

        Creates a timestamped JSON file containing the dataset definition
        (SQL, columns, physical/logical table maps, etc.).

        Backups are saved to ~/.quicksight-mcp/backups/.

        Args:
            dataset_id: The QuickSight dataset ID to back up.

        Returns the file path of the created backup.
        """
        client = get_client()
        os.makedirs(DEFAULT_BACKUP_DIR, exist_ok=True)
        filepath = client.backup_dataset(dataset_id, DEFAULT_BACKUP_DIR)
        return {
            "status": "success",
            "dataset_id": dataset_id,
            "backup_file": filepath,
            "note": "Dataset backup saved.",
        }

    @qs_tool(mcp, get_memory, destructive=True)
    def restore_analysis(backup_file: str, analysis_id: str = "") -> dict:
        """Restore a QuickSight analysis from a JSON backup file.

        WARNING: This overwrites the analysis definition with the backup
        contents. The current state of the analysis will be replaced.

        Args:
            backup_file: Full path to the backup JSON file
                         (e.g., ~/.quicksight-mcp/backups/analysis_xxx_20240101_120000.json).
            analysis_id: Optional analysis ID to restore into. If empty,
                         restores to the original analysis ID stored in
                         the backup file.

        Returns the restored analysis ID and status.
        """
        client = get_client()
        result = client.restore_analysis_from_backup(
            backup_file, analysis_id=analysis_id or None
        )
        return {
            "status": "restored",
            "backup_file": backup_file,
            "analysis_id": result.get("analysis_id", analysis_id),
            "note": (
                "Analysis restored from backup. Works even on FAILED analyses. "
                "Use describe_analysis to verify the structure."
            ),
        }

    @qs_tool(mcp, get_memory)
    def clone_analysis(source_analysis_id: str, new_name: str) -> dict:
        """Clone a QuickSight analysis for safe experimentation.

        Creates a full copy of the analysis with a new name and ID.
        The clone includes all sheets, visuals, calculated fields,
        parameters, and filters. Use this to test changes without
        affecting the original.

        Best practice workflow:
        1. clone_analysis to create a test copy
        2. Make and test changes on the clone
        3. When satisfied, publish_dashboard from the clone
        4. Delete the clone when done

        Args:
            source_analysis_id: The analysis ID to clone.
            new_name: Name for the cloned analysis.
                      Example: "WBR Weekly - Test Copy"
        """
        client = get_client()
        result = client.clone_analysis(source_analysis_id, new_name)
        return {
            "status": "cloned",
            "source_analysis_id": source_analysis_id,
            "new_analysis_id": result.get("analysis_id"),
            "new_name": new_name,
            "note": (
                "Clone created. Make changes on the clone, then "
                "publish_dashboard when ready."
            ),
        }
