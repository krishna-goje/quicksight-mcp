"""Dataset operations: list, search, get, update SQL, refresh SPICE, etc.

Extracted from the monolithic ``QuickSightClient`` into a focused service
that depends only on ``AwsClient`` (for API calls) and ``TTLCache`` (for
list caching).
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from quicksight_mcp.core.aws_client import AwsClient
from quicksight_mcp.core.cache import TTLCache
from quicksight_mcp.config import Settings
from quicksight_mcp.safety.exceptions import (
    ChangeVerificationError,
    QSValidationError,
)

logger = logging.getLogger(__name__)

# Optional top-level dataset keys that must be preserved during updates.
_OPTIONAL_DATASET_KEYS = (
    "ColumnGroups",
    "FieldFolders",
    "RowLevelPermissionDataSet",
    "DataSetUsageConfiguration",
    "ColumnLevelPermissionRules",
    "RowLevelPermissionTagConfiguration",
)


class DatasetService:
    """Service for QuickSight dataset operations.

    Args:
        aws: Low-level AWS client with auto-retry and credential refresh.
        cache: TTL cache instance (shared or dedicated).
        settings: Server-wide configuration.
    """

    def __init__(self, aws: AwsClient, cache: TTLCache, settings: Settings) -> None:
        self._aws = aws
        self._cache = cache
        self._settings = settings

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def list_all(self, use_cache: bool = True) -> List[Dict]:
        """List all datasets with TTL-based caching.

        Args:
            use_cache: Use the cache (default ``True``).

        Returns:
            List of dataset summary dicts.
        """
        if use_cache:
            cached = self._cache.get("datasets")
            if cached is not None:
                return cached

        datasets = self._aws.paginate("list_data_sets", "DataSetSummaries")

        self._cache.set("datasets", datasets)
        logger.debug("Dataset cache refreshed (%d datasets)", len(datasets))
        return datasets

    def search(self, name_contains: str) -> List[Dict]:
        """Search datasets by name (server-side with client-side fallback).

        Args:
            name_contains: Substring to search for in dataset names.
        """
        self._aws.ensure_account_id()

        # Try server-side search first
        try:
            response = self._aws.call(
                "search_data_sets",
                AwsAccountId=self._aws.account_id,
                Filters=[
                    {
                        "Operator": "StringContains",
                        "Name": "DATASET_NAME",
                        "Value": name_contains,
                    }
                ],
                MaxResults=100,
            )
            return response.get("DataSetSummaries", [])
        except Exception:
            logger.debug(
                "Server-side dataset search failed, falling back to client-side"
            )

        # Client-side fallback
        all_datasets = self.list_all()
        needle = name_contains.lower()
        return [d for d in all_datasets if needle in d.get("Name", "").lower()]

    def get(self, dataset_id: str) -> Dict:
        """Get full dataset definition.

        Args:
            dataset_id: QuickSight dataset ID.

        Returns:
            Dataset dict from ``describe_data_set``.
        """
        self._aws.ensure_account_id()
        response = self._aws.call(
            "describe_data_set",
            AwsAccountId=self._aws.account_id,
            DataSetId=dataset_id,
        )
        return response.get("DataSet", {})

    def get_sql(self, dataset_id: str) -> Optional[str]:
        """Extract the SQL query from a dataset's PhysicalTableMap.

        Args:
            dataset_id: QuickSight dataset ID.

        Returns:
            SQL string, or ``None`` if the dataset does not use Custom SQL.
        """
        dataset = self.get(dataset_id)
        for _table_id, table_def in dataset.get("PhysicalTableMap", {}).items():
            if "CustomSql" in table_def:
                return table_def["CustomSql"].get("SqlQuery")
        return None

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def update_sql(
        self,
        dataset_id: str,
        new_sql: str,
        backup_first: bool = True,
        backup_dir: Optional[str] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Update dataset SQL query with optional backup and verification.

        Args:
            dataset_id: Dataset ID.
            new_sql: New SQL query string.
            backup_first: Back up the current dataset before writing (default ``True``).
            backup_dir: Override backup directory.
            verify: Verify the SQL was persisted after update.

        Raises:
            ChangeVerificationError: If verification is enabled and the SQL
                was not updated.
        """
        if backup_first:
            self.backup(dataset_id, backup_dir)

        dataset = self.get(dataset_id)

        # Find and update the CustomSql entry
        physical_map = dataset.get("PhysicalTableMap", {})
        found_custom_sql = False
        for _table_id, table_def in physical_map.items():
            if "CustomSql" in table_def:
                table_def["CustomSql"]["SqlQuery"] = new_sql
                found_custom_sql = True
                break

        if not found_custom_sql:
            raise ValueError(
                f"Dataset {dataset_id} does not use Custom SQL. "
                f"Cannot update SQL query."
            )

        # Build update payload
        update_params: Dict[str, Any] = {
            "AwsAccountId": self._aws.account_id,
            "DataSetId": dataset_id,
            "Name": dataset["Name"],
            "PhysicalTableMap": physical_map,
            "LogicalTableMap": dataset.get("LogicalTableMap", {}),
            "ImportMode": dataset.get("ImportMode", "SPICE"),
        }

        # Preserve optional top-level keys
        for key in _OPTIONAL_DATASET_KEYS:
            if key in dataset:
                update_params[key] = dataset[key]

        response = self._aws.call("update_data_set", **update_params)

        if self._should_verify(verify):
            self._verify_dataset_sql(dataset_id, new_sql)

        logger.info("Dataset %s SQL updated (%d chars)", dataset_id, len(new_sql))
        return response

    def modify_sql(
        self,
        dataset_id: str,
        find: str,
        replace: str,
        backup_first: bool = True,
        backup_dir: Optional[str] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Find and replace text in dataset SQL without full get/edit/update.

        Args:
            dataset_id: Dataset ID.
            find: Text to search for in the current SQL.
            replace: Replacement text.
            backup_first: Back up before writing (default ``True``).
            backup_dir: Override backup directory.
            verify: Verify the SQL was persisted after update.

        Returns:
            Update response dict.

        Raises:
            QSValidationError: If the dataset does not use Custom SQL or
                the ``find`` text is not present.
        """
        current_sql = self.get_sql(dataset_id)
        if current_sql is None:
            raise QSValidationError(
                f"Dataset {dataset_id} does not use Custom SQL. "
                f"Cannot perform find/replace.",
                resource_id=dataset_id,
            )

        if find not in current_sql:
            raise QSValidationError(
                f"Text to find not present in current SQL. "
                f"Find text ({len(find)} chars): {find[:100]}...",
                resource_id=dataset_id,
            )

        new_sql = current_sql.replace(find, replace)
        return self.update_sql(
            dataset_id,
            new_sql,
            backup_first=backup_first,
            backup_dir=backup_dir,
            verify=verify,
        )

    def create(
        self,
        name: str,
        sql: str,
        data_source_arn: str,
        import_mode: str = "SPICE",
        columns: Optional[List[Dict[str, str]]] = None,
    ) -> str:
        """Create a new dataset from a SQL query.

        Args:
            name: Human-readable dataset name.
            sql: SQL query string for the dataset.
            data_source_arn: ARN of the data source to query against.
            import_mode: ``'SPICE'`` (cached) or ``'DIRECT_QUERY'`` (live).
            columns: List of ``{'Name': ..., 'Type': ...}`` column definitions.
                If ``None``, infers a single ``*`` column of type ``STRING``.

        Returns:
            The new dataset ID.
        """
        self._aws.ensure_account_id()

        dataset_id = str(uuid.uuid4())
        physical_table_id = f"physical-{dataset_id[:8]}"
        logical_table_id = f"logical-{dataset_id[:8]}"

        if columns is None:
            columns = [{"Name": "Column1", "Type": "STRING"}]

        physical_table_map = {
            physical_table_id: {
                "CustomSql": {
                    "DataSourceArn": data_source_arn,
                    "Name": name,
                    "SqlQuery": sql,
                    "Columns": columns,
                }
            }
        }

        logical_table_map = {
            logical_table_id: {
                "Alias": name,
                "Source": {"PhysicalTableId": physical_table_id},
            }
        }

        self._aws.call(
            "create_data_set",
            AwsAccountId=self._aws.account_id,
            DataSetId=dataset_id,
            Name=name,
            PhysicalTableMap=physical_table_map,
            LogicalTableMap=logical_table_map,
            ImportMode=import_mode,
        )

        # Invalidate dataset list cache
        self.clear_cache()

        logger.info("Created dataset %s (%s)", dataset_id, name)
        return dataset_id

    def update_definition(
        self,
        dataset_id: str,
        definition: Dict[str, Any],
        backup_first: bool = True,
        backup_dir: Optional[str] = None,
    ) -> Dict:
        """Update full dataset definition (columns, joins, calculated columns).

        Args:
            dataset_id: Dataset ID.
            definition: Full dataset definition dict.  Must include at minimum
                ``Name``, ``PhysicalTableMap``, ``LogicalTableMap``, and
                ``ImportMode``.
            backup_first: Back up the current dataset before writing (default ``True``).
            backup_dir: Override backup directory.

        Returns:
            AWS API response dict.

        Raises:
            QSValidationError: If required keys are missing from *definition*.
        """
        self._aws.ensure_account_id()

        if backup_first:
            self.backup(dataset_id, backup_dir)

        if not definition.get("PhysicalTableMap"):
            raise QSValidationError(
                "definition must include a non-empty PhysicalTableMap",
                resource_id=dataset_id,
            )
        if not definition.get("LogicalTableMap"):
            raise QSValidationError(
                "definition must include a non-empty LogicalTableMap",
                resource_id=dataset_id,
            )
        if "Name" not in definition:
            current = self.get(dataset_id)
            definition["Name"] = current["Name"]

        update_params: Dict[str, Any] = {
            "AwsAccountId": self._aws.account_id,
            "DataSetId": dataset_id,
            "Name": definition["Name"],
            "PhysicalTableMap": definition.get("PhysicalTableMap", {}),
            "LogicalTableMap": definition.get("LogicalTableMap", {}),
            "ImportMode": definition.get("ImportMode", "SPICE"),
        }

        for key in _OPTIONAL_DATASET_KEYS:
            if key in definition:
                update_params[key] = definition[key]

        response = self._aws.call("update_data_set", **update_params)

        self.clear_cache()

        logger.info("Dataset %s definition updated", dataset_id)
        return response

    # ------------------------------------------------------------------
    # SPICE refresh
    # ------------------------------------------------------------------

    def refresh(self, dataset_id: str) -> Dict:
        """Trigger a SPICE refresh (create_ingestion).

        Args:
            dataset_id: Dataset ID.

        Returns:
            dict with ``ingestion_id``, ``status``, ``arn``.
        """
        self._aws.ensure_account_id()
        ingestion_id = f"refresh-{datetime.now():%Y%m%d-%H%M%S}"
        response = self._aws.call(
            "create_ingestion",
            AwsAccountId=self._aws.account_id,
            DataSetId=dataset_id,
            IngestionId=ingestion_id,
        )
        return {
            "ingestion_id": ingestion_id,
            "status": response.get("IngestionStatus"),
            "arn": response.get("Arn"),
        }

    def cancel_refresh(self, dataset_id: str, ingestion_id: str) -> Dict:
        """Cancel a running SPICE ingestion.

        Args:
            dataset_id: Dataset ID.
            ingestion_id: Ingestion ID of the running refresh.

        Returns:
            AWS API response dict with cancellation status.
        """
        self._aws.ensure_account_id()
        response = self._aws.call(
            "cancel_ingestion",
            AwsAccountId=self._aws.account_id,
            DataSetId=dataset_id,
            IngestionId=ingestion_id,
        )
        logger.info("Cancelled ingestion %s for dataset %s", ingestion_id, dataset_id)
        return response

    def get_refresh_status(self, dataset_id: str, ingestion_id: str) -> Dict:
        """Get status of a SPICE refresh.

        Args:
            dataset_id: Dataset ID.
            ingestion_id: Ingestion ID to check.

        Returns:
            dict with ``status``, ``error``, ``row_count``, ``created``.
        """
        self._aws.ensure_account_id()
        response = self._aws.call(
            "describe_ingestion",
            AwsAccountId=self._aws.account_id,
            DataSetId=dataset_id,
            IngestionId=ingestion_id,
        )
        ingestion = response.get("Ingestion", {})
        return {
            "status": ingestion.get("IngestionStatus"),
            "error": ingestion.get("ErrorInfo"),
            "row_count": ingestion.get("RowInfo", {}).get("RowsIngested"),
            "created": ingestion.get("CreatedTime"),
        }

    def list_recent_refreshes(self, dataset_id: str, limit: int = 5) -> List[Dict]:
        """List recent SPICE refreshes for a dataset, newest first.

        Args:
            dataset_id: Dataset ID.
            limit: Maximum number of refreshes to return (default 5).
        """
        self._aws.ensure_account_id()
        response = self._aws.call(
            "list_ingestions",
            AwsAccountId=self._aws.account_id,
            DataSetId=dataset_id,
        )
        ingestions = response.get("Ingestions", [])
        ingestions.sort(key=lambda x: x.get("CreatedTime", ""), reverse=True)
        return ingestions[:limit]

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        """Clear the dataset list cache."""
        self._cache.invalidate("datasets")

    # ------------------------------------------------------------------
    # Backup
    # ------------------------------------------------------------------

    def backup(
        self, dataset_id: str, backup_dir: Optional[str] = None
    ) -> str:
        """Backup dataset definition to a timestamped JSON file.

        Uses atomic write (tempfile + ``os.rename``) to prevent partial files.

        Args:
            dataset_id: Dataset ID to back up.
            backup_dir: Override backup directory (defaults to settings).

        Returns:
            Path to the backup file.
        """
        bdir = backup_dir or self._settings.backup_dir
        Path(bdir).mkdir(parents=True, exist_ok=True, mode=0o700)

        dataset = self.get(dataset_id)
        name = dataset.get("Name", dataset_id).replace(" ", "_").replace("/", "_")
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{bdir}/dataset_{name}_{ts}.json"

        # Atomic write: write to tempfile in the same directory, then rename
        fd, tmp_path = tempfile.mkstemp(
            dir=bdir, prefix=".dataset_backup_", suffix=".json"
        )
        try:
            with os.fdopen(fd, "w") as f:
                json.dump(dataset, f, indent=2, default=str)
            os.rename(tmp_path, filename)
        except BaseException:
            # Clean up temp file on failure
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

        logger.info("Backed up dataset to: %s", filename)
        return filename

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _should_verify(self, verify: Optional[bool]) -> bool:
        """Resolve per-call verify flag against the global default."""
        return verify if verify is not None else self._settings.verify_by_default

    def _verify_dataset_sql(self, dataset_id: str, expected_sql: str) -> bool:
        """Verify dataset SQL matches expected value (whitespace-normalized).

        Raises:
            ChangeVerificationError: If the actual SQL does not match.
        """
        actual_sql = self.get_sql(dataset_id)
        expected_norm = " ".join(expected_sql.split())
        actual_norm = " ".join((actual_sql or "").split())

        if expected_norm != actual_norm:
            raise ChangeVerificationError(
                "update_dataset_sql",
                dataset_id,
                f"SQL not updated. Expected {len(expected_sql)} chars, got "
                f"{len(actual_sql or '')} chars.",
            )
        return True
