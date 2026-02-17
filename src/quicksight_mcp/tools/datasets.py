"""Dataset MCP tools for QuickSight.

Provides tools for listing, searching, inspecting, and modifying
QuickSight datasets and their underlying SQL, plus SPICE refresh management.
"""

import json
import time
import logging
from typing import Callable

from fastmcp import FastMCP

logger = logging.getLogger(__name__)


def register_dataset_tools(mcp: FastMCP, get_client: Callable, get_tracker: Callable):
    """Register all dataset-related MCP tools."""

    @mcp.tool
    def list_datasets() -> dict:
        """List all QuickSight datasets with their names, IDs, and import mode.

        Returns every dataset in the account with:
        - name: Human-readable dataset name
        - id: Dataset ID (use this for other dataset operations)
        - import_mode: SPICE (cached) or DIRECT_QUERY (live)

        Results are cached for 5 minutes. Use this to discover datasets
        before calling get_dataset_sql or update_dataset_sql.
        """
        start = time.time()
        client = get_client()
        try:
            datasets = client.list_datasets()
            result = {
                "count": len(datasets),
                "datasets": [
                    {
                        "name": d.get("Name"),
                        "id": d.get("DataSetId"),
                        "import_mode": d.get("ImportMode"),
                    }
                    for d in datasets
                ],
            }
            get_tracker().record_call(
                "list_datasets", {}, (time.time() - start) * 1000, True
            )
            return result
        except Exception as e:
            get_tracker().record_call(
                "list_datasets", {}, (time.time() - start) * 1000, False, str(e)
            )
            return {"error": str(e)}

    @mcp.tool
    def search_datasets(name: str) -> dict:
        """Search QuickSight datasets by name (case-insensitive partial match).

        Args:
            name: Search string to match against dataset names.
                  Example: "wbr" matches "WBR Weekly", "wbr_ingest", etc.

        Returns matching datasets with their IDs and import modes.
        Useful when you know part of a dataset name but not the exact ID.
        """
        start = time.time()
        client = get_client()
        try:
            results = client.search_datasets(name)
            result = {
                "query": name,
                "count": len(results),
                "datasets": [
                    {
                        "name": d.get("Name"),
                        "id": d.get("DataSetId"),
                        "import_mode": d.get("ImportMode"),
                    }
                    for d in results
                ],
            }
            get_tracker().record_call(
                "search_datasets",
                {"name": name},
                (time.time() - start) * 1000,
                True,
            )
            return result
        except Exception as e:
            get_tracker().record_call(
                "search_datasets",
                {"name": name},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def get_dataset(dataset_id: str) -> dict:
        """Get full metadata for a QuickSight dataset.

        Args:
            dataset_id: The QuickSight dataset ID.

        Returns complete dataset information including name, columns,
        import mode, data source, row-level permissions, and more.
        """
        start = time.time()
        client = get_client()
        try:
            dataset = client.get_dataset(dataset_id)
            get_tracker().record_call(
                "get_dataset",
                {"dataset_id": dataset_id},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "dataset_id": dataset_id,
                "name": dataset.get("Name"),
                "import_mode": dataset.get("ImportMode"),
                "physical_table_count": len(dataset.get("PhysicalTableMap", {})),
                "logical_table_count": len(dataset.get("LogicalTableMap", {})),
                "output_columns": [
                    {"name": c.get("Name"), "type": c.get("Type")}
                    for c in dataset.get("OutputColumns", [])
                ],
            }
        except Exception as e:
            get_tracker().record_call(
                "get_dataset",
                {"dataset_id": dataset_id},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def get_dataset_sql(dataset_id: str) -> dict:
        """Get the SQL query powering a QuickSight dataset.

        Args:
            dataset_id: The QuickSight dataset ID.

        Returns the SQL query string if the dataset uses Custom SQL,
        or indicates if it uses a direct table reference instead.
        Use this to understand what data feeds a dataset before modifying it.
        """
        start = time.time()
        client = get_client()
        try:
            sql = client.get_dataset_sql(dataset_id)
            result = {"dataset_id": dataset_id, "sql": sql}
            if sql is None:
                result["note"] = (
                    "Dataset does not use Custom SQL "
                    "(may use a direct table reference)."
                )
            get_tracker().record_call(
                "get_dataset_sql",
                {"dataset_id": dataset_id},
                (time.time() - start) * 1000,
                True,
            )
            return result
        except Exception as e:
            get_tracker().record_call(
                "get_dataset_sql",
                {"dataset_id": dataset_id},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def update_dataset_sql(
        dataset_id: str, new_sql: str, backup_first: bool = True
    ) -> dict:
        """Update the SQL query for a QuickSight dataset.

        WARNING: This modifies the dataset in place. A backup is created by default.

        Args:
            dataset_id: The QuickSight dataset ID.
            new_sql: The new SQL query to set. Must be valid SQL for the
                     dataset's data source (e.g., Snowflake, Redshift).
            backup_first: Create a backup before updating (default True).
                          Strongly recommended -- set to False only if you
                          already have a manual backup.

        After updating a SPICE dataset, call refresh_dataset to reload data.
        For DIRECT_QUERY datasets the change takes effect immediately.
        """
        start = time.time()
        client = get_client()
        try:
            client.update_dataset_sql(
                dataset_id, new_sql, backup_first=backup_first
            )
            get_tracker().record_call(
                "update_dataset_sql",
                {"dataset_id": dataset_id},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "status": "success",
                "dataset_id": dataset_id,
                "backup_created": backup_first,
                "note": (
                    "If this is a SPICE dataset, call refresh_dataset "
                    "to reload data with the new SQL."
                ),
            }
        except Exception as e:
            get_tracker().record_call(
                "update_dataset_sql",
                {"dataset_id": dataset_id},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def refresh_dataset(dataset_id: str) -> dict:
        """Trigger a SPICE refresh (data reload) for a dataset.

        Use this after updating dataset SQL to reload data into SPICE cache.
        Has no effect on DIRECT_QUERY datasets.

        Args:
            dataset_id: The QuickSight dataset ID to refresh.

        Returns an ingestion_id you can pass to get_refresh_status to
        monitor progress. Typical SPICE refreshes take 30 seconds to
        several minutes depending on data volume.
        """
        start = time.time()
        client = get_client()
        try:
            result = client.refresh_dataset(dataset_id)
            get_tracker().record_call(
                "refresh_dataset",
                {"dataset_id": dataset_id},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "status": "refresh_triggered",
                "dataset_id": dataset_id,
                "ingestion_id": result.get("ingestion_id"),
                "ingestion_status": result.get("status"),
                "note": (
                    "Use get_refresh_status with the ingestion_id "
                    "to monitor progress."
                ),
            }
        except Exception as e:
            get_tracker().record_call(
                "refresh_dataset",
                {"dataset_id": dataset_id},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def get_refresh_status(dataset_id: str, ingestion_id: str) -> dict:
        """Check the status of a SPICE dataset refresh.

        Args:
            dataset_id: The QuickSight dataset ID.
            ingestion_id: The ingestion ID returned by refresh_dataset.

        Returns:
            status: QUEUED, RUNNING, COMPLETED, FAILED, or CANCELLED.
            rows_ingested: Number of rows loaded (available when COMPLETED).
            error: Error message if the refresh FAILED.
        """
        start = time.time()
        client = get_client()
        try:
            result = client.get_refresh_status(dataset_id, ingestion_id)
            get_tracker().record_call(
                "get_refresh_status",
                {"dataset_id": dataset_id, "ingestion_id": ingestion_id},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "dataset_id": dataset_id,
                "ingestion_id": ingestion_id,
                "status": result.get("status"),
                "rows_ingested": result.get("row_count"),
                "error": result.get("error"),
            }
        except Exception as e:
            get_tracker().record_call(
                "get_refresh_status",
                {"dataset_id": dataset_id, "ingestion_id": ingestion_id},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def list_recent_refreshes(dataset_id: str, limit: int = 5) -> dict:
        """List recent SPICE refresh history for a dataset.

        Args:
            dataset_id: The QuickSight dataset ID.
            limit: Maximum number of recent refreshes to return (default 5).

        Useful for checking if a dataset is refreshing normally, diagnosing
        failures, or finding previous ingestion IDs.
        """
        start = time.time()
        client = get_client()
        try:
            refreshes = client.list_recent_refreshes(dataset_id, limit=limit)
            get_tracker().record_call(
                "list_recent_refreshes",
                {"dataset_id": dataset_id, "limit": limit},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "dataset_id": dataset_id,
                "count": len(refreshes),
                "refreshes": refreshes,
            }
        except Exception as e:
            get_tracker().record_call(
                "list_recent_refreshes",
                {"dataset_id": dataset_id, "limit": limit},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def create_dataset(
        name: str, sql: str, data_source_arn: str, import_mode: str = "SPICE"
    ) -> dict:
        """Create a new QuickSight dataset from a SQL query.

        Args:
            name: Human-readable dataset name.
            sql: The SQL query for the dataset. Must be valid SQL for the
                 target data source (e.g., Snowflake, Redshift).
            data_source_arn: ARN of the QuickSight data source to query.
                             Find this in the QuickSight console or via AWS CLI.
            import_mode: 'SPICE' (cached, default) or 'DIRECT_QUERY' (live).

        Returns the new dataset ID. After creating a SPICE dataset, call
        refresh_dataset to load data.
        """
        start = time.time()
        client = get_client()
        try:
            dataset_id = client.create_dataset(
                name=name,
                sql=sql,
                data_source_arn=data_source_arn,
                import_mode=import_mode,
            )
            get_tracker().record_call(
                "create_dataset",
                {"name": name},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "status": "created",
                "dataset_id": dataset_id,
                "name": name,
                "import_mode": import_mode,
                "note": (
                    "Dataset created. If import_mode is SPICE, call "
                    "refresh_dataset to load data."
                ),
            }
        except Exception as e:
            get_tracker().record_call(
                "create_dataset",
                {"name": name},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def update_dataset_definition(dataset_id: str, definition_json: str) -> dict:
        """Update full dataset definition from JSON.

        WARNING: This replaces the entire dataset definition. A backup is
        created automatically before the update.

        Args:
            dataset_id: The QuickSight dataset ID.
            definition_json: JSON string containing the full dataset definition.
                Must include PhysicalTableMap, LogicalTableMap, ImportMode.
                Obtain the current definition from get_dataset first.

        Use this for structural changes (adding joins, calculated columns,
        changing column types) that go beyond simple SQL updates.
        """
        start = time.time()
        client = get_client()
        try:
            definition = json.loads(definition_json)
            client.update_dataset_definition(
                dataset_id, definition, backup_first=True
            )
            get_tracker().record_call(
                "update_dataset_definition",
                {"dataset_id": dataset_id},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "status": "success",
                "dataset_id": dataset_id,
                "backup_created": True,
                "note": (
                    "Dataset definition updated. If this is a SPICE dataset, "
                    "call refresh_dataset to reload data."
                ),
            }
        except json.JSONDecodeError as e:
            get_tracker().record_call(
                "update_dataset_definition",
                {"dataset_id": dataset_id},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": f"Invalid JSON: {e}"}
        except Exception as e:
            get_tracker().record_call(
                "update_dataset_definition",
                {"dataset_id": dataset_id},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def cancel_refresh(dataset_id: str, ingestion_id: str) -> dict:
        """Cancel a running SPICE dataset refresh.

        Use this to stop a SPICE ingestion that is stuck in QUEUED or RUNNING
        state. Useful when old ingestions block new ones.

        Args:
            dataset_id: The QuickSight dataset ID.
            ingestion_id: The ingestion ID to cancel (from refresh_dataset
                          or list_recent_refreshes).
        """
        start = time.time()
        client = get_client()
        try:
            client.cancel_refresh(dataset_id, ingestion_id)
            get_tracker().record_call(
                "cancel_refresh",
                {"dataset_id": dataset_id, "ingestion_id": ingestion_id},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "status": "cancelled",
                "dataset_id": dataset_id,
                "ingestion_id": ingestion_id,
                "note": "Ingestion cancelled. You can now trigger a new refresh.",
            }
        except Exception as e:
            get_tracker().record_call(
                "cancel_refresh",
                {"dataset_id": dataset_id, "ingestion_id": ingestion_id},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}

    @mcp.tool
    def modify_dataset_sql(dataset_id: str, find: str, replace: str) -> dict:
        """Find and replace text in a dataset's SQL query.

        Convenience tool that reads the current SQL, applies a string
        replacement, and updates the dataset. A backup is created automatically.

        Args:
            dataset_id: The QuickSight dataset ID.
            find: Exact text to search for in the current SQL.
            replace: Replacement text.

        Raises an error if the find text is not present in the current SQL.
        After modifying a SPICE dataset, call refresh_dataset to reload data.
        """
        start = time.time()
        client = get_client()
        try:
            client.modify_dataset_sql(
                dataset_id, find, replace, backup_first=True
            )
            get_tracker().record_call(
                "modify_dataset_sql",
                {"dataset_id": dataset_id, "find_len": len(find)},
                (time.time() - start) * 1000,
                True,
            )
            return {
                "status": "success",
                "dataset_id": dataset_id,
                "backup_created": True,
                "find": find[:100] + ("..." if len(find) > 100 else ""),
                "replace": replace[:100] + ("..." if len(replace) > 100 else ""),
                "note": (
                    "SQL updated. If this is a SPICE dataset, call "
                    "refresh_dataset to reload data."
                ),
            }
        except Exception as e:
            get_tracker().record_call(
                "modify_dataset_sql",
                {"dataset_id": dataset_id, "find_len": len(find)},
                (time.time() - start) * 1000,
                False,
                str(e),
            )
            return {"error": str(e)}
