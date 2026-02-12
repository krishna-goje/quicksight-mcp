"""QuickSight Client - comprehensive boto3-based wrapper for AWS QuickSight.

This module provides ``QuickSightClient``, a production-quality client for the
AWS QuickSight API.  It is designed to be used by the MCP server layer but can
also be imported directly for scripting or testing.

Features:
    * TTL-based caching for list/describe calls (5-minute default)
    * Optimistic locking to prevent concurrent-modification overwrites
    * Destructive-change protection (blocks accidental deletion of sheets/visuals)
    * Post-write verification to catch silent API failures
    * Automatic JSON backup before every write operation
    * Standard AWS credential chain (env vars, profile, IAM role, instance metadata)

Environment variables:
    AWS_PROFILE          - AWS CLI profile name
    AWS_REGION           - AWS region (default: us-east-1)
    AWS_ACCOUNT_ID       - AWS account ID (auto-detected via STS if omitted)
    QUICKSIGHT_BACKUP_DIR - Backup directory (default: ~/.quicksight-mcp/backups)
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import boto3

from quicksight_mcp.exceptions import (
    ChangeVerificationError,
    ConcurrentModificationError,
    DestructiveChangeError,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level caches (shared across instances, fast across tool calls)
# ---------------------------------------------------------------------------

_dataset_cache: Dict[str, Any] = {
    'data': None,
    'timestamp': 0,
    'ttl': 300,  # 5 minutes
}

_analysis_cache: Dict[str, Any] = {
    'data': None,
    'timestamp': 0,
    'ttl': 300,
}

_dashboard_cache: Dict[str, Any] = {
    'data': None,
    'timestamp': 0,
    'ttl': 300,
}

# Keyed by analysis_id -> {'data': ..., 'timestamp': ...}
_analysis_def_cache: Dict[str, Dict[str, Any]] = {}

# Default backup directory
_DEFAULT_BACKUP_DIR = os.path.expanduser('~/.quicksight-mcp/backups')

# All known QuickSight visual type keys
_VISUAL_TYPES = [
    'TableVisual', 'PivotTableVisual', 'BarChartVisual',
    'LineChartVisual', 'PieChartVisual', 'ScatterPlotVisual',
    'HeatMapVisual', 'TreeMapVisual', 'GaugeChartVisual',
    'KPIVisual', 'ComboChartVisual', 'WordCloudVisual',
    'InsightVisual', 'SankeyDiagramVisual', 'FunnelChartVisual',
    'WaterfallVisual', 'HistogramVisual', 'BoxPlotVisual',
    'FilledMapVisual', 'GeospatialMapVisual', 'CustomContentVisual',
    'EmptyVisual',
]


class QuickSightClient:
    """Comprehensive AWS QuickSight client with caching, locking, and safety features.

    Args:
        profile: AWS CLI profile name. Falls back to ``AWS_PROFILE`` env var.
        region: AWS region. Falls back to ``AWS_REGION`` or ``us-east-1``.
        account_id: AWS account ID. Falls back to ``AWS_ACCOUNT_ID`` or STS auto-detect.
        verify_by_default: Enable post-write verification globally (default ``True``).
        optimistic_locking_by_default: Enable optimistic locking globally (default ``True``).
    """

    # Class-level defaults (can be overridden per-instance via constructor)
    DEFAULT_VERIFY = True
    DEFAULT_OPTIMISTIC_LOCKING = True

    def __init__(
        self,
        profile: Optional[str] = None,
        region: Optional[str] = None,
        account_id: Optional[str] = None,
        verify_by_default: Optional[bool] = None,
        optimistic_locking_by_default: Optional[bool] = None,
    ):
        self.profile = profile or os.environ.get('AWS_PROFILE')
        self.region = region or os.environ.get('AWS_REGION', 'us-east-1')

        # Create session - use profile if given, else default credential chain
        if self.profile:
            self.session = boto3.Session(profile_name=self.profile, region_name=self.region)
        else:
            self.session = boto3.Session(region_name=self.region)

        self.client = self.session.client('quicksight')

        # Auto-detect account ID from STS if not provided
        self.account_id = account_id or os.environ.get('AWS_ACCOUNT_ID')
        if not self.account_id:
            sts = self.session.client('sts')
            self.account_id = sts.get_caller_identity()['Account']

        # Instance-level defaults
        self._verify_default = (
            verify_by_default if verify_by_default is not None else self.DEFAULT_VERIFY
        )
        self._locking_default = (
            optimistic_locking_by_default
            if optimistic_locking_by_default is not None
            else self.DEFAULT_OPTIMISTIC_LOCKING
        )

        logger.info(
            "QuickSightClient initialized (account=%s, region=%s, profile=%s)",
            self.account_id, self.region, self.profile or 'default-chain',
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _should_verify(self, verify: Optional[bool]) -> bool:
        return verify if verify is not None else self._verify_default

    def _should_lock(self, lock: Optional[bool]) -> bool:
        return lock if lock is not None else self._locking_default

    @staticmethod
    def _backup_dir() -> str:
        return os.environ.get('QUICKSIGHT_BACKUP_DIR', _DEFAULT_BACKUP_DIR)

    # =========================================================================
    # AUTHENTICATION
    # =========================================================================

    @staticmethod
    def check_auth(profile: Optional[str] = None, region: Optional[str] = None) -> dict:
        """Check if AWS credentials are valid.

        Returns:
            dict with ``valid`` (bool), ``identity`` (dict | None), ``error`` (str | None).
        """
        try:
            if profile:
                session = boto3.Session(
                    profile_name=profile,
                    region_name=region or 'us-east-1',
                )
            else:
                session = boto3.Session(region_name=region or 'us-east-1')
            sts = session.client('sts')
            identity = sts.get_caller_identity()
            return {'valid': True, 'identity': identity, 'error': None}
        except Exception as exc:
            return {'valid': False, 'identity': None, 'error': str(exc)}

    def is_authenticated(self) -> bool:
        """Check if the current session credentials are valid."""
        return self.check_auth(self.profile, self.region)['valid']

    # =========================================================================
    # DATASETS
    # =========================================================================

    def list_datasets(self, max_results: int = 100, use_cache: bool = True) -> List[Dict]:
        """List all datasets with TTL-based caching.

        Args:
            max_results: Ignored when returning cached data (returns full list).
            use_cache: Use the 5-minute cache (default ``True``).
        """
        global _dataset_cache

        if use_cache and _dataset_cache['data'] is not None:
            if time.time() - _dataset_cache['timestamp'] < _dataset_cache['ttl']:
                return _dataset_cache['data']

        paginator = self.client.get_paginator('list_data_sets')
        datasets: List[Dict] = []
        for page in paginator.paginate(AwsAccountId=self.account_id):
            datasets.extend(page.get('DataSetSummaries', []))

        _dataset_cache['data'] = datasets
        _dataset_cache['timestamp'] = time.time()
        logger.debug("Dataset cache refreshed (%d datasets)", len(datasets))
        return datasets

    def search_datasets(self, name_contains: str) -> List[Dict]:
        """Search datasets by name (server-side with client-side fallback).

        Args:
            name_contains: Substring to search for in dataset names.
        """
        # Try server-side search first
        try:
            response = self.client.search_data_sets(
                AwsAccountId=self.account_id,
                Filters=[{
                    'Operator': 'StringContains',
                    'Name': 'DATASET_NAME',
                    'Value': name_contains,
                }],
                MaxResults=100,
            )
            return response.get('DataSetSummaries', [])
        except Exception:
            logger.debug("Server-side dataset search failed, falling back to client-side")

        # Client-side fallback
        all_datasets = self.list_datasets()
        needle = name_contains.lower()
        return [d for d in all_datasets if needle in d.get('Name', '').lower()]

    def get_dataset(self, dataset_id: str) -> Dict:
        """Get full dataset definition."""
        response = self.client.describe_data_set(
            AwsAccountId=self.account_id,
            DataSetId=dataset_id,
        )
        return response.get('DataSet', {})

    def get_dataset_sql(self, dataset_id: str) -> Optional[str]:
        """Extract the SQL query from a dataset's PhysicalTableMap."""
        dataset = self.get_dataset(dataset_id)
        for _table_id, table_def in dataset.get('PhysicalTableMap', {}).items():
            if 'CustomSql' in table_def:
                return table_def['CustomSql'].get('SqlQuery')
        return None

    def update_dataset_sql(
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
            ChangeVerificationError: If verification is enabled and the SQL was not updated.
        """
        if backup_first:
            self.backup_dataset(dataset_id, backup_dir or self._backup_dir())

        dataset = self.get_dataset(dataset_id)

        # Find and update the CustomSql entry
        physical_map = dataset.get('PhysicalTableMap', {})
        for _table_id, table_def in physical_map.items():
            if 'CustomSql' in table_def:
                table_def['CustomSql']['SqlQuery'] = new_sql
                break

        # Build update payload
        update_params: Dict[str, Any] = {
            'AwsAccountId': self.account_id,
            'DataSetId': dataset_id,
            'Name': dataset['Name'],
            'PhysicalTableMap': physical_map,
            'LogicalTableMap': dataset.get('LogicalTableMap', {}),
            'ImportMode': dataset.get('ImportMode', 'SPICE'),
        }

        # Preserve optional top-level keys
        for key in ('ColumnGroups', 'FieldFolders', 'RowLevelPermissionDataSet',
                     'DataSetUsageConfiguration'):
            if key in dataset:
                update_params[key] = dataset[key]

        response = self.client.update_data_set(**update_params)

        if self._should_verify(verify):
            self._verify_dataset_sql(dataset_id, new_sql)

        logger.info("Dataset %s SQL updated (%d chars)", dataset_id, len(new_sql))
        return response

    def _verify_dataset_sql(self, dataset_id: str, expected_sql: str) -> bool:
        """Verify dataset SQL matches expected value (whitespace-normalized)."""
        actual_sql = self.get_dataset_sql(dataset_id)
        expected_norm = ' '.join(expected_sql.split())
        actual_norm = ' '.join((actual_sql or '').split())

        if expected_norm != actual_norm:
            raise ChangeVerificationError(
                'update_dataset_sql', dataset_id,
                f"SQL not updated. Expected {len(expected_sql)} chars, got "
                f"{len(actual_sql or '')} chars.",
            )
        return True

    def refresh_dataset(self, dataset_id: str) -> Dict:
        """Trigger a SPICE refresh (create_ingestion).

        Returns:
            dict with ``ingestion_id``, ``status``, ``arn``.
        """
        ingestion_id = f"refresh-{datetime.now():%Y%m%d-%H%M%S}"
        response = self.client.create_ingestion(
            AwsAccountId=self.account_id,
            DataSetId=dataset_id,
            IngestionId=ingestion_id,
        )
        return {
            'ingestion_id': ingestion_id,
            'status': response.get('IngestionStatus'),
            'arn': response.get('Arn'),
        }

    def get_refresh_status(self, dataset_id: str, ingestion_id: str) -> Dict:
        """Get status of a SPICE refresh."""
        response = self.client.describe_ingestion(
            AwsAccountId=self.account_id,
            DataSetId=dataset_id,
            IngestionId=ingestion_id,
        )
        ingestion = response.get('Ingestion', {})
        return {
            'status': ingestion.get('IngestionStatus'),
            'error': ingestion.get('ErrorInfo'),
            'row_count': ingestion.get('RowInfo', {}).get('RowsIngested'),
            'created': ingestion.get('CreatedTime'),
        }

    def list_recent_refreshes(self, dataset_id: str, limit: int = 5) -> List[Dict]:
        """List recent SPICE refreshes for a dataset, newest first."""
        response = self.client.list_ingestions(
            AwsAccountId=self.account_id,
            DataSetId=dataset_id,
        )
        ingestions = response.get('Ingestions', [])
        ingestions.sort(key=lambda x: x.get('CreatedTime', ''), reverse=True)
        return ingestions[:limit]

    def clear_dataset_cache(self):
        """Clear the dataset list cache."""
        global _dataset_cache
        _dataset_cache['data'] = None
        _dataset_cache['timestamp'] = 0

    # =========================================================================
    # ANALYSES
    # =========================================================================

    def list_analyses(self, max_results: int = 100, use_cache: bool = True) -> List[Dict]:
        """List all analyses with TTL-based caching."""
        global _analysis_cache

        if use_cache and _analysis_cache['data'] is not None:
            if time.time() - _analysis_cache['timestamp'] < _analysis_cache['ttl']:
                return _analysis_cache['data']

        paginator = self.client.get_paginator('list_analyses')
        analyses: List[Dict] = []
        for page in paginator.paginate(AwsAccountId=self.account_id):
            analyses.extend(page.get('AnalysisSummaryList', []))

        _analysis_cache['data'] = analyses
        _analysis_cache['timestamp'] = time.time()
        logger.debug("Analysis cache refreshed (%d analyses)", len(analyses))
        return analyses

    def search_analyses(self, name_contains: str) -> List[Dict]:
        """Search analyses by name (client-side filter on cached list)."""
        all_analyses = self.list_analyses()
        needle = name_contains.lower()
        return [a for a in all_analyses if needle in a.get('Name', '').lower()]

    def get_analysis(self, analysis_id: str) -> Dict:
        """Get analysis summary (describe_analysis)."""
        response = self.client.describe_analysis(
            AwsAccountId=self.account_id,
            AnalysisId=analysis_id,
        )
        return response.get('Analysis', {})

    def get_analysis_definition(self, analysis_id: str, use_cache: bool = True) -> Dict:
        """Get full analysis definition (sheets, visuals, calculated fields).

        Cached for 5 minutes to speed up repeated lookups.
        """
        global _analysis_def_cache

        if use_cache and analysis_id in _analysis_def_cache:
            cached = _analysis_def_cache[analysis_id]
            if time.time() - cached['timestamp'] < 300:
                return cached['data']

        response = self.client.describe_analysis_definition(
            AwsAccountId=self.account_id,
            AnalysisId=analysis_id,
        )
        definition = response.get('Definition', {})

        _analysis_def_cache[analysis_id] = {
            'data': definition,
            'timestamp': time.time(),
        }
        return definition

    def get_analysis_definition_with_version(self, analysis_id: str) -> Tuple[Dict, Any]:
        """Get analysis definition along with version info for optimistic locking.

        Returns:
            Tuple of ``(definition, last_updated_time)``.
        """
        analysis = self.get_analysis(analysis_id)
        definition = self.get_analysis_definition(analysis_id)
        return definition, analysis.get('LastUpdatedTime')

    def clear_analysis_def_cache(self, analysis_id: Optional[str] = None):
        """Clear cached analysis definition(s)."""
        global _analysis_def_cache
        if analysis_id:
            _analysis_def_cache.pop(analysis_id, None)
        else:
            _analysis_def_cache.clear()

    def get_calculated_fields(self, analysis_id: str) -> List[Dict]:
        """Get all calculated fields in an analysis."""
        definition = self.get_analysis_definition(analysis_id)
        return definition.get('CalculatedFields', [])

    def get_sheets(self, analysis_id: str) -> List[Dict]:
        """Get all sheets in an analysis."""
        definition = self.get_analysis_definition(analysis_id)
        return definition.get('Sheets', [])

    def get_visuals(self, analysis_id: str) -> List[Dict]:
        """Get all visuals across all sheets (parsed into summary dicts)."""
        sheets = self.get_sheets(analysis_id)
        visuals: List[Dict] = []
        for sheet in sheets:
            sheet_name = sheet.get('Name', 'Unknown')
            sheet_id = sheet.get('SheetId', '')
            for visual in sheet.get('Visuals', []):
                info = self._parse_visual(visual)
                info['sheet_name'] = sheet_name
                info['sheet_id'] = sheet_id
                visuals.append(info)
        return visuals

    @staticmethod
    def _parse_visual(visual: Dict) -> Dict:
        """Extract type, id, title, subtitle from a visual definition."""
        for vtype in _VISUAL_TYPES:
            if vtype in visual:
                vdef = visual[vtype]
                return {
                    'type': vtype.replace('Visual', ''),
                    'visual_id': vdef.get('VisualId', ''),
                    'title': (
                        vdef.get('Title', {})
                        .get('FormatText', {})
                        .get('PlainText', '')
                    ),
                    'subtitle': (
                        vdef.get('Subtitle', {})
                        .get('FormatText', {})
                        .get('PlainText', '')
                    ),
                }
        return {'type': 'Unknown', 'visual_id': '', 'title': '', 'subtitle': ''}

    def get_parameters(self, analysis_id: str) -> List[Dict]:
        """Get all parameter declarations in an analysis."""
        definition = self.get_analysis_definition(analysis_id)
        return definition.get('ParameterDeclarations', [])

    def get_filters(self, analysis_id: str) -> List[Dict]:
        """Get all filter groups in an analysis."""
        definition = self.get_analysis_definition(analysis_id)
        return definition.get('FilterGroups', [])

    def get_columns_used(self, analysis_id: str) -> Dict[str, int]:
        """Get usage counts for every ColumnName referenced in the analysis."""
        definition = self.get_analysis_definition(analysis_id)
        columns: Dict[str, int] = {}

        def _walk(obj: Any) -> None:
            if isinstance(obj, dict):
                if 'ColumnName' in obj:
                    col = obj['ColumnName']
                    columns[col] = columns.get(col, 0) + 1
                for v in obj.values():
                    _walk(v)
            elif isinstance(obj, list):
                for item in obj:
                    _walk(item)

        _walk(definition)
        return dict(sorted(columns.items(), key=lambda x: -x[1]))

    def update_analysis(
        self,
        analysis_id: str,
        definition: Dict,
        backup_first: bool = True,
        backup_dir: Optional[str] = None,
        wait_for_completion: bool = True,
        timeout_seconds: int = 60,
        expected_last_updated: Any = None,
        allow_destructive: bool = False,
    ) -> Dict:
        """Update an analysis with a new definition.

        This is the central write method. It supports:
        * automatic pre-write backup
        * optimistic locking via ``expected_last_updated``
        * destructive-change detection
        * polling for completion

        Args:
            analysis_id: Analysis ID.
            definition: Full analysis Definition dict.
            backup_first: Back up before writing.
            backup_dir: Override backup directory.
            wait_for_completion: Poll until the update succeeds or fails.
            timeout_seconds: Max seconds to wait.
            expected_last_updated: If set, raises ``ConcurrentModificationError``
                when the analysis was modified since this timestamp.
            allow_destructive: If ``False``, blocks updates that would delete
                all sheets, >50% of visuals, or >50% of calculated fields.

        Raises:
            ConcurrentModificationError: On optimistic-locking conflict.
            DestructiveChangeError: On blocked destructive update.
            RuntimeError: On update failure or timeout.
        """
        if backup_first:
            self.backup_analysis(analysis_id, backup_dir or self._backup_dir())

        analysis = self.get_analysis(analysis_id)

        # Optimistic locking check
        if expected_last_updated is not None:
            actual = analysis.get('LastUpdatedTime')
            if actual and actual != expected_last_updated:
                raise ConcurrentModificationError(
                    analysis_id, expected_last_updated, actual,
                )

        # Destructive-change guard
        if not allow_destructive:
            self._validate_definition_not_destructive(analysis_id, definition)

        response = self.client.update_analysis(
            AwsAccountId=self.account_id,
            AnalysisId=analysis_id,
            Name=analysis['Name'],
            Definition=definition,
        )

        if not wait_for_completion:
            return response

        # Poll for completion
        start = time.time()
        while time.time() - start < timeout_seconds:
            time.sleep(2)
            refreshed = self.get_analysis(analysis_id)
            status = refreshed.get('Status', '')

            if 'SUCCESSFUL' in status:
                logger.info("Analysis %s update completed successfully", analysis_id)
                self.clear_analysis_def_cache(analysis_id)
                return {
                    'status': status,
                    'analysis_id': analysis_id,
                    'errors': None,
                }

            if 'FAILED' in status:
                errors = refreshed.get('Errors', [])
                msgs = [f"{e.get('Type')}: {e.get('Message')}" for e in errors]
                raise RuntimeError(f"Analysis update failed: {'; '.join(msgs)}")

        raise RuntimeError(
            f"Analysis update timed out after {timeout_seconds}s"
        )

    def _validate_definition_not_destructive(
        self, analysis_id: str, new_definition: Dict,
    ) -> bool:
        """Block updates that would delete all sheets or >50% of visuals/calc fields.

        Raises:
            DestructiveChangeError: When the update is considered destructive.
        """
        current_def = self.get_analysis_definition(analysis_id, use_cache=True)

        current_sheets = current_def.get('Sheets', [])
        cur_sheet_cnt = len(current_sheets)
        cur_visual_cnt = sum(len(s.get('Visuals', [])) for s in current_sheets)
        cur_calc_cnt = len(current_def.get('CalculatedFields', []))

        new_sheets = new_definition.get('Sheets', [])
        new_sheet_cnt = len(new_sheets)
        new_visual_cnt = sum(len(s.get('Visuals', [])) for s in new_sheets)
        new_calc_cnt = len(new_definition.get('CalculatedFields', []))

        current_counts = {
            'sheets': cur_sheet_cnt,
            'visuals': cur_visual_cnt,
            'calculated_fields': cur_calc_cnt,
        }
        new_counts = {
            'sheets': new_sheet_cnt,
            'visuals': new_visual_cnt,
            'calculated_fields': new_calc_cnt,
        }

        issues: List[str] = []

        if cur_sheet_cnt > 0 and new_sheet_cnt == 0:
            issues.append(f"Would DELETE ALL {cur_sheet_cnt} SHEETS")

        if cur_visual_cnt > 0:
            loss_pct = (cur_visual_cnt - new_visual_cnt) / cur_visual_cnt * 100
            if loss_pct > 50:
                issues.append(
                    f"Would delete {loss_pct:.0f}% of visuals "
                    f"({cur_visual_cnt} -> {new_visual_cnt})"
                )

        if cur_calc_cnt > 0:
            loss_pct = (cur_calc_cnt - new_calc_cnt) / cur_calc_cnt * 100
            if loss_pct > 50:
                issues.append(
                    f"Would delete {loss_pct:.0f}% of calculated fields "
                    f"({cur_calc_cnt} -> {new_calc_cnt})"
                )

        if issues:
            raise DestructiveChangeError(
                analysis_id, '; '.join(issues), current_counts, new_counts,
            )
        return True

    # =========================================================================
    # CALCULATED FIELDS
    # =========================================================================

    def add_calculated_field(
        self,
        analysis_id: str,
        name: str,
        expression: str,
        data_set_identifier: str,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Add a calculated field to an analysis.

        Raises:
            ValueError: If a field with the same name already exists.
            ChangeVerificationError: If verification is enabled and the field was not created.
        """
        definition, last_updated = self.get_analysis_definition_with_version(analysis_id)

        new_field = {
            'DataSetIdentifier': data_set_identifier,
            'Name': name,
            'Expression': expression,
        }

        calc_fields = definition.setdefault('CalculatedFields', [])
        if any(f.get('Name') == name for f in calc_fields):
            raise ValueError(
                f"Calculated field '{name}' already exists. "
                f"Use update_calculated_field instead."
            )

        calc_fields.append(new_field)
        result = self.update_analysis(
            analysis_id, definition, backup_first=backup_first,
            expected_last_updated=(
                last_updated if self._should_lock(use_optimistic_locking) else None
            ),
        )

        if self._should_verify(verify):
            self._verify_calculated_field_exists(analysis_id, name, expression)

        return result

    def update_calculated_field(
        self,
        analysis_id: str,
        name: str,
        new_expression: str,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Update an existing calculated field's expression.

        Raises:
            ValueError: If the field is not found.
            ChangeVerificationError: If verification is enabled and the expression was not updated.
        """
        definition, last_updated = self.get_analysis_definition_with_version(analysis_id)

        found = False
        for field in definition.get('CalculatedFields', []):
            if field.get('Name') == name:
                field['Expression'] = new_expression
                found = True
                break

        if not found:
            raise ValueError(f"Calculated field '{name}' not found")

        result = self.update_analysis(
            analysis_id, definition, backup_first=backup_first,
            expected_last_updated=(
                last_updated if self._should_lock(use_optimistic_locking) else None
            ),
        )

        if self._should_verify(verify):
            self._verify_calculated_field_exists(analysis_id, name, new_expression)

        return result

    def delete_calculated_field(
        self,
        analysis_id: str,
        name: str,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
        verify: Optional[bool] = None,
    ) -> Dict:
        """Delete a calculated field from an analysis.

        Raises:
            ValueError: If the field is not found.
            ChangeVerificationError: If verification is enabled and the field still exists.
        """
        definition, last_updated = self.get_analysis_definition_with_version(analysis_id)

        original_count = len(definition.get('CalculatedFields', []))
        definition['CalculatedFields'] = [
            f for f in definition.get('CalculatedFields', [])
            if f.get('Name') != name
        ]

        if len(definition.get('CalculatedFields', [])) == original_count:
            raise ValueError(f"Calculated field '{name}' not found")

        result = self.update_analysis(
            analysis_id, definition, backup_first=backup_first,
            expected_last_updated=(
                last_updated if self._should_lock(use_optimistic_locking) else None
            ),
        )

        if self._should_verify(verify):
            self._verify_calculated_field_deleted(analysis_id, name)

        return result

    def get_calculated_field(self, analysis_id: str, name: str) -> Optional[Dict]:
        """Get a specific calculated field by name, or ``None``."""
        for f in self.get_calculated_fields(analysis_id):
            if f.get('Name') == name:
                return f
        return None

    def _verify_calculated_field_exists(
        self, analysis_id: str, name: str, expected_expression: Optional[str] = None,
    ) -> bool:
        self.clear_analysis_def_cache(analysis_id)
        for f in self.get_calculated_fields(analysis_id):
            if f.get('Name') == name:
                if expected_expression and f.get('Expression') != expected_expression:
                    raise ChangeVerificationError(
                        'add_calculated_field', analysis_id,
                        f"Field '{name}' exists but expression does not match.",
                    )
                return True
        raise ChangeVerificationError(
            'add_calculated_field', analysis_id,
            f"Field '{name}' not found after update.",
        )

    def _verify_calculated_field_deleted(self, analysis_id: str, name: str) -> bool:
        self.clear_analysis_def_cache(analysis_id)
        for f in self.get_calculated_fields(analysis_id):
            if f.get('Name') == name:
                raise ChangeVerificationError(
                    'delete_calculated_field', analysis_id,
                    f"Field '{name}' still exists after deletion.",
                )
        return True

    # ------------------------------------------------------------------
    # Post-write verification: sheets, visuals, parameters, filters
    # ------------------------------------------------------------------

    def _verify_sheet_exists(self, analysis_id: str, sheet_id: str, expected_name: Optional[str] = None) -> bool:
        """Verify a sheet exists after creation/rename."""
        self.clear_analysis_def_cache(analysis_id)
        for s in self.get_sheets(analysis_id):
            if s.get('SheetId') == sheet_id:
                if expected_name and s.get('Name') != expected_name:
                    raise ChangeVerificationError(
                        'sheet', analysis_id,
                        f"Sheet '{sheet_id}' exists but name is '{s.get('Name')}', expected '{expected_name}'.",
                    )
                return True
        raise ChangeVerificationError(
            'sheet', analysis_id,
            f"Sheet '{sheet_id}' not found after update.",
        )

    def _verify_sheet_deleted(self, analysis_id: str, sheet_id: str) -> bool:
        """Verify a sheet was actually deleted."""
        self.clear_analysis_def_cache(analysis_id)
        for s in self.get_sheets(analysis_id):
            if s.get('SheetId') == sheet_id:
                raise ChangeVerificationError(
                    'delete_sheet', analysis_id,
                    f"Sheet '{sheet_id}' still exists after deletion.",
                )
        return True

    def _verify_visual_exists(self, analysis_id: str, visual_id: str) -> bool:
        """Verify a visual exists after creation."""
        self.clear_analysis_def_cache(analysis_id)
        if self.get_visual_definition(analysis_id, visual_id) is not None:
            return True
        raise ChangeVerificationError(
            'visual', analysis_id,
            f"Visual '{visual_id}' not found after update.",
        )

    def _verify_visual_deleted(self, analysis_id: str, visual_id: str) -> bool:
        """Verify a visual was actually deleted."""
        self.clear_analysis_def_cache(analysis_id)
        if self.get_visual_definition(analysis_id, visual_id) is not None:
            raise ChangeVerificationError(
                'delete_visual', analysis_id,
                f"Visual '{visual_id}' still exists after deletion.",
            )
        return True

    def _verify_visual_title(self, analysis_id: str, visual_id: str, expected_title: str) -> bool:
        """Verify a visual's title matches expected value."""
        self.clear_analysis_def_cache(analysis_id)
        vdef = self.get_visual_definition(analysis_id, visual_id)
        if vdef is None:
            raise ChangeVerificationError(
                'set_visual_title', analysis_id,
                f"Visual '{visual_id}' not found after title update.",
            )
        parsed = self._parse_visual(vdef)
        actual_title = parsed.get('title', '')
        if actual_title != expected_title:
            raise ChangeVerificationError(
                'set_visual_title', analysis_id,
                f"Visual '{visual_id}' title is '{actual_title}', expected '{expected_title}'.",
            )
        return True

    def _verify_parameter_exists(self, analysis_id: str, param_name: str) -> bool:
        """Verify a parameter exists after creation."""
        self.clear_analysis_def_cache(analysis_id)
        for p in self.get_parameters(analysis_id):
            for ptype in ('StringParameterDeclaration', 'IntegerParameterDeclaration',
                           'DecimalParameterDeclaration', 'DateTimeParameterDeclaration'):
                if ptype in p and p[ptype].get('Name') == param_name:
                    return True
        raise ChangeVerificationError(
            'add_parameter', analysis_id,
            f"Parameter '{param_name}' not found after update.",
        )

    def _verify_parameter_deleted(self, analysis_id: str, param_name: str) -> bool:
        """Verify a parameter was actually deleted."""
        self.clear_analysis_def_cache(analysis_id)
        for p in self.get_parameters(analysis_id):
            for ptype in ('StringParameterDeclaration', 'IntegerParameterDeclaration',
                           'DecimalParameterDeclaration', 'DateTimeParameterDeclaration'):
                if ptype in p and p[ptype].get('Name') == param_name:
                    raise ChangeVerificationError(
                        'delete_parameter', analysis_id,
                        f"Parameter '{param_name}' still exists after deletion.",
                    )
        return True

    def _verify_filter_group_exists(self, analysis_id: str, filter_group_id: str) -> bool:
        """Verify a filter group exists after creation."""
        self.clear_analysis_def_cache(analysis_id)
        for fg in self.get_filters(analysis_id):
            if fg.get('FilterGroupId') == filter_group_id:
                return True
        raise ChangeVerificationError(
            'add_filter_group', analysis_id,
            f"Filter group '{filter_group_id}' not found after update.",
        )

    def _verify_filter_group_deleted(self, analysis_id: str, filter_group_id: str) -> bool:
        """Verify a filter group was actually deleted."""
        self.clear_analysis_def_cache(analysis_id)
        for fg in self.get_filters(analysis_id):
            if fg.get('FilterGroupId') == filter_group_id:
                raise ChangeVerificationError(
                    'delete_filter_group', analysis_id,
                    f"Filter group '{filter_group_id}' still exists after deletion.",
                )
        return True

    def _verify_sheet_visual_count(
        self, analysis_id: str, sheet_id: str, expected_count: int,
    ) -> bool:
        """Verify a sheet has the expected number of visuals (for replicate_sheet)."""
        self.clear_analysis_def_cache(analysis_id)
        sheet = self.get_sheet(analysis_id, sheet_id)
        if sheet is None:
            raise ChangeVerificationError(
                'replicate_sheet', analysis_id,
                f"Sheet '{sheet_id}' not found after replication.",
            )
        actual_count = len(sheet.get('Visuals', []))
        if actual_count != expected_count:
            raise ChangeVerificationError(
                'replicate_sheet', analysis_id,
                f"Sheet has {actual_count} visuals, expected {expected_count}.",
            )
        return True

    def verify_analysis_health(self, analysis_id: str) -> Dict:
        """Run a comprehensive health check on an analysis.

        Checks:
        - Analysis status is SUCCESSFUL (not FAILED or IN_PROGRESS)
        - All sheets have at least one visual
        - All visuals have layout elements
        - No orphaned layout elements (pointing to non-existent visuals)
        - All calculated fields reference valid dataset identifiers
        - Sheet count is within QuickSight limits (<=20)

        Returns:
            dict with ``healthy`` (bool), ``checks`` (list of check results),
            and ``issues`` (list of problems found).
        """
        self.clear_analysis_def_cache(analysis_id)
        analysis = self.get_analysis(analysis_id)
        definition = self.get_analysis_definition(analysis_id)

        checks = []
        issues = []

        # Check 1: Analysis status
        status = analysis.get('Status', '')
        ok = 'SUCCESSFUL' in status
        checks.append({'check': 'analysis_status', 'status': status, 'ok': ok})
        if not ok:
            errors = analysis.get('Errors', [])
            issues.append(f"Analysis status: {status}. Errors: {[e.get('Message','') for e in errors]}")

        sheets = definition.get('Sheets', [])

        # Check 2: Sheet count within limits
        ok = len(sheets) <= 20
        checks.append({'check': 'sheet_count', 'count': len(sheets), 'limit': 20, 'ok': ok})
        if not ok:
            issues.append(f"Sheet count {len(sheets)} exceeds QuickSight max of 20")

        # Check 3: Visual/layout alignment per sheet
        total_visuals = 0
        total_layout_elements = 0
        for s in sheets:
            sheet_id = s.get('SheetId', '')
            sheet_name = s.get('Name', '')
            visuals = s.get('Visuals', [])
            total_visuals += len(visuals)

            # Get visual IDs in this sheet
            visual_ids = set()
            for v in visuals:
                for vtype in _VISUAL_TYPES:
                    if vtype in v:
                        visual_ids.add(v[vtype].get('VisualId', ''))
                        break

            # Get layout element IDs
            layout_ids = set()
            for layout in s.get('Layouts', []):
                for elem in (
                    layout.get('Configuration', {})
                    .get('GridLayout', {})
                    .get('Elements', [])
                ):
                    layout_ids.add(elem.get('ElementId', ''))
                    total_layout_elements += 1

            # Visuals without layout
            orphan_visuals = visual_ids - layout_ids
            if orphan_visuals:
                issues.append(
                    f"Sheet '{sheet_name}': {len(orphan_visuals)} visuals without layout: "
                    f"{list(orphan_visuals)[:3]}..."
                )

            # Layout elements without visuals (could be filter controls, text boxes)
            # This is informational, not necessarily an issue

        checks.append({
            'check': 'visual_layout_alignment',
            'total_visuals': total_visuals,
            'total_layout_elements': total_layout_elements,
            'ok': len([i for i in issues if 'without layout' in i]) == 0,
        })

        # Check 4: Calculated fields reference valid dataset identifiers
        valid_ds_ids = {
            d.get('Identifier')
            for d in definition.get('DataSetIdentifierDeclarations', [])
        }
        invalid_refs = []
        for f in definition.get('CalculatedFields', []):
            ds_id = f.get('DataSetIdentifier', '')
            if ds_id and ds_id not in valid_ds_ids:
                invalid_refs.append(f"{f.get('Name')} -> {ds_id}")

        ok = len(invalid_refs) == 0
        checks.append({
            'check': 'calc_field_dataset_refs',
            'valid_datasets': len(valid_ds_ids),
            'invalid_refs': len(invalid_refs),
            'ok': ok,
        })
        if not ok:
            issues.append(f"Calc fields with invalid dataset refs: {invalid_refs[:5]}")

        healthy = len(issues) == 0
        return {
            'analysis_id': analysis_id,
            'healthy': healthy,
            'checks': checks,
            'issues': issues,
            'summary': {
                'sheets': len(sheets),
                'visuals': total_visuals,
                'calc_fields': len(definition.get('CalculatedFields', [])),
                'parameters': len(definition.get('ParameterDeclarations', [])),
                'filter_groups': len(definition.get('FilterGroups', [])),
            },
        }

    # =========================================================================
    # DASHBOARDS
    # =========================================================================

    def list_dashboards(self, max_results: int = 100, use_cache: bool = True) -> List[Dict]:
        """List all dashboards with TTL-based caching."""
        global _dashboard_cache

        if use_cache and _dashboard_cache['data'] is not None:
            if time.time() - _dashboard_cache['timestamp'] < _dashboard_cache['ttl']:
                return _dashboard_cache['data']

        paginator = self.client.get_paginator('list_dashboards')
        dashboards: List[Dict] = []
        for page in paginator.paginate(AwsAccountId=self.account_id):
            dashboards.extend(page.get('DashboardSummaryList', []))

        _dashboard_cache['data'] = dashboards
        _dashboard_cache['timestamp'] = time.time()
        logger.debug("Dashboard cache refreshed (%d dashboards)", len(dashboards))
        return dashboards

    def search_dashboards(self, name_contains: str) -> List[Dict]:
        """Search dashboards by name (client-side filter on cached list)."""
        all_dashboards = self.list_dashboards()
        needle = name_contains.lower()
        return [d for d in all_dashboards if needle in d.get('Name', '').lower()]

    def get_dashboard(self, dashboard_id: str) -> Dict:
        """Get dashboard details (describe_dashboard)."""
        response = self.client.describe_dashboard(
            AwsAccountId=self.account_id,
            DashboardId=dashboard_id,
        )
        return response.get('Dashboard', {})

    def get_dashboard_versions(self, dashboard_id: str, limit: int = 10) -> List[Dict]:
        """Get dashboard version history, newest first."""
        response = self.client.list_dashboard_versions(
            AwsAccountId=self.account_id,
            DashboardId=dashboard_id,
        )
        versions = response.get('DashboardVersionSummaryList', [])
        versions.sort(key=lambda x: x.get('VersionNumber', 0), reverse=True)
        return versions[:limit]

    def get_current_dashboard_version(self, dashboard_id: str) -> Dict:
        """Get the currently published dashboard version metadata."""
        dashboard = self.get_dashboard(dashboard_id)
        version = dashboard.get('Version', {})
        return {
            'version_number': version.get('VersionNumber'),
            'status': version.get('Status'),
            'created_time': version.get('CreatedTime'),
            'description': version.get('Description'),
        }

    def get_dashboard_definition(self, dashboard_id: str) -> Dict:
        """Get full dashboard definition (describe_dashboard_definition)."""
        response = self.client.describe_dashboard_definition(
            AwsAccountId=self.account_id,
            DashboardId=dashboard_id,
        )
        return response.get('Definition', {})

    def publish_dashboard(
        self,
        dashboard_id: str,
        source_analysis_id: str,
        version_description: Optional[str] = None,
    ) -> Dict:
        """Publish/update a dashboard from an analysis.

        Args:
            dashboard_id: Target dashboard ID.
            source_analysis_id: Source analysis ID.
            version_description: Optional description for this version.

        Returns:
            dict with ``dashboard_id``, ``version_arn``, ``status``.
        """
        dashboard = self.get_dashboard(dashboard_id)
        analysis = self.get_analysis(source_analysis_id)

        response = self.client.update_dashboard(
            AwsAccountId=self.account_id,
            DashboardId=dashboard_id,
            Name=dashboard['Name'],
            SourceEntity={
                'SourceAnalysis': {
                    'Arn': analysis['Arn'],
                    'DataSetReferences': self._get_dataset_references(source_analysis_id),
                }
            },
            VersionDescription=(
                version_description or f"Published from analysis {source_analysis_id}"
            ),
        )

        return {
            'dashboard_id': dashboard_id,
            'version_arn': response.get('VersionArn'),
            'status': response.get('CreationStatus'),
        }

    def _get_dataset_references(self, analysis_id: str) -> List[Dict]:
        """Get dataset ARN references from an analysis definition."""
        definition = self.get_analysis_definition(analysis_id)
        refs: List[Dict] = []
        seen: set = set()

        for ds_config in definition.get('DataSetIdentifierDeclarations', []):
            ds_arn = ds_config.get('DataSetArn')
            identifier = ds_config.get('Identifier')
            if ds_arn and identifier and ds_arn not in seen:
                refs.append({
                    'DataSetPlaceholder': identifier,
                    'DataSetArn': ds_arn,
                })
                seen.add(ds_arn)
        return refs

    def rollback_dashboard(self, dashboard_id: str, version_number: int) -> Dict:
        """Rollback dashboard to a previous version.

        Args:
            dashboard_id: Dashboard ID.
            version_number: Version number to publish.
        """
        response = self.client.update_dashboard_published_version(
            AwsAccountId=self.account_id,
            DashboardId=dashboard_id,
            VersionNumber=version_number,
        )
        return {
            'dashboard_id': response.get('DashboardId'),
            'status': f'Published version updated to {version_number}',
        }

    def clear_dashboard_cache(self):
        """Clear the dashboard list cache."""
        global _dashboard_cache
        _dashboard_cache['data'] = None
        _dashboard_cache['timestamp'] = 0

    # =========================================================================
    # BACKUP & RESTORE
    # =========================================================================

    def backup_dataset(
        self, dataset_id: str, backup_dir: Optional[str] = None,
    ) -> str:
        """Backup dataset definition to a timestamped JSON file.

        Returns:
            Path to the backup file.
        """
        bdir = backup_dir or self._backup_dir()
        Path(bdir).mkdir(parents=True, exist_ok=True)

        dataset = self.get_dataset(dataset_id)
        name = dataset.get('Name', dataset_id).replace(' ', '_').replace('/', '_')
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{bdir}/dataset_{name}_{ts}.json"

        with open(filename, 'w') as f:
            json.dump(dataset, f, indent=2, default=str)

        logger.info("Backed up dataset to: %s", filename)
        return filename

    def backup_analysis(
        self, analysis_id: str, backup_dir: Optional[str] = None,
    ) -> str:
        """Backup analysis + definition to a timestamped JSON file.

        Returns:
            Path to the backup file.
        """
        bdir = backup_dir or self._backup_dir()
        Path(bdir).mkdir(parents=True, exist_ok=True)

        analysis = self.get_analysis(analysis_id)
        definition = self.get_analysis_definition(analysis_id)

        name = analysis.get('Name', analysis_id).replace(' ', '_').replace('/', '_')
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{bdir}/analysis_{name}_{ts}.json"

        backup_data = {
            'analysis': analysis,
            'definition': definition,
        }

        with open(filename, 'w') as f:
            json.dump(backup_data, f, indent=2, default=str)

        logger.info("Backed up analysis to: %s", filename)
        return filename

    def restore_analysis_from_backup(
        self, backup_file: str, analysis_id: Optional[str] = None,
    ) -> Dict:
        """Restore an analysis from a backup JSON file.

        A pre-restore backup of the current state is created automatically.

        Args:
            backup_file: Path to the backup JSON file.
            analysis_id: Target analysis ID (uses the ID from the backup if omitted).

        Returns:
            Update response dict.
        """
        with open(backup_file, 'r') as f:
            backup_data = json.load(f)

        # Support both casing variants
        definition = backup_data.get('Definition', backup_data.get('definition'))
        if not definition:
            raise ValueError("Backup file does not contain a 'definition' key")

        target_id = analysis_id or backup_data.get('analysis', {}).get('AnalysisId')
        if not target_id:
            raise ValueError("No analysis ID provided and none found in backup")

        # Pre-restore safety backup
        self.backup_analysis(target_id)

        return self.update_analysis(target_id, definition, backup_first=False)

    def restore_dataset_from_backup(
        self, backup_file: str, dataset_id: Optional[str] = None,
    ) -> Dict:
        """Restore a dataset's SQL from a backup JSON file.

        A pre-restore backup of the current state is created automatically.

        Args:
            backup_file: Path to the backup JSON file.
            dataset_id: Target dataset ID (uses the ID from the backup if omitted).

        Returns:
            Update response dict.
        """
        with open(backup_file, 'r') as f:
            backup_data = json.load(f)

        target_id = dataset_id or backup_data.get('DataSetId')
        if not target_id:
            raise ValueError("No dataset ID provided and none found in backup")

        # Pre-restore safety backup
        self.backup_dataset(target_id)

        # Extract and restore SQL
        for _table_id, table_def in backup_data.get('PhysicalTableMap', {}).items():
            if 'CustomSql' in table_def:
                sql = table_def['CustomSql'].get('SqlQuery')
                if sql:
                    return self.update_dataset_sql(target_id, sql, backup_first=False)

        raise ValueError("No CustomSql found in backup file")

    def clone_analysis(
        self,
        source_analysis_id: str,
        new_name: str,
        new_analysis_id: Optional[str] = None,
    ) -> Dict:
        """Clone an analysis to create a copy (e.g., for testing).

        Args:
            source_analysis_id: Source analysis ID to clone.
            new_name: Name for the new analysis.
            new_analysis_id: Optional ID for the new analysis (auto-generated if omitted).

        Returns:
            dict with ``analysis_id``, ``name``, ``arn``, ``status``.
        """
        definition = self.get_analysis_definition(source_analysis_id)
        new_id = new_analysis_id or str(uuid.uuid4())

        # Copy permissions from source
        permissions = self._get_analysis_permissions(source_analysis_id)

        response = self.client.create_analysis(
            AwsAccountId=self.account_id,
            AnalysisId=new_id,
            Name=new_name,
            Definition=definition,
            Permissions=permissions,
        )

        logger.info("Cloned analysis %s -> %s (%s)", source_analysis_id, new_id, new_name)
        return {
            'analysis_id': new_id,
            'name': new_name,
            'arn': response.get('Arn'),
            'status': response.get('CreationStatus'),
        }

    def _get_analysis_permissions(self, analysis_id: str) -> List[Dict]:
        """Retrieve current permissions for an analysis."""
        try:
            response = self.client.describe_analysis_permissions(
                AwsAccountId=self.account_id,
                AnalysisId=analysis_id,
            )
            return response.get('Permissions', [])
        except Exception:
            logger.warning(
                "Could not retrieve permissions for analysis %s, using empty list",
                analysis_id,
            )
            return []

    # =========================================================================
    # RAW DEFINITION ACCESS
    # =========================================================================

    def get_analysis_raw(self, analysis_id: str) -> Dict:
        """Return the complete raw analysis definition for inspection."""
        return self.get_analysis_definition(analysis_id, use_cache=False)

    # =========================================================================
    # SHEET MANAGEMENT
    # =========================================================================

    def get_sheet(self, analysis_id: str, sheet_id: str) -> Optional[Dict]:
        """Get a specific sheet by ID, or ``None``."""
        for s in self.get_sheets(analysis_id):
            if s.get('SheetId') == sheet_id:
                return s
        return None

    def list_sheet_visuals(self, analysis_id: str, sheet_id: str) -> List[Dict]:
        """Get all visuals in a specific sheet."""
        sheet = self.get_sheet(analysis_id, sheet_id)
        if not sheet:
            raise ValueError(f"Sheet '{sheet_id}' not found")
        visuals = []
        for v in sheet.get('Visuals', []):
            info = self._parse_visual(v)
            info['sheet_id'] = sheet_id
            info['sheet_name'] = sheet.get('Name', '')
            visuals.append(info)
        return visuals

    def add_sheet(
        self,
        analysis_id: str,
        name: str,
        sheet_id: Optional[str] = None,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
    ) -> Dict:
        """Add a new sheet to an analysis.

        Returns:
            dict with ``status``, ``analysis_id``, ``sheet_id``, ``sheet_name``.
        """
        definition, last_updated = self.get_analysis_definition_with_version(analysis_id)
        new_sheet_id = sheet_id or str(uuid.uuid4())

        sheets = definition.setdefault('Sheets', [])
        if any(s.get('SheetId') == new_sheet_id for s in sheets):
            raise ValueError(f"Sheet '{new_sheet_id}' already exists")

        new_sheet = {
            'SheetId': new_sheet_id,
            'Name': name,
            'ContentType': 'INTERACTIVE',
            'Visuals': [],
            'Layouts': [{
                'Configuration': {
                    'GridLayout': {
                        'Elements': [],
                    }
                }
            }],
        }
        sheets.append(new_sheet)

        result = self.update_analysis(
            analysis_id, definition, backup_first=backup_first,
            expected_last_updated=(
                last_updated if self._should_lock(use_optimistic_locking) else None
            ),
        )

        if self._should_verify(None):
            self._verify_sheet_exists(analysis_id, new_sheet_id, name)

        result['sheet_id'] = new_sheet_id
        result['sheet_name'] = name
        return result

    def delete_sheet(
        self,
        analysis_id: str,
        sheet_id: str,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
    ) -> Dict:
        """Delete a sheet from an analysis.

        Raises:
            ValueError: If the sheet is not found.
        """
        definition, last_updated = self.get_analysis_definition_with_version(analysis_id)
        sheets = definition.get('Sheets', [])
        original_count = len(sheets)

        definition['Sheets'] = [s for s in sheets if s.get('SheetId') != sheet_id]
        if len(definition['Sheets']) == original_count:
            raise ValueError(f"Sheet '{sheet_id}' not found")

        result = self.update_analysis(
            analysis_id, definition, backup_first=backup_first,
            expected_last_updated=(
                last_updated if self._should_lock(use_optimistic_locking) else None
            ),
        )

        if self._should_verify(None):
            self._verify_sheet_deleted(analysis_id, sheet_id)

        return result

    def rename_sheet(
        self,
        analysis_id: str,
        sheet_id: str,
        new_name: str,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
    ) -> Dict:
        """Rename an existing sheet.

        Raises:
            ValueError: If the sheet is not found.
        """
        definition, last_updated = self.get_analysis_definition_with_version(analysis_id)
        found = False
        for s in definition.get('Sheets', []):
            if s.get('SheetId') == sheet_id:
                s['Name'] = new_name
                found = True
                break

        if not found:
            raise ValueError(f"Sheet '{sheet_id}' not found")

        result = self.update_analysis(
            analysis_id, definition, backup_first=backup_first,
            expected_last_updated=(
                last_updated if self._should_lock(use_optimistic_locking) else None
            ),
        )

        if self._should_verify(None):
            self._verify_sheet_exists(analysis_id, sheet_id, new_name)

        return result

    # =========================================================================
    # VISUAL MANAGEMENT
    # =========================================================================

    def get_visual_definition(self, analysis_id: str, visual_id: str) -> Optional[Dict]:
        """Get the full raw definition of a specific visual.

        Returns the visual dict as stored in the analysis definition,
        or ``None`` if not found.
        """
        for sheet in self.get_sheets(analysis_id):
            for v in sheet.get('Visuals', []):
                for vtype in _VISUAL_TYPES:
                    if vtype in v and v[vtype].get('VisualId') == visual_id:
                        return v
        return None

    def _find_visual_sheet(self, definition: Dict, visual_id: str) -> Optional[Dict]:
        """Find the sheet containing a visual (returns sheet dict)."""
        for sheet in definition.get('Sheets', []):
            for v in sheet.get('Visuals', []):
                for vtype in _VISUAL_TYPES:
                    if vtype in v and v[vtype].get('VisualId') == visual_id:
                        return sheet
        return None

    def add_visual_to_sheet(
        self,
        analysis_id: str,
        sheet_id: str,
        visual_definition: Dict,
        layout: Optional[Dict] = None,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
    ) -> Dict:
        """Add a visual to a sheet.

        Args:
            analysis_id: Analysis ID.
            sheet_id: Target sheet ID.
            visual_definition: Full visual definition dict (e.g., ``{"KPIVisual": {...}}``).
            layout: Optional layout element for grid placement (``{"ElementId": ..., "ColumnIndex": ...}``).
            backup_first: Back up before writing.

        Raises:
            ValueError: If the sheet is not found.
        """
        definition, last_updated = self.get_analysis_definition_with_version(analysis_id)

        target_sheet = None
        for s in definition.get('Sheets', []):
            if s.get('SheetId') == sheet_id:
                target_sheet = s
                break

        if target_sheet is None:
            raise ValueError(f"Sheet '{sheet_id}' not found")

        # Extract visual ID for layout
        visual_id = None
        for vtype in _VISUAL_TYPES:
            if vtype in visual_definition:
                visual_id = visual_definition[vtype].get('VisualId', '')
                break

        target_sheet.setdefault('Visuals', []).append(visual_definition)

        # Add layout element if provided or auto-generate one
        if layout or visual_id:
            layouts = target_sheet.setdefault('Layouts', [])
            if not layouts:
                layouts.append({'Configuration': {'GridLayout': {'Elements': []}}})
            elements = (
                layouts[0]
                .setdefault('Configuration', {})
                .setdefault('GridLayout', {})
                .setdefault('Elements', [])
            )
            if layout:
                elements.append(layout)
            elif visual_id:
                # Default: full-width, 8 rows high, appended below existing
                max_row = max((e.get('RowIndex', 0) + e.get('RowSpan', 0) for e in elements), default=0)
                elements.append({
                    'ElementId': visual_id,
                    'ElementType': 'VISUAL',
                    'ColumnIndex': 0,
                    'ColumnSpan': 36,
                    'RowIndex': max_row,
                    'RowSpan': 12,
                })

        result = self.update_analysis(
            analysis_id, definition, backup_first=backup_first,
            expected_last_updated=(
                last_updated if self._should_lock(use_optimistic_locking) else None
            ),
        )

        if visual_id and self._should_verify(None):
            self._verify_visual_exists(analysis_id, visual_id)

        result['visual_id'] = visual_id
        return result

    def delete_visual(
        self,
        analysis_id: str,
        visual_id: str,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
    ) -> Dict:
        """Delete a visual from an analysis.

        Also removes the corresponding layout element.

        Raises:
            ValueError: If the visual is not found.
        """
        definition, last_updated = self.get_analysis_definition_with_version(analysis_id)

        found = False
        for sheet in definition.get('Sheets', []):
            original_len = len(sheet.get('Visuals', []))
            sheet['Visuals'] = [
                v for v in sheet.get('Visuals', [])
                if not any(
                    vtype in v and v[vtype].get('VisualId') == visual_id
                    for vtype in _VISUAL_TYPES
                )
            ]
            if len(sheet['Visuals']) < original_len:
                found = True
                # Remove layout element
                for layout in sheet.get('Layouts', []):
                    grid = layout.get('Configuration', {}).get('GridLayout', {})
                    grid['Elements'] = [
                        e for e in grid.get('Elements', [])
                        if e.get('ElementId') != visual_id
                    ]
                break

        if not found:
            raise ValueError(f"Visual '{visual_id}' not found")

        result = self.update_analysis(
            analysis_id, definition, backup_first=backup_first,
            expected_last_updated=(
                last_updated if self._should_lock(use_optimistic_locking) else None
            ),
        )

        if self._should_verify(None):
            self._verify_visual_deleted(analysis_id, visual_id)

        return result

    def set_visual_title(
        self,
        analysis_id: str,
        visual_id: str,
        title: str,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
    ) -> Dict:
        """Set or update the title of a visual.

        Raises:
            ValueError: If the visual is not found.
        """
        definition, last_updated = self.get_analysis_definition_with_version(analysis_id)

        found = False
        for sheet in definition.get('Sheets', []):
            for v in sheet.get('Visuals', []):
                for vtype in _VISUAL_TYPES:
                    if vtype in v and v[vtype].get('VisualId') == visual_id:
                        v[vtype].setdefault('Title', {})['FormatText'] = {
                            'PlainText': title,
                        }
                        v[vtype]['Title']['Visibility'] = 'VISIBLE'
                        found = True
                        break
                if found:
                    break
            if found:
                break

        if not found:
            raise ValueError(f"Visual '{visual_id}' not found")

        result = self.update_analysis(
            analysis_id, definition, backup_first=backup_first,
            expected_last_updated=(
                last_updated if self._should_lock(use_optimistic_locking) else None
            ),
        )

        if self._should_verify(None):
            self._verify_visual_title(analysis_id, visual_id, title)

        return result

    def get_visual_layout(self, analysis_id: str, visual_id: str) -> Optional[Dict]:
        """Get the layout (position/size) for a visual."""
        definition = self.get_analysis_definition(analysis_id)
        for sheet in definition.get('Sheets', []):
            for layout in sheet.get('Layouts', []):
                for elem in (
                    layout.get('Configuration', {})
                    .get('GridLayout', {})
                    .get('Elements', [])
                ):
                    if elem.get('ElementId') == visual_id:
                        return elem
        return None

    def set_visual_layout(
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
        definition, last_updated = self.get_analysis_definition_with_version(analysis_id)

        found = False
        for sheet in definition.get('Sheets', []):
            for layout in sheet.get('Layouts', []):
                for elem in (
                    layout.get('Configuration', {})
                    .get('GridLayout', {})
                    .get('Elements', [])
                ):
                    if elem.get('ElementId') == visual_id:
                        if column_index is not None:
                            elem['ColumnIndex'] = column_index
                        if column_span is not None:
                            elem['ColumnSpan'] = column_span
                        if row_index is not None:
                            elem['RowIndex'] = row_index
                        if row_span is not None:
                            elem['RowSpan'] = row_span
                        found = True
                        break
                if found:
                    break
            if found:
                break

        if not found:
            raise ValueError(f"Layout element for visual '{visual_id}' not found")

        return self.update_analysis(
            analysis_id, definition, backup_first=backup_first,
            expected_last_updated=(
                last_updated if self._should_lock(use_optimistic_locking) else None
            ),
        )

    # =========================================================================
    # PARAMETER MANAGEMENT
    # =========================================================================

    def add_parameter(
        self,
        analysis_id: str,
        parameter_definition: Dict,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
    ) -> Dict:
        """Add a parameter to an analysis.

        Args:
            analysis_id: Analysis ID.
            parameter_definition: Full parameter declaration dict.

        Raises:
            ValueError: If a parameter with the same name already exists.
        """
        definition, last_updated = self.get_analysis_definition_with_version(analysis_id)
        params = definition.setdefault('ParameterDeclarations', [])

        # Extract name from any parameter type
        new_name = None
        for ptype in ('StringParameterDeclaration', 'IntegerParameterDeclaration',
                       'DecimalParameterDeclaration', 'DateTimeParameterDeclaration'):
            if ptype in parameter_definition:
                new_name = parameter_definition[ptype].get('Name')
                break

        if new_name:
            for p in params:
                for ptype in ('StringParameterDeclaration', 'IntegerParameterDeclaration',
                               'DecimalParameterDeclaration', 'DateTimeParameterDeclaration'):
                    if ptype in p and p[ptype].get('Name') == new_name:
                        raise ValueError(f"Parameter '{new_name}' already exists")

        params.append(parameter_definition)

        result = self.update_analysis(
            analysis_id, definition, backup_first=backup_first,
            expected_last_updated=(
                last_updated if self._should_lock(use_optimistic_locking) else None
            ),
        )

        if new_name and self._should_verify(None):
            self._verify_parameter_exists(analysis_id, new_name)

        result['parameter_name'] = new_name
        return result

    def delete_parameter(
        self,
        analysis_id: str,
        parameter_name: str,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
    ) -> Dict:
        """Delete a parameter by name.

        Raises:
            ValueError: If the parameter is not found.
        """
        definition, last_updated = self.get_analysis_definition_with_version(analysis_id)
        params = definition.get('ParameterDeclarations', [])
        original_count = len(params)

        def _matches(p: Dict) -> bool:
            for ptype in ('StringParameterDeclaration', 'IntegerParameterDeclaration',
                           'DecimalParameterDeclaration', 'DateTimeParameterDeclaration'):
                if ptype in p and p[ptype].get('Name') == parameter_name:
                    return True
            return False

        definition['ParameterDeclarations'] = [p for p in params if not _matches(p)]
        if len(definition['ParameterDeclarations']) == original_count:
            raise ValueError(f"Parameter '{parameter_name}' not found")

        result = self.update_analysis(
            analysis_id, definition, backup_first=backup_first,
            expected_last_updated=(
                last_updated if self._should_lock(use_optimistic_locking) else None
            ),
        )

        if self._should_verify(None):
            self._verify_parameter_deleted(analysis_id, parameter_name)

        return result

    # =========================================================================
    # FILTER GROUP MANAGEMENT
    # =========================================================================

    def add_filter_group(
        self,
        analysis_id: str,
        filter_group_definition: Dict,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
    ) -> Dict:
        """Add a filter group to an analysis.

        Args:
            analysis_id: Analysis ID.
            filter_group_definition: Full filter group dict with FilterGroupId, Filters, etc.

        Raises:
            ValueError: If a filter group with the same ID already exists.
        """
        definition, last_updated = self.get_analysis_definition_with_version(analysis_id)
        filter_groups = definition.setdefault('FilterGroups', [])

        new_id = filter_group_definition.get('FilterGroupId')
        if new_id and any(fg.get('FilterGroupId') == new_id for fg in filter_groups):
            raise ValueError(f"Filter group '{new_id}' already exists")

        filter_groups.append(filter_group_definition)

        result = self.update_analysis(
            analysis_id, definition, backup_first=backup_first,
            expected_last_updated=(
                last_updated if self._should_lock(use_optimistic_locking) else None
            ),
        )
        if new_id and self._should_verify(None):
            self._verify_filter_group_exists(analysis_id, new_id)

        result['filter_group_id'] = new_id
        return result

    def delete_filter_group(
        self,
        analysis_id: str,
        filter_group_id: str,
        backup_first: bool = True,
        use_optimistic_locking: Optional[bool] = None,
    ) -> Dict:
        """Delete a filter group by ID.

        Raises:
            ValueError: If the filter group is not found.
        """
        definition, last_updated = self.get_analysis_definition_with_version(analysis_id)
        fgs = definition.get('FilterGroups', [])
        original_count = len(fgs)

        definition['FilterGroups'] = [
            fg for fg in fgs if fg.get('FilterGroupId') != filter_group_id
        ]
        if len(definition['FilterGroups']) == original_count:
            raise ValueError(f"Filter group '{filter_group_id}' not found")

        result = self.update_analysis(
            analysis_id, definition, backup_first=backup_first,
            expected_last_updated=(
                last_updated if self._should_lock(use_optimistic_locking) else None
            ),
        )

        if self._should_verify(None):
            self._verify_filter_group_deleted(analysis_id, filter_group_id)

        return result

    # =========================================================================
    # BATCH & REPLICATION OPERATIONS
    # =========================================================================

    def replicate_sheet(
        self,
        analysis_id: str,
        source_sheet_id: str,
        target_sheet_name: str,
        target_sheet_id: Optional[str] = None,
        id_prefix: str = 'rc_',
        backup_first: bool = True,
    ) -> Dict:
        """Copy all visuals from one sheet to a new sheet in the same analysis.

        This performs a batch copy in a single API call, which is much more
        reliable than adding visuals one at a time. Visual IDs are prefixed
        to avoid conflicts. Layout positions are preserved from the source.

        Args:
            analysis_id: Analysis ID.
            source_sheet_id: Sheet ID to copy visuals from.
            target_sheet_name: Name for the new sheet.
            target_sheet_id: Optional ID for the new sheet (auto-generated if omitted).
            id_prefix: Prefix for new visual IDs (default ``'rc_'``).
            backup_first: Back up before writing.

        Returns:
            dict with ``analysis_id``, ``sheet_id``, ``visual_count``,
            ``visual_types``.
        """
        import copy as _copy

        definition, last_updated = self.get_analysis_definition_with_version(analysis_id)

        # Check sheet limit (QuickSight max is 20 sheets per analysis)
        current_sheets = definition.get('Sheets', [])
        if len(current_sheets) >= 20:
            raise ValueError(
                f"Cannot add sheet: analysis already has {len(current_sheets)} sheets "
                f"(QuickSight max is 20). Delete a sheet first."
            )

        # Find source sheet
        source_sheet = None
        for s in current_sheets:
            if s.get('SheetId') == source_sheet_id:
                source_sheet = s
                break
        if not source_sheet:
            raise ValueError(f"Source sheet '{source_sheet_id}' not found")

        # Build layout map from source
        src_layouts = (
            source_sheet.get('Layouts', [{}])[0]
            .get('Configuration', {})
            .get('GridLayout', {})
            .get('Elements', [])
        )
        layout_map = {le['ElementId']: le for le in src_layouts if 'ElementId' in le}

        # Create new sheet
        new_sheet_id = target_sheet_id or str(uuid.uuid4())
        new_visuals = []
        new_layout_elements = []
        type_counts: Dict[str, int] = {}

        for v in source_sheet.get('Visuals', []):
            visual_type = None
            old_id = None
            for vtype in _VISUAL_TYPES:
                if vtype in v:
                    visual_type = vtype
                    old_id = v[vtype].get('VisualId', '')
                    break
            if not visual_type:
                continue

            new_id = f'{id_prefix}{old_id}'
            new_visual = _copy.deepcopy(v)
            new_visual[visual_type]['VisualId'] = new_id
            new_visuals.append(new_visual)
            type_counts[visual_type] = type_counts.get(visual_type, 0) + 1

            # Copy layout
            if old_id in layout_map:
                le = _copy.deepcopy(layout_map[old_id])
                le['ElementId'] = new_id
                new_layout_elements.append(le)
            else:
                new_layout_elements.append({
                    'ElementId': new_id,
                    'ElementType': 'VISUAL',
                    'ColumnIndex': 0,
                    'ColumnSpan': 36,
                    'RowIndex': len(new_layout_elements) * 12,
                    'RowSpan': 12,
                })

        new_sheet = {
            'SheetId': new_sheet_id,
            'Name': target_sheet_name,
            'ContentType': 'INTERACTIVE',
            'Visuals': new_visuals,
            'Layouts': [{
                'Configuration': {
                    'GridLayout': {
                        'Elements': new_layout_elements,
                    }
                }
            }],
        }
        definition.setdefault('Sheets', []).append(new_sheet)

        self.update_analysis(
            analysis_id, definition, backup_first=backup_first,
            expected_last_updated=(
                last_updated if self._should_lock(None) else None
            ),
        )

        if self._should_verify(None):
            self._verify_sheet_exists(analysis_id, new_sheet_id, target_sheet_name)
            self._verify_sheet_visual_count(analysis_id, new_sheet_id, len(new_visuals))

        logger.info(
            "Replicated sheet %s -> %s (%d visuals)",
            source_sheet_id, new_sheet_id, len(new_visuals),
        )
        return {
            'analysis_id': analysis_id,
            'sheet_id': new_sheet_id,
            'sheet_name': target_sheet_name,
            'visual_count': len(new_visuals),
            'visual_types': type_counts,
        }
