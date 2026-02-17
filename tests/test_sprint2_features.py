"""Tests for Sprint 2 features.

Covers:
- create_combo_chart: ComboChartVisual definition, data labels, bar/line formats
- create_pie_chart: PieChartVisual definition, format_string propagation
- modify_dataset_sql: find/replace on dataset SQL, ValueError on missing text
- cancel_refresh: cancel_ingestion API delegation
- _paginate: paginated list helper, auto-retry on ExpiredToken
"""

from unittest.mock import MagicMock, patch

import pytest

from quicksight_mcp.client import QuickSightClient


# ---------------------------------------------------------------------------
# Helpers: build a QuickSightClient with AWS calls patched out
# ---------------------------------------------------------------------------

def _make_client() -> QuickSightClient:
    """Return a QuickSightClient whose __init__ skips real AWS setup."""
    with patch.object(QuickSightClient, '_init_aws_session'):
        client = QuickSightClient.__new__(QuickSightClient)
        client.profile = None
        client.region = 'us-east-1'
        client._account_id_override = '123456789012'
        client.account_id = '123456789012'
        client._verify_default = False
        client._locking_default = False
        return client


def _stub_analysis_mocks(client: QuickSightClient) -> None:
    """Attach mocked analysis get/update to skip AWS calls."""
    client.get_analysis_definition_with_version = MagicMock(
        return_value=(
            {
                'Sheets': [{
                    'SheetId': 'sheet1',
                    'Visuals': [],
                    'Layouts': [{
                        'Configuration': {
                            'GridLayout': {'Elements': []},
                        },
                    }],
                }],
            },
            '2026-02-17T00:00:00Z',
        )
    )
    client.update_analysis = MagicMock(return_value={'status': 'ok'})


# =========================================================================
# ComboChart
# =========================================================================

class TestCreateComboChartStructure:
    """Verify combo chart definition structure (bars + line on shared axis)."""

    def setup_method(self):
        self.client = _make_client()
        _stub_analysis_mocks(self.client)

    def test_create_combo_chart_structure(self):
        """ComboChartAggregatedFieldWells must have BarValues and LineValues."""
        result = self.client.create_combo_chart(
            analysis_id='an-001',
            sheet_id='sheet1',
            title='Revenue vs Margin',
            category_column='MARKET_NAME',
            bar_column='REVENUE',
            bar_aggregation='SUM',
            line_column='MARGIN_PCT',
            line_aggregation='AVG',
            dataset_identifier='ds1',
            backup_first=False,
        )
        assert result['visual_id'].startswith('combo_')

        call_args = self.client.update_analysis.call_args
        definition = call_args[0][1]
        visual = definition['Sheets'][0]['Visuals'][0]

        assert 'ComboChartVisual' in visual
        combo = visual['ComboChartVisual']
        assert combo['Title']['FormatText']['PlainText'] == 'Revenue vs Margin'

        field_wells = combo['ChartConfiguration']['FieldWells']
        agg = field_wells['ComboChartAggregatedFieldWells']
        assert len(agg['BarValues']) == 1
        assert len(agg['LineValues']) == 1
        assert len(agg['Category']) == 1


class TestCreateComboChartDataLabels:
    """Verify data label toggling on combo chart."""

    def setup_method(self):
        self.client = _make_client()
        _stub_analysis_mocks(self.client)

    def test_create_combo_chart_data_labels(self):
        """show_data_labels=True adds DataLabels with VISIBLE/OUTSIDE."""
        self.client.create_combo_chart(
            analysis_id='an-001',
            sheet_id='sheet1',
            title='Labels On',
            category_column='MARKET_NAME',
            bar_column='REVENUE',
            bar_aggregation='SUM',
            line_column='MARGIN_PCT',
            line_aggregation='AVG',
            dataset_identifier='ds1',
            show_data_labels=True,
            backup_first=False,
        )
        call_args = self.client.update_analysis.call_args
        definition = call_args[0][1]
        combo = definition['Sheets'][0]['Visuals'][0]['ComboChartVisual']
        chart_cfg = combo['ChartConfiguration']

        assert 'BarDataLabels' in chart_cfg
        assert chart_cfg['BarDataLabels']['Visibility'] == 'VISIBLE'
        assert chart_cfg['BarDataLabels']['Position'] == 'OUTSIDE'
        assert 'LineDataLabels' in chart_cfg
        assert chart_cfg['LineDataLabels']['Visibility'] == 'VISIBLE'
        assert chart_cfg['LineDataLabels']['Position'] == 'TOP'

    def test_create_combo_chart_no_data_labels_by_default(self):
        """Without show_data_labels, BarDataLabels/LineDataLabels keys are absent."""
        self.client.create_combo_chart(
            analysis_id='an-001',
            sheet_id='sheet1',
            title='Labels Off',
            category_column='MARKET_NAME',
            bar_column='REVENUE',
            bar_aggregation='SUM',
            line_column='MARGIN_PCT',
            line_aggregation='AVG',
            dataset_identifier='ds1',
            backup_first=False,
        )
        call_args = self.client.update_analysis.call_args
        definition = call_args[0][1]
        combo = definition['Sheets'][0]['Visuals'][0]['ComboChartVisual']
        assert 'BarDataLabels' not in combo['ChartConfiguration']
        assert 'LineDataLabels' not in combo['ChartConfiguration']


class TestCreateComboChartFormatStrings:
    """Verify that bar and line measures get independent format configurations."""

    def setup_method(self):
        self.client = _make_client()
        _stub_analysis_mocks(self.client)

    def test_create_combo_chart_format_strings(self):
        """bar_format_string and line_format_string produce separate FormatConfigurations."""
        self.client.create_combo_chart(
            analysis_id='an-001',
            sheet_id='sheet1',
            title='Formatted Combo',
            category_column='MARKET_NAME',
            bar_column='REVENUE',
            bar_aggregation='SUM',
            line_column='MARGIN_PCT',
            line_aggregation='AVG',
            dataset_identifier='ds1',
            bar_format_string='$#,##0',
            line_format_string='0.0%',
            backup_first=False,
        )
        call_args = self.client.update_analysis.call_args
        definition = call_args[0][1]
        combo = definition['Sheets'][0]['Visuals'][0]['ComboChartVisual']
        agg = combo['ChartConfiguration']['FieldWells']['ComboChartAggregatedFieldWells']

        # Bar value should have Currency format ($ prefix)
        bar_field = agg['BarValues'][0]
        bar_inner = bar_field['NumericalMeasureField']
        bar_fmt = bar_inner['FormatConfiguration']['NumericFormatConfiguration']
        assert 'CurrencyDisplayFormatConfiguration' in bar_fmt

        # Line value should have Percentage format (% suffix)
        line_field = agg['LineValues'][0]
        line_inner = line_field['NumericalMeasureField']
        line_fmt = line_inner['FormatConfiguration']['NumericFormatConfiguration']
        assert 'PercentageDisplayFormatConfiguration' in line_fmt


# =========================================================================
# PieChart
# =========================================================================

class TestCreatePieChartStructure:
    """Verify pie chart definition structure."""

    def setup_method(self):
        self.client = _make_client()
        _stub_analysis_mocks(self.client)

    def test_create_pie_chart_structure(self):
        """PieChartAggregatedFieldWells must have Category and Values."""
        result = self.client.create_pie_chart(
            analysis_id='an-001',
            sheet_id='sheet1',
            title='Market Share',
            group_column='MARKET_NAME',
            value_column='REVENUE',
            value_aggregation='SUM',
            dataset_identifier='ds1',
            backup_first=False,
        )
        assert result['visual_id'].startswith('pie_')

        call_args = self.client.update_analysis.call_args
        definition = call_args[0][1]
        visual = definition['Sheets'][0]['Visuals'][0]

        assert 'PieChartVisual' in visual
        pie = visual['PieChartVisual']
        assert pie['Title']['FormatText']['PlainText'] == 'Market Share'

        field_wells = pie['ChartConfiguration']['FieldWells']
        agg = field_wells['PieChartAggregatedFieldWells']
        assert len(agg['Category']) == 1
        assert len(agg['Values']) == 1


class TestCreatePieChartFormat:
    """Verify format_string propagation on pie chart values."""

    def setup_method(self):
        self.client = _make_client()
        _stub_analysis_mocks(self.client)

    def test_create_pie_chart_format(self):
        """format_string produces FormatConfiguration on the measure field."""
        self.client.create_pie_chart(
            analysis_id='an-001',
            sheet_id='sheet1',
            title='Pct Share',
            group_column='MARKET_NAME',
            value_column='SHARE_PCT',
            value_aggregation='AVG',
            dataset_identifier='ds1',
            format_string='0.0%',
            backup_first=False,
        )
        call_args = self.client.update_analysis.call_args
        definition = call_args[0][1]
        pie = definition['Sheets'][0]['Visuals'][0]['PieChartVisual']
        agg = pie['ChartConfiguration']['FieldWells']['PieChartAggregatedFieldWells']

        value_field = agg['Values'][0]
        inner = value_field['NumericalMeasureField']
        assert 'FormatConfiguration' in inner
        fmt = inner['FormatConfiguration']['NumericFormatConfiguration']
        assert 'PercentageDisplayFormatConfiguration' in fmt


# =========================================================================
# modify_dataset_sql
# =========================================================================

class TestModifyDatasetSql:
    """Verify find/replace on dataset SQL."""

    def setup_method(self):
        self.client = _make_client()

    def test_modify_dataset_sql_replaces(self):
        """modify_dataset_sql calls get_dataset_sql, replaces text, then update_dataset_sql."""
        original_sql = "SELECT * FROM orders WHERE status = 'active'"
        self.client.get_dataset_sql = MagicMock(return_value=original_sql)
        self.client.update_dataset_sql = MagicMock(return_value={'status': 'ok'})

        result = self.client.modify_dataset_sql(
            dataset_id='ds-123',
            find="status = 'active'",
            replace="status = 'completed'",
            backup_first=False,
        )

        # Verify get was called
        self.client.get_dataset_sql.assert_called_once_with('ds-123')

        # Verify update was called with the replaced SQL
        self.client.update_dataset_sql.assert_called_once()
        call_args = self.client.update_dataset_sql.call_args
        new_sql = call_args[0][1]  # second positional arg
        assert new_sql == "SELECT * FROM orders WHERE status = 'completed'"
        assert result == {'status': 'ok'}

    def test_modify_dataset_sql_not_found_raises(self):
        """When find text is not in current SQL, raises ValueError."""
        original_sql = "SELECT * FROM orders WHERE status = 'active'"
        self.client.get_dataset_sql = MagicMock(return_value=original_sql)

        with pytest.raises(ValueError, match="Text to find not present"):
            self.client.modify_dataset_sql(
                dataset_id='ds-123',
                find="nonexistent_text",
                replace="something_else",
                backup_first=False,
            )

    def test_modify_dataset_sql_no_custom_sql_raises(self):
        """When dataset has no Custom SQL, raises ValueError."""
        self.client.get_dataset_sql = MagicMock(return_value=None)

        with pytest.raises(ValueError, match="does not use Custom SQL"):
            self.client.modify_dataset_sql(
                dataset_id='ds-123',
                find="anything",
                replace="something",
                backup_first=False,
            )


# =========================================================================
# cancel_refresh
# =========================================================================

class TestCancelRefresh:
    """Verify cancel_refresh delegates to the cancel_ingestion API."""

    def setup_method(self):
        self.client = _make_client()

    def test_cancel_refresh_calls_api(self):
        """cancel_refresh passes correct params to _call('cancel_ingestion', ...)."""
        mock_response = {
            'IngestionId': 'ing-abc',
            'RequestId': 'req-123',
            'Status': 200,
        }
        self.client._call = MagicMock(return_value=mock_response)

        result = self.client.cancel_refresh(
            dataset_id='ds-456',
            ingestion_id='ing-abc',
        )

        self.client._call.assert_called_once_with(
            'cancel_ingestion',
            AwsAccountId='123456789012',
            DataSetId='ds-456',
            IngestionId='ing-abc',
        )
        assert result == mock_response


# =========================================================================
# _paginate
# =========================================================================

class TestPaginate:
    """Verify paginated list helper."""

    def setup_method(self):
        self.client = _make_client()
        # _paginate uses self.client (boto3 client), so we mock at that level
        self.client.client = MagicMock()

    def test_paginate_returns_results(self):
        """_paginate combines results from multiple pages."""
        # Simulate a paginator that yields two pages
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {'DataSetSummaries': [{'DataSetId': 'ds-1'}, {'DataSetId': 'ds-2'}]},
            {'DataSetSummaries': [{'DataSetId': 'ds-3'}]},
        ]
        self.client.client.get_paginator.return_value = mock_paginator

        results = self.client._paginate('list_data_sets', 'DataSetSummaries')

        assert len(results) == 3
        assert results[0]['DataSetId'] == 'ds-1'
        assert results[2]['DataSetId'] == 'ds-3'
        self.client.client.get_paginator.assert_called_once_with('list_data_sets')

    def test_paginate_retries_on_expired(self):
        """_paginate retries once after ExpiredToken, using refreshed session."""
        # First call to get_paginator: create a paginator that raises ExpiredToken
        expired_paginator = MagicMock()
        expired_paginator.paginate.side_effect = Exception(
            "An error occurred (ExpiredToken): The security token is expired"
        )

        # Second call after refresh: returns results
        success_paginator = MagicMock()
        success_paginator.paginate.return_value = [
            {'AnalysisSummaryList': [{'AnalysisId': 'an-1'}]},
        ]

        self.client.client.get_paginator.side_effect = [
            expired_paginator,
            success_paginator,
        ]

        # Mock _refresh_on_expired to succeed (simulates session refresh)
        self.client._refresh_on_expired = MagicMock(return_value=True)

        results = self.client._paginate('list_analyses', 'AnalysisSummaryList')

        assert len(results) == 1
        assert results[0]['AnalysisId'] == 'an-1'
        self.client._refresh_on_expired.assert_called_once()

    def test_paginate_empty_pages(self):
        """_paginate returns empty list when pages have no results."""
        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {'DataSetSummaries': []},
            {},  # missing key entirely
        ]
        self.client.client.get_paginator.return_value = mock_paginator

        results = self.client._paginate('list_data_sets', 'DataSetSummaries')
        assert results == []
