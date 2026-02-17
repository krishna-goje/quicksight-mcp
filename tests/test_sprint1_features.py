"""Tests for Sprint 1 formatting features.

Covers:
- _count_decimals: decimal place detection from format strings
- _build_format_config: QuickSight FormatConfiguration builder
- _is_date_column: date column heuristic detection
- _make_measure_field: measure field struct generation
- create_kpi / create_bar_chart / create_line_chart: visual definition structure
"""

from unittest.mock import MagicMock, patch

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
        client._verify_default = False
        client._locking_default = False
        return client


# =========================================================================
# _count_decimals
# =========================================================================

class TestCountDecimals:
    """Verify decimal-place counting from format strings."""

    def test_count_decimals_integer_format(self):
        assert QuickSightClient._count_decimals('#,##0') == 0

    def test_count_decimals_two_decimal(self):
        assert QuickSightClient._count_decimals('#,##0.00') == 2

    def test_count_decimals_percentage(self):
        assert QuickSightClient._count_decimals('0.0%') == 1

    def test_count_decimals_no_dot(self):
        assert QuickSightClient._count_decimals('0%') == 0

    def test_count_decimals_three_decimal(self):
        assert QuickSightClient._count_decimals('0.000') == 3

    def test_count_decimals_lone_dot(self):
        assert QuickSightClient._count_decimals('.') == 0


# =========================================================================
# _build_format_config
# =========================================================================

class TestBuildFormatConfig:
    """Verify QuickSight FormatConfiguration generation."""

    def test_build_format_config_number(self):
        cfg = QuickSightClient._build_format_config('#,##0')
        assert 'NumberDisplayFormatConfiguration' in cfg
        num = cfg['NumberDisplayFormatConfiguration']
        assert num['DecimalPlacesConfiguration']['DecimalPlaces'] == 0

    def test_build_format_config_currency(self):
        cfg = QuickSightClient._build_format_config('$#,##0.00')
        assert 'CurrencyDisplayFormatConfiguration' in cfg
        cur = cfg['CurrencyDisplayFormatConfiguration']
        assert cur['Prefix'] == '$'
        assert cur['DecimalPlacesConfiguration']['DecimalPlaces'] == 2

    def test_build_format_config_percentage(self):
        cfg = QuickSightClient._build_format_config('0.0%')
        assert 'PercentageDisplayFormatConfiguration' in cfg
        pct = cfg['PercentageDisplayFormatConfiguration']
        assert pct['DecimalPlacesConfiguration']['DecimalPlaces'] == 1

    def test_build_format_config_no_comma(self):
        cfg = QuickSightClient._build_format_config('0.00')
        num = cfg['NumberDisplayFormatConfiguration']
        sep = num['SeparatorConfiguration']['ThousandsSeparator']
        assert sep['Visibility'] == 'HIDDEN'

    def test_build_format_config_with_comma(self):
        cfg = QuickSightClient._build_format_config('#,##0')
        num = cfg['NumberDisplayFormatConfiguration']
        sep = num['SeparatorConfiguration']['ThousandsSeparator']
        assert sep['Visibility'] == 'VISIBLE'

    def test_build_format_config_currency_always_comma_visible(self):
        """Currency format always has thousands separator VISIBLE."""
        cfg = QuickSightClient._build_format_config('$#,##0.00')
        cur = cfg['CurrencyDisplayFormatConfiguration']
        sep = cur['SeparatorConfiguration']['ThousandsSeparator']
        assert sep['Visibility'] == 'VISIBLE'
        assert sep['Symbol'] == 'COMMA'

    def test_build_format_config_percentage_decimal_separator(self):
        cfg = QuickSightClient._build_format_config('0.0%')
        pct = cfg['PercentageDisplayFormatConfiguration']
        assert pct['SeparatorConfiguration']['DecimalSeparator'] == 'DOT'

    def test_build_format_config_number_scale_none(self):
        """Number configs should have NumberScale=NONE (no auto abbreviation)."""
        cfg = QuickSightClient._build_format_config('#,##0')
        assert cfg['NumberDisplayFormatConfiguration']['NumberScale'] == 'NONE'


# =========================================================================
# _is_date_column
# =========================================================================

class TestIsDateColumn:
    """Verify heuristic date column detection."""

    def test_is_date_column_at_suffix(self):
        assert QuickSightClient._is_date_column('COMPLETED_AT') is True

    def test_is_date_column_date_suffix(self):
        assert QuickSightClient._is_date_column('CLOSE_DATE') is True

    def test_is_date_column_day_suffix(self):
        assert QuickSightClient._is_date_column('PURCHASE_DAY') is True

    def test_is_date_column_string(self):
        assert QuickSightClient._is_date_column('MARKET_NAME') is False

    def test_is_date_column_id(self):
        assert QuickSightClient._is_date_column('ASSESSMENT_ID') is False

    def test_is_date_column_timestamp_suffix(self):
        assert QuickSightClient._is_date_column('CREATED_TIMESTAMP') is True

    def test_is_date_column_on_suffix(self):
        assert QuickSightClient._is_date_column('SUBMITTED_ON') is True

    def test_is_date_column_dt_suffix(self):
        assert QuickSightClient._is_date_column('SUBMITTED_DT') is True

    def test_is_date_column_exact_created(self):
        assert QuickSightClient._is_date_column('CREATED') is True

    def test_is_date_column_exact_updated(self):
        assert QuickSightClient._is_date_column('UPDATED') is True

    def test_is_date_column_case_insensitive(self):
        assert QuickSightClient._is_date_column('close_date') is True

    def test_is_date_column_time_suffix(self):
        assert QuickSightClient._is_date_column('ARRIVAL_TIME') is True

    def test_is_date_column_amount_not_date(self):
        assert QuickSightClient._is_date_column('AMOUNT') is False


# =========================================================================
# _make_measure_field
# =========================================================================

class TestMakeMeasureField:
    """Verify measure field struct generation (no AWS calls needed)."""

    def test_make_measure_field_count_string(self):
        """COUNT on a non-date column produces CategoricalMeasureField."""
        field = QuickSightClient._make_measure_field(
            'MARKET_NAME', 'ds1', 'COUNT', field_id='f1.MARKET_NAME',
        )
        assert 'CategoricalMeasureField' in field
        inner = field['CategoricalMeasureField']
        assert inner['AggregationFunction'] == 'COUNT'
        assert inner['Column']['ColumnName'] == 'MARKET_NAME'

    def test_make_measure_field_count_date(self):
        """COUNT on a date column produces DateMeasureField."""
        field = QuickSightClient._make_measure_field(
            'CREATED_AT', 'ds1', 'COUNT', field_id='f1.CREATED_AT',
        )
        assert 'DateMeasureField' in field
        inner = field['DateMeasureField']
        assert inner['AggregationFunction'] == 'COUNT'
        assert inner['Column']['ColumnName'] == 'CREATED_AT'

    def test_make_measure_field_sum(self):
        """SUM produces NumericalMeasureField with SimpleNumericalAggregation."""
        field = QuickSightClient._make_measure_field(
            'REVENUE', 'ds1', 'SUM', field_id='f1.REVENUE',
        )
        assert 'NumericalMeasureField' in field
        inner = field['NumericalMeasureField']
        assert inner['AggregationFunction'] == {'SimpleNumericalAggregation': 'SUM'}
        assert inner['Column']['ColumnName'] == 'REVENUE'

    def test_make_measure_field_with_format(self):
        """Format string produces FormatConfiguration on NumericalMeasureField."""
        field = QuickSightClient._make_measure_field(
            'REVENUE', 'ds1', 'SUM', field_id='f1.REVENUE',
            format_string='$#,##0.00',
        )
        inner = field['NumericalMeasureField']
        assert 'FormatConfiguration' in inner
        fmt = inner['FormatConfiguration']['NumericFormatConfiguration']
        assert 'CurrencyDisplayFormatConfiguration' in fmt

    def test_make_measure_field_count_with_format(self):
        """Format string on COUNT produces FormatConfiguration on CategoricalMeasureField."""
        field = QuickSightClient._make_measure_field(
            'FLIP_TOKEN', 'ds1', 'COUNT', field_id='f1.FLIP_TOKEN',
            format_string='#,##0',
        )
        assert 'CategoricalMeasureField' in field
        inner = field['CategoricalMeasureField']
        assert 'FormatConfiguration' in inner
        fmt = inner['FormatConfiguration']['NumericFormatConfiguration']
        assert 'NumberDisplayFormatConfiguration' in fmt

    def test_make_measure_field_distinct_count(self):
        """DISTINCT_COUNT on a non-date column uses CategoricalMeasureField."""
        field = QuickSightClient._make_measure_field(
            'CUSTOMER_ID', 'ds1', 'DISTINCT_COUNT', field_id='f1.CUSTOMER_ID',
        )
        assert 'CategoricalMeasureField' in field
        inner = field['CategoricalMeasureField']
        assert inner['AggregationFunction'] == 'DISTINCT_COUNT'

    def test_make_measure_field_avg_maps_to_average(self):
        """AVG is normalized to AVERAGE in the aggregation."""
        field = QuickSightClient._make_measure_field(
            'PRICE', 'ds1', 'AVG', field_id='f1.PRICE',
        )
        inner = field['NumericalMeasureField']
        assert inner['AggregationFunction'] == {'SimpleNumericalAggregation': 'AVERAGE'}

    def test_make_measure_field_auto_generates_field_id(self):
        """When field_id is omitted, a UUID-based ID is auto-generated."""
        field = QuickSightClient._make_measure_field('COL', 'ds1', 'SUM')
        inner = field['NumericalMeasureField']
        fid = inner['FieldId']
        assert fid.endswith('.COL')
        assert len(fid) > len('.COL')  # has UUID prefix

    def test_make_measure_field_dataset_identifier_propagated(self):
        """DataSetIdentifier is correctly set in the Column dict."""
        field = QuickSightClient._make_measure_field(
            'X', 'my_dataset', 'SUM', field_id='f1.X',
        )
        col = field['NumericalMeasureField']['Column']
        assert col['DataSetIdentifier'] == 'my_dataset'

    def test_make_measure_field_date_count_with_format(self):
        """Format on DATE COUNT produces FormatConfiguration on DateMeasureField."""
        field = QuickSightClient._make_measure_field(
            'CLOSE_DATE', 'ds1', 'COUNT', field_id='f1.CLOSE_DATE',
            format_string='#,##0',
        )
        assert 'DateMeasureField' in field
        inner = field['DateMeasureField']
        assert 'FormatConfiguration' in inner
        fmt = inner['FormatConfiguration']['NumericFormatConfiguration']
        assert 'NumberDisplayFormatConfiguration' in fmt


# =========================================================================
# Visual definition structure tests (mock update_analysis)
# =========================================================================

class TestCreateKpiVisualDef:
    """Verify KPI visual definition structure (AWS calls mocked)."""

    def setup_method(self):
        self.client = _make_client()
        # Stub the methods that talk to AWS
        self.client.get_analysis_definition_with_version = MagicMock(
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
        self.client.update_analysis = MagicMock(return_value={'status': 'ok'})

    def test_create_kpi_visual_def_structure(self):
        """create_kpi should produce a valid KPIVisual definition and return visual_id."""
        result = self.client.create_kpi(
            analysis_id='an-001',
            sheet_id='sheet1',
            title='Total Revenue',
            column='REVENUE',
            aggregation='SUM',
            dataset_identifier='ds1',
            format_string='$#,##0.00',
            backup_first=False,
        )
        assert 'visual_id' in result
        assert result['visual_id'].startswith('kpi_')

        # Verify the definition passed to update_analysis
        call_args = self.client.update_analysis.call_args
        definition = call_args[0][1]  # second positional arg
        sheet = definition['Sheets'][0]
        visuals = sheet['Visuals']
        assert len(visuals) == 1
        kpi_visual = visuals[0]
        assert 'KPIVisual' in kpi_visual
        kpi = kpi_visual['KPIVisual']
        assert kpi['Title']['FormatText']['PlainText'] == 'Total Revenue'
        assert kpi['ChartConfiguration']['FieldWells']['Values']

    def test_create_kpi_with_conditional_format(self):
        """create_kpi with conditional_format adds ConditionalFormatting."""
        self.client.create_kpi(
            analysis_id='an-001',
            sheet_id='sheet1',
            title='Contracts',
            column='FLIP_TOKEN',
            aggregation='COUNT',
            dataset_identifier='ds1',
            format_string='#,##0',
            conditional_format=[
                {'condition': '>= 100', 'color': '#2CAF4A'},
                {'condition': '< 100', 'color': '#DE3B00'},
            ],
            backup_first=False,
        )
        call_args = self.client.update_analysis.call_args
        definition = call_args[0][1]
        kpi_visual = definition['Sheets'][0]['Visuals'][0]['KPIVisual']
        assert 'ConditionalFormatting' in kpi_visual
        options = kpi_visual['ConditionalFormatting']['ConditionalFormattingOptions']
        assert len(options) == 2


class TestCreateBarChartVisualDef:
    """Verify bar chart visual definition structure."""

    def setup_method(self):
        self.client = _make_client()
        self.client.get_analysis_definition_with_version = MagicMock(
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
        self.client.update_analysis = MagicMock(return_value={'status': 'ok'})

    def test_create_bar_chart_data_labels(self):
        """create_bar_chart with show_data_labels=True includes DataLabels key."""
        result = self.client.create_bar_chart(
            analysis_id='an-001',
            sheet_id='sheet1',
            title='Revenue by Market',
            category_column='MARKET_NAME',
            value_column='REVENUE',
            value_aggregation='SUM',
            dataset_identifier='ds1',
            show_data_labels=True,
            backup_first=False,
        )
        assert result['visual_id'].startswith('bar_')

        call_args = self.client.update_analysis.call_args
        definition = call_args[0][1]
        bar_visual = definition['Sheets'][0]['Visuals'][0]['BarChartVisual']
        chart_cfg = bar_visual['ChartConfiguration']
        assert 'DataLabels' in chart_cfg
        assert chart_cfg['DataLabels']['Visibility'] == 'VISIBLE'
        assert chart_cfg['DataLabels']['Position'] == 'OUTSIDE'

    def test_create_bar_chart_no_data_labels_by_default(self):
        """create_bar_chart without show_data_labels omits DataLabels."""
        self.client.create_bar_chart(
            analysis_id='an-001',
            sheet_id='sheet1',
            title='Counts',
            category_column='MARKET_NAME',
            value_column='FLIP_TOKEN',
            value_aggregation='COUNT',
            dataset_identifier='ds1',
            backup_first=False,
        )
        call_args = self.client.update_analysis.call_args
        definition = call_args[0][1]
        bar_visual = definition['Sheets'][0]['Visuals'][0]['BarChartVisual']
        chart_cfg = bar_visual['ChartConfiguration']
        assert 'DataLabels' not in chart_cfg

    def test_create_bar_chart_orientation(self):
        """create_bar_chart passes orientation to ChartConfiguration."""
        self.client.create_bar_chart(
            analysis_id='an-001',
            sheet_id='sheet1',
            title='Horizontal',
            category_column='MARKET_NAME',
            value_column='REVENUE',
            value_aggregation='SUM',
            dataset_identifier='ds1',
            orientation='HORIZONTAL',
            backup_first=False,
        )
        call_args = self.client.update_analysis.call_args
        definition = call_args[0][1]
        bar_visual = definition['Sheets'][0]['Visuals'][0]['BarChartVisual']
        assert bar_visual['ChartConfiguration']['Orientation'] == 'HORIZONTAL'


class TestCreateLineChartVisualDef:
    """Verify line chart visual definition structure."""

    def setup_method(self):
        self.client = _make_client()
        self.client.get_analysis_definition_with_version = MagicMock(
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
        self.client.update_analysis = MagicMock(return_value={'status': 'ok'})

    def test_create_line_chart_data_labels(self):
        """create_line_chart with show_data_labels=True includes DataLabels key."""
        result = self.client.create_line_chart(
            analysis_id='an-001',
            sheet_id='sheet1',
            title='Revenue Trend',
            date_column='CLOSE_DATE',
            value_column='REVENUE',
            value_aggregation='SUM',
            dataset_identifier='ds1',
            show_data_labels=True,
            backup_first=False,
        )
        assert result['visual_id'].startswith('line_')

        call_args = self.client.update_analysis.call_args
        definition = call_args[0][1]
        line_visual = definition['Sheets'][0]['Visuals'][0]['LineChartVisual']
        chart_cfg = line_visual['ChartConfiguration']
        assert 'DataLabels' in chart_cfg
        assert chart_cfg['DataLabels']['Visibility'] == 'VISIBLE'

    def test_create_line_chart_no_data_labels_by_default(self):
        """create_line_chart without show_data_labels omits DataLabels."""
        self.client.create_line_chart(
            analysis_id='an-001',
            sheet_id='sheet1',
            title='Trend',
            date_column='CLOSE_DATE',
            value_column='REVENUE',
            value_aggregation='SUM',
            dataset_identifier='ds1',
            backup_first=False,
        )
        call_args = self.client.update_analysis.call_args
        definition = call_args[0][1]
        line_visual = definition['Sheets'][0]['Visuals'][0]['LineChartVisual']
        chart_cfg = line_visual['ChartConfiguration']
        assert 'DataLabels' not in chart_cfg

    def test_create_line_chart_date_granularity(self):
        """create_line_chart uses the specified date_granularity for the category field."""
        self.client.create_line_chart(
            analysis_id='an-001',
            sheet_id='sheet1',
            title='Monthly Trend',
            date_column='CLOSE_DATE',
            value_column='REVENUE',
            value_aggregation='SUM',
            dataset_identifier='ds1',
            date_granularity='MONTH',
            backup_first=False,
        )
        call_args = self.client.update_analysis.call_args
        definition = call_args[0][1]
        line_visual = definition['Sheets'][0]['Visuals'][0]['LineChartVisual']
        field_wells = line_visual['ChartConfiguration']['FieldWells']
        category = field_wells['LineChartAggregatedFieldWells']['Category'][0]
        assert category['DateDimensionField']['DateGranularity'] == 'MONTH'

    def test_create_line_chart_with_format_string(self):
        """create_line_chart passes format_string through to the measure field."""
        self.client.create_line_chart(
            analysis_id='an-001',
            sheet_id='sheet1',
            title='Pct Trend',
            date_column='CLOSE_DATE',
            value_column='CONVERSION_RATE',
            value_aggregation='AVG',
            dataset_identifier='ds1',
            format_string='0.0%',
            backup_first=False,
        )
        call_args = self.client.update_analysis.call_args
        definition = call_args[0][1]
        line_visual = definition['Sheets'][0]['Visuals'][0]['LineChartVisual']
        field_wells = line_visual['ChartConfiguration']['FieldWells']
        value = field_wells['LineChartAggregatedFieldWells']['Values'][0]
        inner = value['NumericalMeasureField']
        assert 'FormatConfiguration' in inner
        fmt = inner['FormatConfiguration']['NumericFormatConfiguration']
        assert 'PercentageDisplayFormatConfiguration' in fmt
