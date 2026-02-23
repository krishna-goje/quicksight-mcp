"""High-level chart builder service for creating QuickSight visuals.

Constructs KPI, Bar, Line, Pivot, Table, Combo, and Pie chart visuals
from simple parameters and appends them to an analysis sheet.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Dict, List, Optional, TYPE_CHECKING

from quicksight_mcp.core.cache import TTLCache
from quicksight_mcp.core.types import AGG_MAP, is_date_column

if TYPE_CHECKING:
    from quicksight_mcp.core.aws_client import AwsClient
    from quicksight_mcp.services.analyses import AnalysisService

logger = logging.getLogger(__name__)


class ChartBuilderService:
    """Creates QuickSight visuals from simple, high-level parameters.

    Each ``create_*`` method:
    1. Fetches the current analysis definition (with optimistic-locking version).
    2. Builds the visual definition dict.
    3. Appends the visual + layout element to the target sheet.
    4. Calls ``analysis_service.update()`` to persist the change.
    5. Optionally verifies the visual was persisted.

    Args:
        aws: Low-level AWS client.
        cache: TTL cache instance.
        analysis_service: AnalysisService for read/update operations.
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
    # Static field-building helpers
    # ------------------------------------------------------------------

    @staticmethod
    def make_measure_field(
        column: str,
        dataset_identifier: str,
        aggregation: str = "SUM",
        field_id: Optional[str] = None,
        format_string: Optional[str] = None,
    ) -> Dict:
        """Construct a measure field definition.

        Uses ``CategoricalMeasureField`` for COUNT/DISTINCT_COUNT
        (works on any column type) and ``NumericalMeasureField`` for
        numeric aggregations (SUM, AVG, MIN, MAX, etc.).

        For date columns with COUNT/DISTINCT_COUNT, uses ``DateMeasureField``.

        Args:
            column: Column name.
            dataset_identifier: Dataset identifier string.
            aggregation: Aggregation function name (SUM, COUNT, AVG, ...).
            field_id: Optional explicit field ID (auto-generated if omitted).
            format_string: Display format (e.g. ``'#,##0'``, ``'$#,##0.00'``, ``'0.0%'``).
        """
        agg = AGG_MAP.get(aggregation.upper(), aggregation.upper())
        fid = field_id or f"{uuid.uuid4().hex[:8]}.{column}"
        col = {"DataSetIdentifier": dataset_identifier, "ColumnName": column}

        if agg in ("COUNT", "DISTINCT_COUNT"):
            count_fn = "COUNT" if agg == "COUNT" else "DISTINCT_COUNT"
            if is_date_column(column):
                field: Dict[str, Any] = {
                    "DateMeasureField": {
                        "FieldId": fid,
                        "Column": col,
                        "AggregationFunction": count_fn,
                    }
                }
                if format_string:
                    field["DateMeasureField"]["FormatConfiguration"] = {
                        "NumericFormatConfiguration": ChartBuilderService.build_format_config(
                        format_string,
                    ),
                    }
                return field

            field = {
                "CategoricalMeasureField": {
                    "FieldId": fid,
                    "Column": col,
                    "AggregationFunction": count_fn,
                }
            }
            if format_string:
                field["CategoricalMeasureField"]["FormatConfiguration"] = {
                    "NumericFormatConfiguration": ChartBuilderService.build_format_config(
                        format_string,
                    ),
                }
            return field

        # Numeric aggregations (SUM, AVG, etc.)
        field = {
            "NumericalMeasureField": {
                "FieldId": fid,
                "Column": col,
                "AggregationFunction": {
                    "SimpleNumericalAggregation": agg,
                },
            }
        }
        if format_string:
            field["NumericalMeasureField"]["FormatConfiguration"] = {
                "NumericFormatConfiguration": ChartBuilderService.build_format_config(
                        format_string,
                    ),
            }
        return field

    @staticmethod
    def make_dimension_field(
        column: str,
        dataset_identifier: str,
        field_id: Optional[str] = None,
        is_date: Optional[bool] = None,
        date_granularity: str = "DAY",
    ) -> Dict:
        """Construct a CategoricalDimensionField or DateDimensionField.

        Auto-detects date columns by naming convention if ``is_date`` is ``None``.
        """
        fid = field_id or f"{uuid.uuid4().hex[:8]}.{column}"
        if is_date is None:
            is_date = is_date_column(column)
        if is_date:
            return {
                "DateDimensionField": {
                    "FieldId": fid,
                    "Column": {
                        "DataSetIdentifier": dataset_identifier,
                        "ColumnName": column,
                    },
                    "DateGranularity": date_granularity.upper(),
                }
            }
        return {
            "CategoricalDimensionField": {
                "FieldId": fid,
                "Column": {
                    "DataSetIdentifier": dataset_identifier,
                    "ColumnName": column,
                },
            }
        }

    @staticmethod
    def count_decimals(fmt: str) -> int:
        """Count decimal places from a format string like ``'#,##0.00'`` -> 2."""
        if "." not in fmt:
            return 0
        after_dot = fmt.split(".")[-1].rstrip("%").rstrip("$")
        return len([c for c in after_dot if c == "0"])

    @staticmethod
    def build_format_config(format_string: str) -> Dict:
        """Build a QuickSight FormatConfiguration from a format pattern.

        Supported patterns:
        - ``'#,##0'`` / ``'#,##0.00'`` -> NumberDisplayFormatConfiguration
        - ``'$#,##0'`` / ``'$#,##0.00'`` -> CurrencyDisplayFormatConfiguration
        - ``'0.0%'`` / ``'0%'`` -> PercentageDisplayFormatConfiguration
        """
        decimals = ChartBuilderService.count_decimals(format_string)
        has_comma = "," in format_string

        if "$" in format_string:
            return {
                "CurrencyDisplayFormatConfiguration": {
                    "Prefix": "$",
                    "NumberScale": "NONE",
                    "DecimalPlacesConfiguration": {"DecimalPlaces": decimals},
                    "SeparatorConfiguration": {
                        "ThousandsSeparator": {
                            "Visibility": "VISIBLE",
                            "Symbol": "COMMA",
                        },
                        "DecimalSeparator": "DOT",
                    },
                }
            }
        if "%" in format_string:
            return {
                "PercentageDisplayFormatConfiguration": {
                    "DecimalPlacesConfiguration": {"DecimalPlaces": decimals},
                    "SeparatorConfiguration": {"DecimalSeparator": "DOT"},
                }
            }
        return {
            "NumberDisplayFormatConfiguration": {
                "NumberScale": "NONE",
                "DecimalPlacesConfiguration": {"DecimalPlaces": decimals},
                "SeparatorConfiguration": {
                    "ThousandsSeparator": {
                        "Visibility": "VISIBLE" if has_comma else "HIDDEN",
                        "Symbol": "COMMA",
                    },
                    "DecimalSeparator": "DOT",
                },
            }
        }

    # ------------------------------------------------------------------
    # Internal: append visual to a sheet in the definition
    # ------------------------------------------------------------------

    @staticmethod
    def _append_visual_to_sheet(
        definition: Dict,
        sheet_id: str,
        visual_def: Dict,
        visual_id: str,
        col_span: int = 36,
        row_span: int = 12,
    ) -> None:
        """Add visual + layout element to a sheet within a definition dict."""
        for sheet in definition.get("Sheets", []):
            if sheet.get("SheetId") == sheet_id:
                sheet.setdefault("Visuals", []).append(visual_def)
                layouts = sheet.setdefault("Layouts", [])
                if not layouts:
                    layouts.append(
                        {"Configuration": {"GridLayout": {"Elements": []}}}
                    )
                elements = (
                    layouts[0]
                    .setdefault("Configuration", {})
                    .setdefault("GridLayout", {})
                    .setdefault("Elements", [])
                )
                max_row = max(
                    (
                        e.get("RowIndex", 0) + e.get("RowSpan", 0)
                        for e in elements
                    ),
                    default=0,
                )
                elements.append({
                    "ElementId": visual_id,
                    "ElementType": "VISUAL",
                    "ColumnIndex": 0,
                    "ColumnSpan": col_span,
                    "RowIndex": max_row,
                    "RowSpan": row_span,
                })
                return
        raise ValueError(f"Sheet '{sheet_id}' not found")

    # ------------------------------------------------------------------
    # Internal: extract field ID from a measure dict
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_field_id(measure: Dict) -> Optional[str]:
        for key in (
            "NumericalMeasureField",
            "CategoricalMeasureField",
            "DateMeasureField",
        ):
            if key in measure:
                return measure[key].get("FieldId")
        return None

    # ------------------------------------------------------------------
    # Chart creation methods
    # ------------------------------------------------------------------

    def create_kpi(
        self,
        analysis_id: str,
        sheet_id: str,
        title: str,
        column: str,
        aggregation: str,
        dataset_identifier: str,
        format_string: Optional[str] = None,
        conditional_format: Optional[List[Dict]] = None,
        backup_first: bool = True,
    ) -> Dict:
        """Create a KPI visual.

        Args:
            analysis_id: Analysis ID.
            sheet_id: Target sheet.
            title: Display title (e.g. "Total Contracts").
            column: Column name (e.g. "FLIP_TOKEN").
            aggregation: SUM, COUNT, AVG, MIN, MAX, DISTINCT_COUNT.
            dataset_identifier: Dataset identifier string.
            format_string: Display format (e.g. ``'#,##0'``).
            conditional_format: List of threshold rules for color coding.
                Each rule: ``{"condition": ">= 100", "color": "#2CAF4A"}``.
            backup_first: Back up before writing.

        Returns:
            dict with ``visual_id`` and update result.
        """
        definition, last_updated = self._analysis.get_definition_with_version(
            analysis_id
        )
        visual_id = f"kpi_{uuid.uuid4().hex[:12]}"
        measure = self.make_measure_field(
            column, dataset_identifier, aggregation, format_string=format_string
        )

        visual_def: Dict[str, Any] = {
            "KPIVisual": {
                "VisualId": visual_id,
                "Title": {
                    "Visibility": "VISIBLE",
                    "FormatText": {"PlainText": title},
                },
                "Subtitle": {"Visibility": "HIDDEN"},
                "ChartConfiguration": {
                    "FieldWells": {
                        "Values": [measure],
                        "TargetValues": [],
                        "TrendGroups": [],
                    },
                },
            }
        }

        if conditional_format:
            field_id = self._extract_field_id(measure)
            if field_id:
                agg_fn = aggregation.upper()
                agg_expr = f"{agg_fn}({{{column}}})"
                conditions = []
                for rule in conditional_format:
                    cond = rule.get("condition", "")
                    color = rule.get("color", "#2CAF4A")
                    conditions.append({
                        "PrimaryValue": {
                            "TextColor": {
                                "Solid": {
                                    "Expression": f"{agg_expr} {cond}",
                                    "Color": color,
                                }
                            }
                        }
                    })
                visual_def["KPIVisual"]["ConditionalFormatting"] = {
                    "ConditionalFormattingOptions": conditions
                }

        self._append_visual_to_sheet(
            definition, sheet_id, visual_def, visual_id,
            col_span=12, row_span=6,
        )

        result = self._analysis.update(
            analysis_id,
            definition,
            backup_first=backup_first,
            expected_last_updated=last_updated,
        )
        result["visual_id"] = visual_id
        return result

    def create_bar_chart(
        self,
        analysis_id: str,
        sheet_id: str,
        title: str,
        category_column: str,
        value_column: str,
        value_aggregation: str,
        dataset_identifier: str,
        orientation: str = "VERTICAL",
        format_string: Optional[str] = None,
        show_data_labels: bool = False,
        backup_first: bool = True,
    ) -> Dict:
        """Create a bar chart visual.

        Args:
            analysis_id: Analysis ID.
            sheet_id: Target sheet.
            title: Display title.
            category_column: Dimension column (X-axis).
            value_column: Measure column (Y-axis).
            value_aggregation: SUM, COUNT, etc.
            dataset_identifier: Dataset identifier.
            orientation: VERTICAL or HORIZONTAL.
            format_string: Display format for values.
            show_data_labels: Show value labels on bars.
            backup_first: Back up before writing.

        Returns:
            dict with ``visual_id``.
        """
        definition, last_updated = self._analysis.get_definition_with_version(
            analysis_id
        )
        visual_id = f"bar_{uuid.uuid4().hex[:12]}"

        category = self.make_dimension_field(category_column, dataset_identifier)
        value = self.make_measure_field(
            value_column, dataset_identifier, value_aggregation,
            format_string=format_string,
        )

        visual_def: Dict[str, Any] = {
            "BarChartVisual": {
                "VisualId": visual_id,
                "Title": {
                    "Visibility": "VISIBLE",
                    "FormatText": {"PlainText": title},
                },
                "Subtitle": {"Visibility": "HIDDEN"},
                "ChartConfiguration": {
                    "FieldWells": {
                        "BarChartAggregatedFieldWells": {
                            "Category": [category],
                            "Values": [value],
                            "Colors": [],
                            "SmallMultiples": [],
                        }
                    },
                    "Orientation": orientation.upper(),
                    "BarsArrangement": "CLUSTERED",
                },
            }
        }

        if show_data_labels:
            visual_def["BarChartVisual"]["ChartConfiguration"]["DataLabels"] = {
                "Visibility": "VISIBLE",
                "Position": "OUTSIDE",
            }

        self._append_visual_to_sheet(definition, sheet_id, visual_def, visual_id)

        result = self._analysis.update(
            analysis_id,
            definition,
            backup_first=backup_first,
            expected_last_updated=last_updated,
        )
        result["visual_id"] = visual_id
        return result

    def create_line_chart(
        self,
        analysis_id: str,
        sheet_id: str,
        title: str,
        date_column: str,
        value_column: str,
        value_aggregation: str,
        dataset_identifier: str,
        date_granularity: str = "WEEK",
        format_string: Optional[str] = None,
        show_data_labels: bool = False,
        backup_first: bool = True,
    ) -> Dict:
        """Create a line chart visual.

        Args:
            analysis_id: Analysis ID.
            sheet_id: Target sheet.
            title: Display title.
            date_column: Date column for X-axis.
            value_column: Measure column for Y-axis.
            value_aggregation: SUM, COUNT, etc.
            dataset_identifier: Dataset identifier.
            date_granularity: DAY, WEEK, MONTH, QUARTER, YEAR.
            format_string: Display format for values.
            show_data_labels: Show value labels on data points.
            backup_first: Back up before writing.

        Returns:
            dict with ``visual_id``.
        """
        definition, last_updated = self._analysis.get_definition_with_version(
            analysis_id
        )
        visual_id = f"line_{uuid.uuid4().hex[:12]}"

        category = self.make_dimension_field(
            date_column, dataset_identifier, is_date=True,
            date_granularity=date_granularity,
        )
        value = self.make_measure_field(
            value_column, dataset_identifier, value_aggregation,
            format_string=format_string,
        )

        visual_def: Dict[str, Any] = {
            "LineChartVisual": {
                "VisualId": visual_id,
                "Title": {
                    "Visibility": "VISIBLE",
                    "FormatText": {"PlainText": title},
                },
                "Subtitle": {"Visibility": "HIDDEN"},
                "ChartConfiguration": {
                    "FieldWells": {
                        "LineChartAggregatedFieldWells": {
                            "Category": [category],
                            "Values": [value],
                            "Colors": [],
                            "SmallMultiples": [],
                        }
                    },
                },
            }
        }

        if show_data_labels:
            visual_def["LineChartVisual"]["ChartConfiguration"]["DataLabels"] = {
                "Visibility": "VISIBLE",
            }

        self._append_visual_to_sheet(definition, sheet_id, visual_def, visual_id)

        result = self._analysis.update(
            analysis_id,
            definition,
            backup_first=backup_first,
            expected_last_updated=last_updated,
        )
        result["visual_id"] = visual_id
        return result

    def create_pivot_table(
        self,
        analysis_id: str,
        sheet_id: str,
        title: str,
        row_columns: List[str],
        value_columns: List[str],
        value_aggregations: List[str],
        dataset_identifier: str,
        format_strings: Optional[List[str]] = None,
        backup_first: bool = True,
    ) -> Dict:
        """Create a pivot table visual.

        Args:
            analysis_id: Analysis ID.
            sheet_id: Target sheet.
            title: Display title.
            row_columns: Dimension columns for rows.
            value_columns: Measure columns for values.
            value_aggregations: Aggregations (one per value column).
            dataset_identifier: Dataset identifier.
            format_strings: Display formats (one per value column, optional).
            backup_first: Back up before writing.

        Returns:
            dict with ``visual_id``.
        """
        definition, last_updated = self._analysis.get_definition_with_version(
            analysis_id
        )
        visual_id = f"pivot_{uuid.uuid4().hex[:12]}"

        rows = [
            self.make_dimension_field(c, dataset_identifier) for c in row_columns
        ]
        fmts = format_strings or [None] * len(value_columns)
        values = [
            self.make_measure_field(c, dataset_identifier, a, format_string=f)
            for c, a, f in zip(value_columns, value_aggregations, fmts)
        ]

        visual_def: Dict[str, Any] = {
            "PivotTableVisual": {
                "VisualId": visual_id,
                "Title": {
                    "Visibility": "VISIBLE",
                    "FormatText": {"PlainText": title},
                },
                "Subtitle": {"Visibility": "HIDDEN"},
                "ChartConfiguration": {
                    "FieldWells": {
                        "PivotTableAggregatedFieldWells": {
                            "Rows": rows,
                            "Columns": [],
                            "Values": values,
                        }
                    },
                },
            }
        }

        self._append_visual_to_sheet(
            definition, sheet_id, visual_def, visual_id, row_span=16
        )

        result = self._analysis.update(
            analysis_id,
            definition,
            backup_first=backup_first,
            expected_last_updated=last_updated,
        )
        result["visual_id"] = visual_id
        return result

    def create_table(
        self,
        analysis_id: str,
        sheet_id: str,
        title: str,
        columns: List[str],
        dataset_identifier: str,
        backup_first: bool = True,
    ) -> Dict:
        """Create a flat table visual.

        Args:
            analysis_id: Analysis ID.
            sheet_id: Target sheet.
            title: Display title.
            columns: Column names to display.
            dataset_identifier: Dataset identifier.
            backup_first: Back up before writing.

        Returns:
            dict with ``visual_id``.
        """
        definition, last_updated = self._analysis.get_definition_with_version(
            analysis_id
        )
        visual_id = f"tbl_{uuid.uuid4().hex[:12]}"

        grouped = [
            self.make_dimension_field(c, dataset_identifier) for c in columns
        ]

        visual_def: Dict[str, Any] = {
            "TableVisual": {
                "VisualId": visual_id,
                "Title": {
                    "Visibility": "VISIBLE",
                    "FormatText": {"PlainText": title},
                },
                "Subtitle": {"Visibility": "HIDDEN"},
                "ChartConfiguration": {
                    "FieldWells": {
                        "TableAggregatedFieldWells": {
                            "GroupBy": grouped,
                            "Values": [],
                        }
                    },
                },
            }
        }

        self._append_visual_to_sheet(
            definition, sheet_id, visual_def, visual_id, row_span=16
        )

        result = self._analysis.update(
            analysis_id,
            definition,
            backup_first=backup_first,
            expected_last_updated=last_updated,
        )
        result["visual_id"] = visual_id
        return result

    def create_combo_chart(
        self,
        analysis_id: str,
        sheet_id: str,
        title: str,
        category_column: str,
        bar_column: str,
        bar_aggregation: str,
        line_column: str,
        line_aggregation: str,
        dataset_identifier: str,
        bar_format_string: Optional[str] = None,
        line_format_string: Optional[str] = None,
        show_data_labels: bool = False,
        backup_first: bool = True,
    ) -> Dict:
        """Create a combo chart (bars + line) visual.

        Args:
            analysis_id: Analysis ID.
            sheet_id: Target sheet.
            title: Display title.
            category_column: Dimension column for the shared X-axis.
            bar_column: Measure column rendered as bars.
            bar_aggregation: Aggregation for bar measure.
            line_column: Measure column rendered as a line.
            line_aggregation: Aggregation for line measure.
            dataset_identifier: Dataset identifier.
            bar_format_string: Format for bar values.
            line_format_string: Format for line values.
            show_data_labels: Show value labels.
            backup_first: Back up before writing.

        Returns:
            dict with ``visual_id``.
        """
        definition, last_updated = self._analysis.get_definition_with_version(
            analysis_id
        )
        visual_id = f"combo_{uuid.uuid4().hex[:12]}"

        category = self.make_dimension_field(category_column, dataset_identifier)
        bar_value = self.make_measure_field(
            bar_column, dataset_identifier, bar_aggregation,
            format_string=bar_format_string,
        )
        line_value = self.make_measure_field(
            line_column, dataset_identifier, line_aggregation,
            format_string=line_format_string,
        )

        visual_def: Dict[str, Any] = {
            "ComboChartVisual": {
                "VisualId": visual_id,
                "Title": {
                    "Visibility": "VISIBLE",
                    "FormatText": {"PlainText": title},
                },
                "Subtitle": {"Visibility": "HIDDEN"},
                "ChartConfiguration": {
                    "FieldWells": {
                        "ComboChartAggregatedFieldWells": {
                            "Category": [category],
                            "BarValues": [bar_value],
                            "LineValues": [line_value],
                            "Colors": [],
                        }
                    },
                    "BarsArrangement": "CLUSTERED",
                },
            }
        }

        if show_data_labels:
            visual_def["ComboChartVisual"]["ChartConfiguration"]["BarDataLabels"] = {
                "Visibility": "VISIBLE",
                "Position": "OUTSIDE",
            }
            visual_def["ComboChartVisual"]["ChartConfiguration"]["LineDataLabels"] = {
                "Visibility": "VISIBLE",
                "Position": "TOP",
            }

        self._append_visual_to_sheet(definition, sheet_id, visual_def, visual_id)

        result = self._analysis.update(
            analysis_id,
            definition,
            backup_first=backup_first,
            expected_last_updated=last_updated,
        )
        result["visual_id"] = visual_id
        return result

    def create_pie_chart(
        self,
        analysis_id: str,
        sheet_id: str,
        title: str,
        group_column: str,
        value_column: str,
        value_aggregation: str,
        dataset_identifier: str,
        format_string: Optional[str] = None,
        backup_first: bool = True,
    ) -> Dict:
        """Create a pie chart visual.

        Args:
            analysis_id: Analysis ID.
            sheet_id: Target sheet.
            title: Display title.
            group_column: Dimension column for pie slices.
            value_column: Measure column for slice sizes.
            value_aggregation: SUM, COUNT, AVG, etc.
            dataset_identifier: Dataset identifier.
            format_string: Display format for values.
            backup_first: Back up before writing.

        Returns:
            dict with ``visual_id``.
        """
        definition, last_updated = self._analysis.get_definition_with_version(
            analysis_id
        )
        visual_id = f"pie_{uuid.uuid4().hex[:12]}"

        category = self.make_dimension_field(group_column, dataset_identifier)
        value = self.make_measure_field(
            value_column, dataset_identifier, value_aggregation,
            format_string=format_string,
        )

        visual_def: Dict[str, Any] = {
            "PieChartVisual": {
                "VisualId": visual_id,
                "Title": {
                    "Visibility": "VISIBLE",
                    "FormatText": {"PlainText": title},
                },
                "Subtitle": {"Visibility": "HIDDEN"},
                "ChartConfiguration": {
                    "FieldWells": {
                        "PieChartAggregatedFieldWells": {
                            "Category": [category],
                            "Values": [value],
                        }
                    },
                },
            }
        }

        self._append_visual_to_sheet(definition, sheet_id, visual_def, visual_id)

        result = self._analysis.update(
            analysis_id,
            definition,
            backup_first=backup_first,
            expected_last_updated=last_updated,
        )
        result["visual_id"] = visual_id
        return result
