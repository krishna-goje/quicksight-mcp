"""Shared constants and type helpers used across services.

Extracted from client.py module-level constants and static helper methods.
"""

from __future__ import annotations

from typing import Dict, List

# All known QuickSight visual type keys
VISUAL_TYPES: List[str] = [
    "TableVisual",
    "PivotTableVisual",
    "BarChartVisual",
    "LineChartVisual",
    "PieChartVisual",
    "ScatterPlotVisual",
    "HeatMapVisual",
    "TreeMapVisual",
    "GaugeChartVisual",
    "KPIVisual",
    "ComboChartVisual",
    "WordCloudVisual",
    "InsightVisual",
    "SankeyDiagramVisual",
    "FunnelChartVisual",
    "WaterfallVisual",
    "HistogramVisual",
    "BoxPlotVisual",
    "FilledMapVisual",
    "GeospatialMapVisual",
    "CustomContentVisual",
    "EmptyVisual",
]

# Maps user-friendly aggregation names to QuickSight API values
AGG_MAP: Dict[str, str] = {
    "SUM": "SUM",
    "COUNT": "COUNT",
    "AVG": "AVERAGE",
    "AVERAGE": "AVERAGE",
    "MIN": "MIN",
    "MAX": "MAX",
    "DISTINCT_COUNT": "DISTINCT_COUNT",
}

# Suffixes that indicate a date column (for auto-detection)
DATE_SUFFIXES = (
    "_AT",
    "_DATE",
    "_TIME",
    "_TIMESTAMP",
    "_DT",
    "DATE",
    "TIMESTAMP",
)

# Parameter type keys used in QuickSight analysis definitions
PARAMETER_TYPES = (
    "StringParameterDeclaration",
    "IntegerParameterDeclaration",
    "DecimalParameterDeclaration",
    "DateTimeParameterDeclaration",
)


def is_date_column(column_name: str) -> bool:
    """Heuristic: return True if the column name looks like a date field."""
    upper = column_name.upper()
    return any(upper.endswith(s) for s in DATE_SUFFIXES)


def parse_visual(visual: Dict) -> Dict:
    """Extract type, id, title, subtitle from a visual definition dict."""
    for vtype in VISUAL_TYPES:
        if vtype in visual:
            vdef = visual[vtype]
            return {
                "type": vtype.replace("Visual", ""),
                "visual_id": vdef.get("VisualId", ""),
                "title": (
                    vdef.get("Title", {})
                    .get("FormatText", {})
                    .get("PlainText", "")
                ),
                "subtitle": (
                    vdef.get("Subtitle", {})
                    .get("FormatText", {})
                    .get("PlainText", "")
                ),
            }
    return {"type": "Unknown", "visual_id": "", "title": "", "subtitle": ""}


def extract_visual_id(visual_definition: Dict) -> str | None:
    """Extract the VisualId from a visual definition dict."""
    for vtype in VISUAL_TYPES:
        if vtype in visual_definition:
            return visual_definition[vtype].get("VisualId")
    return None
