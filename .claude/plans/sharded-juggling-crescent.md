# Developer Workflow MCP Enhancement Plan

## Context

The MCP currently has 48 tools but they're all low-level (add raw JSON visual, set raw layout). Real-world developer workflow is:

1. **Stakeholder**: "Add a KPI showing contract-to-close cycle time on SLA sheet"
2. **Developer**: Creates visual with simple params (column, aggregation, dataset)
3. **QA**: Verifies the visual exists, data is correct, nothing else broke

We need **high-level builder tools** that accept simple parameters (column name, aggregation, dataset) and construct the full QuickSight JSON internally — plus **snapshot/diff** for QA.

## What We're Adding

### A. Chart Builder Tools (Developer Experience)

**`create_kpi`** — Create a KPI visual from simple params
```
create_kpi(analysis_id, sheet_id, name="Total Contracts", column="FLIP_TOKEN",
           aggregation="COUNT", dataset_identifier="acq_l2_flip_details")
```
- Auto-generates visual ID, layout element, title
- Accepts: column name + aggregation (SUM, COUNT, AVG, MIN, MAX, DISTINCT_COUNT)
- Returns: visual_id for further customization

**`create_bar_chart`** — Create a bar chart
```
create_bar_chart(analysis_id, sheet_id, title="Contracts by Market",
                 category_column="MARKET_NAME", value_column="FLIP_TOKEN",
                 value_aggregation="COUNT", dataset_identifier="acq_l2_flip_details")
```

**`create_line_chart`** — Create a line chart
```
create_line_chart(analysis_id, sheet_id, title="Weekly Contracts",
                  date_column="PURCHASE_AGREEMENT_COMPLETED_AT", value_column="FLIP_TOKEN",
                  value_aggregation="COUNT", dataset_identifier="acq_l2_flip_details",
                  date_granularity="WEEK")
```

**`create_pivot_table`** — Create a pivot table
```
create_pivot_table(analysis_id, sheet_id, title="Market Performance",
                   row_columns=["MARKET_NAME"], value_columns=["FLIP_TOKEN"],
                   value_aggregations=["COUNT"], dataset_identifier="acq_l2_flip_details")
```

**`create_table`** — Create a flat table
```
create_table(analysis_id, sheet_id, title="Recent Contracts",
             columns=["FLIP_TOKEN", "MARKET_NAME", "PURCHASE_AGREEMENT_COMPLETED_AT"],
             dataset_identifier="acq_l2_flip_details")
```

### B. Visual Editing Tools (Modify Existing)

**`add_visual_field`** — Add a field (column) to an existing visual
```
add_visual_field(analysis_id, visual_id, column="REVENUE",
                 aggregation="SUM", dataset_identifier="acq_l2", role="value")
```
- role: "category", "value", "color", "row", "column" (depends on chart type)

**`remove_visual_field`** — Remove a field from a visual
```
remove_visual_field(analysis_id, visual_id, column="OLD_COLUMN")
```

### C. QA / Snapshot / Diff Tools

**`snapshot_analysis`** — Capture current state as a baseline
```
snapshot_analysis(analysis_id)
→ Returns: {snapshot_id, sheets, visual_count, calc_field_count, ...}
  Saves JSON to ~/.quicksight-mcp/snapshots/
```

**`diff_analysis`** — Compare current state against a snapshot
```
diff_analysis(analysis_id, snapshot_id)
→ Returns: {
    sheets_added: [...], sheets_removed: [...],
    visuals_added: [...], visuals_removed: [...],
    calc_fields_added: [...], calc_fields_removed: [...],
    visual_changes: [{visual_id, field: "title", old: "X", new: "Y"}, ...]
  }
```

## Files to Modify

| File | What |
|------|------|
| `src/quicksight_mcp/client.py` | Add builder methods + snapshot/diff methods |
| `src/quicksight_mcp/tools/visuals.py` | Add create_kpi, create_bar_chart, create_line_chart, create_pivot_table, create_table, add_visual_field |
| `src/quicksight_mcp/tools/analyses.py` | Add snapshot_analysis, diff_analysis |

## Implementation Details

### Chart Builder Pattern (from qs_utils.py create_kpi_row)

Each builder:
1. Gets definition with version (optimistic locking)
2. Finds the target sheet
3. Generates a unique visual ID
4. Constructs the full visual definition with field mappings:
   - Dimensions: `CategoricalDimensionField` or `DateDimensionField`
   - Measures: `NumericalMeasureField` with `SimpleNumericalAggregation`
5. Adds to sheet's Visuals array
6. Adds GridLayout element (auto-positioned below existing content)
7. Calls update_analysis with verification
8. Returns the visual_id for further customization

### Aggregation Alias Map
```python
AGG_MAP = {
    'SUM': 'SUM', 'COUNT': 'COUNT', 'AVG': 'AVERAGE',
    'AVERAGE': 'AVERAGE', 'MIN': 'MIN', 'MAX': 'MAX',
    'DISTINCT_COUNT': 'DISTINCT_COUNT', 'STDEV': 'STDEV',
    'VAR': 'VAR', 'MEDIAN': 'MEDIAN',
}
```

### Snapshot Format
```json
{
  "snapshot_id": "snap_20260212_193000",
  "analysis_id": "...",
  "timestamp": "2026-02-12T19:30:00",
  "sheets": [{"id": "...", "name": "...", "visual_count": 21}],
  "visuals": [{"id": "...", "type": "KPI", "title": "...", "sheet_id": "..."}],
  "calc_fields": [{"name": "...", "expression": "..."}],
  "parameters": [...],
  "filter_group_count": 343
}
```

## Verification

1. Syntax check: `python3 -c "import ast; ast.parse(open(f).read())"`
2. Tool count: grep `@mcp.tool` across all files
3. Reinstall: `uv pip install -e . --python .venv/bin/python`
4. Live test: Create a KPI on the clone → verify_analysis_health → snapshot → edit title → diff
