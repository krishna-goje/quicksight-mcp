# QuickSight MCP Enhancement Plan

## Context

The QuickSight MCP server has 35 tools but only covers ~40% of our mature qs_utils.py library (120+ methods). The server is excellent for reads and simple SQL/calc-field updates, but **cannot create visuals, add sheets, manage filters, or configure parameters** — all critical for building analyses programmatically.

We'll exercise the MCP by cloning the T&O Homes analysis (174 visuals, 140 calc fields, 19 sheets) and replicating the "Assessments Performance" sheet (KPIs + LineCharts + PivotTables + BarCharts) using MCP tools alone. This will validate our enhancements end-to-end.

---

## Phase 0: Bug Fixes (2 confirmed bugs)

### Bug 1: `describe_analysis` returns empty data
**File:** `src/quicksight_mcp/tools/analyses.py` lines 123-146
**Root cause:** `definition.get("Definition", {}).get("Sheets", [])` double-nests the lookup. `client.get_analysis_definition()` (client.py:422) already unwraps `response.get('Definition', {})`, so `definition` IS the inner dict.
**Fix:** Change 5 lines from `definition.get("Definition", {}).get(X)` → `definition.get(X)`:
- Line 123: `Sheets`
- Line 135: `CalculatedFields`
- Line 138: `ParameterDeclarations`
- Line 141: `FilterGroups`
- Line 144: `DataSetIdentifierDeclarations`

### Bug 2: `clone_analysis` returns None for new_analysis_id
**File:** `src/quicksight_mcp/tools/backup.py` line 191
**Root cause:** `result.get("AnalysisId")` but client.py:1121 returns `{'analysis_id': new_id, ...}` (lowercase, underscore)
**Fix:** Change to `result.get("analysis_id")`

---

## Phase 1: Add Client Methods to `client.py`

Port from qs_utils.py. All follow the same pattern: get definition → mutate → call `update_analysis()`.

### 1A. Sheet Management (~150 lines)
```python
def add_sheet(self, analysis_id, name, sheet_id=None, backup_first=True) -> Dict
def delete_sheet(self, analysis_id, sheet_id, backup_first=True) -> Dict
def rename_sheet(self, analysis_id, sheet_id, new_name, backup_first=True) -> Dict
def list_sheet_visuals(self, analysis_id, sheet_id) -> List[Dict]
```
Reference: qs_utils.py lines 1497-1703

### 1B. Visual Management (~200 lines)
```python
def get_visual_definition(self, analysis_id, visual_id) -> Optional[Dict]
def add_visual_to_sheet(self, analysis_id, sheet_id, visual_def, layout=None, backup_first=True) -> Dict
def delete_visual(self, analysis_id, visual_id, backup_first=True) -> Dict
def set_visual_title(self, analysis_id, visual_id, title, backup_first=True) -> Dict
def get_visual_layout(self, analysis_id, visual_id) -> Optional[Dict]
def set_visual_layout(self, analysis_id, visual_id, col_idx, col_span, row_idx, row_span, backup_first=True) -> Dict
```
Reference: qs_utils.py lines 1211-1497

### 1C. Parameter Management (~60 lines)
```python
def add_parameter(self, analysis_id, param_def, backup_first=True) -> Dict
def delete_parameter(self, analysis_id, param_name, backup_first=True) -> Dict
```
Reference: qs_utils.py lines 1108-1162

### 1D. Filter Management (~50 lines)
```python
def add_filter_group(self, analysis_id, filter_group_def, backup_first=True) -> Dict
def delete_filter_group(self, analysis_id, filter_group_id, backup_first=True) -> Dict
```
Reference: qs_utils.py lines 1168-1205

### 1E. Raw Definition Access (~5 lines)
```python
def get_analysis_raw(self, analysis_id) -> Dict
```

---

## Phase 2: Add MCP Tool Wrappers

Each new file follows existing pattern: `register_X_tools(mcp, get_client, get_tracker)` with try/except, timing, tracker.

### 2A. New file: `src/quicksight_mcp/tools/sheets.py` (~120 lines)
| Tool | Description |
|------|-------------|
| `add_sheet` | Add a new sheet to an analysis |
| `delete_sheet` | Delete a sheet from an analysis |
| `rename_sheet` | Rename an existing sheet |
| `list_sheet_visuals` | List all visuals in a specific sheet |

### 2B. New file: `src/quicksight_mcp/tools/visuals.py` (~250 lines)
| Tool | Description |
|------|-------------|
| `get_visual_definition` | Get the full raw definition of a visual |
| `add_visual` | Add a visual to a sheet (accepts JSON definition) |
| `delete_visual` | Delete a visual from an analysis |
| `set_visual_title` | Set/update a visual's title |
| `set_visual_layout` | Set visual position and size on the grid |

### 2C. New file: `src/quicksight_mcp/tools/parameters.py` (~80 lines)
| Tool | Description |
|------|-------------|
| `add_parameter` | Add a parameter to an analysis (accepts JSON definition) |
| `delete_parameter` | Delete a parameter by name |

### 2D. New file: `src/quicksight_mcp/tools/filters.py` (~80 lines)
| Tool | Description |
|------|-------------|
| `add_filter_group` | Add a filter group (accepts JSON definition) |
| `delete_filter_group` | Delete a filter group by ID |

### 2E. Add to `src/quicksight_mcp/tools/analyses.py` (~30 lines)
| Tool | Description |
|------|-------------|
| `get_analysis_raw` | Return the complete raw analysis definition |

### 2F. Register in `src/quicksight_mcp/server.py` (~8 lines)
Import and call all 4 new registration functions.

**Total: ~15 new tools, bringing total from 35 → 50**

---

## Phase 3: Exercise — Clone & Replicate "Assessments Performance"

### Step 1: Clone
```
clone_analysis("7c6589ac-3d9b-4d23-8d5f-fb6de7d86d9d", "T&O Homes - MCP Exercise Clone")
```

### Step 2: Verify describe_analysis (bug fix validation)
```
describe_analysis(<clone_id>)  → should now show 19 sheets, 140 calc fields, 18 params
```

### Step 3: Create blank analysis + add single sheet
Clone the analysis, then delete all sheets except a new one we add. Or simpler: use the clone and just verify we can manipulate it.

### Step 4: Copy visuals from "Assessments Performance" sheet
The sheet has ~25 visuals (10 KPIs, 6 LineCharts, 3 PivotTables, 1 BarChart, etc.):
1. `get_visual_definition(clone_id, visual_id)` for each
2. Verify definitions are complete and inspectable
3. Test `set_visual_title`, `set_visual_layout` on a few visuals

### Step 5: Test parameter & filter operations
- `add_parameter`, `delete_parameter`
- `add_filter_group`, `delete_filter_group`
- Verify round-trip integrity

---

## Phase 4: Reviewer Agent Validation

Launch a reviewer agent that runs these checks:

1. **Bug fixes verified:** `describe_analysis` returns non-zero counts
2. **Clone works:** `clone_analysis` returns valid `new_analysis_id`
3. **Sheet tools work:** `add_sheet` → `list_sheet_visuals` → `rename_sheet` → `delete_sheet`
4. **Visual tools work:** `get_visual_definition` returns non-null; `set_visual_title` persists
5. **Parameter tools work:** `add_parameter` → `get_parameters` shows it → `delete_parameter`
6. **Filter tools work:** `add_filter_group` → `get_filters` shows it → `delete_filter_group`
7. **No regressions:** All existing 35 tools still work (spot-check list_datasets, search_analyses, add_calculated_field)

---

## Files Modified Summary

| File | Action | Lines Changed |
|------|--------|---------------|
| `src/quicksight_mcp/tools/analyses.py` | Fix bug + add `get_analysis_raw` | ~35 lines |
| `src/quicksight_mcp/tools/backup.py` | Fix bug | 1 line |
| `src/quicksight_mcp/client.py` | Add sheet/visual/param/filter/layout/raw methods | ~545 lines |
| `src/quicksight_mcp/tools/sheets.py` | **New file** | ~120 lines |
| `src/quicksight_mcp/tools/visuals.py` | **New file** | ~250 lines |
| `src/quicksight_mcp/tools/parameters.py` | **New file** | ~80 lines |
| `src/quicksight_mcp/tools/filters.py` | **New file** | ~80 lines |
| `src/quicksight_mcp/server.py` | Register new tool modules | ~8 lines |

---

## Verification

1. **Unit test:** Run `pytest tests/` to ensure no import errors
2. **MCP startup:** Run `python -m quicksight_mcp.server` and verify all 50 tools register
3. **Integration test:** Clone T&O Homes → describe_analysis → get_visual_definition → add_sheet → set_visual_title → cleanup
4. **Reviewer agent:** Automated validation across all new tools
