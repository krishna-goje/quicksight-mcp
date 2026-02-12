# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-02-12

### Added

- **Chart builder tools** (5): Create visuals from simple parameters -- no raw JSON needed
  - `create_kpi` -- KPI from column + aggregation
  - `create_bar_chart` -- bar chart with category + value columns
  - `create_line_chart` -- line chart with date + value + granularity
  - `create_pivot_table` -- pivot table with row/value columns
  - `create_table` -- flat table with column list
- **Visual management tools** (5): Full visual lifecycle
  - `get_visual_definition` -- inspect any visual's raw definition
  - `add_visual` -- add visual from JSON definition (advanced)
  - `delete_visual` -- remove visual with layout cleanup
  - `set_visual_title` -- update display title
  - `set_visual_layout` -- set position/size on grid (36-column layout)
- **Sheet management tools** (5): Sheet lifecycle + replication
  - `add_sheet` -- add new sheet to analysis
  - `delete_sheet` -- delete sheet and its visuals
  - `rename_sheet` -- rename existing sheet
  - `list_sheet_visuals` -- list visuals on a specific sheet
  - `replicate_sheet` -- copy entire sheet with all visuals in single API call
- **Parameter tools** (2): `add_parameter`, `delete_parameter`
- **Filter tools** (2): `add_filter_group`, `delete_filter_group`
- **QA tools** (3): Snapshot/diff system for before-and-after verification
  - `snapshot_analysis` -- capture current state as baseline
  - `diff_analysis` -- compare current state against snapshot (shows added/removed/changed)
  - `verify_analysis_health` -- comprehensive health check (status, layouts, dataset refs)
- **Raw access**: `get_analysis_raw` -- get complete analysis definition for inspection
- **Post-write verification** on all new write operations:
  - Sheets: verify exists/deleted/renamed after every operation
  - Visuals: verify exists/deleted/title after every operation
  - Parameters: verify exists/deleted after every operation
  - Filters: verify exists/deleted after every operation
  - Replicate: verify sheet + visual count match
- **Failed analysis guard** -- refuses to update FAILED analyses to prevent cascading corruption
- **20-sheet limit guard** -- checks before adding sheets, provides clear error
- **Smart measure fields** -- `CategoricalMeasureField` for COUNT/DISTINCT_COUNT (works on any column type), `NumericalMeasureField` for SUM/AVG/etc.
- **Aggregation alias map** -- accepts common variants (AVG→AVERAGE, STDEV→STDEV, etc.)
- **Cache invalidation** after successful `update_analysis` calls
- Analysis inspection tools: `get_parameters`, `get_filters`, `list_recent_refreshes`, `backup_dataset`

### Fixed

- `describe_analysis` returning 0 for sheets, calc fields, parameters, filters, and datasets (double-nested definition key lookup)
- `clone_analysis` returning null for `new_analysis_id` (wrong dict key in response)

### Changed

- Tool count: 27 → 55
- README rewritten with full developer workflow examples

## [0.1.0] - 2026-02-12

### Added

- Initial release with 27 MCP tools for AWS QuickSight
- **Dataset tools** (7): list, search, get metadata, get SQL, update SQL, refresh SPICE, check refresh status
- **Analysis tools** (6): list, search, describe structure, list visuals, list calculated fields, get column usage
- **Calculated field tools** (4): add, update, delete, get details
- **Dashboard tools** (5): list, search, version history, publish, rollback
- **Backup tools** (3): backup analysis, restore analysis, clone analysis
- **Self-learning tools** (2): usage insights, error patterns
- Self-learning engine with usage tracking, workflow detection, and optimization suggestions
- Production safety features:
  - Auto-backup before destructive operations
  - Optimistic locking for concurrent modification detection
  - Destructive change protection (blocks deletion of sheets/visuals/fields)
  - Post-write change verification
- Custom exception types: `ConcurrentModificationError`, `ChangeVerificationError`, `DestructiveChangeError`
- Standard AWS credential chain authentication with auto-detected account ID
- Local learning data storage (no telemetry)
- PyPI packaging with `quicksight-mcp` CLI entry point
- Apache 2.0 license
