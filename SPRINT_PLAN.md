# QuickSight MCP Server -- Revised Sprint Plan (Sprints 3-6)

**Version**: 1.1.0 (current) -> 1.5.0 (target)
**Date**: 2026-04-11
**Based on**: 448 tool calls across 6 weeks of production use, error logs, user feedback
**Author**: QA/Scrum Master review

---

## Sprint Overview

| Sprint | Theme | Duration | Key Deliverable | Version |
|--------|-------|----------|-----------------|---------|
| **3** | Stop the Bleeding | 1 week | Fix data-loss bugs, auth crashes, restore reliability | 1.2.0 |
| **4** | Safe Update Workflow | 1.5 weeks | Auto-backup-before-mutate, clone-based dev, JSON diff verification | 1.3.0 |
| **5** | Performance + Polish | 1 week | Search caching, case-insensitive matching, ID validation, conditional formatting | 1.4.0 |
| **6** | New Capabilities + Release | 1 week | update_parameter, update_visual_fields, new chart types, PyPI publish | 1.5.0 |

---

## Production Failure Analysis (what's driving priorities)

| Issue | Failure Rate | Calls | Root Cause |
|-------|-------------|-------|------------|
| AwsAccountId = None | 78% of errors (23/29) | Across all tools | `_init_session` STS call fails silently, `account_id` stays None |
| restore_analysis | 62% (16/26) | 26 calls | Pushes broken definitions without pre-validation |
| create_kpi | 83% (10/12) | 12 calls | Invalid aggregation on calculated columns (no column-type detection) |
| Sheets deleted | Unknown (user-reported) | -- | `update_analysis` replaces entire definition; stale cache returns 0 sheets |
| set_visual_title overwrites visual | User-reported | -- | Title update re-pushes full definition; any cache staleness corrupts |

---

## Sprint 3: Stop the Bleeding (1 week)

**Theme**: Fix every bug that causes data loss or complete tool failure. Zero new features.

**Exit Criteria**: restore_analysis failure rate < 10%, create_kpi failure rate < 20%, zero sheet-deletion incidents, auth resolves on first call.

### Items

| # | Item | Issue Ref | Effort | Priority |
|---|------|-----------|--------|----------|
| 3.1 | **Fix account_id = None at startup** | Cat C, #8 | **S** | P0 |
| | Problem: `_init_session` catches STS exception, logs warning, leaves `account_id = None`. Every subsequent call hits `ensure_account_id` which may also fail. | | | |
| | Fix: Retry STS 3x with backoff during `_init_session`. If still None, set a `_needs_auth` flag. On first `call()`/`paginate()`, force re-auth before proceeding. Never let a tool execute with `account_id = None`. | | | |
| | File: `src/quicksight_mcp/core/aws_client.py` lines 45-75 | | | |
| 3.2 | **Fix ExpiredTokenException -- add auto-retry wrapper** | Cat C, #9 | **S** | P0 |
| | Problem: `_refresh_on_expired` exists but only triggers on exact string match. Some boto3 errors use `ExpiredTokenException` class name, not substring. | | | |
| | Fix: Also check `error.__class__.__name__` and `error.response['Error']['Code']` for expired-token variants. Add single retry after successful refresh (current code retries but does not verify the refreshed session is valid). | | | |
| | File: `src/quicksight_mcp/core/aws_client.py` lines 176-212 | | | |
| 3.3 | **Fix sheet deletion during update_analysis** | Cat A, #1 | **M** | P0 |
| | Problem: When definition cache is stale (returns 0 sheets), the update pushes an empty-sheet definition. The destructive guard catches 0-sheet case but NOT the case where `get_definition` returns stale data from cache. | | | |
| | Fix: In `AnalysisService.update_analysis`, ALWAYS call `get_definition(use_cache=False)` for the destructive-change comparison (line 566). Currently it does `get_definition(analysis_id, use_cache=False)` -- verify this is actually bypassing cache. Add assertion: `if len(new_definition.get('Sheets', [])) == 0 and len(current_def.get('Sheets', [])) > 0: raise`. Also add a **sheet-count invariant check**: if `new_sheets < current_sheets * 0.5`, block unless `allow_destructive=True`. | | | |
| | Files: `src/quicksight_mcp/services/analyses.py` lines 556-625, `src/quicksight_mcp/safety/destructive_guard.py` | | | |
| 3.4 | **Fix set_visual_title overwriting entire visual** | Cat A, #2 | **M** | P0 |
| | Problem: `VisualService.set_title` fetches the full definition, modifies the Title key, then pushes the ENTIRE definition back. If the cached definition is stale or incomplete, the push overwrites the real visual with a corrupted version. | | | |
| | Fix: (a) Always fetch definition with `use_cache=False` before title-only mutations. (b) After modifying only the Title dict, run a structural comparison: verify every other key in the visual dict is unchanged before pushing. If any non-Title key differs from what was fetched, abort with error. (c) Add a `_surgical_update` helper that takes the current definition, a path to the change (sheet_id, visual_id, key), and the new value, and produces a minimal diff. | | | |
| | File: `src/quicksight_mcp/services/visuals.py` lines 228-282 | | | |
| 3.5 | **Fix restore_analysis reliability** | Cat A, #3 | **L** | P0 |
| | Problem: 62% failure rate. Broken definitions are pushed blindly. No pre-validation of the backup JSON structure. | | | |
| | Fix: (a) Validate backup JSON before attempting restore: check that `Definition` has `Sheets`, `DataSetIdentifierDeclarations`, and at least 1 sheet with visuals. (b) Run `verify_analysis_health`-style checks on the backup definition (not the live analysis) before pushing. (c) If the target analysis is in FAILED state, create a new analysis instead of updating (avoids the FAILED-state update block). (d) After restore completes, run `verify_analysis_health` and return the result alongside the restore status. | | | |
| | File: `src/quicksight_mcp/services/backup.py` lines 179-344 | | | |
| 3.6 | **Fix create_kpi calculated column failure** | Cat A, #4 | **M** | P1 |
| | Problem: 83% failure rate. When `column` is a calculated field (not a physical column), `NumericalMeasureField` with `SimpleNumericalAggregation` fails because QS expects calculated fields to be referenced differently. COUNT/DISTINCT_COUNT on calc fields also fails. | | | |
| | Fix: (a) Before building the measure field, check if `column` exists in the analysis's `CalculatedFields` list. If yes, use the calculated field's expression directly in a custom aggregation or wrap it properly. (b) For COUNT on calculated fields, use a workaround: create a simple `ifelse(isNotNull({calc_field}), 1, 0)` wrapper. (c) Add clear error message when aggregation is incompatible with column type. | | | |
| | File: `src/quicksight_mcp/services/chart_builders.py` lines 286-376 | | | |

### Test Requirements (Sprint 3)

| Test | Type | Count |
|------|------|-------|
| `test_account_id_retry_on_startup` | Unit | 3 (success, retry-success, retry-fail) |
| `test_expired_token_class_detection` | Unit | 4 (string match, class match, response code, non-expired) |
| `test_destructive_guard_stale_cache` | Unit | 3 (stale 0-sheets, stale half-sheets, fresh-ok) |
| `test_set_title_no_visual_corruption` | Unit | 3 (title-only change, stale-cache abort, visual-intact check) |
| `test_restore_validates_backup_json` | Unit | 5 (valid, no-sheets, no-definition, no-datasets, corrupt-json) |
| `test_restore_failed_analysis` | Unit | 2 (create-new path, update path) |
| `test_create_kpi_with_calculated_field` | Unit | 3 (calc field SUM, calc field COUNT, physical column baseline) |
| `test_restore_runs_health_check` | Integration | 1 |
| **Total new tests** | | **24** |

### Definition of Done (Sprint 3)
- [ ] All 6 items merged to `kgoje/v1-rewrite`
- [ ] 277+ tests passing (253 existing + 24 new)
- [ ] Zero `AwsAccountId = None` errors in manual test with expired/missing creds
- [ ] `restore_analysis` succeeds on a valid backup file in manual test
- [ ] `create_kpi` succeeds with a calculated field column in manual test
- [ ] `set_visual_title` only modifies title (verified by snapshot diff in manual test)
- [ ] CodeRabbit review on PR passes

---

## Sprint 4: Safe Update Workflow (1.5 weeks)

**Theme**: The architectural centerpiece. Implement the "never corrupt production" workflow that users are asking for.

**Exit Criteria**: Every mutating tool automatically backs up before writing, clone-based workflow is available, JSON diff verification is built-in.

### Items

| # | Item | Issue Ref | Effort | Priority |
|---|------|-----------|--------|----------|
| 4.1 | **Auto-backup middleware for all mutations** | Cat B, #5 | **M** | P0 |
| | Problem: `backup_first=True` is the default in `update_analysis`, but individual tool calls can (and do) override it to `False`. Some tools bypass `update_analysis` entirely. | | | |
| | Fix: (a) Audit every write path -- ensure ALL go through `update_analysis`. Remove any direct `_aws.call("update_analysis")` calls outside the central gateway. (b) Make `backup_first` non-overridable for destructive operations (deletes, restores). (c) Add a config flag `QUICKSIGHT_MCP_FORCE_BACKUP=true` (default true) that makes backup mandatory regardless of caller preference. (d) Log every backup path for auditability. | | | |
| | Files: `src/quicksight_mcp/services/analyses.py`, `src/quicksight_mcp/services/backup.py` | | | |
| 4.2 | **Clone-based development workflow tool** | Cat B, #5 | **L** | P0 |
| | New tool: `safe_update_analysis` (or enhance existing `clone_analysis`). Workflow: | | | |
| | 1. `clone_analysis(source_id)` -> creates `[name]-DRAFT` copy | | | |
| | 2. All subsequent mutations target the clone | | | |
| | 3. User reviews clone in QuickSight UI | | | |
| | 4. `promote_clone(clone_id, source_id)` -> copies clone definition back to source | | | |
| | 5. `cleanup_clone(clone_id)` -> deletes the draft | | | |
| | This requires: (a) A `_draft_registry` in memory that maps source_id -> clone_id. (b) New tools: `start_draft`, `promote_draft`, `discard_draft`, `list_drafts`. (c) The `promote_draft` tool runs full health check + destructive guard before applying. | | | |
| | Files: New `src/quicksight_mcp/services/drafts.py`, new `src/quicksight_mcp/tools/drafts.py` | | | |
| 4.3 | **Pre/post JSON diff verification on every mutation** | Cat B, #6 | **L** | P0 |
| | Problem: Claude says "change completed" but the change did not actually apply. Current verification checks existence/title but not structural correctness. | | | |
| | Fix: (a) Enhance `SnapshotService.diff` to accept two definition dicts (not just snapshot files). New method: `diff_definitions(before_def, after_def)` -> returns structured diff. (b) In `update_analysis`, capture `before_def = get_definition(use_cache=False)` BEFORE the API call, then `after_def = get_definition(use_cache=False)` AFTER completion. Run `diff_definitions` and include the diff in the return value. (c) Add a `verify_intended_change` helper: takes the diff and the "intent" (e.g., "title change on visual X") and confirms only the intended change occurred. If unintended changes detected, log a warning and include in response. | | | |
| | Files: `src/quicksight_mcp/services/snapshots.py`, `src/quicksight_mcp/services/analyses.py` | | | |
| 4.4 | **QuickSight JSON manipulation library (qs_json)** | Cat B, #7 | **XL** | P1 |
| | Problem: Raw dict manipulation of QuickSight definitions is error-prone. Small mistakes (missing layout element, wrong visual type key) cause sheet deletions. | | | |
| | Fix: Create `src/quicksight_mcp/qs_json/` module with typed helpers: | | | |
| | - `QSDefinition` -- wrapper around the raw dict with safe accessors | | | |
| | - `QSDefinition.get_visual(visual_id)` -> returns visual or raises | | | |
| | - `QSDefinition.set_visual_title(visual_id, title)` -> modifies only title, returns new QSDefinition (immutable) | | | |
| | - `QSDefinition.add_visual(sheet_id, visual_def)` -> validates layout, adds element, returns new QSDefinition | | | |
| | - `QSDefinition.remove_visual(visual_id)` -> removes visual + layout + filter refs, returns new QSDefinition | | | |
| | - `QSDefinition.validate()` -> runs structural checks (layout alignment, dataset refs, sheet limits) | | | |
| | - `QSDefinition.diff(other)` -> structural diff | | | |
| | Migrate `VisualService`, `SheetService`, `CalculatedFieldService` to use `QSDefinition` instead of raw dict manipulation. | | | |
| | Files: New `src/quicksight_mcp/qs_json/__init__.py`, `definition.py`, `validators.py` | | | |
| 4.5 | **Backup path validation -- accept /tmp** | Cat E, #14 | **S** | P2 |
| | Problem: `_validate_restore_path` rejects paths outside `~/.quicksight-mcp/backups` and `/tmp/qs_backup`. Users want to use `/tmp` directly. | | | |
| | Fix: Add `/tmp` to `_allowed_restore_dirs()`. Also accept any user-configured `QUICKSIGHT_BACKUP_DIR`. | | | |
| | File: `src/quicksight_mcp/services/backup.py` line 94-98 | | | |

### Test Requirements (Sprint 4)

| Test | Type | Count |
|------|------|-------|
| `test_backup_mandatory_for_destructive_ops` | Unit | 3 |
| `test_force_backup_config_flag` | Unit | 2 |
| `test_draft_lifecycle` (start, modify, promote, discard) | Unit | 6 |
| `test_draft_promote_runs_health_check` | Unit | 2 |
| `test_diff_definitions_structural` | Unit | 5 (add visual, remove visual, change title, change calc field, no change) |
| `test_update_analysis_includes_diff` | Unit | 2 |
| `test_verify_intended_change` | Unit | 4 (intended-only, unintended detected, no change, multiple changes) |
| `test_qs_definition_immutable_operations` | Unit | 8 (get/set/add/remove for visuals and calc fields) |
| `test_qs_definition_validate` | Unit | 5 (valid, orphan visual, bad dataset ref, too many sheets, empty sheets) |
| `test_backup_path_accepts_tmp` | Unit | 2 |
| `test_draft_workflow_integration` | Integration (live) | 1 |
| **Total new tests** | | **40** |

### Definition of Done (Sprint 4)
- [ ] All 5 items merged
- [ ] 317+ tests passing (277 + 40)
- [ ] Every mutating tool response includes a `changes_detected` field with before/after diff
- [ ] `start_draft` / `promote_draft` / `discard_draft` tools registered and functional
- [ ] `QSDefinition` class used by at least VisualService and SheetService
- [ ] Manual test: modify a visual title via draft workflow, verify original analysis unchanged until promote
- [ ] CodeRabbit review passes

---

## Sprint 5: Performance + Polish (1 week)

**Theme**: Fix the latency pain points and the "papercuts" that cause unnecessary tool failures.

**Exit Criteria**: search_datasets < 5 seconds (warm), calculated field lookup case-insensitive, Dashboard/Analysis ID confusion caught with clear error.

### Items

| # | Item | Issue Ref | Effort | Priority |
|---|------|-----------|--------|----------|
| 5.1 | **Fix search_datasets cold start (37-52s)** | Cat D, #10 | **M** | P1 |
| | Problem: `list_datasets` paginates ALL 4,894 datasets on first call. | | | |
| | Fix: (a) Use QS `SearchDataSets` API with server-side `Name CONTAINS` filter instead of client-side filter on full list. (b) Keep the paginate-all approach for `list_datasets` but add a progress callback or background pre-warm. (c) Cache the full list with longer TTL (15 min instead of 5 min). (d) Add `search_datasets_fast` that uses the API filter -- fallback to client-side if API filter is unavailable. | | | |
| | File: `src/quicksight_mcp/services/datasets.py` | | | |
| 5.2 | **Fix search_dashboards latency (25-36s)** | Cat D, #11 | **M** | P1 |
| | Same approach as 5.1 but for dashboards. Use `SearchDashboards` API. | | | |
| | File: `src/quicksight_mcp/services/dashboards.py` | | | |
| 5.3 | **Case-insensitive calculated field matching** | Cat E, #13 | **S** | P1 |
| | Problem: `CalculatedFieldService.get()` uses exact `f.get("Name") == name`, causing 4 failures when users pass wrong casing. | | | |
| | Fix: (a) Add case-insensitive matching as fallback: if exact match fails, try `.lower()` comparison. (b) If case-insensitive match finds exactly 1 result, use it and log a warning. If multiple matches, raise with all candidates. (c) Apply same pattern to `CalculatedFieldService.update()` and `.delete()`. | | | |
| | File: `src/quicksight_mcp/services/calculated_fields.py` lines 43-48, 147-153, 202-209 | | | |
| 5.4 | **Dashboard ID vs Analysis ID confusion guard** | Cat E, #15 | **M** | P1 |
| | Problem: Users pass a dashboard ID to analysis tools (or vice versa). The AWS error is cryptic. | | | |
| | Fix: (a) Add ID-format detection: QS analysis IDs and dashboard IDs are UUIDs but their ARNs differ (`arn:aws:quicksight:...:analysis/ID` vs `arn:aws:quicksight:...:dashboard/ID`). (b) In the `@qs_tool` decorator, intercept `ResourceNotFoundException` errors and check if the ID matches a resource of the wrong type. If so, return: "You passed a dashboard ID to an analysis tool. The correct analysis ID is: [lookup]". (c) Add a `resolve_resource_type(resource_id)` utility that checks both analysis and dashboard APIs. | | | |
| | Files: `src/quicksight_mcp/tools/_decorator.py`, new `src/quicksight_mcp/core/id_resolver.py` | | | |
| 5.5 | **Conditional formatting tool** | Cat E, #12 | **L** | P2 |
| | New tool: `add_conditional_format(analysis_id, visual_id, rules)` | | | |
| | Supports: (a) Text color based on value thresholds. (b) Background color. (c) Icon sets. (d) Data bar formatting. | | | |
| | Reads the current visual definition, adds `ConditionalFormatting` block, pushes via `update_analysis`. | | | |
| | Files: New `src/quicksight_mcp/services/formatting.py`, new `src/quicksight_mcp/tools/formatting.py` | | | |
| 5.6 | **Layout bounds validation** | Cat F, #18 | **S** | P2 |
| | Add validation in `set_visual_layout`: column_index + column_span <= 36, row_span > 0, row_index >= 0. Reject with clear error instead of letting QS API fail cryptically. | | | |
| | File: `src/quicksight_mcp/services/visuals.py` lines 305-364 | | | |

### Test Requirements (Sprint 5)

| Test | Type | Count |
|------|------|-------|
| `test_search_datasets_uses_api_filter` | Unit | 3 |
| `test_search_dashboards_uses_api_filter` | Unit | 3 |
| `test_calc_field_case_insensitive_match` | Unit | 4 (exact, case-insensitive, multiple matches, no match) |
| `test_dashboard_id_vs_analysis_id_detection` | Unit | 4 (correct ID, wrong type, unknown ID, non-UUID input) |
| `test_conditional_formatting_add` | Unit | 4 (text color, background, icon set, data bar) |
| `test_layout_bounds_validation` | Unit | 5 (valid, col overflow, negative row, zero span, max span) |
| `test_search_datasets_cached_fast` | Unit | 1 |
| **Total new tests** | | **24** |

### Definition of Done (Sprint 5)
- [ ] All 6 items merged
- [ ] 341+ tests passing (317 + 24)
- [ ] `search_datasets` returns in < 5s on warm cache, < 15s on cold (down from 37-52s)
- [ ] Case-insensitive calc field lookup works in manual test
- [ ] Passing a dashboard ID to `describe_analysis` returns helpful error
- [ ] `add_conditional_format` tool registered and functional
- [ ] Layout validation rejects invalid bounds with clear message

---

## Sprint 6: New Capabilities + Release (1 week)

**Theme**: Add the most-requested features from the original sprint 3-5 plan and publish to PyPI.

**Exit Criteria**: PyPI v1.5.0 published, all new tools documented in README, integration test suite in repo.

### Items

| # | Item | Issue Ref | Effort | Priority |
|---|------|-----------|--------|----------|
| 6.1 | **update_parameter tool** | Cat F, #19 | **M** | P2 |
| | Add ability to update parameter default values and configuration. Currently only add/delete exists. | | | |
| | File: New methods in `src/quicksight_mcp/services/parameters.py`, new tool in `tools/parameters.py` | | | |
| 6.2 | **update_visual_fields (swap columns)** | Cat F, #20 | **L** | P2 |
| | New tool: change which columns are bound to a visual's field wells without recreating the visual. Takes `visual_id`, `field_role` (category/value/color), `old_column`, `new_column`. | | | |
| | Files: `src/quicksight_mcp/services/visuals.py`, `src/quicksight_mcp/tools/visuals.py` | | | |
| 6.3 | **New chart builders: histogram, scatter, funnel** | Cat F, #21 | **L** | P2 |
| | Add `create_histogram`, `create_scatter_plot`, `create_funnel_chart` following the same pattern as existing chart builders. | | | |
| | File: `src/quicksight_mcp/services/chart_builders.py` | | | |
| 6.4 | **IAM permissions documentation in README** | Cat F, #16 | **S** | P2 |
| | Document the minimum IAM policy required. Include: `quicksight:Describe*`, `quicksight:List*`, `quicksight:Search*`, `quicksight:Update*`, `quicksight:Create*`, `quicksight:Delete*`, `sts:GetCallerIdentity`. | | | |
| | File: `README.md` | | | |
| 6.5 | **Integration test suite (CI-compatible)** | Cat F, #17 | **L** | P2 |
| | Refactor existing `tests/integration_sprint*_live.py` files into a proper `tests/integration/` directory with pytest markers. Add `pytest -m integration` runner. Use moto for mock-based integration tests that run in CI without AWS credentials. | | | |
| | Files: New `tests/integration/` directory | | | |
| 6.6 | **PyPI v1.5.0 publish** | Cat F, #23 | **M** | P2 |
| | Update `pyproject.toml` version, update CHANGELOG.md with all sprint 3-6 changes, build and publish. | | | |
| | Files: `pyproject.toml`, `CHANGELOG.md` | | | |

### Test Requirements (Sprint 6)

| Test | Type | Count |
|------|------|-------|
| `test_update_parameter` | Unit | 4 (update default, update type error, not-found, verify) |
| `test_update_visual_fields` | Unit | 5 (swap category, swap value, invalid role, not-found, verify) |
| `test_create_histogram` | Unit | 2 |
| `test_create_scatter_plot` | Unit | 2 |
| `test_create_funnel_chart` | Unit | 2 |
| `test_integration_suite_moto` | Integration | 10 (happy-path for core workflows) |
| **Total new tests** | | **25** |

### Definition of Done (Sprint 6)
- [ ] All 6 items merged
- [ ] 366+ tests passing (341 + 25)
- [ ] `pytest -m integration` runs without AWS credentials (uses moto mocks)
- [ ] README includes IAM policy section
- [ ] CHANGELOG covers all sprint 3-6 changes
- [ ] PyPI `quicksight-mcp==1.5.0` published and installable
- [ ] Tool count: 61 -> ~70

---

## Risk Register

| Risk | Probability | Impact | Sprint | Mitigation |
|------|-------------|--------|--------|------------|
| **QSDefinition refactor breaks existing tests** | High | High | 4 | Implement QSDefinition as wrapper (new code calls wrapper, old code continues working). Migrate services one at a time with full test suite run after each. |
| **Clone-based workflow hits QS analysis limit** | Medium | Medium | 4 | Check account's analysis quota before cloning. Auto-cleanup drafts older than 24h. Add `list_drafts` tool so users can see/clean up stale drafts. |
| **SearchDataSets API not available in all regions** | Medium | Low | 5 | Feature-detect: try `SearchDataSets` first, fall back to paginate-all if API returns `UnknownOperationException`. |
| **Backup JSON format changes break restore** | Medium | High | 3 | Add `schema_version` field to all backup files. Restore validates schema version and migrates if needed. |
| **Sprint 4 scope creep (qs_json is XL)** | High | Medium | 4 | Timebox qs_json to 2 days. Start with `QSDefinition` wrapper for visuals + sheets only. Calc fields and filters migrate in Sprint 5. |
| **Optimistic locking false positives block updates** | Low | Medium | 3-4 | Already configurable via settings. Add `--force` flag to tools for emergency override (with extra logging). |
| **set_visual_title surgical-update approach is fragile** | Medium | High | 3 | Combine with Sprint 4's QSDefinition immutable approach. Sprint 3 fix is temporary (always-fresh-fetch); Sprint 4 makes it structural. |

---

## Dependencies Between Sprints

```
Sprint 3 (bugs) ──> Sprint 4 (safe workflow)
    │                    │
    │                    ├── 4.3 (diff verification) depends on 3.3 (stale cache fix)
    │                    ├── 4.4 (qs_json) replaces 3.4 (surgical update) with proper solution
    │                    │
    │                    └──> Sprint 5 (perf + polish)
    │                              │
    │                              ├── 5.3 (case-insensitive) independent
    │                              ├── 5.5 (conditional format) depends on 4.4 (qs_json)
    │                              │
    │                              └──> Sprint 6 (features + release)
    │                                        │
    │                                        ├── 6.2 (update_visual_fields) depends on 4.4 (qs_json)
    │                                        ├── 6.5 (integration tests) depends on all bug fixes
    │                                        └── 6.6 (PyPI) depends on all other sprint 6 items
    │
    └── Sprint 3 is fully independent (can start immediately)
```

### Critical Path
Sprint 3.3 (stale cache fix) -> Sprint 4.3 (diff verification) -> Sprint 4.4 (qs_json) -> Sprint 6.2 (update_visual_fields)

This is the longest dependency chain. If Sprint 3 slips, everything downstream shifts.

---

## Cumulative Metrics

| Metric | Current (1.1.0) | After Sprint 3 | After Sprint 4 | After Sprint 5 | After Sprint 6 |
|--------|-----------------|-----------------|-----------------|-----------------|-----------------|
| Tools | 61 | 61 | 65 (+4 draft tools) | 67 (+2 new) | ~70 (+3 charts, +2 tools) |
| Tests | 253 | 277 | 317 | 341 | 366 |
| Known P0 bugs | 6 | 0 | 0 | 0 | 0 |
| Failure rate (auth) | 78% | < 5% | < 5% | < 5% | < 5% |
| Failure rate (restore) | 62% | < 10% | < 5% | < 5% | < 5% |
| Failure rate (create_kpi) | 83% | < 20% | < 10% | < 10% | < 10% |
| search_datasets latency | 37-52s | 37-52s | 37-52s | < 5s (warm) | < 5s (warm) |

---

## Effort Summary

| Sprint | S (1-2h) | M (half day) | L (full day) | XL (2+ days) | Total |
|--------|----------|--------------|--------------|---------------|-------|
| 3 | 2 | 3 | 1 | 0 | ~4 days |
| 4 | 1 | 1 | 2 | 1 | ~7 days |
| 5 | 2 | 3 | 1 | 0 | ~4 days |
| 6 | 1 | 2 | 3 | 0 | ~5 days |
| **Total** | **6** | **9** | **7** | **1** | **~20 days** |

---

## Items NOT Included (Backlog)

These items from the original plan are deferred beyond Sprint 6:

| Item | Reason for Deferral |
|------|---------------------|
| Sheet controls (dropdowns, date pickers) | Low usage signal; no production failures |
| batch_create_visuals | Nice-to-have; individual creates work |
| Heatmap chart builder | Lower priority than histogram/scatter/funnel |
| Dataset permission management | Not requested by users |

---

*Last updated: 2026-04-11. Review at end of each sprint.*
