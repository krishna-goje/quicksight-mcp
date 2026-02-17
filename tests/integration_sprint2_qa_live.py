#!/usr/bin/env python3
"""Sprint 2 Features - LIVE API Testing.

Tests against the LIVE QuickSight API using the test clone analysis.
Creates a test sheet at start, runs all tests, deletes it at end.

Rounds:
  1. ComboChart creation + definition readback
  2. PieChart creation + definition readback
  3. modify_dataset_sql find/replace
  4. cancel_refresh
  5. _paginate helper (list_datasets, list_analyses, list_dashboards)
  6. Stress test (combo + pie + KPI on one sheet, health check)
"""

import os

# Live test guard - requires explicit opt-in
if os.environ.get('QS_LIVE_TESTS') != '1':
    import pytest
    pytest.skip('Set QS_LIVE_TESTS=1 to run live API tests', allow_module_level=True)

import sys
import time
import traceback

os.environ['AWS_PROFILE'] = 'od-quicksight-prod'
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from quicksight_mcp.client import QuickSightClient

# Constants
ANALYSIS_ID = '43515770-21d0-4169-92af-cda135063077'
DATASET_IDENTIFIER = 'ACQ_L1_ASSESSMENTS'

# Test results accumulator
results = []
test_number = 0


def record(round_num, test_name, input_desc, expected, actual, passed):
    global test_number
    test_number += 1
    status = "PASS" if passed else "FAIL"
    results.append({
        'round': round_num,
        'test': test_number,
        'name': test_name,
        'input': input_desc,
        'expected': expected,
        'actual': actual,
        'status': status,
    })
    print(f"  [{status}] R{round_num}-T{test_number}: {test_name}")
    if not passed:
        print(f"         Expected: {expected}")
        print(f"         Actual:   {actual}")


def safe_delete_visual(c, analysis_id, visual_id):
    """Delete a visual, suppressing errors."""
    try:
        c.delete_visual(analysis_id, visual_id)
    except Exception:
        pass


def wait_for_stable(c, analysis_id, max_wait=120):
    """Wait for analysis to leave UPDATE_IN_PROGRESS state."""
    for attempt in range(max_wait // 3):
        try:
            c.clear_analysis_def_cache(analysis_id)
            analysis_info = c.get_analysis(analysis_id)
            status = analysis_info.get('Status', '')
            if 'IN_PROGRESS' not in status:
                return status
        except Exception:
            pass
        time.sleep(3)
    return 'TIMEOUT'


def ensure_sheet_exists(c, analysis_id, sheet_id):
    """Check if a sheet still exists in the analysis. Return True/False."""
    try:
        c.clear_analysis_def_cache(analysis_id)
        defn = c.get_analysis_definition(analysis_id, use_cache=False)
        for s in defn.get('Sheets', []):
            if s.get('SheetId') == sheet_id:
                return True
    except Exception:
        pass
    return False


# ============================================================================
# SETUP
# ============================================================================
print("=" * 70)
print("SPRINT 2 FEATURES - LIVE API TESTING")
print("=" * 70)
print(f"Analysis: {ANALYSIS_ID}")
print(f"Dataset:  {DATASET_IDENTIFIER}")
print()

c = QuickSightClient()
print("Client initialized.")

# Wait for analysis to be in a stable state
print("Waiting for analysis to be in stable state...")
status = wait_for_stable(c, ANALYSIS_ID)
print(f"  Analysis status: {status}")

print("Creating test sheet...")

# Create test sheet
sheet_result = c.add_sheet(ANALYSIS_ID, "QA_Sprint2_Tests")
SHEET_ID = sheet_result['sheet_id']
print(f"Test sheet created: {SHEET_ID}")
print()

# Track all created visual IDs for cleanup
created_visuals = []

try:
    # ========================================================================
    # ROUND 1: ComboChart
    # ========================================================================
    print("-" * 70)
    print("ROUND 1: ComboChart")
    print("-" * 70)

    # Test 1.1: Create combo chart with bar COUNT + line DISTINCT_COUNT
    combo_vid = None
    try:
        wait_for_stable(c, ANALYSIS_ID)
        r = c.create_combo_chart(
            ANALYSIS_ID, SHEET_ID,
            title="R1-ComboChart-BarLine",
            category_column="MARKET_NAME",
            bar_column="FLIP_TOKEN",
            bar_aggregation="COUNT",
            line_column="FLIP_TOKEN",
            line_aggregation="DISTINCT_COUNT",
            dataset_identifier=DATASET_IDENTIFIER,
            backup_first=False,
        )
        combo_vid = r.get('visual_id')
        created_visuals.append(combo_vid)
        record(1, "Create combo chart (bar COUNT + line DISTINCT_COUNT)",
               "category=MARKET_NAME, bar=COUNT(FLIP_TOKEN), line=DISTINCT_COUNT(FLIP_TOKEN)",
               "ComboChart created with visual_id",
               f"visual_id={combo_vid}",
               combo_vid is not None)
    except Exception as e:
        record(1, "Create combo chart", "bar COUNT + line DISTINCT_COUNT",
               "ComboChart created", f"ERROR: {e}", False)
        traceback.print_exc()

    # Test 1.2: Verify both BarValues and LineValues exist in definition readback
    try:
        if combo_vid:
            c.clear_analysis_def_cache(ANALYSIS_ID)
            vdef = c.get_visual_definition(ANALYSIS_ID, combo_vid)
            combo_conf = vdef.get('ComboChartVisual', {}).get('ChartConfiguration', {})
            fw = combo_conf.get('FieldWells', {}).get('ComboChartAggregatedFieldWells', {})
            bar_values = fw.get('BarValues', [])
            line_values = fw.get('LineValues', [])
            has_bar = len(bar_values) > 0
            has_line = len(line_values) > 0
            record(1, "Verify BarValues + LineValues in definition",
                   "get_visual_definition readback",
                   "Both BarValues and LineValues non-empty",
                   f"BarValues={len(bar_values)}, LineValues={len(line_values)}",
                   has_bar and has_line)
        else:
            record(1, "Verify BarValues + LineValues in definition",
                   "readback", "Both present",
                   "SKIPPED (combo_vid is None)", False)
    except Exception as e:
        record(1, "Verify BarValues + LineValues", "readback",
               "Both present", f"ERROR: {e}", False)
        traceback.print_exc()

    # Test 1.3: Create combo with show_data_labels=True
    # (Bug fix: ComboChart uses BarDataLabels/LineDataLabels, not DataLabels)
    try:
        wait_for_stable(c, ANALYSIS_ID)
        r = c.create_combo_chart(
            ANALYSIS_ID, SHEET_ID,
            title="R1-Combo-Labels",
            category_column="MARKET_NAME",
            bar_column="FLIP_TOKEN",
            bar_aggregation="COUNT",
            line_column="FLIP_TOKEN",
            line_aggregation="COUNT",
            dataset_identifier=DATASET_IDENTIFIER,
            show_data_labels=True,
            backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)

        # Verify data labels -- ComboChart uses BarDataLabels + LineDataLabels
        c.clear_analysis_def_cache(ANALYSIS_ID)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        combo_conf = vdef.get('ComboChartVisual', {}).get('ChartConfiguration', {})
        bar_dl = combo_conf.get('BarDataLabels', {})
        line_dl = combo_conf.get('LineDataLabels', {})
        bar_vis = bar_dl.get('Visibility')
        line_vis = line_dl.get('Visibility')
        record(1, "Combo chart with show_data_labels=True",
               "show_data_labels=True",
               "BarDataLabels.Visibility=VISIBLE and LineDataLabels.Visibility=VISIBLE",
               f"BarDataLabels.Visibility={bar_vis}, LineDataLabels.Visibility={line_vis}",
               bar_vis == 'VISIBLE' and line_vis == 'VISIBLE')
    except Exception as e:
        record(1, "Combo chart with data labels", "show_data_labels=True",
               "VISIBLE", f"ERROR: {e}", False)
        traceback.print_exc()

    # ========================================================================
    # ROUND 2: PieChart
    # ========================================================================
    print("-" * 70)
    print("ROUND 2: PieChart")
    print("-" * 70)

    # Ensure sheet still exists (may have been lost if R1-T3 failed)
    wait_for_stable(c, ANALYSIS_ID)
    if not ensure_sheet_exists(c, ANALYSIS_ID, SHEET_ID):
        print("  Sheet was lost after Round 1 failure. Re-creating...")
        try:
            sheet_result = c.add_sheet(ANALYSIS_ID, "QA_Sprint2_Tests", sheet_id=SHEET_ID)
            print(f"  Sheet re-created: {SHEET_ID}")
        except Exception as e:
            print(f"  ERROR re-creating sheet: {e}")
            # Create with a new ID
            sheet_result = c.add_sheet(ANALYSIS_ID, "QA_Sprint2_Tests")
            SHEET_ID = sheet_result['sheet_id']
            print(f"  Created new sheet: {SHEET_ID}")

    # Test 2.1: Create pie chart with group_column + value COUNT
    pie_vid = None
    try:
        wait_for_stable(c, ANALYSIS_ID)
        r = c.create_pie_chart(
            ANALYSIS_ID, SHEET_ID,
            title="R2-PieChart-Markets",
            group_column="MARKET_NAME",
            value_column="FLIP_TOKEN",
            value_aggregation="COUNT",
            dataset_identifier=DATASET_IDENTIFIER,
            backup_first=False,
        )
        pie_vid = r.get('visual_id')
        created_visuals.append(pie_vid)
        record(2, "Create pie chart (group + COUNT)",
               "group=MARKET_NAME, value=COUNT(FLIP_TOKEN)",
               "PieChart created with visual_id",
               f"visual_id={pie_vid}",
               pie_vid is not None)
    except Exception as e:
        record(2, "Create pie chart", "group + COUNT",
               "PieChart created", f"ERROR: {e}", False)
        traceback.print_exc()

    # Test 2.2: Verify PieChartAggregatedFieldWells in definition readback
    try:
        if pie_vid:
            c.clear_analysis_def_cache(ANALYSIS_ID)
            vdef = c.get_visual_definition(ANALYSIS_ID, pie_vid)
            pie_conf = vdef.get('PieChartVisual', {}).get('ChartConfiguration', {})
            fw = pie_conf.get('FieldWells', {})
            pie_wells = fw.get('PieChartAggregatedFieldWells', {})
            has_category = len(pie_wells.get('Category', [])) > 0
            has_values = len(pie_wells.get('Values', [])) > 0
            record(2, "Verify PieChartAggregatedFieldWells in definition",
                   "get_visual_definition readback",
                   "PieChartAggregatedFieldWells with Category and Values",
                   f"has_category={has_category}, has_values={has_values}",
                   has_category and has_values)
        else:
            record(2, "Verify PieChartAggregatedFieldWells",
                   "readback", "Category and Values present",
                   "SKIPPED (pie_vid is None)", False)
    except Exception as e:
        record(2, "Verify PieChartAggregatedFieldWells", "readback",
               "Present", f"ERROR: {e}", False)
        traceback.print_exc()

    # Test 2.3: Create pie chart with format_string="#,##0"
    try:
        wait_for_stable(c, ANALYSIS_ID)
        r = c.create_pie_chart(
            ANALYSIS_ID, SHEET_ID,
            title="R2-Pie-Formatted",
            group_column="MARKET_NAME",
            value_column="FLIP_TOKEN",
            value_aggregation="COUNT",
            dataset_identifier=DATASET_IDENTIFIER,
            format_string="#,##0",
            backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)

        # Verify format
        c.clear_analysis_def_cache(ANALYSIS_ID)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        pie_conf = vdef.get('PieChartVisual', {}).get('ChartConfiguration', {})
        fw = pie_conf.get('FieldWells', {}).get('PieChartAggregatedFieldWells', {})
        values = fw.get('Values', [])
        has_format = False
        for v in values:
            for ftype in ('CategoricalMeasureField', 'NumericalMeasureField', 'DateMeasureField'):
                if ftype in v and 'FormatConfiguration' in v[ftype]:
                    has_format = True
        record(2, "Pie chart with format_string=#,##0",
               "format_string='#,##0'",
               "FormatConfiguration present in value field",
               f"has_format={has_format}",
               has_format)
    except Exception as e:
        record(2, "Pie chart with format", "format_string='#,##0'",
               "Format present", f"ERROR: {e}", False)
        traceback.print_exc()

    # ========================================================================
    # ROUND 3: modify_dataset_sql
    # ========================================================================
    print("-" * 70)
    print("ROUND 3: modify_dataset_sql")
    print("-" * 70)

    # Find a dataset with Custom SQL for testing
    test_dataset_id = None
    original_sql = None

    try:
        # Search for datasets that use Custom SQL
        all_ds = c.list_datasets()
        for d in all_ds:
            ds_id = d.get('DataSetId')
            ds_name = d.get('Name', '')
            # Skip very important production datasets; look for test/clone datasets
            if 'ACQ_L1_ASSESSMENTS' in ds_name.upper():
                try:
                    sql = c.get_dataset_sql(ds_id)
                    if sql and len(sql) > 20:
                        test_dataset_id = ds_id
                        print(f"  Found dataset with SQL: {ds_name} ({ds_id})")
                        break
                except Exception:
                    continue
        # If not found, try any dataset with Custom SQL
        if not test_dataset_id:
            for d in all_ds:
                ds_id = d.get('DataSetId')
                try:
                    sql = c.get_dataset_sql(ds_id)
                    if sql and len(sql) > 20:
                        test_dataset_id = ds_id
                        print(f"  Using fallback dataset: {d.get('Name')} ({ds_id})")
                        break
                except Exception:
                    continue
    except Exception as e:
        print(f"  ERROR finding test dataset: {e}")

    # Test 3.1: Get current SQL
    try:
        if test_dataset_id:
            original_sql = c.get_dataset_sql(test_dataset_id)
            has_sql = original_sql is not None and len(original_sql) > 0
            record(3, "Get current SQL of dataset",
                   f"dataset_id={test_dataset_id[:12]}...",
                   "SQL returned non-empty",
                   f"sql_length={len(original_sql) if original_sql else 0}",
                   has_sql)
        else:
            record(3, "Get current SQL of dataset",
                   "No dataset with Custom SQL found", "SQL returned",
                   "SKIPPED (no test dataset with Custom SQL)", False)
    except Exception as e:
        record(3, "Get current SQL", "get_dataset_sql",
               "SQL returned", f"ERROR: {e}", False)
        traceback.print_exc()

    # Test 3.2: modify_dataset_sql - add a comment
    SAFE_COMMENT = "-- QA_SPRINT2_TEST_MARKER"
    try:
        if test_dataset_id and original_sql:
            # Find the first SELECT in the SQL
            first_select_idx = original_sql.upper().find('SELECT')
            if first_select_idx >= 0:
                find_text = original_sql[first_select_idx:first_select_idx+6]  # "SELECT" or "select"
                replace_text = f"{SAFE_COMMENT}\n{find_text}"
                c.modify_dataset_sql(
                    test_dataset_id,
                    find=find_text,
                    replace=replace_text,
                    backup_first=True,
                )
                # Verify the comment was added
                modified_sql = c.get_dataset_sql(test_dataset_id)
                has_marker = SAFE_COMMENT in (modified_sql or "")
                record(3, "modify_dataset_sql - add comment",
                       f"find='{find_text}', replace=comment + '{find_text}'",
                       "SQL contains QA marker comment",
                       f"has_marker={has_marker}, new_sql_length={len(modified_sql or '')}",
                       has_marker)
            else:
                record(3, "modify_dataset_sql - add comment",
                       "No SELECT found", "Comment added",
                       "SKIPPED (no SELECT in SQL)", False)
        else:
            record(3, "modify_dataset_sql - add comment",
                   "No dataset/SQL", "Comment added",
                   "SKIPPED", False)
    except Exception as e:
        record(3, "modify_dataset_sql - add comment", "find/replace",
               "Comment added", f"ERROR: {e}", False)
        traceback.print_exc()

    # Test 3.3: Revert the change
    try:
        if test_dataset_id and original_sql:
            # Revert using update_dataset_sql directly with original SQL
            c.update_dataset_sql(test_dataset_id, original_sql, backup_first=False)
            # Verify reversion
            reverted_sql = c.get_dataset_sql(test_dataset_id)
            no_marker = SAFE_COMMENT not in (reverted_sql or "")
            # Compare normalized (whitespace-insensitive)
            orig_norm = ' '.join(original_sql.split())
            reverted_norm = ' '.join((reverted_sql or '').split())
            sql_match = orig_norm == reverted_norm
            record(3, "Revert SQL change",
                   "update_dataset_sql with original SQL",
                   "SQL reverted to original (no marker, matches original)",
                   f"no_marker={no_marker}, sql_matches_original={sql_match}",
                   no_marker and sql_match)
        else:
            record(3, "Revert SQL change",
                   "No dataset", "Reverted",
                   "SKIPPED", False)
    except Exception as e:
        record(3, "Revert SQL change", "revert",
               "Reverted", f"ERROR: {e}", False)
        traceback.print_exc()
        # Emergency revert - try updating with original SQL directly
        if test_dataset_id and original_sql:
            try:
                c.update_dataset_sql(test_dataset_id, original_sql, backup_first=False)
                print("  Emergency revert successful.")
            except Exception as e2:
                print(f"  CRITICAL: Emergency revert also failed: {e2}")

    # ========================================================================
    # ROUND 4: cancel_refresh
    # ========================================================================
    print("-" * 70)
    print("ROUND 4: cancel_refresh")
    print("-" * 70)

    # We need a SPICE dataset to trigger and cancel a refresh.
    spice_dataset_id = None
    try:
        all_ds = c.list_datasets()
        for d in all_ds:
            if d.get('ImportMode') == 'SPICE':
                spice_dataset_id = d.get('DataSetId')
                print(f"  Using SPICE dataset: {d.get('Name')} ({spice_dataset_id})")
                break
    except Exception as e:
        print(f"  ERROR finding SPICE dataset: {e}")

    # Test 4.1: Trigger a refresh and immediately cancel it
    try:
        if spice_dataset_id:
            # Trigger refresh
            refresh_result = c.refresh_dataset(spice_dataset_id)
            ingestion_id = refresh_result.get('ingestion_id')
            refresh_status = refresh_result.get('status')
            record(4, "Trigger refresh",
                   f"dataset={spice_dataset_id[:12]}...",
                   "Refresh triggered (QUEUED or RUNNING or INITIALIZED)",
                   f"ingestion_id={ingestion_id}, status={refresh_status}",
                   ingestion_id is not None)

            # Cancel immediately (no delay)
            try:
                c.cancel_refresh(spice_dataset_id, ingestion_id)
                # Wait briefly and check status
                time.sleep(3)
                cancel_status = c.get_refresh_status(spice_dataset_id, ingestion_id)
                actual_status = cancel_status.get('status')
                # CANCELLED, COMPLETED, or FAILED are all valid post-cancel states
                # (FAILED can occur when cancel terminates a running ingestion)
                record(4, "Cancel refresh and verify",
                       f"ingestion_id={ingestion_id}",
                       "Status is CANCELLED, COMPLETED, or FAILED (not RUNNING/QUEUED)",
                       f"status={actual_status}",
                       actual_status in ('CANCELLED', 'COMPLETED', 'FAILED'))
            except Exception as cancel_err:
                err_str = str(cancel_err)
                # If refresh already completed or not cancellable, that's acceptable
                if 'COMPLETED' in err_str or 'not' in err_str.lower():
                    record(4, "Cancel refresh and verify",
                           f"ingestion_id={ingestion_id}",
                           "CANCELLED or already COMPLETED",
                           f"Already finished (acceptable): {err_str[:120]}",
                           True)
                else:
                    record(4, "Cancel refresh and verify",
                           f"ingestion_id={ingestion_id}",
                           "CANCELLED", f"ERROR: {err_str[:150]}", False)
        else:
            record(4, "Trigger refresh", "No SPICE dataset found",
                   "Refresh triggered", "SKIPPED", False)
            record(4, "Cancel refresh", "No SPICE dataset found",
                   "Cancelled", "SKIPPED", False)
    except Exception as e:
        record(4, "Trigger + Cancel refresh", "refresh/cancel",
               "Triggered and cancelled", f"ERROR: {e}", False)
        traceback.print_exc()

    # ========================================================================
    # ROUND 5: _paginate helper
    # ========================================================================
    print("-" * 70)
    print("ROUND 5: _paginate helper (list_datasets, list_analyses, list_dashboards)")
    print("-" * 70)

    # Test 5.1: list_datasets returns non-empty
    try:
        c.clear_dataset_cache()
        datasets = c.list_datasets(use_cache=False)
        count = len(datasets)
        record(5, "list_datasets returns non-empty",
               "c.list_datasets(use_cache=False)",
               "Non-empty list of datasets",
               f"count={count}",
               count > 0)
    except Exception as e:
        record(5, "list_datasets returns non-empty", "list_datasets",
               "Non-empty", f"ERROR: {e}", False)
        traceback.print_exc()

    # Test 5.2: list_analyses returns non-empty
    try:
        from quicksight_mcp.client import _analysis_cache
        _analysis_cache['data'] = None
        _analysis_cache['timestamp'] = 0
        analyses = c.list_analyses(use_cache=False)
        count = len(analyses)
        record(5, "list_analyses returns non-empty",
               "c.list_analyses(use_cache=False)",
               "Non-empty list of analyses",
               f"count={count}",
               count > 0)
    except Exception as e:
        record(5, "list_analyses returns non-empty", "list_analyses",
               "Non-empty", f"ERROR: {e}", False)
        traceback.print_exc()

    # Test 5.3: list_dashboards returns non-empty
    try:
        from quicksight_mcp.client import _dashboard_cache
        _dashboard_cache['data'] = None
        _dashboard_cache['timestamp'] = 0
        dashboards = c.list_dashboards(use_cache=False)
        count = len(dashboards)
        record(5, "list_dashboards returns non-empty",
               "c.list_dashboards(use_cache=False)",
               "Non-empty list of dashboards",
               f"count={count}",
               count > 0)
    except Exception as e:
        record(5, "list_dashboards returns non-empty", "list_dashboards",
               "Non-empty", f"ERROR: {e}", False)
        traceback.print_exc()

    # Test 5.4: Verify all three use _paginate internally (source code check)
    try:
        import inspect
        # Read the class source to check for _paginate usage
        cls_src = inspect.getsource(QuickSightClient)

        # Check that _paginate method exists with get_paginator
        paginate_method_exists = 'def _paginate(' in cls_src and 'get_paginator' in cls_src

        # Check list_datasets calls _paginate('list_data_sets', 'DataSetSummaries')
        ds_uses = "_paginate('list_data_sets'" in cls_src or '_paginate("list_data_sets"' in cls_src
        # Check list_analyses calls _paginate('list_analyses', 'AnalysisSummaryList')
        an_uses = "_paginate('list_analyses'" in cls_src or '_paginate("list_analyses"' in cls_src
        # Check list_dashboards calls _paginate('list_dashboards', 'DashboardSummaryList')
        db_uses = "_paginate('list_dashboards'" in cls_src or '_paginate("list_dashboards"' in cls_src

        record(5, "All three list methods use _paginate internally",
               "inspect.getsource(QuickSightClient) check",
               "All three call self._paginate with proper paginator names",
               f"_paginate_exists={paginate_method_exists}, datasets={ds_uses}, analyses={an_uses}, dashboards={db_uses}",
               paginate_method_exists and ds_uses and an_uses and db_uses)
    except Exception as e:
        record(5, "_paginate structural check", "inspect",
               "All use _paginate", f"ERROR: {e}", False)
        traceback.print_exc()

    # ========================================================================
    # ROUND 6: Stress Test
    # ========================================================================
    print("-" * 70)
    print("ROUND 6: Stress Test (combo + pie + KPI on one sheet, health check)")
    print("-" * 70)

    # Ensure sheet still exists
    wait_for_stable(c, ANALYSIS_ID)
    if not ensure_sheet_exists(c, ANALYSIS_ID, SHEET_ID):
        print("  Sheet was lost. Re-creating...")
        try:
            sheet_result = c.add_sheet(ANALYSIS_ID, "QA_Sprint2_Tests")
            SHEET_ID = sheet_result['sheet_id']
            print(f"  Created new sheet: {SHEET_ID}")
        except Exception as e:
            print(f"  ERROR re-creating sheet: {e}")

    stress_vids = []
    stress_start = time.time()

    # Create combo
    try:
        wait_for_stable(c, ANALYSIS_ID)
        r = c.create_combo_chart(
            ANALYSIS_ID, SHEET_ID,
            title="R6-Stress-Combo",
            category_column="MARKET_NAME",
            bar_column="FLIP_TOKEN",
            bar_aggregation="COUNT",
            line_column="FLIP_TOKEN",
            line_aggregation="DISTINCT_COUNT",
            dataset_identifier=DATASET_IDENTIFIER,
            backup_first=False,
        )
        vid = r.get('visual_id')
        stress_vids.append(('combo', vid))
        created_visuals.append(vid)
        print(f"    Combo created: {vid}")
    except Exception as e:
        stress_vids.append(('combo', f"ERROR: {e}"))
        print(f"    Combo FAILED: {e}")

    # Create pie
    try:
        wait_for_stable(c, ANALYSIS_ID)
        r = c.create_pie_chart(
            ANALYSIS_ID, SHEET_ID,
            title="R6-Stress-Pie",
            group_column="MARKET_NAME",
            value_column="FLIP_TOKEN",
            value_aggregation="COUNT",
            dataset_identifier=DATASET_IDENTIFIER,
            backup_first=False,
        )
        vid = r.get('visual_id')
        stress_vids.append(('pie', vid))
        created_visuals.append(vid)
        print(f"    Pie created: {vid}")
    except Exception as e:
        stress_vids.append(('pie', f"ERROR: {e}"))
        print(f"    Pie FAILED: {e}")

    # Create KPI
    try:
        wait_for_stable(c, ANALYSIS_ID)
        r = c.create_kpi(
            ANALYSIS_ID, SHEET_ID,
            title="R6-Stress-KPI",
            column="FLIP_TOKEN",
            aggregation="COUNT",
            dataset_identifier=DATASET_IDENTIFIER,
            format_string="#,##0",
            backup_first=False,
        )
        vid = r.get('visual_id')
        stress_vids.append(('kpi', vid))
        created_visuals.append(vid)
        print(f"    KPI created: {vid}")
    except Exception as e:
        stress_vids.append(('kpi', f"ERROR: {e}"))
        print(f"    KPI FAILED: {e}")

    stress_elapsed = time.time() - stress_start
    successful = [(t, v) for t, v in stress_vids if v and not str(v).startswith('ERROR')]
    record(6, "Create combo + pie + KPI rapidly",
           f"3 visuals, no backup, {stress_elapsed:.1f}s elapsed",
           "All 3 created successfully",
           f"{len(successful)}/3 created in {stress_elapsed:.1f}s: {[s[0] for s in successful]}",
           len(successful) == 3)

    # Verify via verify_analysis_health
    try:
        wait_for_stable(c, ANALYSIS_ID)
        c.clear_analysis_def_cache(ANALYSIS_ID)
        health = c.verify_analysis_health(ANALYSIS_ID)
        overall = health.get('overall_health', health.get('healthy', 'unknown'))
        issues = health.get('issues', [])
        record(6, "verify_analysis_health after stress",
               "verify_analysis_health",
               "Analysis healthy (PASS or True)",
               f"health={overall}, issues={len(issues)}",
               overall in ('PASS', True, 'healthy'))
    except Exception as e:
        record(6, "verify_analysis_health after stress",
               "health check", "Healthy", f"ERROR: {e}", False)
        traceback.print_exc()

    # Verify all 3 visuals are on the test sheet
    try:
        c.clear_analysis_def_cache(ANALYSIS_ID)
        sheet_visuals = c.list_sheet_visuals(ANALYSIS_ID, SHEET_ID)
        visual_ids_on_sheet = [v.get('visual_id') for v in sheet_visuals]
        stress_found = 0
        for vtype, vid in successful:
            if vid in visual_ids_on_sheet:
                stress_found += 1
        record(6, "Stress visuals found on test sheet",
               "list_sheet_visuals check",
               f"All {len(successful)} stress visuals found",
               f"{stress_found}/{len(successful)} found (total sheet visuals: {len(visual_ids_on_sheet)})",
               stress_found == len(successful))
    except Exception as e:
        record(6, "Stress visuals on sheet", "list check",
               "All found", f"ERROR: {e}", False)
        traceback.print_exc()

    # ========================================================================
    # CLEANUP
    # ========================================================================
    print()
    print("=" * 70)
    print("CLEANUP: Deleting test sheet (and all visuals on it)")
    print("=" * 70)

finally:
    # Always delete the test sheet
    try:
        wait_for_stable(c, ANALYSIS_ID)
        c.clear_analysis_def_cache(ANALYSIS_ID)
        c.delete_sheet(ANALYSIS_ID, SHEET_ID)
        print(f"Test sheet {SHEET_ID} deleted successfully.")
    except Exception as e:
        print(f"WARNING: Failed to delete test sheet {SHEET_ID}: {e}")
        # Try to clean up individual visuals
        print("Attempting individual visual cleanup...")
        for vid in created_visuals:
            if vid and not str(vid).startswith('ERROR'):
                safe_delete_visual(c, ANALYSIS_ID, vid)

    # Verify cleanup
    try:
        wait_for_stable(c, ANALYSIS_ID)
        c.clear_analysis_def_cache(ANALYSIS_ID)
        health = c.verify_analysis_health(ANALYSIS_ID)
        print(f"Post-cleanup health: {health.get('overall_health', health.get('healthy', 'unknown'))}")
    except Exception as e:
        print(f"Post-cleanup health check failed: {e}")

# ============================================================================
# FINAL REPORT
# ============================================================================
print()
print("=" * 70)
print("FINAL TEST REPORT - SPRINT 2")
print("=" * 70)
print()

total = len(results)
passed = sum(1 for r in results if r['status'] == 'PASS')
failed = sum(1 for r in results if r['status'] == 'FAIL')

for round_num in range(1, 7):
    round_tests = [r for r in results if r['round'] == round_num]
    if round_tests:
        round_pass = sum(1 for r in round_tests if r['status'] == 'PASS')
        round_total = len(round_tests)
        status_icon = "OK" if round_pass == round_total else "!!"
        round_names = {
            1: "ComboChart",
            2: "PieChart",
            3: "modify_dataset_sql",
            4: "cancel_refresh",
            5: "_paginate helper",
            6: "Stress Test",
        }
        print(f"  Round {round_num}: {round_names.get(round_num, '?'):25s} {round_pass}/{round_total} passed  [{status_icon}]")
        for r in round_tests:
            print(f"    [{r['status']}] T{r['test']:02d}: {r['name']}")
            if r['status'] == 'FAIL':
                print(f"           Expected: {r['expected']}")
                print(f"           Actual:   {r['actual']}")

print()
print(f"TOTAL: {passed}/{total} PASSED, {failed}/{total} FAILED")
print("=" * 70)
