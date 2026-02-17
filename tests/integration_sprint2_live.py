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
import sys
import time
import traceback

os.environ['AWS_PROFILE'] = 'od-quicksight-prod'
sys.path.insert(0, '/Users/krishnagoje/quicksight-mcp/src')

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

# Wait for analysis to be in a stable state (not UPDATE_IN_PROGRESS)
print("Waiting for analysis to be in stable state...")
for attempt in range(30):
    analysis_info = c.get_analysis(ANALYSIS_ID)
    status = analysis_info.get('Status', '')
    if 'IN_PROGRESS' not in status:
        print(f"  Analysis status: {status} (stable)")
        break
    print(f"  Attempt {attempt+1}: status={status}, waiting 5s...")
    time.sleep(5)
else:
    print("  WARNING: Analysis still in progress after 150s, proceeding anyway...")

# Clear all caches to avoid stale data
c.clear_analysis_def_cache(ANALYSIS_ID)

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

    # Test 1.1: Create combo chart with bar COUNT + line COUNT
    combo_vid = None
    try:
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
        record(1, "Create combo chart", "bar COUNT + line COUNT",
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
    try:
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

        # Verify data labels
        c.clear_analysis_def_cache(ANALYSIS_ID)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        combo_conf = vdef.get('ComboChartVisual', {}).get('ChartConfiguration', {})
        dl = combo_conf.get('DataLabels', {})
        vis = dl.get('Visibility')
        record(1, "Combo chart with show_data_labels=True",
               "show_data_labels=True",
               "DataLabels.Visibility=VISIBLE",
               f"Visibility={vis}",
               vis == 'VISIBLE')
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

    # Test 2.1: Create pie chart with group_column + value COUNT
    pie_vid = None
    try:
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

    # Find a suitable SPICE dataset for SQL modification
    # Use the ACQ_L1_ASSESSMENTS dataset (search for it by name)
    test_dataset_id = None
    original_sql = None

    try:
        ds_results = c.search_datasets("ACQ_L1_ASSESSMENTS")
        if ds_results:
            test_dataset_id = ds_results[0].get('DataSetId')
            print(f"  Found dataset: {test_dataset_id}")
        else:
            print("  WARNING: Could not find ACQ_L1_ASSESSMENTS dataset, searching alternatives...")
            # Fallback: list datasets and pick one with Custom SQL
            all_ds = c.list_datasets()
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
                   f"dataset_id={test_dataset_id}",
                   "SQL returned non-empty",
                   f"sql_length={len(original_sql) if original_sql else 0}",
                   has_sql)
        else:
            record(3, "Get current SQL of dataset",
                   "No dataset found", "SQL returned",
                   "SKIPPED (no test dataset)", False)
    except Exception as e:
        record(3, "Get current SQL", "get_dataset_sql",
               "SQL returned", f"ERROR: {e}", False)
        traceback.print_exc()

    # Test 3.2: modify_dataset_sql - add a comment
    SAFE_COMMENT = "-- QA_SPRINT2_TEST_MARKER"
    try:
        if test_dataset_id and original_sql:
            c.modify_dataset_sql(
                test_dataset_id,
                find="SELECT",
                replace=f"{SAFE_COMMENT}\nSELECT",
                backup_first=True,
            )
            # Verify the comment was added
            modified_sql = c.get_dataset_sql(test_dataset_id)
            has_marker = SAFE_COMMENT in (modified_sql or "")
            record(3, "modify_dataset_sql - add comment",
                   f"find='SELECT', replace='{SAFE_COMMENT}\\nSELECT'",
                   "SQL contains QA marker comment",
                   f"has_marker={has_marker}, new_sql_length={len(modified_sql or '')}",
                   has_marker)
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
            c.modify_dataset_sql(
                test_dataset_id,
                find=f"{SAFE_COMMENT}\nSELECT",
                replace="SELECT",
                backup_first=False,
            )
            # Verify reversion
            reverted_sql = c.get_dataset_sql(test_dataset_id)
            no_marker = SAFE_COMMENT not in (reverted_sql or "")
            # Compare normalized (whitespace-insensitive)
            orig_norm = ' '.join(original_sql.split())
            reverted_norm = ' '.join((reverted_sql or '').split())
            sql_match = orig_norm == reverted_norm
            record(3, "Revert SQL change",
                   f"Remove '{SAFE_COMMENT}' marker",
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
    # Use the same test_dataset_id if it's SPICE, otherwise find one.
    spice_dataset_id = None
    try:
        if test_dataset_id:
            ds_detail = c.get_dataset(test_dataset_id)
            if ds_detail.get('ImportMode') == 'SPICE':
                spice_dataset_id = test_dataset_id
        if not spice_dataset_id:
            all_ds = c.list_datasets()
            for d in all_ds:
                if d.get('ImportMode') == 'SPICE':
                    spice_dataset_id = d.get('DataSetId')
                    print(f"  Using SPICE dataset: {d.get('Name')} ({spice_dataset_id})")
                    break
    except Exception as e:
        print(f"  ERROR finding SPICE dataset: {e}")

    # Test 4.1: Trigger a refresh and cancel it
    try:
        if spice_dataset_id:
            # Trigger refresh
            refresh_result = c.refresh_dataset(spice_dataset_id)
            ingestion_id = refresh_result.get('ingestion_id')
            refresh_status = refresh_result.get('status')
            record(4, "Trigger refresh",
                   f"dataset={spice_dataset_id}",
                   "Refresh triggered (QUEUED or RUNNING or INITIALIZED)",
                   f"ingestion_id={ingestion_id}, status={refresh_status}",
                   ingestion_id is not None)

            # Small delay to let it register
            time.sleep(1)

            # Cancel it
            try:
                c.cancel_refresh(spice_dataset_id, ingestion_id)
                # Check the status after cancellation
                cancel_status = c.get_refresh_status(spice_dataset_id, ingestion_id)
                actual_status = cancel_status.get('status')
                record(4, "Cancel refresh",
                       f"ingestion_id={ingestion_id}",
                       "Status is CANCELLED (or already COMPLETED)",
                       f"status={actual_status}",
                       actual_status in ('CANCELLED', 'COMPLETED'))
            except Exception as cancel_err:
                err_str = str(cancel_err)
                # If refresh already completed, that's acceptable
                if 'COMPLETED' in err_str or 'not in a cancellable state' in err_str.lower():
                    record(4, "Cancel refresh",
                           f"ingestion_id={ingestion_id}",
                           "CANCELLED or already COMPLETED",
                           f"Already completed (acceptable): {err_str[:100]}",
                           True)
                else:
                    record(4, "Cancel refresh",
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
        # Clear cache to force fresh pagination
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

    # Test 5.4: Verify all three use _paginate internally (structural check)
    try:
        import inspect
        paginate_src = inspect.getsource(c._paginate)
        has_paginate = 'get_paginator' in paginate_src and 'paginate' in paginate_src

        # Verify list_datasets, list_analyses, list_dashboards all call _paginate
        ds_src = inspect.getsource(c.list_datasets)
        an_src = inspect.getsource(c.list_analyses)
        db_src = inspect.getsource(c.list_dashboards)

        ds_uses = '_paginate' in ds_src
        an_uses = '_paginate' in an_src
        db_uses = '_paginate' in db_src

        record(5, "All three list methods use _paginate internally",
               "inspect.getsource check",
               "All three call self._paginate",
               f"datasets={ds_uses}, analyses={an_uses}, dashboards={db_uses}",
               ds_uses and an_uses and db_uses)
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

    stress_vids = []
    stress_start = time.time()

    # Test 6.1: Create combo rapidly
    try:
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
    except Exception as e:
        stress_vids.append(('combo', f"ERROR: {e}"))

    # Test 6.2: Create pie rapidly
    try:
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
    except Exception as e:
        stress_vids.append(('pie', f"ERROR: {e}"))

    # Test 6.3: Create KPI rapidly
    try:
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
    except Exception as e:
        stress_vids.append(('kpi', f"ERROR: {e}"))

    stress_elapsed = time.time() - stress_start
    successful = [(t, v) for t, v in stress_vids if v and not str(v).startswith('ERROR')]
    record(6, "Create combo + pie + KPI rapidly",
           f"3 visuals, no backup, {stress_elapsed:.1f}s elapsed",
           "All 3 created successfully",
           f"{len(successful)}/3 created in {stress_elapsed:.1f}s: {[s[0] for s in successful]}",
           len(successful) == 3)

    # Test 6.4: Verify all 3 exist via verify_analysis_health
    try:
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

    # Test 6.5: Verify all 3 visuals are on the test sheet
    try:
        c.clear_analysis_def_cache(ANALYSIS_ID)
        sheet_visuals = c.list_sheet_visuals(ANALYSIS_ID, SHEET_ID)
        visual_ids_on_sheet = [v.get('visual_id') for v in sheet_visuals]
        # Find the successful stress visual IDs
        stress_found = 0
        for vtype, vid in successful:
            if vid in visual_ids_on_sheet:
                stress_found += 1
        record(6, "Stress visuals found on test sheet",
               "list_sheet_visuals check",
               f"All {len(successful)} stress visuals found",
               f"{stress_found}/{len(successful)} found on sheet (total sheet visuals: {len(visual_ids_on_sheet)})",
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
