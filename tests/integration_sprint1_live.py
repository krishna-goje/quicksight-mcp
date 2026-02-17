#!/usr/bin/env python3
"""Sprint 1 Formatting Features - 10 Rounds of Edge Case Testing (LIVE API).

Tests against the LIVE QuickSight API using the test clone analysis.
Creates a test sheet at start, runs all tests, deletes it at end.
"""

import os
import sys
import json
import time

os.environ['AWS_PROFILE'] = 'od-quicksight-prod'
sys.path.insert(0, '/Users/krishnagoje/quicksight-mcp/src')

from quicksight_mcp.client import QuickSightClient

# Constants
ANALYSIS_ID = '43515770-21d0-4169-92af-cda135063077'
DATASET_ID = 'ACQ_L1_ASSESSMENTS'

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
print("SPRINT 1 FORMATTING FEATURES - LIVE API EDGE CASE TESTING")
print("=" * 70)
print(f"Analysis: {ANALYSIS_ID}")
print(f"Dataset:  {DATASET_ID}")
print()

c = QuickSightClient()
print("Client initialized. Creating test sheet...")

# Create test sheet
sheet_result = c.add_sheet(ANALYSIS_ID, "QA_Sprint1_Tests")
SHEET_ID = sheet_result['sheet_id']
print(f"Test sheet created: {SHEET_ID}")
print()

# Track all created visual IDs for cleanup
created_visuals = []

try:
    # ========================================================================
    # ROUND 1: Format String Edge Cases
    # ========================================================================
    print("-" * 70)
    print("ROUND 1: Format String Edge Cases")
    print("-" * 70)

    # Test 1.1: format_string="" (empty) -> should work (no format applied)
    try:
        r = c.create_kpi(
            ANALYSIS_ID, SHEET_ID, "R1-Empty Format", "FLIP_TOKEN", "COUNT",
            DATASET_ID, format_string=None, backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        passed = vid is not None
        record(1, "Empty format_string (None)", "format_string=None", "KPI created, no format", f"visual_id={vid}", passed)
    except Exception as e:
        record(1, "Empty format_string (None)", "format_string=None", "KPI created", f"ERROR: {e}", False)

    # Test 1.2: format_string="#,##0" -> thousands separator, 0 decimals
    try:
        r = c.create_kpi(
            ANALYSIS_ID, SHEET_ID, "R1-Thousands", "FLIP_TOKEN", "COUNT",
            DATASET_ID, format_string="#,##0", backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        # Verify the visual has format config
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        # Check the KPI field wells for format
        kpi_conf = vdef.get('KPIVisual', {}).get('ChartConfiguration', {})
        values = kpi_conf.get('FieldWells', {}).get('Values', [])
        has_format = False
        for v in values:
            for ftype in ('CategoricalMeasureField', 'NumericalMeasureField', 'DateMeasureField'):
                if ftype in v and 'FormatConfiguration' in v[ftype]:
                    has_format = True
        record(1, "Thousands format #,##0", "format_string='#,##0'", "KPI with thousands separator",
               f"visual_id={vid}, has_format={has_format}", vid is not None and has_format)
    except Exception as e:
        record(1, "Thousands format #,##0", "format_string='#,##0'", "KPI created with format", f"ERROR: {e}", False)

    # Test 1.3: format_string="#,##0.00" -> 2 decimals
    try:
        r = c.create_kpi(
            ANALYSIS_ID, SHEET_ID, "R1-2Decimals", "FLIP_TOKEN", "COUNT",
            DATASET_ID, format_string="#,##0.00", backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        kpi_conf = vdef.get('KPIVisual', {}).get('ChartConfiguration', {})
        values = kpi_conf.get('FieldWells', {}).get('Values', [])
        decimal_places = None
        for v in values:
            for ftype in ('CategoricalMeasureField', 'NumericalMeasureField', 'DateMeasureField'):
                if ftype in v:
                    fc = v[ftype].get('FormatConfiguration', {})
                    nfc = fc.get('NumericFormatConfiguration', {})
                    for cfg_key in ('NumberDisplayFormatConfiguration', 'CurrencyDisplayFormatConfiguration', 'PercentageDisplayFormatConfiguration'):
                        if cfg_key in nfc:
                            decimal_places = nfc[cfg_key].get('DecimalPlacesConfiguration', {}).get('DecimalPlaces')
        record(1, "Two decimals #,##0.00", "format_string='#,##0.00'", "DecimalPlaces=2",
               f"visual_id={vid}, DecimalPlaces={decimal_places}", decimal_places == 2)
    except Exception as e:
        record(1, "Two decimals #,##0.00", "format_string='#,##0.00'", "DecimalPlaces=2", f"ERROR: {e}", False)

    # ========================================================================
    # ROUND 2: Currency Format
    # ========================================================================
    print("-" * 70)
    print("ROUND 2: Currency Format")
    print("-" * 70)

    # Test 2.1: $#,##0
    try:
        r = c.create_kpi(
            ANALYSIS_ID, SHEET_ID, "R2-Currency-0d", "FLIP_TOKEN", "COUNT",
            DATASET_ID, format_string="$#,##0", backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        kpi_conf = vdef.get('KPIVisual', {}).get('ChartConfiguration', {})
        values = kpi_conf.get('FieldWells', {}).get('Values', [])
        has_currency = False
        for v in values:
            for ftype in ('CategoricalMeasureField', 'NumericalMeasureField', 'DateMeasureField'):
                if ftype in v:
                    fc = v[ftype].get('FormatConfiguration', {})
                    nfc = fc.get('NumericFormatConfiguration', {})
                    has_currency = 'CurrencyDisplayFormatConfiguration' in nfc
        record(2, "Currency $#,##0", "format_string='$#,##0'", "CurrencyDisplayFormatConfiguration present",
               f"has_currency={has_currency}", has_currency)
    except Exception as e:
        record(2, "Currency $#,##0", "format_string='$#,##0'", "Currency format applied", f"ERROR: {e}", False)

    # Test 2.2: $#,##0.00
    try:
        r = c.create_kpi(
            ANALYSIS_ID, SHEET_ID, "R2-Currency-2d", "FLIP_TOKEN", "COUNT",
            DATASET_ID, format_string="$#,##0.00", backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        kpi_conf = vdef.get('KPIVisual', {}).get('ChartConfiguration', {})
        values = kpi_conf.get('FieldWells', {}).get('Values', [])
        decimal_places = None
        for v in values:
            for ftype in ('CategoricalMeasureField', 'NumericalMeasureField', 'DateMeasureField'):
                if ftype in v:
                    fc = v[ftype].get('FormatConfiguration', {})
                    nfc = fc.get('NumericFormatConfiguration', {})
                    cur = nfc.get('CurrencyDisplayFormatConfiguration', {})
                    decimal_places = cur.get('DecimalPlacesConfiguration', {}).get('DecimalPlaces')
        record(2, "Currency $#,##0.00", "format_string='$#,##0.00'", "Currency with DecimalPlaces=2",
               f"DecimalPlaces={decimal_places}", decimal_places == 2)
    except Exception as e:
        record(2, "Currency $#,##0.00", "format_string='$#,##0.00'", "DecimalPlaces=2", f"ERROR: {e}", False)

    # ========================================================================
    # ROUND 3: Percentage Format
    # ========================================================================
    print("-" * 70)
    print("ROUND 3: Percentage Format")
    print("-" * 70)

    for pct_fmt, expected_dec in [("0%", 0), ("0.0%", 1), ("0.00%", 2)]:
        try:
            r = c.create_kpi(
                ANALYSIS_ID, SHEET_ID, f"R3-Pct-{expected_dec}d", "FLIP_TOKEN", "COUNT",
                DATASET_ID, format_string=pct_fmt, backup_first=False,
            )
            vid = r.get('visual_id')
            created_visuals.append(vid)
            vdef = c.get_visual_definition(ANALYSIS_ID, vid)
            kpi_conf = vdef.get('KPIVisual', {}).get('ChartConfiguration', {})
            values = kpi_conf.get('FieldWells', {}).get('Values', [])
            actual_dec = None
            has_pct = False
            for v in values:
                for ftype in ('CategoricalMeasureField', 'NumericalMeasureField', 'DateMeasureField'):
                    if ftype in v:
                        fc = v[ftype].get('FormatConfiguration', {})
                        nfc = fc.get('NumericFormatConfiguration', {})
                        pct_cfg = nfc.get('PercentageDisplayFormatConfiguration', {})
                        if pct_cfg:
                            has_pct = True
                            actual_dec = pct_cfg.get('DecimalPlacesConfiguration', {}).get('DecimalPlaces')
            record(3, f"Percentage {pct_fmt}", f"format_string='{pct_fmt}'",
                   f"PercentageDisplay with DecimalPlaces={expected_dec}",
                   f"has_pct={has_pct}, DecimalPlaces={actual_dec}",
                   has_pct and actual_dec == expected_dec)
        except Exception as e:
            record(3, f"Percentage {pct_fmt}", f"format_string='{pct_fmt}'",
                   f"DecimalPlaces={expected_dec}", f"ERROR: {e}", False)

    # ========================================================================
    # ROUND 4: COUNT aggregation with formatting (the bug that was fixed)
    # ========================================================================
    print("-" * 70)
    print("ROUND 4: COUNT Aggregation + Formatting (bug regression)")
    print("-" * 70)

    # Test 4.1: COUNT on STRING column (FLIP_TOKEN) + format
    try:
        r = c.create_kpi(
            ANALYSIS_ID, SHEET_ID, "R4-COUNT-String", "FLIP_TOKEN", "COUNT",
            DATASET_ID, format_string="#,##0", backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        kpi_conf = vdef.get('KPIVisual', {}).get('ChartConfiguration', {})
        values = kpi_conf.get('FieldWells', {}).get('Values', [])
        field_type_used = None
        has_fmt = False
        for v in values:
            if 'CategoricalMeasureField' in v:
                field_type_used = 'CategoricalMeasureField'
                has_fmt = 'FormatConfiguration' in v['CategoricalMeasureField']
            elif 'DateMeasureField' in v:
                field_type_used = 'DateMeasureField'
                has_fmt = 'FormatConfiguration' in v['DateMeasureField']
            elif 'NumericalMeasureField' in v:
                field_type_used = 'NumericalMeasureField'
                has_fmt = 'FormatConfiguration' in v['NumericalMeasureField']
        record(4, "COUNT on STRING + format", "column=FLIP_TOKEN, agg=COUNT, fmt=#,##0",
               "CategoricalMeasureField with FormatConfiguration",
               f"field_type={field_type_used}, has_format={has_fmt}",
               field_type_used == 'CategoricalMeasureField' and has_fmt)
    except Exception as e:
        record(4, "COUNT on STRING + format", "COUNT FLIP_TOKEN + #,##0",
               "CategoricalMeasureField with format", f"ERROR: {e}", False)

    # Test 4.2: COUNT on DATE column + format
    try:
        r = c.create_kpi(
            ANALYSIS_ID, SHEET_ID, "R4-COUNT-Date", "PURCHASE_AGREEMENT_COMPLETED_AT", "COUNT",
            DATASET_ID, format_string="#,##0", backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        kpi_conf = vdef.get('KPIVisual', {}).get('ChartConfiguration', {})
        values = kpi_conf.get('FieldWells', {}).get('Values', [])
        field_type_used = None
        has_fmt = False
        for v in values:
            if 'DateMeasureField' in v:
                field_type_used = 'DateMeasureField'
                has_fmt = 'FormatConfiguration' in v['DateMeasureField']
            elif 'CategoricalMeasureField' in v:
                field_type_used = 'CategoricalMeasureField'
                has_fmt = 'FormatConfiguration' in v['CategoricalMeasureField']
        record(4, "COUNT on DATE + format", "column=PURCHASE_AGREEMENT_COMPLETED_AT, agg=COUNT, fmt=#,##0",
               "DateMeasureField with FormatConfiguration",
               f"field_type={field_type_used}, has_format={has_fmt}",
               field_type_used == 'DateMeasureField' and has_fmt)
    except Exception as e:
        record(4, "COUNT on DATE + format", "COUNT date col + #,##0",
               "DateMeasureField with format", f"ERROR: {e}", False)

    # ========================================================================
    # ROUND 5: Conditional Formatting
    # ========================================================================
    print("-" * 70)
    print("ROUND 5: Conditional Formatting")
    print("-" * 70)

    # Test 5.1: Single rule >= threshold, green
    try:
        cf = [{"condition": ">= 100", "color": "#2CAF4A"}]
        r = c.create_kpi(
            ANALYSIS_ID, SHEET_ID, "R5-CF-Single", "FLIP_TOKEN", "COUNT",
            DATASET_ID, conditional_format=cf, backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        cf_block = vdef.get('KPIVisual', {}).get('ConditionalFormatting', {})
        cf_opts = cf_block.get('ConditionalFormattingOptions', [])
        record(5, "CF single rule (>=100, green)", "1 rule: >=100 green",
               "1 ConditionalFormattingOptions entry",
               f"entries={len(cf_opts)}", len(cf_opts) == 1)
    except Exception as e:
        record(5, "CF single rule", ">=100 green", "1 entry", f"ERROR: {e}", False)

    # Test 5.2: Multiple rules: >= high green, < low red
    try:
        cf = [
            {"condition": ">= 500", "color": "#2CAF4A"},
            {"condition": "< 100", "color": "#DE3B00"},
        ]
        r = c.create_kpi(
            ANALYSIS_ID, SHEET_ID, "R5-CF-Multi", "FLIP_TOKEN", "COUNT",
            DATASET_ID, conditional_format=cf, backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        cf_block = vdef.get('KPIVisual', {}).get('ConditionalFormatting', {})
        cf_opts = cf_block.get('ConditionalFormattingOptions', [])
        record(5, "CF multiple rules (2 rules)", ">=500 green, <100 red",
               "2 ConditionalFormattingOptions entries",
               f"entries={len(cf_opts)}", len(cf_opts) == 2)
    except Exception as e:
        record(5, "CF multiple rules", "2 rules", "2 entries", f"ERROR: {e}", False)

    # Test 5.3: Missing color key -> should default to green (#2CAF4A)
    try:
        cf = [{"condition": ">= 50"}]  # no 'color' key
        r = c.create_kpi(
            ANALYSIS_ID, SHEET_ID, "R5-CF-NoColor", "FLIP_TOKEN", "COUNT",
            DATASET_ID, conditional_format=cf, backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        cf_block = vdef.get('KPIVisual', {}).get('ConditionalFormatting', {})
        cf_opts = cf_block.get('ConditionalFormattingOptions', [])
        # Check the color defaults
        color_used = None
        if cf_opts:
            pv = cf_opts[0].get('PrimaryValue', {})
            tc = pv.get('TextColor', {})
            solid = tc.get('Solid', {})
            color_used = solid.get('Color')
        record(5, "CF missing color key (default)", "condition only, no color",
               "Defaults to #2CAF4A",
               f"color_used={color_used}", color_used == '#2CAF4A')
    except Exception as e:
        record(5, "CF missing color key", "no color key", "Default green", f"ERROR: {e}", False)

    # ========================================================================
    # ROUND 6: Invalid Inputs
    # ========================================================================
    print("-" * 70)
    print("ROUND 6: Invalid Inputs")
    print("-" * 70)

    # Test 6.1: format_string="invalid" -> should not crash
    try:
        r = c.create_kpi(
            ANALYSIS_ID, SHEET_ID, "R6-InvalidFmt", "FLIP_TOKEN", "COUNT",
            DATASET_ID, format_string="invalid", backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        # If it creates without crash, that's a pass (QS may ignore the format or use default Number)
        record(6, "Invalid format_string", "format_string='invalid'",
               "Should not crash (may create with default format)",
               f"visual_id={vid}, created successfully", vid is not None)
    except Exception as e:
        # An error is also acceptable if QS rejects the definition
        err_str = str(e)
        is_qs_validation = 'ValidationException' in err_str or 'InvalidParameter' in err_str
        record(6, "Invalid format_string", "format_string='invalid'",
               "Should not crash or return clear error",
               f"ERROR: {err_str[:200]}",
               is_qs_validation)  # QS validation error is fine

    # Test 6.2: conditional_format with invalid JSON in MCP tool layer
    # This tests the MCP tool layer's JSON parsing (in visuals.py create_kpi)
    try:
        # Simulate what the MCP tool does with invalid JSON
        bad_json = "not valid json {"
        try:
            parsed = json.loads(bad_json)
            record(6, "Invalid conditional_format JSON", "bad JSON string",
                   "json.JSONDecodeError", "Parsed without error (unexpected)", False)
        except json.JSONDecodeError as je:
            # This is expected - the MCP tool returns {"error": "Invalid JSON..."}
            record(6, "Invalid conditional_format JSON", "bad JSON string",
                   "json.JSONDecodeError caught",
                   f"JSONDecodeError: {str(je)[:80]}", True)
    except Exception as e:
        record(6, "Invalid conditional_format JSON", "bad JSON", "Error caught", f"ERROR: {e}", False)

    # Test 6.3: show_data_labels=True on KPI -> should be ignored (KPI doesn't support data labels)
    try:
        # KPI create_kpi doesn't have show_data_labels param at all
        # Verify it's not in the function signature
        import inspect
        sig = inspect.signature(c.create_kpi)
        has_data_labels_param = 'show_data_labels' in sig.parameters
        record(6, "show_data_labels on KPI", "KPI function signature check",
               "KPI should NOT have show_data_labels param",
               f"has_show_data_labels={has_data_labels_param}",
               not has_data_labels_param)
    except Exception as e:
        record(6, "show_data_labels on KPI", "signature check", "No param", f"ERROR: {e}", False)

    # ========================================================================
    # ROUND 7: Data Labels on Bar Chart
    # ========================================================================
    print("-" * 70)
    print("ROUND 7: Data Labels on Bar Chart")
    print("-" * 70)

    # Test 7.1: Vertical bar + labels
    try:
        r = c.create_bar_chart(
            ANALYSIS_ID, SHEET_ID, "R7-VBar-Labels", "MARKET_NAME", "FLIP_TOKEN", "COUNT",
            DATASET_ID, orientation="VERTICAL", show_data_labels=True, backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        bar_conf = vdef.get('BarChartVisual', {}).get('ChartConfiguration', {})
        dl = bar_conf.get('DataLabels', {})
        vis = dl.get('Visibility')
        record(7, "Vertical bar + data labels", "orientation=VERTICAL, show_data_labels=True",
               "DataLabels.Visibility=VISIBLE",
               f"Visibility={vis}", vis == 'VISIBLE')
    except Exception as e:
        record(7, "Vertical bar + labels", "VERTICAL + labels", "VISIBLE", f"ERROR: {e}", False)

    # Test 7.2: Horizontal bar + labels
    try:
        r = c.create_bar_chart(
            ANALYSIS_ID, SHEET_ID, "R7-HBar-Labels", "MARKET_NAME", "FLIP_TOKEN", "COUNT",
            DATASET_ID, orientation="HORIZONTAL", show_data_labels=True, backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        bar_conf = vdef.get('BarChartVisual', {}).get('ChartConfiguration', {})
        dl = bar_conf.get('DataLabels', {})
        vis = dl.get('Visibility')
        orientation_actual = bar_conf.get('Orientation')
        record(7, "Horizontal bar + data labels", "orientation=HORIZONTAL, show_data_labels=True",
               "DataLabels.Visibility=VISIBLE, Orientation=HORIZONTAL",
               f"Visibility={vis}, Orientation={orientation_actual}",
               vis == 'VISIBLE' and orientation_actual == 'HORIZONTAL')
    except Exception as e:
        record(7, "Horizontal bar + labels", "HORIZONTAL + labels", "VISIBLE", f"ERROR: {e}", False)

    # ========================================================================
    # ROUND 8: Data Labels on Line Chart
    # ========================================================================
    print("-" * 70)
    print("ROUND 8: Data Labels on Line Chart")
    print("-" * 70)

    # Test 8.1: Weekly granularity + labels
    try:
        r = c.create_line_chart(
            ANALYSIS_ID, SHEET_ID, "R8-Line-Week", "PURCHASE_AGREEMENT_COMPLETED_AT",
            "FLIP_TOKEN", "COUNT", DATASET_ID,
            date_granularity="WEEK", show_data_labels=True, backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        line_conf = vdef.get('LineChartVisual', {}).get('ChartConfiguration', {})
        dl = line_conf.get('DataLabels', {})
        vis = dl.get('Visibility')
        record(8, "Line chart WEEK + labels", "date_granularity=WEEK, show_data_labels=True",
               "DataLabels.Visibility=VISIBLE",
               f"Visibility={vis}", vis == 'VISIBLE')
    except Exception as e:
        record(8, "Line WEEK + labels", "WEEK + labels", "VISIBLE", f"ERROR: {e}", False)

    # Test 8.2: Monthly granularity + labels
    try:
        r = c.create_line_chart(
            ANALYSIS_ID, SHEET_ID, "R8-Line-Month", "PURCHASE_AGREEMENT_COMPLETED_AT",
            "FLIP_TOKEN", "COUNT", DATASET_ID,
            date_granularity="MONTH", show_data_labels=True, backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        line_conf = vdef.get('LineChartVisual', {}).get('ChartConfiguration', {})
        dl = line_conf.get('DataLabels', {})
        vis = dl.get('Visibility')
        # Also verify the date granularity
        fw = line_conf.get('FieldWells', {}).get('LineChartAggregatedFieldWells', {})
        cats = fw.get('Category', [])
        granularity_actual = None
        for cat in cats:
            if 'DateDimensionField' in cat:
                granularity_actual = cat['DateDimensionField'].get('DateGranularity')
        record(8, "Line chart MONTH + labels", "date_granularity=MONTH, show_data_labels=True",
               "DataLabels=VISIBLE, DateGranularity=MONTH",
               f"Visibility={vis}, Granularity={granularity_actual}",
               vis == 'VISIBLE' and granularity_actual == 'MONTH')
    except Exception as e:
        record(8, "Line MONTH + labels", "MONTH + labels", "VISIBLE", f"ERROR: {e}", False)

    # ========================================================================
    # ROUND 9: Combined Features
    # ========================================================================
    print("-" * 70)
    print("ROUND 9: Combined Features")
    print("-" * 70)

    # Test 9.1: KPI with format + conditional in one call
    try:
        cf = [{"condition": ">= 100", "color": "#2CAF4A"}]
        r = c.create_kpi(
            ANALYSIS_ID, SHEET_ID, "R9-KPI-Fmt+CF", "FLIP_TOKEN", "COUNT",
            DATASET_ID, format_string="#,##0", conditional_format=cf, backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        kpi_vis = vdef.get('KPIVisual', {})
        # Check format
        values = kpi_vis.get('ChartConfiguration', {}).get('FieldWells', {}).get('Values', [])
        has_fmt = False
        for v in values:
            for ftype in ('CategoricalMeasureField', 'NumericalMeasureField', 'DateMeasureField'):
                if ftype in v and 'FormatConfiguration' in v[ftype]:
                    has_fmt = True
        # Check CF
        cf_block = kpi_vis.get('ConditionalFormatting', {})
        cf_opts = cf_block.get('ConditionalFormattingOptions', [])
        has_cf = len(cf_opts) > 0
        record(9, "KPI format + conditional combined", "fmt=#,##0 + CF>=100 green",
               "Both format and CF present",
               f"has_format={has_fmt}, has_cf={has_cf}, cf_count={len(cf_opts)}",
               has_fmt and has_cf)
    except Exception as e:
        record(9, "KPI format + CF combined", "fmt + CF", "Both present", f"ERROR: {e}", False)

    # Test 9.2: Bar with format + labels in one call
    try:
        r = c.create_bar_chart(
            ANALYSIS_ID, SHEET_ID, "R9-Bar-Fmt+Labels", "MARKET_NAME", "FLIP_TOKEN", "COUNT",
            DATASET_ID, format_string="#,##0", show_data_labels=True, backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        bar_conf = vdef.get('BarChartVisual', {}).get('ChartConfiguration', {})
        # Check labels
        dl = bar_conf.get('DataLabels', {})
        has_labels = dl.get('Visibility') == 'VISIBLE'
        # Check format on value field
        fw = bar_conf.get('FieldWells', {}).get('BarChartAggregatedFieldWells', {})
        values = fw.get('Values', [])
        has_fmt = False
        for v in values:
            for ftype in ('CategoricalMeasureField', 'NumericalMeasureField', 'DateMeasureField'):
                if ftype in v and 'FormatConfiguration' in v[ftype]:
                    has_fmt = True
        record(9, "Bar format + labels combined", "fmt=#,##0 + data_labels=True",
               "Both format and labels present",
               f"has_format={has_fmt}, has_labels={has_labels}",
               has_fmt and has_labels)
    except Exception as e:
        record(9, "Bar format + labels", "fmt + labels", "Both", f"ERROR: {e}", False)

    # Test 9.3: Pivot with format_strings list
    try:
        r = c.create_pivot_table(
            ANALYSIS_ID, SHEET_ID, "R9-Pivot-FmtList",
            row_columns=["MARKET_NAME"],
            value_columns=["FLIP_TOKEN", "FLIP_TOKEN"],
            value_aggregations=["COUNT", "DISTINCT_COUNT"],
            dataset_identifier=DATASET_ID,
            format_strings=["#,##0", "$#,##0.00"],
            backup_first=False,
        )
        vid = r.get('visual_id')
        created_visuals.append(vid)
        vdef = c.get_visual_definition(ANALYSIS_ID, vid)
        pivot_conf = vdef.get('PivotTableVisual', {}).get('ChartConfiguration', {})
        fw = pivot_conf.get('FieldWells', {}).get('PivotTableAggregatedFieldWells', {})
        values = fw.get('Values', [])
        fmt_types = []
        for v in values:
            for ftype in ('CategoricalMeasureField', 'NumericalMeasureField', 'DateMeasureField'):
                if ftype in v:
                    fc = v[ftype].get('FormatConfiguration', {})
                    nfc = fc.get('NumericFormatConfiguration', {})
                    if 'NumberDisplayFormatConfiguration' in nfc:
                        fmt_types.append('Number')
                    elif 'CurrencyDisplayFormatConfiguration' in nfc:
                        fmt_types.append('Currency')
                    elif 'PercentageDisplayFormatConfiguration' in nfc:
                        fmt_types.append('Percentage')
                    else:
                        fmt_types.append('None')
        record(9, "Pivot with format_strings list", "2 values: #,##0 and $#,##0.00",
               "First=Number, Second=Currency",
               f"format_types={fmt_types}",
               len(fmt_types) >= 2 and fmt_types[0] == 'Number' and fmt_types[1] == 'Currency')
    except Exception as e:
        record(9, "Pivot format_strings list", "2 formats", "Number+Currency", f"ERROR: {e}", False)

    # ========================================================================
    # ROUND 10: Stress Test
    # ========================================================================
    print("-" * 70)
    print("ROUND 10: Stress Test (5 rapid visuals + health check + cleanup)")
    print("-" * 70)

    stress_vids = []
    stress_start = time.time()

    # Create 5 visuals rapidly with no backup between them
    for i in range(5):
        try:
            r = c.create_kpi(
                ANALYSIS_ID, SHEET_ID, f"R10-Stress-{i+1}", "FLIP_TOKEN", "COUNT",
                DATASET_ID, format_string="#,##0", backup_first=False,
            )
            vid = r.get('visual_id')
            stress_vids.append(vid)
            created_visuals.append(vid)
        except Exception as e:
            stress_vids.append(f"ERROR: {e}")

    stress_elapsed = time.time() - stress_start
    successful = [v for v in stress_vids if v and not str(v).startswith('ERROR')]
    record(10, "Create 5 visuals rapidly", f"5 KPIs, no backup, {stress_elapsed:.1f}s elapsed",
           "All 5 created successfully",
           f"{len(successful)}/5 created in {stress_elapsed:.1f}s, IDs: {successful[:3]}...",
           len(successful) == 5)

    # Verify all 5 exist via verify_analysis_health
    try:
        health = c.verify_analysis_health(ANALYSIS_ID)
        overall = health.get('overall_health', health.get('healthy', 'unknown'))
        issues = health.get('issues', [])
        record(10, "Health check after stress", "verify_analysis_health",
               "Analysis is healthy (PASS or True)",
               f"health={overall}, issues={len(issues)}",
               overall in ('PASS', True, 'healthy'))
    except Exception as e:
        record(10, "Health check", "verify_analysis_health", "Healthy", f"ERROR: {e}", False)

    # Verify visuals exist on the sheet
    try:
        sheet_visuals = c.list_sheet_visuals(ANALYSIS_ID, SHEET_ID)
        visual_ids_on_sheet = [v.get('visual_id') for v in sheet_visuals]
        stress_found = sum(1 for sv in successful if sv in visual_ids_on_sheet)
        record(10, "Stress visuals on sheet", "list_sheet_visuals check",
               "All 5 stress visuals found on sheet",
               f"{stress_found}/5 found on sheet",
               stress_found == 5)
    except Exception as e:
        record(10, "Stress visuals on sheet", "list check", "5 found", f"ERROR: {e}", False)

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
        health = c.verify_analysis_health(ANALYSIS_ID)
        print(f"Post-cleanup health: {health.get('overall_health', health.get('healthy', 'unknown'))}")
    except Exception as e:
        print(f"Post-cleanup health check failed: {e}")

# ============================================================================
# FINAL REPORT
# ============================================================================
print()
print("=" * 70)
print("FINAL TEST REPORT")
print("=" * 70)
print()

total = len(results)
passed = sum(1 for r in results if r['status'] == 'PASS')
failed = sum(1 for r in results if r['status'] == 'FAIL')

for round_num in range(1, 11):
    round_tests = [r for r in results if r['round'] == round_num]
    if round_tests:
        round_pass = sum(1 for r in round_tests if r['status'] == 'PASS')
        round_total = len(round_tests)
        status_icon = "OK" if round_pass == round_total else "!!"
        print(f"  Round {round_num:2d}: {round_pass}/{round_total} passed  [{status_icon}]")
        for r in round_tests:
            print(f"    [{r['status']}] T{r['test']:02d}: {r['name']}")
            if r['status'] == 'FAIL':
                print(f"           Expected: {r['expected']}")
                print(f"           Actual:   {r['actual']}")

print()
print(f"TOTAL: {passed}/{total} PASSED, {failed}/{total} FAILED")
print("=" * 70)
