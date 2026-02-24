"""Destructive-change guard for QuickSight analysis updates.

Prevents accidental deletion of major content (all sheets, >50% of
visuals, or >50% of calculated fields) by comparing the proposed
definition against the current one.
"""

from __future__ import annotations

from typing import Dict, List

from quicksight_mcp.safety.exceptions import DestructiveChangeError


def validate_definition_not_destructive(
    current_definition: Dict,
    new_definition: Dict,
    analysis_id: str,
) -> bool:
    """Block updates that would delete all sheets or >50% of visuals/calc fields.

    Compares the proposed ``new_definition`` against ``current_definition``
    and raises ``DestructiveChangeError`` if the update would cause
    significant content loss.

    Args:
        current_definition: The current analysis Definition dict.
        new_definition: The proposed replacement Definition dict.
        analysis_id: Analysis ID (used in the error message).

    Returns:
        ``True`` if the update is safe.

    Raises:
        DestructiveChangeError: When the update is considered destructive.
    """
    current_sheets = current_definition.get("Sheets", [])
    cur_sheet_cnt = len(current_sheets)
    cur_visual_cnt = sum(len(s.get("Visuals", [])) for s in current_sheets)
    cur_calc_cnt = len(current_definition.get("CalculatedFields", []))

    new_sheets = new_definition.get("Sheets", [])
    new_sheet_cnt = len(new_sheets)
    new_visual_cnt = sum(len(s.get("Visuals", [])) for s in new_sheets)
    new_calc_cnt = len(new_definition.get("CalculatedFields", []))

    current_counts = {
        "sheets": cur_sheet_cnt,
        "visuals": cur_visual_cnt,
        "calculated_fields": cur_calc_cnt,
    }
    new_counts = {
        "sheets": new_sheet_cnt,
        "visuals": new_visual_cnt,
        "calculated_fields": new_calc_cnt,
    }

    issues: List[str] = []

    if cur_sheet_cnt > 0 and new_sheet_cnt == 0:
        issues.append(f"Would DELETE ALL {cur_sheet_cnt} SHEETS")

    if cur_visual_cnt > 0:
        loss_pct = (cur_visual_cnt - new_visual_cnt) / cur_visual_cnt * 100
        if loss_pct > 50:
            issues.append(
                f"Would delete {loss_pct:.0f}% of visuals "
                f"({cur_visual_cnt} -> {new_visual_cnt})"
            )

    if cur_calc_cnt > 0:
        loss_pct = (cur_calc_cnt - new_calc_cnt) / cur_calc_cnt * 100
        if loss_pct > 50:
            issues.append(
                f"Would delete {loss_pct:.0f}% of calculated fields "
                f"({cur_calc_cnt} -> {new_calc_cnt})"
            )

    if issues:
        raise DestructiveChangeError(
            analysis_id, "; ".join(issues), current_counts, new_counts
        )

    return True
