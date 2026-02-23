"""Post-write verification functions for QuickSight operations.

Each function clears the definition cache, re-reads the analysis, and
checks that the expected change was actually persisted.  Raises
``ChangeVerificationError`` on mismatch.

These are standalone functions (not a class) so they can be called from
any service without circular dependencies.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from quicksight_mcp.core.types import PARAMETER_TYPES, parse_visual
from quicksight_mcp.safety.exceptions import ChangeVerificationError

if TYPE_CHECKING:
    from quicksight_mcp.services.analyses import AnalysisService


def verify_sheet_exists(
    analysis_service: AnalysisService,
    analysis_id: str,
    sheet_id: str,
    expected_name: Optional[str] = None,
) -> bool:
    """Verify a sheet exists after creation/rename.

    Raises:
        ChangeVerificationError: If the sheet is missing or name mismatches.
    """
    analysis_service.clear_def_cache(analysis_id)
    definition = analysis_service.get_definition(analysis_id)
    for s in definition.get("Sheets", []):
        if s.get("SheetId") == sheet_id:
            if expected_name and s.get("Name") != expected_name:
                raise ChangeVerificationError(
                    "sheet",
                    analysis_id,
                    f"Sheet '{sheet_id}' exists but name is "
                    f"'{s.get('Name')}', expected '{expected_name}'.",
                )
            return True
    raise ChangeVerificationError(
        "sheet",
        analysis_id,
        f"Sheet '{sheet_id}' not found after update.",
    )


def verify_sheet_deleted(
    analysis_service: AnalysisService,
    analysis_id: str,
    sheet_id: str,
) -> bool:
    """Verify a sheet was actually deleted.

    Raises:
        ChangeVerificationError: If the sheet still exists.
    """
    analysis_service.clear_def_cache(analysis_id)
    definition = analysis_service.get_definition(analysis_id)
    for s in definition.get("Sheets", []):
        if s.get("SheetId") == sheet_id:
            raise ChangeVerificationError(
                "delete_sheet",
                analysis_id,
                f"Sheet '{sheet_id}' still exists after deletion.",
            )
    return True


def verify_visual_exists(
    analysis_service: AnalysisService,
    analysis_id: str,
    visual_id: str,
) -> bool:
    """Verify a visual exists after creation.

    Raises:
        ChangeVerificationError: If the visual is not found.
    """
    analysis_service.clear_def_cache(analysis_id)
    definition = analysis_service.get_definition(analysis_id)
    for sheet in definition.get("Sheets", []):
        for v in sheet.get("Visuals", []):
            parsed = parse_visual(v)
            if parsed.get("visual_id") == visual_id:
                return True
    raise ChangeVerificationError(
        "visual",
        analysis_id,
        f"Visual '{visual_id}' not found after update.",
    )


def verify_visual_deleted(
    analysis_service: AnalysisService,
    analysis_id: str,
    visual_id: str,
) -> bool:
    """Verify a visual was actually deleted.

    Raises:
        ChangeVerificationError: If the visual still exists.
    """
    analysis_service.clear_def_cache(analysis_id)
    definition = analysis_service.get_definition(analysis_id)
    for sheet in definition.get("Sheets", []):
        for v in sheet.get("Visuals", []):
            parsed = parse_visual(v)
            if parsed.get("visual_id") == visual_id:
                raise ChangeVerificationError(
                    "delete_visual",
                    analysis_id,
                    f"Visual '{visual_id}' still exists after deletion.",
                )
    return True


def verify_visual_title(
    analysis_service: AnalysisService,
    analysis_id: str,
    visual_id: str,
    expected_title: str,
) -> bool:
    """Verify a visual's title matches the expected value.

    Raises:
        ChangeVerificationError: If the visual is missing or title mismatches.
    """
    analysis_service.clear_def_cache(analysis_id)
    definition = analysis_service.get_definition(analysis_id)
    for sheet in definition.get("Sheets", []):
        for v in sheet.get("Visuals", []):
            parsed = parse_visual(v)
            if parsed.get("visual_id") == visual_id:
                actual_title = parsed.get("title", "")
                if actual_title != expected_title:
                    raise ChangeVerificationError(
                        "set_visual_title",
                        analysis_id,
                        f"Visual '{visual_id}' title is '{actual_title}', "
                        f"expected '{expected_title}'.",
                    )
                return True
    raise ChangeVerificationError(
        "set_visual_title",
        analysis_id,
        f"Visual '{visual_id}' not found after title update.",
    )


def verify_parameter_exists(
    analysis_service: AnalysisService,
    analysis_id: str,
    param_name: str,
) -> bool:
    """Verify a parameter exists after creation.

    Raises:
        ChangeVerificationError: If the parameter is not found.
    """
    analysis_service.clear_def_cache(analysis_id)
    definition = analysis_service.get_definition(analysis_id)
    for p in definition.get("ParameterDeclarations", []):
        for ptype in PARAMETER_TYPES:
            if ptype in p and p[ptype].get("Name") == param_name:
                return True
    raise ChangeVerificationError(
        "add_parameter",
        analysis_id,
        f"Parameter '{param_name}' not found after update.",
    )


def verify_parameter_deleted(
    analysis_service: AnalysisService,
    analysis_id: str,
    param_name: str,
) -> bool:
    """Verify a parameter was actually deleted.

    Raises:
        ChangeVerificationError: If the parameter still exists.
    """
    analysis_service.clear_def_cache(analysis_id)
    definition = analysis_service.get_definition(analysis_id)
    for p in definition.get("ParameterDeclarations", []):
        for ptype in PARAMETER_TYPES:
            if ptype in p and p[ptype].get("Name") == param_name:
                raise ChangeVerificationError(
                    "delete_parameter",
                    analysis_id,
                    f"Parameter '{param_name}' still exists after deletion.",
                )
    return True


def verify_filter_group_exists(
    analysis_service: AnalysisService,
    analysis_id: str,
    filter_group_id: str,
) -> bool:
    """Verify a filter group exists after creation.

    Raises:
        ChangeVerificationError: If the filter group is not found.
    """
    analysis_service.clear_def_cache(analysis_id)
    definition = analysis_service.get_definition(analysis_id)
    for fg in definition.get("FilterGroups", []):
        if fg.get("FilterGroupId") == filter_group_id:
            return True
    raise ChangeVerificationError(
        "add_filter_group",
        analysis_id,
        f"Filter group '{filter_group_id}' not found after update.",
    )


def verify_filter_group_deleted(
    analysis_service: AnalysisService,
    analysis_id: str,
    filter_group_id: str,
) -> bool:
    """Verify a filter group was actually deleted.

    Raises:
        ChangeVerificationError: If the filter group still exists.
    """
    analysis_service.clear_def_cache(analysis_id)
    definition = analysis_service.get_definition(analysis_id)
    for fg in definition.get("FilterGroups", []):
        if fg.get("FilterGroupId") == filter_group_id:
            raise ChangeVerificationError(
                "delete_filter_group",
                analysis_id,
                f"Filter group '{filter_group_id}' still exists after deletion.",
            )
    return True


def verify_sheet_visual_count(
    analysis_service: AnalysisService,
    analysis_id: str,
    sheet_id: str,
    expected_count: int,
) -> bool:
    """Verify a sheet has the expected number of visuals.

    Useful for validating replicate_sheet operations.

    Raises:
        ChangeVerificationError: If the sheet is missing or visual count mismatches.
    """
    analysis_service.clear_def_cache(analysis_id)
    definition = analysis_service.get_definition(analysis_id)
    for s in definition.get("Sheets", []):
        if s.get("SheetId") == sheet_id:
            actual_count = len(s.get("Visuals", []))
            if actual_count != expected_count:
                raise ChangeVerificationError(
                    "replicate_sheet",
                    analysis_id,
                    f"Sheet has {actual_count} visuals, expected {expected_count}.",
                )
            return True
    raise ChangeVerificationError(
        "replicate_sheet",
        analysis_id,
        f"Sheet '{sheet_id}' not found after replication.",
    )
