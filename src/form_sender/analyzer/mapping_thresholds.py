from __future__ import annotations

"""動的しきい値計算ユーティリティ（FieldMapper から委譲、挙動不変）。"""

from typing import Dict, Set


def get_dynamic_quality_threshold(
    field_name: str,
    settings: Dict[str, any],
    essential_fields_completed: Set[str],
    optional_high_priority_fields: Set[str],
) -> float:
    per_field = settings.get("min_score_threshold_per_field", {}) or {}
    if field_name in per_field:
        return per_field[field_name]

    if field_name in settings.get("essential_fields", []):
        return settings["min_score_threshold"]

    if not settings.get("quality_first_mode", False):
        return settings["min_score_threshold"]

    if field_name in optional_high_priority_fields:
        return min(
            settings["min_score_threshold"] + settings["quality_threshold_boost"],
            settings["max_quality_threshold"],
        )

    if len(essential_fields_completed) >= len(settings.get("essential_fields", [])):
        return min(
            settings["min_score_threshold"] + settings["quality_threshold_boost"] + 50,
            400,
        )
    else:
        return min(
            settings["min_score_threshold"] + settings["quality_threshold_boost"],
            settings["max_quality_threshold"],
        )

