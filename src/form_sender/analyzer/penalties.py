"""ペナルティ計算ロジック（ElementScorer から分離）

機能互換のため、公開関数 `calculate_penalties` を経由して
既存のスコアリングと同一の振る舞いを提供する。
"""

from __future__ import annotations

import logging
from typing import Dict, Tuple, Any

from playwright.async_api import Locator
import re

_OPACITY_RE = re.compile(r"opacity\s*:\s*([0-9]*\.?[0-9]+)", re.IGNORECASE)

logger = logging.getLogger(__name__)


async def calculate_penalties(
    element: Locator,
    element_info: Dict[str, Any],
    score_weights: Dict[str, int],
) -> Tuple[int, list[str]]:
    penalty = 0
    penalties: list[str] = []

    # 非表示要素ペナルティ（強化）
    if not element_info.get("visible", True):
        penalty += score_weights.get("visibility_penalty", -200)
        penalties.append("element_not_visible")

    # 非有効要素ペナルティ
    if not element_info.get("enabled", True):
        penalty += score_weights.get("visibility_penalty", -200) // 2
        penalties.append("element_not_enabled")

    # style="display:none" 等（不可視・非操作スタイルの強ペナルティ）
    try:
        style = element_info.get("style")
        if style is None:
            style = await element.get_attribute("style") or ""
        style_nospace = style.replace(" ", "").lower()

        def _opacity_is_zero(s: str) -> bool:
            # opacity: <number> を厳密に数値抽出して 0 のみ True
            try:
                m = _OPACITY_RE.search(s or "")
                return bool(m and float(m.group(1)) == 0.0)
            except Exception:
                return False

        if (
            "display:none" in style_nospace
            or "visibility:hidden" in style_nospace
            or "pointer-events:none" in style_nospace
            or _opacity_is_zero(style)
        ):
            penalty += score_weights.get("visibility_penalty", -200)
            # 具体的な理由を記録
            if "pointer-events:none" in style_nospace:
                penalties.append("pointer_events_none")
            if _opacity_is_zero(style):
                penalties.append("opacity_zero")
            if ("display:none" in style_nospace) or ("visibility:hidden" in style_nospace):
                penalties.append("style_hidden")
    except Exception:
        pass

    # type="hidden"
    try:
        if str(element_info.get("type", "")).lower() == "hidden":
            penalty += score_weights.get("visibility_penalty", -200)
            penalties.append("hidden_input_type")
    except Exception:
        pass

    # aria-hidden
    try:
        aria_hidden = element_info.get("aria_hidden")
        if aria_hidden is None:
            aria_hidden = await element.get_attribute("aria-hidden")
        if aria_hidden and str(aria_hidden).lower() == "true":
            penalty += score_weights.get("visibility_penalty", -200)
            penalties.append("aria_hidden_true")
    except Exception:
        pass

    # tabindex="-1"
    try:
        tabindex = element_info.get("tabindex")
        if tabindex is None:
            tabindex = await element.get_attribute("tabindex")
        if str(tabindex) == "-1":
            penalty += score_weights.get("visibility_penalty", -200) // 2
            penalties.append("tabindex_negative")
    except Exception:
        pass

    # position:absolute のハニーポット（極小サイズ/画面外/クリック不能系）
    try:
        style = element_info.get("style")
        if style is None:
            style = await element.get_attribute("style") or ""
        style_nospace = style.replace(" ", "").lower()
        if "position:absolute" in style_nospace and (
            "height:1px" in style_nospace
            or "width:1px" in style_nospace
            or "overflow:hidden" in style_nospace
            or "left:-9999px" in style_nospace
            or "top:-9999px" in style_nospace
        ):
            penalty += score_weights.get("visibility_penalty", -200)
            penalties.append("honeypot_style_detected")
    except Exception:
        pass

    if penalties:
        logger.debug(f"Penalties applied: {penalties} (total: {penalty})")

    return penalty, penalties
