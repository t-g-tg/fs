"""除外ルール（ElementScorer から分離）

属性・コンテキスト双方の除外判定をモジュール化。
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict

from playwright.async_api import Locator

from .text_utils import contains_token_with_boundary, has_cjk

logger = logging.getLogger(__name__)


# ElementScorer と整合させるためのセキュリティ重要トークン
# ※循環参照を避けるために必要最小限をここにも定義
CRITICAL_CLASS_EXCLUDE_TOKENS = {
    "auth",
    "login",
    "signin",
    "otp",
    "mfa",
    "totp",
    "password",
    "verify",
    "verification",
    "token",
    "captcha",
    "confirm",
    "confirmation",
    "confirm_email",
    "email_confirmation",
    "csrf",
    "session",
}


def is_excluded_element(
    element_info: Dict[str, Any], field_patterns: Dict[str, Any]
) -> bool:
    exclude_patterns = field_patterns.get("exclude_patterns", [])
    if not exclude_patterns:
        return False

    attributes_to_check = ["name", "id", "class", "placeholder"]

    for attr in attributes_to_check:
        attr_value = (element_info.get(attr, "") or "").lower()
        if not attr_value:
            continue

        if attr == "class":
            class_tokens = [
                t
                for t in (attr_value.split() if isinstance(attr_value, str) else [])
                if t
            ]
            if not class_tokens:
                continue
            # 厳格一致（クラス単位）
            for exclude_pattern in exclude_patterns:
                exclude_lower = exclude_pattern.lower()
                if exclude_lower in class_tokens:
                    logger.debug("EXCLUSION: class includes '%s'", exclude_pattern)
                    return True
            # 語境界（-_）を考慮: 重要トークン or 長語のみ
            for exclude_pattern in exclude_patterns:
                exclude_lower = exclude_pattern.lower()
                if not (
                    exclude_lower in CRITICAL_CLASS_EXCLUDE_TOKENS
                    or len(exclude_lower) >= 5
                ):
                    continue  # 短い汎用語（例: 'name'）では誤除外を避ける
                for token in class_tokens:
                    if (
                        re.search(r"\b" + re.escape(exclude_lower) + r"\b", token)
                        or re.search(
                            r"[_-]" + re.escape(exclude_lower) + r"[_-]", token
                        )
                        or token.startswith(exclude_lower + "_")
                        or token.startswith(exclude_lower + "-")
                        or token.endswith("_" + exclude_lower)
                        or token.endswith("-" + exclude_lower)
                    ):
                        logger.debug(
                            "EXCLUSION: class token contains '%s'", exclude_pattern
                        )
                        return True
            # 長語の部分一致
            for exclude_pattern in exclude_patterns:
                exclude_lower = exclude_pattern.lower()
                if len(exclude_lower) >= 5 and any(
                    exclude_lower in t for t in class_tokens
                ):
                    logger.debug(
                        "EXCLUSION: class token contains long '%s'", exclude_pattern
                    )
                    return True
            continue

        # class 以外の属性
        for exclude_pattern in exclude_patterns:
            exclude_lower = exclude_pattern.lower()
            # 1. CJK/短語は日本語境界つき
            if len(exclude_lower) <= 2 or has_cjk(exclude_lower):
                if contains_token_with_boundary(attr_value, exclude_lower):
                    logger.debug(
                        "EXCLUSION(boundary jp): %s contains '%s'",
                        attr,
                        exclude_pattern,
                    )
                    return True
                continue
            # 2. 単語境界/下線・ハイフン境界
            if (
                re.search(r"\b" + re.escape(exclude_lower) + r"\b", attr_value)
                or re.search(r"[_-]" + re.escape(exclude_lower) + r"[_-]", attr_value)
                or attr_value.startswith(exclude_lower + "_")
                or attr_value.startswith(exclude_lower + "-")
                or attr_value.endswith("_" + exclude_lower)
                or attr_value.endswith("-" + exclude_lower)
            ):
                logger.debug(
                    "EXCLUSION(word boundary): %s contains '%s'", attr, exclude_pattern
                )
                return True
            # 3. 長語の部分一致
            if len(exclude_lower) >= 5 and exclude_lower in attr_value:
                logger.debug(
                    "EXCLUSION(long contains): %s contains '%s'", attr, exclude_pattern
                )
                return True

    return False


async def is_excluded_element_with_context(
    element_info: Dict[str, Any],
    element: Locator,
    field_patterns: Dict[str, Any],
    context_extractor=None,
) -> bool:
    exclude_patterns = field_patterns.get("exclude_patterns", [])
    logger.debug("Context exclusion check - patterns: %s", exclude_patterns)
    if not exclude_patterns:
        return False

    # まず属性のみでの除外
    if is_excluded_element(element_info, field_patterns):
        logger.debug("Excluded by attribute patterns")
        return True

    try:
        if context_extractor:
            contexts = await context_extractor.extract_context_for_element(element)
        else:
            from .context_text_extractor import ContextTextExtractor  # 局所 import

            context_extractor = ContextTextExtractor(element.page)
            contexts = await context_extractor.extract_context_for_element(element)

        if not contexts:
            return False

        allowed_sources = {
            "dt_label",
            "th_label",
            "label_for",
            "label_parent",
            "aria_labelledby",
            "label_element",
        }

        for ctx in contexts:
            if getattr(ctx, "source_type", "") not in allowed_sources:
                continue
            context_text = (getattr(ctx, "text", "") or "").lower()
            if not context_text:
                continue

            for exclude_pattern in exclude_patterns:
                exclude_lower = exclude_pattern.lower()
                if len(exclude_lower) <= 2 or has_cjk(exclude_lower):
                    if contains_token_with_boundary(context_text, exclude_lower):
                        logger.debug(
                            "CONTEXT_EXCLUSION(boundary jp): '***' contains '%s'",
                            exclude_pattern,
                        )
                        return True
                else:
                    if (
                        re.search(
                            r"\b" + re.escape(exclude_lower) + r"\b", context_text
                        )
                        or re.search(
                            r"[_-]" + re.escape(exclude_lower) + r"[_-]", context_text
                        )
                        or context_text.startswith(exclude_lower + "_")
                        or context_text.startswith(exclude_lower + "-")
                        or context_text.endswith("_" + exclude_lower)
                        or context_text.endswith("-" + exclude_lower)
                        or (len(exclude_lower) >= 5 and exclude_lower in context_text)
                    ):
                        logger.debug(
                            "CONTEXT_EXCLUSION: '***' contains '%s'", exclude_pattern
                        )
                        return True
        return False
    except Exception:
        return False
