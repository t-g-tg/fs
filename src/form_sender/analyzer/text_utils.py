"""文字列ユーティリティ（スコアリング共有関数）

ElementScorer 内で広く使われる CJK 判定や語境界付きトークン判定を
独立モジュールとして切り出し（機能互換・振る舞い不変）。
"""

from __future__ import annotations

import re
from typing import Optional

# 事前コンパイル済みの CJK 検出用パターン
_CJK_RE: Optional[re.Pattern[str]]
try:
    _CJK_RE = re.compile(r"[\u3040-\u30ff\u3400-\u9fff\uff66-\uff9f]")
except Exception:
    _CJK_RE = None


def has_cjk(s: str) -> bool:
    """日本語(CJK)文字を含むかの軽量判定。"""
    try:
        if not s:
            return False
        if _CJK_RE is None:
            return bool(re.search(r"[\u3040-\u30ff\u3400-\u9fff\uff66-\uff9f]", s))
        return _CJK_RE.search(s) is not None
    except Exception:
        return False


def contains_token_with_boundary(text: str, token: str) -> bool:
    """語境界を考慮した包含判定（日本語対応）。

    - 半角/全角スペース、各種括弧・句読点・中点・スラッシュ等を境界として扱う。
    - トークンが CJK を含む場合は、日本語では語境界が空白で区切られない前提から
      安全側の部分一致を許容。ただし『名』のような単文字一般トークンは除外し、
      例外的に『姓』は『姓名』対応のため許容する。
    """
    try:
        if not text or not token:
            return False

        boundary_chars = (
            r"_\-\./\\\s"  # 半角
            + r"\u3000（）［］｛｝「」『』【】。、・：；！？”“’‘？／＼＜＞《》〈〉【】『』—－ー〜･・，．｡"  # 全角
        )
        left_boundary = rf"(^|[{boundary_chars}])"
        right_boundary = rf"($|[{boundary_chars}])"
        pattern = left_boundary + re.escape(token) + right_boundary
        if re.search(pattern, text, flags=re.IGNORECASE):
            return True

        # CJK を含む短語は安全側の部分一致を許容（ただし『名』は除外、
        # 『姓』は『姓名』ケースのため許容）
        if has_cjk(token):
            if token == "名":
                return False
            if token == "姓":
                return "姓" in text
            return token in text

        return False
    except Exception:
        return False

