"""
Bot検出・関連判定で使う共有定数
"""

from typing import Tuple

# ページ/エラーメッセージの簡易キーワード検出用（小文字で扱う前提）
BOT_DETECTION_KEYWORDS: Tuple[str, ...] = (
    "recaptcha",
    "cloudflare",
    "bot",
)

