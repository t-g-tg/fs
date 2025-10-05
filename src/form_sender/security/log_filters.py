"""
ログ用フィルタ群

目的:
- マッピング関連ログのみを抑制するためのフィルタを提供する。
  INFO/DEBUG レベルの詳細ログを対象とし、WARNING 以上は通す。
"""

import logging
from typing import Iterable, Optional, Tuple


class MappingLogFilter(logging.Filter):
    """Field mapping 由来の詳細ログのみ抑制するフィルタ。

    - 対象: 指定されたロガー名プレフィックスに一致するレコード
    - レベル: INFO/DEBUG のみ抑制（WARNING 以上は通す）
    """

    DEFAULT_PREFIXES = (
        # マッピング決定やスコアリング詳細が出力される主要モジュール
        "form_sender.analyzer.field_mapper",
        "form_sender.analyzer.input_value_assigner",
        "form_sender.analyzer.field_combination_manager",
        "form_sender.analyzer.element_scorer",
        "form_sender.analyzer.duplicate_prevention",
    )

    def __init__(self, prefixes: Optional[Iterable[str]] = None):
        super().__init__()
        self.prefixes: Tuple[str, ...] = tuple(prefixes) if prefixes else self.DEFAULT_PREFIXES

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        try:
            if record.levelno >= logging.WARNING:
                return True
            name = getattr(record, "name", "")
            return not any(name.startswith(p) for p in self.prefixes)
        except Exception:
            # フィルタ中エラー時は安全側で通す
            return True
