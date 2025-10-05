"""
送信/確認ボタン関連の設定ローダー

- config/button_keywords.json を読み込み、欠損時は安全なデフォルトを返す。
- 解析系・実行系の両方で共通利用することでDRYと一貫性を確保する。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Any


_DEFAULT_CONFIG: Dict[str, Any] = {
    "submit_button_keywords": {
        "primary": ["送信", "送る", "submit", "send"],
        "secondary": [
            "完了",
            "complete",
            "確定",
            "confirm",
            "実行",
            "execute",
            "登録",
            "register",
        ],
        "confirmation": [
            "確認",
            "次",
            "review",
            "confirm",
            "進む",
            "next",
            "続行",
            "continue",
            "確認画面",
            "確認する",
            "内容確認",
            "入力内容を確認",
            "確認画面へ",
            "確認ページ",
            "チェック",
            "check",
        ],
        # 確認ページ上の「最終送信」向けキーワード（広め）
        # 目的: 企業サイトで多用される業務語彙（応募/申込/エントリー等）や
        #       UI文言（「この内容で送信」「確認して送信」）を取りこぼさないための既定セット。
        # 運用で増減しやすいため、コード側のハードコードは避け、設定で管理する。
        "final": [
            "送信する",
            "この内容で送信",
            "確認して送信",
            "応募",
            "応募する",
            "申込",
            "申し込み",
            "申込み",
            "エントリー",
            "エントリーする",
            "お問い合わせ送信",
            "登録する",
            "確定する",
            "決定する",
            "注文する",
            "送信",
            "submit",
            "send",
            "完了",
            "決定",
            "確定",
        ],
    },
    "fallback_selectors": {
        "primary": [
            "button[type=\"submit\"]",
            "input[type=\"submit\"]",
        ],
        "secondary": [
            "button:has-text(\"送信\")",
            "button:has-text(\"Submit\")",
            "form button:first-of-type",
        ],
        "by_attributes": [
            "#submit",
            ".submit",
            "[name*=\"submit\"]",
            "[class*=\"submit\"]",
        ],
    },
    "exclude_keywords": [
        "キャンセル",
        "cancel",
        "戻る",
        "back",
        "リセット",
        "reset",
        "クリア",
        "clear",
        "検索",
        "search",
        "ログイン",
        "login",
    ],
}


def _project_root() -> Path:
    # src/form_sender/utils/button_config.py からプロジェクトルートへ
    return Path(__file__).resolve().parents[3]


def _load_json_safe(path: Path) -> Dict[str, Any]:
    try:
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        # 破損・読み込み失敗時は空で返す（デフォルト使用）
        return {}


def load_button_config() -> Dict[str, Any]:
    """button_keywords.json を読み込み、欠損時はデフォルトをマージして返す"""
    cfg_path = _project_root() / "config" / "button_keywords.json"
    file_cfg = _load_json_safe(cfg_path)

    # 深いマージ（簡易）
    result = {**_DEFAULT_CONFIG, **file_cfg}
    # 内部の辞書もマージ
    for key in ("submit_button_keywords", "fallback_selectors"):
        if key in _DEFAULT_CONFIG:
            merged = {**_DEFAULT_CONFIG[key], **file_cfg.get(key, {})}
            result[key] = merged
    return result


def get_button_keywords_config() -> Dict[str, List[str]]:
    cfg = load_button_config()
    return cfg.get("submit_button_keywords", _DEFAULT_CONFIG["submit_button_keywords"])  # type: ignore


def get_exclude_keywords() -> List[str]:
    cfg = load_button_config()
    return cfg.get("exclude_keywords", _DEFAULT_CONFIG["exclude_keywords"])  # type: ignore


def get_fallback_selectors() -> Dict[str, List[str]]:
    cfg = load_button_config()
    return cfg.get("fallback_selectors", _DEFAULT_CONFIG["fallback_selectors"])  # type: ignore
