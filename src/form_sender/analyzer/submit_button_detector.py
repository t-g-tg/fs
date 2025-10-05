import logging
from typing import Dict, List, Any, Callable, Awaitable, Optional
from playwright.async_api import Page, Locator

from ..utils.button_config import (
    get_button_keywords_config,
    get_exclude_keywords,
    get_fallback_selectors,
)

logger = logging.getLogger(__name__)

class SubmitButtonDetector:
    """送信ボタンの検出を担当するクラス"""

    def __init__(self, page: Page, generate_playwright_selector_func: Callable[[Locator], Awaitable[str]]):
        self.page = page
        self._generate_playwright_selector = generate_playwright_selector_func

    async def detect_submit_buttons(self, form_locator: Optional[Locator] = None) -> List[Dict[str, Any]]:
        """送信/確認ボタン候補を収集し、重複排除した一覧を返す

        - Playwright互換の `:has-text()` を利用
        - 設定ファイルのキーワード/除外語/フォールバックセレクタを統一的に適用
        - セレクタは可能な限り一意化（id/name/value/text を反映）
        """
        submit_buttons: List[Dict[str, Any]] = []

        # フォーム境界内のみを対象にスコープする（ページ全体の誤検出を抑制）
        container = form_locator if form_locator is not None else self.page

        kw = get_button_keywords_config()
        exclude_kw = [k.lower() for k in get_exclude_keywords()]
        fb = get_fallback_selectors()

        # 基本セレクタ
        base_selectors: List[str] = [
            "button[type=\"submit\"]",
            "input[type=\"submit\"]",
        ]

        # キーワード駆動のセレクタ
        text_keywords: List[str] = list(
            {*(kw.get("primary", [])), *(kw.get("secondary", [])), *(kw.get("confirmation", []))}
        )
        for t in text_keywords:
            t_escaped = t.replace("\"", "\\\"")
            base_selectors.append(f"button:has-text(\"{t_escaped}\")")
            base_selectors.append(f"[role=\"button\"]:has-text(\"{t_escaped}\")")
            base_selectors.append(f"input[value*=\"{t_escaped}\"]")

        # フォールバックセレクタを併合
        for group in ("primary", "secondary", "by_attributes"):
            base_selectors.extend(fb.get(group, []))

        # 重複除去して安定順
        seen_sel = set()
        submit_selectors = []
        for s in base_selectors:
            if s not in seen_sel:
                seen_sel.add(s)
                submit_selectors.append(s)

        # 収集
        for selector in submit_selectors:
            try:
                loc = container.locator(selector)
                candidates = await loc.all()
                for el in candidates:
                    try:
                        if not await el.is_visible():
                            continue
                        # 重要: disabledな送信ボタンは解析段階では除外しない
                        # 実送信処理側で有効化待機やフォース有効化を行うため、
                        # ここでは候補として確保しておく（検出精度向上）。

                        text = (await el.text_content()) or ""
                        value = (await el.get_attribute("value")) or ""
                        merged_text = (text or value or "").strip()
                        if merged_text:
                            low = merged_text.lower()
                            if any(x in low for x in exclude_kw):
                                # 戻る/キャンセル/リセット/検索などは除外
                                continue

                        unique_selector = await self._build_prefer_unique_selector(el, text.strip(), value.strip())
                        submit_info = {
                            "element": el,
                            "text": (text or value or "").strip(),
                            "selector": unique_selector,
                        }
                        submit_buttons.append(submit_info)
                    except Exception:
                        continue
            except Exception as e:
                logger.debug(f"Submit button scan error for '{selector}': {e}")

        # テキストxセレクタで一意化
        unique_buttons: List[Dict[str, Any]] = []
        seen = set()
        for b in submit_buttons:
            key = (b.get("text", ""), b.get("selector", ""))
            if key in seen:
                continue
            unique_buttons.append(b)
            seen.add(key)

        logger.info(f"Detected {len(unique_buttons)} submit/confirm button candidates")
        return unique_buttons

    async def _build_prefer_unique_selector(self, element: Locator, text: str, value: str) -> str:
        """id/name 優先で一意なセレクタを構築。無い場合は value/text をセレクタ条件に含める。

        例:
          - #submitBtn
          - input[name="commit"][type="submit"]
          - input[type="submit"][value*="送信"]
          - button:has-text("確認する")
        """
        try:
            info = await element.evaluate(
                "el => ({ id: el.id || '', name: el.name || '', tag: el.tagName.toLowerCase(), type: (el.tagName.toLowerCase()==='input' ? (el.type || '') : '') })"
            )
            el_id = info.get("id") or ""
            if el_id:
                esc = str(el_id).replace('\\', r'\\').replace('"', r'\"')
                return f"[id=\"{esc}\"]"

            tag = info.get("tag", "button")
            name = info.get("name") or ""
            typ = info.get("type") or ""

            if name:
                esc_name = str(name).replace('\\', r'\\').replace('"', r'\"')
                sel = f"{tag}[name=\"{esc_name}\"]"
                if tag == "input" and typ:
                    esc_type = str(typ).replace('\\', r'\\').replace('"', r'\"')
                    sel += f"[type=\"{esc_type}\"]"
                return sel

            # value/text を条件に含めて差別化
            def _shorten(s: str) -> str:
                s = s.strip().replace("\"", "\\\"")
                # 過度に長いテキストは短縮（誤一致を避けるため先頭12文字に限定）
                return s[:12]

            if tag == "input":
                # type=submit/按钮 などが取得できる場合は付与
                esc_typ = str(typ or 'submit').replace('\\', r'\\').replace('"', r'\"')
                base = f"input[type=\"{esc_typ}\"]"
                if value:
                    return f"{base}[value*=\"{_shorten(value)}\"]"
                if text:
                    return f"{base}[value*=\"{_shorten(text)}\"]"
                return base

            # button系
            t = _shorten(text or value)
            if t:
                return f"{tag}:has-text(\"{t}\")"
            return await self._generate_playwright_selector(element)
        except Exception:
            # フォールバック
            try:
                return await self._generate_playwright_selector(element)
            except Exception:
                return "button"
