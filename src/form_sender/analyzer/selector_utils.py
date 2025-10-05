from __future__ import annotations

"""Playwright セレクタ生成ユーティリティ（挙動不変）。"""

from playwright.async_api import Locator


async def generate_stable_selector(element: Locator) -> str:
    """RuleBasedAnalyzer._generate_playwright_selector と同一ロジック。

    優先順位:
    - id があれば `[id="..."]`
    - name があれば `tag[name="..."]` (+ `type` 属性があれば `[type="..."]`)
    - name が無ければ、input の `type` 属性が存在する場合のみ `[type]` を付与
    """
    try:
        info = await element.evaluate(
            """
            el => ({
              id: el.getAttribute('id') || '',
              name: el.getAttribute('name') || '',
              tagName: (el.tagName || '').toLowerCase(),
              // 属性としてのtype（存在しない場合は空文字）
              typeAttr: el.getAttribute('type') || ''
            })
            """
        )
        el_id = info.get("id")
        if el_id:
            esc = str(el_id).replace("\\", r"\\").replace('"', r"\"")
            return f'[id="{esc}"]'

        name = info.get("name")
        tag = info.get("tagName", "input")
        type_attr = info.get("typeAttr") if tag == "input" else ""

        if name:
            esc_name = str(name).replace("\\", r"\\").replace('"', r"\"")
            selector = f'{tag}[name="{esc_name}"]'
            if type_attr:
                esc_type = str(type_attr).replace("\\", r"\\").replace('"', r"\"")
                selector += f'[type="{esc_type}"]'
            return selector

        if tag == "input" and type_attr:
            esc_type2 = str(type_attr).replace("\\", r"\\").replace('"', r"\"")
            return f'{tag}[type="{esc_type2}"]'
        return tag
    except Exception:
        return "input"

