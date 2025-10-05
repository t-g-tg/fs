import logging
import re
import time
from typing import List, Sequence, Optional

from playwright.async_api import Page
from config.manager import get_cookie_consent_config

logger = logging.getLogger(__name__)


class CookieConsentHandler:
    """Cookie同意バナーを自動処理するユーティリティ

    特徴:
    - 設定駆動（config/cookie_consent.json）
    - 主要CMP（OneTrust/Cookiebot/Quantcast/Didomi/TrustArc/CookieYes等）の既知セレクタ対応
    - iframe内のバナーにも対応（frameLocatorで探索）
    - 文言パターン（多言語一部含む）でのフォールバック
    - 最後の手段としてバナーDOMを非表示にするフェイルセーフ
    """

    @classmethod
    async def handle(cls, page: Page) -> bool:
        """Cookie同意ポップアップを閉じる

        Args:
            page: Playwrightのページオブジェクト

        Returns:
            True: 何らかの手段で妨げとなるバナーを閉じ/無効化できた
            False: 変化なし（バナー未検出または操作不要）
        """
        cfg = get_cookie_consent_config()
        prefer_reject: bool = bool(cfg.get("prefer_reject", True))
        max_wait_ms: int = int(cfg.get("max_wait_ms", 7000))
        interval_ms: int = int(cfg.get("attempt_interval_ms", 300))

        sels = cfg.get("selectors", {})
        reject_selectors: Sequence[str] = sels.get("reject", [])
        accept_selectors: Sequence[str] = sels.get("accept", [])
        manage_selectors: Sequence[str] = sels.get("manage", [])
        save_selectors: Sequence[str] = sels.get("save", [])
        container_selectors: Sequence[str] = sels.get("containers", [])
        iframe_selectors: Sequence[str] = sels.get("iframes", [])

        patterns = cfg.get("text_patterns", {})
        reject_texts: Sequence[str] = patterns.get("reject", [])
        accept_texts: Sequence[str] = patterns.get("accept", [])

        start = time.time()

        async def attempt_all() -> bool:
            # 1) 直接「同意」を試行（ページ直下＋iframe）
            if await cls._click_any_selector(page, accept_selectors, iframe_selectors):
                logger.info("Cookie banner handled by known accept selector")
                return True
            if await cls._click_by_text(page, accept_texts, roles=["button", "link"], iframe_selectors=iframe_selectors):
                logger.info("Cookie banner handled by accept text pattern")
                return True

            # 同意が見つからないが、拒否要素が既に見えているなら即時クリック
            if await cls._exists_any_selector(page, reject_selectors, iframe_selectors) or \
               await cls._exists_by_text(page, reject_texts, roles=["button", "link"], iframe_selectors=iframe_selectors):
                if await cls._click_any_selector(page, reject_selectors, iframe_selectors):
                    logger.info("Cookie banner closed by immediate reject selector")
                    return True
                if await cls._click_by_text(page, reject_texts, roles=["button", "link"], iframe_selectors=iframe_selectors):
                    logger.info("Cookie banner closed by immediate reject text")
                    return True

            # 2) 設定を開いてから「同意/保存」を試行
            if await cls._click_any_selector(page, manage_selectors, iframe_selectors) or \
               await cls._click_by_text(page, ["Manage preferences", "Options"], roles=["button","link"], iframe_selectors=iframe_selectors) or \
               await cls._js_click_by_inner_text(page, ["Manage preferences", "Options"], iframe_selectors):
                await page.wait_for_timeout(300)
                # まずは同意を優先
                if await cls._click_any_selector(page, accept_selectors, iframe_selectors) or \
                   await cls._click_by_text(page, accept_texts, roles=["button", "link"], iframe_selectors=iframe_selectors):
                    await page.wait_for_timeout(200)
                    await cls._click_any_selector(page, save_selectors, iframe_selectors)
                    logger.info("Cookie banner handled via manage -> accept flow")
                    return True
                # 同意が無い場合は拒否を選択
                if await cls._click_any_selector(page, reject_selectors, iframe_selectors) or \
                   await cls._click_by_text(page, reject_texts, roles=["button", "link"], iframe_selectors=iframe_selectors):
                    await page.wait_for_timeout(200)
                    await cls._click_any_selector(page, save_selectors, iframe_selectors)
                    logger.info("Cookie banner handled via manage -> reject flow")
                    return True
                # JSフォールバック（iframe含む）
                if await cls._js_click_by_inner_text(page, ["Reject all", "Reject All", "拒否", "拒否する"], iframe_selectors):
                    await page.wait_for_timeout(200)
                    await cls._click_any_selector(page, save_selectors, iframe_selectors)
                    logger.info("Cookie banner handled via manage -> reject (JS fallback)")
                    return True

            # 3) フォールバックで「拒否」を試行（運用方針によりprefer_rejectがTrueの場合、または時間切れ間近）
            elapsed_ms = int((time.time() - start) * 1000)
            if prefer_reject or elapsed_ms > max_wait_ms * 0.6:
                if await cls._click_any_selector(page, reject_selectors, iframe_selectors):
                    logger.info("Cookie banner closed by reject as fallback")
                    return True
                if await cls._click_by_text(page, reject_texts, roles=["button", "link"], iframe_selectors=iframe_selectors):
                    logger.info("Cookie banner closed by reject text fallback")
                    return True

            return False

        # --- 無駄操作回避: 事前に存在チェック（超軽量） ---
        should_try = await cls._should_attempt_handling(
            page,
            container_selectors,
            iframe_selectors,
            accept_selectors,
            reject_selectors,
            accept_texts,
            reject_texts,
        )
        if not should_try:
            # 無駄操作を避けつつ、ワンショットだけ試す（極小オーバーヘッド）
            try:
                if await attempt_all():
                    await page.wait_for_timeout(300)
                    return True
            except Exception:
                pass
            return False

        # 繰り返し試行（バナーの遅延表示に備える）
        while int((time.time() - start) * 1000) < max_wait_ms:
            try:
                if await attempt_all():
                    await page.wait_for_timeout(300)
                    return True
            except Exception:
                # 失敗しても次のループで再試行
                pass
            await page.wait_for_timeout(interval_ms)

        # 4) 最終手段: 既知コンテナを非表示化（DOMは残すが操作阻害は除去）
        hidden = await cls._hide_known_containers(page, container_selectors, iframe_selectors)
        if hidden:
            logger.info("Cookie banner containers hidden as last-resort fallback")
        return hidden

    @classmethod
    async def _js_click_by_inner_text(
        cls, page: Page, texts: Sequence[str], iframe_selectors: Sequence[str]
    ) -> bool:
        # ページ直下
        try:
            clicked = await page.evaluate(
                "(words) => {\n"
                "  const match = (el, ws) => ws.some(w => (el.innerText||'').toLowerCase().includes(w.toLowerCase()));\n"
                "  const els = Array.from(document.querySelectorAll('button, [role=button], a'));\n"
                "  for (const el of els) { if (match(el, words)) { el.click(); return true; } }\n"
                "  return false;\n"
                "}",
                list(texts),
            )
            if clicked:
                return True
        except Exception:
            pass

        # 既知iframe
        for iframe_sel in iframe_selectors or []:
            try:
                frame = page.frame_locator(iframe_sel)
                try:
                    clicked = await frame.evaluate(
                        "(words) => {\n"
                        "  const match = (el, ws) => ws.some(w => (el.innerText||'').toLowerCase().includes(w.toLowerCase()));\n"
                        "  const els = Array.from(document.querySelectorAll('button, [role=button], a'));\n"
                        "  for (const el of els) { if (match(el, words)) { el.click(); return true; } }\n"
                        "  return false;\n"
                        "}",
                        list(texts),
                    )
                    if clicked:
                        return True
                except Exception:
                    pass
            except Exception:
                continue

        # 全iframeフォールバック
        try:
            for f in page.frames:
                if hasattr(page, 'main_frame') and f == page.main_frame:
                    continue
                try:
                    clicked = await f.evaluate(
                        "(words) => {\n"
                        "  const match = (el, ws) => ws.some(w => (el.innerText||'').toLowerCase().includes(w.toLowerCase()));\n"
                        "  const els = Array.from(document.querySelectorAll('button, [role=button], a'));\n"
                        "  for (const el of els) { if (match(el, words)) { el.click(); return true; } }\n"
                        "  return false;\n"
                        "}",
                        list(texts),
                    )
                    if clicked:
                        return True
                except Exception:
                    continue
        except Exception:
            pass
        return False

    @classmethod
    async def _exists_any_selector(
        cls, page: Page, selectors: Sequence[str], iframe_selectors: Sequence[str]
    ) -> bool:
        for sel in selectors or []:
            try:
                if await page.locator(sel).count():
                    return True
            except Exception:
                pass
        for iframe_sel in iframe_selectors or []:
            try:
                frame_loc = page.frame_locator(iframe_sel)
                for sel in selectors or []:
                    try:
                        if await frame_loc.locator(sel).count():
                            return True
                    except Exception:
                        continue
            except Exception:
                continue
        # 既知外iframeのフォールバック
        try:
            for f in page.frames:
                if hasattr(page, 'main_frame') and f == page.main_frame:
                    continue
                for sel in selectors or []:
                    try:
                        if await f.locator(sel).count():
                            return True
                    except Exception:
                        continue
        except Exception:
            pass
        return False

    @classmethod
    async def _exists_by_text(
        cls,
        page: Page,
        texts: Sequence[str],
        roles: Sequence[str] = ("button",),
        iframe_selectors: Optional[Sequence[str]] = None,
    ) -> bool:
        import re as _re
        for word in texts or []:
            regex = _re.compile(word, _re.IGNORECASE)
            for role in roles:
                try:
                    if await page.get_by_role(role, name=regex).count():
                        return True
                except Exception:
                    pass
        for iframe_sel in iframe_selectors or []:
            try:
                frame_loc = page.frame_locator(iframe_sel)
                for word in texts or []:
                    regex = _re.compile(word, _re.IGNORECASE)
                    for role in roles:
                        try:
                            if await frame_loc.get_by_role(role, name=regex).count():
                                return True
                        except Exception:
                            pass
            except Exception:
                continue
        # 既知外iframeのフォールバック
        try:
            for f in page.frames:
                if hasattr(page, 'main_frame') and f == page.main_frame:
                    continue
                for word in texts or []:
                    regex = _re.compile(word, _re.IGNORECASE)
                    for role in roles:
                        try:
                            if await f.get_by_role(role, name=regex).count():
                                return True
                        except Exception:
                            pass
        except Exception:
            pass
        return False

    @classmethod
    async def _should_attempt_handling(
        cls,
        page: Page,
        container_selectors: Sequence[str],
        iframe_selectors: Sequence[str],
        accept_selectors: Sequence[str],
        reject_selectors: Sequence[str],
        accept_texts: Sequence[str],
        reject_texts: Sequence[str],
    ) -> bool:
        """バナーが無いページでの無駄操作を避けるための超軽量検出。

        以下のいずれかに該当すれば試行:
          - 既知のバナーコンテナが存在
          - 既知のCMP iframeが存在
          - 既知のaccept/rejectセレクタの要素が存在
          - 'cookie' or 'consent' を含むid/classを持つdialogが存在
        """
        try:
            # コンテナ/iframeの存在
            for sel in list(container_selectors)[:5]:  # ごく一部のみクイックチェック
                if await page.locator(sel).count():
                    return True
            for sel in list(iframe_selectors)[:3]:
                if await page.locator(sel).count():
                    return True

            # 既知ボタンの存在（強いセレクタのみクイックチェック）
            strong_accept = [s for s in accept_selectors if s.startswith("#") or s.startswith(".")]
            strong_reject = [s for s in reject_selectors if s.startswith("#") or s.startswith(".")]
            for sel in (strong_accept[:3] + strong_reject[:3]):
                if await page.locator(sel).count():
                    return True

            # テキストパターンでの軽量検出（上位数件＋代表英語）
            import re as _re
            common_extra = ["Accept All", "Accept all", "Reject All", "Reject all", "Manage preferences", "Options"]
            for word in list(accept_texts)[:3] + list(reject_texts)[:3] + common_extra:
                try:
                    if await page.get_by_role("button", name=_re.compile(word, _re.IGNORECASE)).count():
                        return True
                except Exception:
                    pass
            # 既知外iframeでもテキストを軽く探索
            try:
                for f in page.frames:
                    if hasattr(page, 'main_frame') and f == page.main_frame:
                        continue
                    for word in list(accept_texts)[:3] + list(reject_texts)[:3] + common_extra:
                        try:
                            if await f.get_by_role("button", name=_re.compile(word, _re.IGNORECASE)).count():
                                return True
                        except Exception:
                            pass
            except Exception:
                pass

            # 追加JS走査は行わない（軽量化のため）
            return False

        except Exception:
            return False

    # ---------- 内部ユーティリティ ----------
    @classmethod
    async def _click_any_selector(
        cls, page: Page, selectors: Sequence[str], iframe_selectors: Sequence[str]
    ) -> bool:
        if not selectors:
            return False
        # ページ直下
        for sel in selectors:
            try:
                loc = page.locator(sel)
                if await loc.count():
                    await loc.first.click(force=True, timeout=1000)
                    await page.wait_for_timeout(200)
                    return True
            except Exception:
                pass

        # iframe内
        for iframe_sel in iframe_selectors or []:
            try:
                frame_loc = page.frame_locator(iframe_sel)
                for sel in selectors:
                    try:
                        loc = frame_loc.locator(sel)
                        if await loc.count():
                            await loc.first.click(force=True, timeout=1000)
                            await page.wait_for_timeout(200)
                            return True
                    except Exception:
                        continue
            except Exception:
                continue
        # 既知外iframeのフォールバック（全frame探索）
        try:
            for f in page.frames:
                if hasattr(page, 'main_frame') and f == page.main_frame:
                    continue
                for sel in selectors:
                    try:
                        loc = f.locator(sel)
                        if await loc.count():
                            await loc.first.click(force=True, timeout=1000)
                            await page.wait_for_timeout(200)
                            return True
                    except Exception:
                        continue
        except Exception:
            pass
        return False

    @classmethod
    async def _click_by_text(
        cls,
        page: Page,
        texts: Sequence[str],
        roles: Sequence[str] = ("button",),
        iframe_selectors: Optional[Sequence[str]] = None,
    ) -> bool:
        if not texts:
            return False
        # ページ直下: roleベース → テキストベース
        for word in texts:
            regex = re.compile(word, re.IGNORECASE)
            for role in roles:
                try:
                    loc = page.get_by_role(role, name=regex)
                    if await loc.count():
                        await loc.first.click(force=True, timeout=1000)
                        await page.wait_for_timeout(200)
                        return True
                except Exception:
                    pass
            try:
                loc = page.locator(f"text={word}")
                if await loc.count():
                    await loc.first.click(force=True, timeout=1000)
                    await page.wait_for_timeout(200)
                    return True
            except Exception:
                pass

        # iframe内
        for iframe_sel in iframe_selectors or []:
            try:
                frame_loc = page.frame_locator(iframe_sel)
                for word in texts:
                    regex = re.compile(word, re.IGNORECASE)
                    for role in roles:
                        try:
                            loc = frame_loc.get_by_role(role, name=regex)
                            if await loc.count():
                                await loc.first.click(force=True, timeout=1000)
                                await page.wait_for_timeout(200)
                                return True
                        except Exception:
                            pass
                    try:
                        loc = frame_loc.locator(f"text={word}")
                        if await loc.count():
                            await loc.first.click(force=True, timeout=1000)
                            await page.wait_for_timeout(200)
                            return True
                    except Exception:
                        pass
            except Exception:
                continue
        # 既知外iframeのフォールバック
        try:
            for f in page.frames:
                if hasattr(page, 'main_frame') and f == page.main_frame:
                    continue
                for word in texts:
                    regex = re.compile(word, re.IGNORECASE)
                    for role in roles:
                        try:
                            loc = f.get_by_role(role, name=regex)
                            if await loc.count():
                                await loc.first.click(force=True, timeout=1000)
                                await page.wait_for_timeout(200)
                                return True
                        except Exception:
                            pass
                    try:
                        loc = f.locator(f"text={word}")
                        if await loc.count():
                            await loc.first.click(force=True, timeout=1000)
                            await page.wait_for_timeout(200)
                            return True
                    except Exception:
                        pass
        except Exception:
            pass
        return False

    @classmethod
    async def _hide_known_containers(
        cls, page: Page, containers: Sequence[str], iframe_selectors: Sequence[str]
    ) -> bool:
        if not containers:
            return False
        removed = False
        try:
            await page.evaluate(
                "(sels) => { try { sels.forEach(s => { const el = document.querySelector(s); if (el) { el.style.setProperty('display','none','important'); el.style.setProperty('visibility','hidden','important'); el.style.setProperty('pointer-events','none','important'); } }); } catch(e){} }",
                list(containers),
            )
            removed = True
        except Exception:
            pass

        # iframe内のコンテナも可能な範囲で非表示
        # 1) セレクタで特定できるiframeを優先し、そのcontent_frame()に対してevaluate
        for iframe_sel in iframe_selectors or []:
            try:
                locator = page.locator(iframe_sel)
                count = await locator.count()
                for i in range(count):
                    try:
                        handle = await locator.nth(i).element_handle()
                        if not handle:
                            continue
                        frame = await handle.content_frame()
                        if not frame:
                            continue
                        try:
                            await frame.evaluate(
                                "(sels) => { try { sels.forEach(s => { const el = document.querySelector(s); if (el) { el.style.setProperty('display','none','important'); el.style.setProperty('visibility','hidden','important'); el.style.setProperty('pointer-events','none','important'); } }); } catch(e){} }",
                                list(containers),
                            )
                            removed = True
                        except Exception:
                            pass
                    except Exception:
                        continue
            except Exception:
                continue

        # 2) 既知セレクタに該当しないiframeにも一括で適用
        try:
            for f in page.frames:
                if hasattr(page, 'main_frame') and f == page.main_frame:
                    continue
                try:
                    await f.evaluate(
                        "(sels) => { try { sels.forEach(s => { const el = document.querySelector(s); if (el) { el.style.setProperty('display','none','important'); el.style.setProperty('visibility','hidden','important'); el.style.setProperty('pointer-events','none','important'); } }); } catch(e){} }",
                        list(containers),
                    )
                    removed = True
                except Exception:
                    pass
        except Exception:
            pass
        return removed
