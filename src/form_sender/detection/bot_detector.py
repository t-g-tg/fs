"""
Bot検知システム

reCAPTCHA、Cloudflare等のBot保護システム検出機能
"""

import logging
from typing import Optional, Tuple
from playwright.async_api import Page

logger = logging.getLogger(__name__)


class BotDetectionThresholds:
    """Bot検知に使用する閾値定数"""

    NORMAL_PAGE_MIN_TEXT_LENGTH = 500  # 通常ページの最小テキスト長
    NORMAL_PAGE_MIN_HTML_LENGTH = 1000  # 通常ページの最小HTML長
    RECAPTCHA_PAGE_MAX_TEXT_LENGTH = 200  # reCAPTCHAページの最大テキスト長


class BotDetectionSystem:
    """Bot検知システム（偽陽性防止最優先版）"""

    @staticmethod
    async def detect_bot_protection(page: Page) -> Tuple[bool, Optional[str]]:
        """Bot保護システムを検出（reCAPTCHA/Cloudflareを先に評価）"""
        try:
            # Step 1: まず明示的な Bot 保護の存在を評価（通常ページ判定より先）
            recaptcha_detected, recaptcha_type = await BotDetectionSystem._detect_strict_recaptcha(page)
            if recaptcha_detected:
                return True, recaptcha_type

            cloudflare_detected, cloudflare_type = await BotDetectionSystem._detect_strict_cloudflare(page)
            if cloudflare_detected:
                return True, cloudflare_type

            # Step 2: 通常ページの特徴が強ければ非Botとみなす
            if await BotDetectionSystem._is_normal_page(page):
                return False, None

            return False, None

        except Exception as e:
            logger.error(f"Bot detection error: {e}")
            # エラー時は安全側（通常ページ）に倒す
            return False, None

    @staticmethod
    async def _is_normal_page(page: Page) -> bool:
        """通常ページの特徴をチェック（除外条件）"""
        try:
            # フォーム要素の存在チェック
            form_elements = await page.locator("form, input, textarea, select").count()
            if form_elements > 0:
                return True

            # 通常のサイト構造要素
            nav_elements = await page.locator(
                "nav, header, footer, .header, .footer, .navigation"
            ).count()
            if nav_elements > 0:
                return True

            # パフォーマンス最適化: テキスト長を先にチェック
            text_content = await page.locator("body").inner_text()
            if (
                len(text_content.strip())
                > BotDetectionThresholds.NORMAL_PAGE_MIN_TEXT_LENGTH
            ):
                return True

            # テキストが少ない場合のみHTMLコンテンツをチェック
            page_content = await page.content()
            if len(page_content) > BotDetectionThresholds.NORMAL_PAGE_MIN_HTML_LENGTH:
                return True

            return False

        except Exception:
            # エラー時は通常ページとして扱う（安全側）
            return True

    @staticmethod
    async def _detect_strict_recaptcha(page: Page) -> Tuple[bool, Optional[str]]:
        """reCAPTCHA検出（厳格→スコアリング緩和の2段構え）"""
        try:
            # 厳格: v2 visible（anchor iframe + .g-recaptcha 可視）- DOM往復削減のため evaluate に集約
            try:
                # 暗黙の文字列リテラル結合は Python パーサ差異で SyntaxError になり得るため
                # 三重引用符の単一リテラルに変更して互換性を確保
                rec = await page.evaluate(
                    (
                        """
                        () => ({
                          anchor: document.querySelectorAll('iframe[src*="recaptcha/api2/anchor"]').length,
                          sitekey: document.querySelectorAll('.g-recaptcha[data-sitekey]').length,
                          visible: !!document.querySelector('.g-recaptcha') && (function(el){
                            const s = getComputedStyle(el);
                            return s && s.display !== 'none' && s.visibility !== 'hidden';
                          })(document.querySelector('.g-recaptcha'))
                        })
                        """
                    ).strip()
                )
            except Exception:
                rec = {"anchor": 0, "sitekey": 0, "visible": False}

            recaptcha_iframe = int(rec.get("anchor", 0) or 0)
            g_recaptcha_cnt = int(rec.get("sitekey", 0) or 0)
            visible_recaptcha = bool(rec.get("visible", False))

            if recaptcha_iframe > 0 and g_recaptcha_cnt > 0 and visible_recaptcha:
                # v2可視が明確
                return True, "reCAPTCHA"

            # 緩和: v2 invisible / v3 など。複合シグナルの合算で判定。
            signals = 0
            # script / iframe 存在
            try:
                counts = await page.evaluate(
                    (
                        """
                        () => ({
                          s: document.querySelectorAll('script[src*="recaptcha/api.js"]').length,
                          i: document.querySelectorAll('iframe[src*="recaptcha"]').length,
                          g: document.querySelectorAll('[name="g-recaptcha-response"]').length,
                          b: document.querySelectorAll('.grecaptcha-badge, .g-recaptcha').length
                        })
                        """
                    ).strip()
                )
                if int(counts.get("s", 0) or 0) > 0:
                    signals += 1
                if recaptcha_iframe > 0 or int(counts.get("i", 0) or 0) > 0:
                    signals += 1
                if int(counts.get("g", 0) or 0) > 0:
                    signals += 1
                if int(counts.get("b", 0) or 0) > 0:
                    signals += 1
            except Exception:
                pass

            # window.grecaptcha があれば強いシグナル
            try:
                has_grecaptcha = await page.evaluate(
                    "() => typeof window.grecaptcha !== 'undefined'"
                )
                if has_grecaptcha:
                    signals += 1
            except Exception:
                pass

            if signals >= 2:
                return True, "reCAPTCHA"

            return False, None

        except Exception:
            return False, None

    @staticmethod
    async def _detect_strict_cloudflare(page: Page) -> Tuple[bool, Optional[str]]:
        """厳格なCloudflare Challenge検出（複数条件をANDで組み合わせ）"""
        try:
            current_url = page.url
            page_title = await page.title()

            # 条件1: Challenge URLの完全一致
            if "/cdn-cgi/challenge-platform/" not in current_url:
                return False, None

            # 条件2: タイトルの完全一致
            if page_title != "Just a moment...":
                return False, None

            # 条件3: Cloudflare特有の要素が存在
            cf_elements = await page.locator(".cf-browser-verification, #cf-wrapper").count()
            if cf_elements == 0:
                return False, None

            # 条件4: 通常ページの特徴をチェック（重複ロジック統合）
            if await BotDetectionSystem._is_normal_page(page):
                return False, None

            # 条件5: 特定のCloudflareテキストが存在（必要な場合のみページコンテンツ取得）
            page_content = await page.content()
            required_texts = ["Cloudflare", "Checking your browser"]
            for text in required_texts:
                if text not in page_content:
                    return False, None

            # 全条件を満たした場合のみCloudflare Challenge検出
            return True, "Cloudflare Challenge"

        except Exception:
            return False, None

