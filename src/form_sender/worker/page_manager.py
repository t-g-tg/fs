"""
ページ管理モジュール

ブラウザページの初期化、ナビゲーション、状態管理を担当
"""

import asyncio
import logging
import time
from typing import Dict, Any, Optional, List
from playwright.async_api import Page, Browser, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth
from ..utils.cookie_blocker import install_init_script, install_cookie_routes, try_reject_banners
from config.manager import get_worker_config

from ..security.logger import SecurityLogger
from ..utils.secure_logger import get_secure_logger
from ..detection.bot_detector import BotDetectionSystem
from ..utils.privacy_consent_handler import PrivacyConsentHandler

logger = get_secure_logger(__name__)
security_logger = SecurityLogger()


class PageManager:
    """ページ管理を担当するクラス"""

    def __init__(self, browser: Browser = None):
        self.browser = browser
        self.page: Optional[Page] = None
        self.bot_detector = BotDetectionSystem()
        self.current_url = None

    async def initialize_page(self) -> Page:
        """新しいページを初期化"""
        if not self.browser:
            raise RuntimeError("ブラウザが初期化されていません")

        try:
            # 設定の読込（存在しない場合は既定値でフェイルセーフ）
            try:
                worker_cfg = get_worker_config()
            except Exception:
                worker_cfg = {}
            browser_cfg = (worker_cfg.get("browser") or {}) if isinstance(worker_cfg, dict) else {}
            rb_cfg = (browser_cfg.get("resource_blocking") or {}) if isinstance(browser_cfg, dict) else {}
            stealth_cfg = (browser_cfg.get("stealth") or {}) if isinstance(browser_cfg, dict) else {}
            cookie_cfg = (browser_cfg.get("cookie_control") or {}) if isinstance(browser_cfg, dict) else {}

            # フラグ（デフォルトはON）
            stealth_enabled = bool(stealth_cfg.get("enabled", True))
            # 既定は安全側（OFF）。設定で明示有効化時のみON。
            cookie_blackhole = bool(cookie_cfg.get("override_document_cookie", False))
            cookie_block_cmp = bool(cookie_cfg.get("block_cmp_scripts", True))
            # 既定は安全側（OFF）。設定で明示有効化時のみON。
            cookie_strip_set = bool(cookie_cfg.get("strip_set_cookie", False))
            ui_reject_enabled = bool(cookie_cfg.get("ui_reject_banners", True))

            # RB 既定（PageManagerは保守的：ここではOFF既定、RBはBrowserManager側が本筋）
            rb_images = bool(rb_cfg.get("block_images", False))
            rb_fonts = bool(rb_cfg.get("block_fonts", False))
            rb_styles = bool(rb_cfg.get("block_stylesheets", False))

            # 新しいコンテキストとページを作成
            context = await self.browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                locale="ja-JP",
                timezone_id="Asia/Tokyo",
                extra_http_headers={
                    "Accept-Language": "ja, en-US;q=0.8, en;q=0.7",
                },
            )
            # cookie ブラックホール（設定尊重）
            try:
                await install_init_script(context, cookie_blackhole)
            except Exception:
                pass
            # playwright-stealth を適用（設定尊重）: 言語をBrowserManagerと同一に上書き
            try:
                if stealth_enabled:
                    langs = None
                    try:
                        # worker_config.browser.stealth.languages を尊重（無ければ ja-JP/ja）
                        langs = (worker_cfg.get("browser", {}).get("stealth", {}).get("languages") if isinstance(worker_cfg, dict) else None)
                    except Exception:
                        langs = None
                    if not isinstance(langs, (list, tuple)) or not langs:
                        langs = ["ja-JP", "ja"]
                    await Stealth(navigator_languages_override=tuple(langs)).apply_stealth_async(context)
            except Exception:
                pass
            self.page = await context.new_page()
            try:
                await self.page.set_extra_http_headers({
                    "Accept-Language": "ja, en-US;q=0.8, en;q=0.7",
                })
            except Exception:
                pass
            # ネットワーク層（設定尊重: CMP/Set-Cookie）
            try:
                await install_cookie_routes(
                    self.page,
                    block_cmp_scripts=cookie_block_cmp,
                    strip_set_cookie=cookie_strip_set,
                    resource_block_rules={"images": rb_images, "fonts": rb_fonts, "stylesheets": rb_styles},
                    strip_set_cookie_third_party_only=bool(cookie_cfg.get("strip_set_cookie_third_party_only", True)),
                    strip_set_cookie_domains=list(cookie_cfg.get("strip_set_cookie_domains", []) or []),
                    strip_set_cookie_exclude_domains=list(cookie_cfg.get("strip_set_cookie_exclude_domains", []) or []),
                )
            except Exception:
                pass
            
            # タイムアウト設定
            self.page.set_default_timeout(30000)
            self.page.set_default_navigation_timeout(30000)

            logger.info("ページを初期化しました")
            return self.page

        except Exception as e:
            logger.error(f"ページ初期化に失敗: {e}")
            raise

    async def navigate_to_url(self, url: str) -> Dict[str, Any]:
        """指定されたURLに移動"""
        if not self.page:
            await self.initialize_page()

        try:
            logger.info(f"URLへアクセス開始: {security_logger.mask_url(url)}")
            
            # ナビゲーション実行
            response = await self.page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=30000
            )

            self.current_url = self.page.url

            # レスポンスステータスチェック
            if response:
                status = response.status
                if status >= 400:
                    logger.warning(f"HTTPエラー: ステータス {status}")
                    return {
                        "success": False,
                        "error_type": "HTTP_ERROR",
                        "error_message": f"HTTPステータス {status}",
                        "status_code": status
                    }

            # ページロード完了待機
            await self.wait_for_page_load()

            # コンフィグに基づき、ナビゲーション後にクッキーバナーを拒否（UI層）
            try:
                from config.manager import get_worker_config
                worker_cfg = get_worker_config()
                cookie_cfg = (worker_cfg.get("browser", {}).get("cookie_control", {}) if isinstance(worker_cfg, dict) else {})
                ui_reject_enabled = bool(cookie_cfg.get("ui_reject_banners", True))
            except Exception:
                ui_reject_enabled = True
            try:
                await try_reject_banners(self.page, enabled=ui_reject_enabled, timeout_ms=2000)
            except Exception:
                pass

            logger.info("ページアクセス成功")
            return {"success": True}

        except PlaywrightTimeoutError:
            logger.error("ページアクセスがタイムアウトしました")
            return {
                "success": False,
                "error_type": "TIMEOUT",
                "error_message": "ページロードタイムアウト"
            }
        except Exception as e:
            logger.error(f"ページアクセス中にエラー: {e}")
            return {
                "success": False,
                "error_type": "ACCESS_ERROR",
                "error_message": str(e)
            }

    async def wait_for_page_load(self) -> None:
        """ページロード完了を待機"""
        if not self.page:
            return

        try:
            # DOMContentLoadedを待機
            await self.page.wait_for_load_state("domcontentloaded", timeout=10000)
            
            # networkidleを短時間待機
            try:
                await self.page.wait_for_load_state("networkidle", timeout=5000)
            except PlaywrightTimeoutError:
                logger.debug("networkidle待機がタイムアウト（続行）")

            # 追加の安定化待機
            await asyncio.sleep(1)

        except Exception as e:
            logger.debug(f"ページロード待機中にエラー（続行）: {e}")

    async def check_bot_detection(self, record_id: int) -> Dict[str, Any]:
        """ボット検出チェック"""
        if not self.page:
            return {"detected": False}

        try:
            result = await self.bot_detector.check_detection(self.page, record_id)
            
            if result.get("detected"):
                logger.warning(f"ボット検出システムを検知: {result.get('detection_type')}")
                return result

            return {"detected": False}

        except Exception as e:
            logger.error(f"ボット検出チェック中にエラー: {e}")
            return {"detected": False}

    async def perform_dynamic_content_loading(self) -> None:
        """動的コンテンツのロード処理"""
        if not self.page:
            return

        try:
            logger.debug("動的コンテンツのロード処理を開始")
            
            # スクロールによるコンテンツ読み込み
            await self._perform_staged_scrolling()
            
            # Ajax完了待機
            await asyncio.sleep(2)
            
            # 追加の動的要素待機
            await self._wait_for_dynamic_elements()

        except Exception as e:
            logger.debug(f"動的コンテンツロード中にエラー: {e}")

    async def _perform_staged_scrolling(self) -> None:
        """段階的スクロール処理"""
        if not self.page:
            return

        try:
            scroll_positions = [0, 0.25, 0.5, 0.75, 1.0]
            
            for position in scroll_positions:
                await self.page.evaluate(f"""
                    () => {{
                        const maxScroll = document.body.scrollHeight - window.innerHeight;
                        window.scrollTo(0, maxScroll * {position});
                    }}
                """)
                await asyncio.sleep(0.5)

            # 最上部に戻る
            await self.page.evaluate("() => window.scrollTo(0, 0)")
            await asyncio.sleep(0.5)

        except Exception as e:
            logger.debug(f"スクロール処理中にエラー: {e}")

    async def _wait_for_dynamic_elements(self) -> None:
        """動的要素の待機"""
        if not self.page:
            return

        try:
            # フォーム要素の待機
            await self.page.wait_for_selector("form", timeout=3000, state="visible")
        except PlaywrightTimeoutError:
            logger.debug("フォーム要素が見つかりません（続行）")
        except Exception as e:
            logger.debug(f"動的要素待機中にエラー: {e}")

    async def detect_popups_and_modals(self) -> Dict[str, Any]:
        """ポップアップとモーダルの検出"""
        if not self.page:
            return {"has_popup": False, "has_modal": False}

        try:
            result = {
                "has_popup": False,
                "has_modal": False,
                "popup_selectors": [],
                "modal_selectors": []
            }

            # 一般的なモーダル/ポップアップセレクタ
            popup_selectors = [
                "[role='dialog']",
                "[role='alertdialog']",
                ".modal",
                ".popup",
                ".overlay",
                "#modal",
                "#popup",
                "[class*='modal']",
                "[class*='popup']",
                "[class*='dialog']",
            ]

            for selector in popup_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    for element in elements:
                        if await element.is_visible():
                            bounds = await element.bounding_box()
                            if bounds and bounds.get("width", 0) > 100 and bounds.get("height", 0) > 100:
                                if "modal" in selector.lower():
                                    result["has_modal"] = True
                                    result["modal_selectors"].append(selector)
                                else:
                                    result["has_popup"] = True
                                    result["popup_selectors"].append(selector)
                                
                                logger.debug(f"ポップアップ/モーダル検出: {selector}")
                                break
                except Exception as e:
                    logger.debug(f"セレクタ {selector} のチェック中にエラー: {e}")

            return result

        except Exception as e:
            logger.error(f"ポップアップ/モーダル検出中にエラー: {e}")
            return {"has_popup": False, "has_modal": False}

    async def handle_confirmation_page_pattern(self, original_button_selector: str) -> bool:
        """確認ページパターンの処理"""
        if not self.page:
            return False

        try:
            logger.info("確認ページパターンの処理を開始")
            
            # 確認ページへの遷移を確認
            if not await self._verify_confirmation_page_transition(original_button_selector):
                return False

            # 確認ページの入力要素を分析
            analysis = await self._analyze_confirmation_page_inputs()
            
            # 追加の入力が必要な場合は処理
            if analysis.get("has_required_inputs"):
                await self._handle_confirmation_page_inputs(analysis)

            # 最終送信ボタンを探して送信
            return await self._find_and_submit_final_button()

        except Exception as e:
            logger.error(f"確認ページ処理中にエラー: {e}")
            return False

    async def _verify_confirmation_page_transition(self, original_button_selector: str) -> bool:
        """確認ページへの遷移を確認"""
        try:
            # URL変更をチェック
            initial_url = self.current_url
            await asyncio.sleep(2)
            
            if self.page.url != initial_url:
                logger.info("URLの変更を検出（確認ページの可能性）")
                return True

            # ページ内容の変化をチェック
            confirmation_keywords = ["確認", "confirm", "review", "内容をご確認", "入力内容"]
            page_text = await self.page.text_content("body") or ""
            
            for keyword in confirmation_keywords:
                if keyword in page_text:
                    logger.info(f"確認ページキーワード検出: {keyword}")
                    return True

            return False

        except Exception as e:
            logger.debug(f"確認ページ遷移チェック中にエラー: {e}")
            return False

    async def _analyze_confirmation_page_inputs(self) -> Dict[str, Any]:
        """確認ページの入力要素を分析"""
        if not self.page:
            return {"has_required_inputs": False}

        try:
            analysis = {
                "has_required_inputs": False,
                "required_fields": [],
                "optional_fields": [],
                "checkboxes": [],
                "radio_buttons": []
            }

            # 必須入力フィールドの検出
            required_selectors = [
                "input[required]:visible",
                "textarea[required]:visible",
                "select[required]:visible",
                "[class*='required']:visible input",
                "[class*='required']:visible textarea",
            ]

            for selector in required_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    for element in elements:
                        field_name = await element.get_attribute("name") or await element.get_attribute("id")
                        if field_name:
                            analysis["required_fields"].append(field_name)
                            analysis["has_required_inputs"] = True
                except Exception:
                    pass

            # チェックボックスの検出
            checkboxes = await self.page.query_selector_all("input[type='checkbox']:visible")
            for checkbox in checkboxes:
                name = await checkbox.get_attribute("name")
                if name:
                    analysis["checkboxes"].append(name)

            logger.debug(f"確認ページ分析結果: {analysis}")
            return analysis

        except Exception as e:
            logger.error(f"確認ページ分析中にエラー: {e}")
            return {"has_required_inputs": False}

    async def _handle_confirmation_page_inputs(self, analysis: Dict[str, Any]) -> None:
        """確認ページの入力処理"""
        if not self.page:
            return

        try:
            # 必須チェックボックスの処理（同意など）
            for checkbox_name in analysis.get("checkboxes", []):
                if "agree" in checkbox_name.lower() or "consent" in checkbox_name.lower() or "同意" in checkbox_name:
                    selector = f"input[name='{checkbox_name}']"
                    element = await self.page.query_selector(selector)
                    if element and not await element.is_checked():
                        await element.check()
                        logger.info(f"同意チェックボックスをチェック: {checkbox_name}")

        except Exception as e:
            logger.error(f"確認ページ入力処理中にエラー: {e}")

    async def _find_and_submit_final_button(self) -> bool:
        """最終送信ボタンを見つけて送信"""
        if not self.page:
            return False

        try:
            # 送信ボタンのセレクタ候補
            submit_selectors = [
                "button[type='submit']:visible",
                "input[type='submit']:visible",
                "button:has-text('送信'):visible",
                "button:has-text('送る'):visible",
                "button:has-text('確定'):visible",
                "button:has-text('完了'):visible",
                "input[value*='送信']:visible",
                "input[value*='確定']:visible",
            ]

            for selector in submit_selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element and await element.is_visible():
                        await element.scroll_into_view_if_needed()
                        await asyncio.sleep(0.5)
                        # 確認ページ上の同意チェックもボタン直前で強制ON
                        try:
                            await PrivacyConsentHandler.ensure_near_button(self.page, element, context_hint="final-submit")
                        except Exception as _consent_err:
                            logger.debug(f"Privacy consent ensure near final submit failed: {_consent_err}")
                        await element.click()
                        logger.info(f"最終送信ボタンをクリック: {selector}")
                        return True
                except Exception as e:
                    logger.debug(f"ボタンクリック失敗: {selector}, エラー: {e}")

            logger.warning("最終送信ボタンが見つかりません")
            return False

        except Exception as e:
            logger.error(f"最終送信ボタン検索中にエラー: {e}")
            return False

    async def cleanup(self) -> None:
        """ページのクリーンアップ"""
        try:
            if self.page:
                await self.page.close()
                self.page = None
                logger.debug("ページをクローズしました")
        except Exception as e:
            logger.debug(f"ページクリーンアップ中にエラー: {e}")
