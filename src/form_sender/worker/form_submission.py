"""
フォーム送信処理モジュール

フォームフィールドの入力と送信に関する処理を管理
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Union
from playwright.async_api import Page, ElementHandle, Locator, TimeoutError as PlaywrightTimeoutError

from ..security.logger import SecurityLogger
from ..utils.secure_logger import get_secure_logger

logger = get_secure_logger(__name__)
security_logger = SecurityLogger()


class FormSubmissionHandler:
    """フォーム送信処理を管理するクラス"""

    def __init__(self, page: Page, keyword_matcher=None):
        self.page = page
        self.keyword_matcher = keyword_matcher
        self.selector_cache = {}

    async def fill_form_field(self, field_name: str, field_config: Dict[str, Any]) -> None:
        """フォームフィールドに値を入力"""
        if not self._validate_field_config(field_name, field_config):
            return

        selector = await self._resolve_field_selector(field_name, field_config)
        if not selector:
            logger.warning(f"フィールド '{field_name}' のセレクタが見つかりません")
            return

        await self._execute_field_input(field_name, selector, field_config)

    def _validate_field_config(
        self, field_name: str, field_config: Dict[str, Any]
    ) -> bool:
        """フィールド設定の妥当性検証"""
        if not field_config:
            logger.warning(f"フィールド '{field_name}' の設定が空です")
            return False

        if "selector" not in field_config and "auto_action" not in field_config:
            logger.warning(f"フィールド '{field_name}' にセレクタまたは自動アクションがありません")
            return False

        return True

    async def _resolve_field_selector(
        self, field_name: str, field_config: Dict[str, Any]
    ) -> Optional[str]:
        """フィールドのセレクタを解決"""
        if "selector" in field_config:
            return field_config["selector"]

        if "auto_action" in field_config:
            return await self._handle_selector_fallback(field_name, field_config)

        return None

    async def _handle_selector_fallback(
        self, field_name: str, field_config: Dict[str, Any]
    ) -> Optional[str]:
        """セレクタが見つからない場合のフォールバック処理"""
        logger.debug(f"フィールド '{field_name}' のセレクタフォールバック処理を開始")

        auto_action = field_config.get("auto_action")
        if auto_action == "skip":
            logger.info(f"フィールド '{field_name}' はスキップ設定のためスキップします")
            return None

        return await self._generic_selector_fallback(field_name, field_config)

    async def _generic_selector_fallback(
        self, field_name: str, field_config: Dict[str, Any]
    ) -> Optional[str]:
        """汎用セレクタフォールバック処理"""
        field_variations = self._generate_field_variations(field_name)
        
        for variation in field_variations:
            selectors_to_try = [
                f"input[name*='{variation}' i]",
                f"textarea[name*='{variation}' i]",
                f"select[name*='{variation}' i]",
                f"input[id*='{variation}' i]",
                f"textarea[id*='{variation}' i]",
                f"select[id*='{variation}' i]",
                f"input[placeholder*='{variation}' i]",
                f"textarea[placeholder*='{variation}' i]",
            ]

            for selector in selectors_to_try:
                try:
                    elements = await self.page.query_selector_all(selector)
                    if elements:
                        visible_elements = []
                        for elem in elements:
                            if await elem.is_visible():
                                visible_elements.append(elem)

                        if visible_elements:
                            logger.info(
                                f"フィールド '{field_name}' の代替セレクタを発見: {selector} "
                                f"({len(visible_elements)}個の要素)"
                            )
                            return selector
                except Exception as e:
                    logger.debug(f"セレクタ {selector} のチェック中にエラー: {e}")
                    continue

        logger.warning(f"フィールド '{field_name}' の代替セレクタが見つかりませんでした")
        return None

    def _generate_field_variations(self, field_name: str) -> List[str]:
        """フィールド名のバリエーションを生成"""
        variations = [field_name]
        
        # 日本語の場合のバリエーション
        japanese_mappings = {
            "会社名": ["company", "company_name", "corp", "企業名", "法人名"],
            "担当者名": ["name", "contact", "person", "氏名", "お名前"],
            "メール": ["email", "mail", "e-mail", "メールアドレス"],
            "電話": ["tel", "phone", "telephone", "電話番号"],
            "お問い合わせ": ["inquiry", "message", "content", "内容", "本文"],
        }
        
        for key, values in japanese_mappings.items():
            if key in field_name:
                variations.extend(values)
        
        # 英語の場合のバリエーション
        english_mappings = {
            "company": ["会社", "企業", "corp", "company_name"],
            "name": ["氏名", "名前", "担当者", "contact"],
            "email": ["メール", "mail", "メールアドレス"],
            "phone": ["電話", "tel", "telephone"],
            "message": ["メッセージ", "お問い合わせ", "内容", "本文"],
        }
        
        for key, values in english_mappings.items():
            if key.lower() in field_name.lower():
                variations.extend(values)
        
        return list(set(variations))

    async def _execute_field_input(
        self, field_name: str, selector: str, field_config: Dict[str, Any]
    ) -> None:
        """フィールドへの入力実行"""
        if not self.page:
            logger.error("Page is not initialized for field input")
            return
            
        value = field_config.get("value", "")
        if field_config.get("type") == "checkbox":
            await self._handle_checkbox_input(field_name, selector, value)
        else:
            await self.page.fill(selector, str(value))
            logger.debug(f"フィールド '{field_name}' に値を入力しました")

    async def _handle_checkbox_input(
        self, field_name: str, selector: str, value: Union[str, bool]
    ) -> None:
        """チェックボックスの処理"""
        if not self.page:
            logger.error("Page is not initialized for checkbox input")
            return
            
        should_check = value in [True, "true", "True", "1", 1, "check"]
        
        element = await self.page.query_selector(selector)
        if element:
            is_checked = await element.is_checked()
            if should_check and not is_checked:
                await element.check()
                logger.debug(f"チェックボックス '{field_name}' をチェックしました")
            elif not should_check and is_checked:
                await element.uncheck()
                logger.debug(f"チェックボックス '{field_name}' のチェックを外しました")

    async def submit_form(self, submit_config: Dict[str, Any]) -> bool:
        """フォームを送信"""
        try:
            submit_button_selector = await self._prepare_submit_button(submit_config)
            if not submit_button_selector:
                logger.warning("送信ボタンが見つかりません")
                return False

            # 送信前の準備
            await asyncio.sleep(1)
            
            # 送信実行
            await self.page.click(submit_button_selector)
            logger.info("フォーム送信ボタンをクリックしました")
            
            # 送信後の待機
            await asyncio.sleep(3)
            return True

        except PlaywrightTimeoutError:
            logger.warning("フォーム送信がタイムアウトしました")
            return False
        except Exception as e:
            logger.error(f"フォーム送信中にエラー: {e}")
            return False

    async def _prepare_submit_button(self, submit_config: Dict[str, Any]) -> Optional[str]:
        """送信ボタンの準備"""
        if "selector" in submit_config:
            selector = submit_config["selector"]
            element = await self.page.query_selector(selector)
            if element and await element.is_visible():
                return selector

        return await self._try_fallback_selectors()

    async def _try_fallback_selectors(self) -> Optional[str]:
        """フォールバックセレクタを試行"""
        fallback_selectors = [
            "button[type='submit']",
            "input[type='submit']",
            "button:has-text('送信')",
            "button:has-text('送る')",
            "input[value*='送信' i]",
            "input[value*='submit' i]",
        ]

        for selector in fallback_selectors:
            try:
                element = await self.page.query_selector(selector)
                if element and await element.is_visible():
                    logger.info(f"代替送信ボタンを発見: {selector}")
                    return selector
            except Exception:
                continue

        return None

    async def find_and_submit_final_button(self) -> bool:
        """最終送信ボタンを検索して送信"""
        try:
            buttons = await self._get_all_visible_buttons()
            if not buttons:
                logger.warning("送信可能なボタンが見つかりません")
                return False

            for button in buttons:
                button_texts = await self._extract_button_texts(button)
                if self._is_submit_button(button_texts):
                    await self._execute_element_click(button, "最終送信ボタン")
                    return True

            logger.warning("送信ボタンの条件に合うものが見つかりません")
            return False

        except Exception as e:
            logger.error(f"最終送信ボタンの検索中にエラー: {e}")
            return False

    async def _get_all_visible_buttons(self) -> List[ElementHandle]:
        """表示されているすべてのボタンを取得"""
        selectors = [
            "button",
            "input[type='submit']",
            "input[type='button']",
            "a.btn",
            "a.button",
            "div[role='button']",
            "span[role='button']",
        ]

        visible_buttons = []
        for selector in selectors:
            try:
                elements = await self.page.query_selector_all(selector)
                for elem in elements:
                    if await elem.is_visible():
                        visible_buttons.append(elem)
            except Exception as e:
                logger.debug(f"セレクタ {selector} の処理中にエラー: {e}")

        return visible_buttons

    async def _extract_button_texts(self, button: ElementHandle) -> List[str]:
        """ボタンから関連するテキストを抽出"""
        texts = []
        
        try:
            # テキストコンテンツ
            text = await button.text_content()
            if text:
                texts.append(text.strip())
            
            # value属性
            value = await button.get_attribute("value")
            if value:
                texts.append(value.strip())
            
            # aria-label
            aria_label = await button.get_attribute("aria-label")
            if aria_label:
                texts.append(aria_label.strip())
            
            # title属性
            title = await button.get_attribute("title")
            if title:
                texts.append(title.strip())
        except Exception as e:
            logger.debug(f"ボタンテキスト抽出中にエラー: {e}")

        return texts

    def _is_submit_button(self, button_texts: List[str]) -> bool:
        """ボタンが送信ボタンかどうか判定"""
        if not self.keyword_matcher:
            # キーワードマッチャーがない場合はデフォルト判定
            submit_keywords = ["送信", "送る", "submit", "send", "確定", "完了"]
            for text in button_texts:
                for keyword in submit_keywords:
                    if keyword in text.lower():
                        return True
            return False

        # キーワードマッチャーを使用
        for text in button_texts:
            if self.keyword_matcher.match(text):
                return True
        return False

    async def _execute_element_click(self, element: ElementHandle, description: str) -> bool:
        """要素のクリック実行"""
        try:
            await element.scroll_into_view_if_needed()
            await asyncio.sleep(0.5)
            await element.click()
            logger.info(f"{description}をクリックしました")
            return True
        except Exception as e:
            logger.error(f"{description}のクリック中にエラー: {e}")
            return False