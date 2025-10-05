"""
フォーム要素への入力処理を担当するハンドラ
"""
import logging
from typing import Dict, Any, Optional

from playwright.async_api import Page, ElementHandle, TimeoutError as PlaywrightTimeoutError


class FormInputHandler:
    """ページ上のフォーム要素への具体的な入力操作をカプセル化する"""

    def __init__(self, page: Page, worker_id: int, post_input_delay_ms: int = 200):
        self.page = page
        self.worker_id = worker_id
        self._post_input_delay_ms = int(post_input_delay_ms) if post_input_delay_ms is not None else 200
        self.logger = logging.getLogger(f"{__name__}.w{worker_id}")

    async def fill_rule_based_field(self, field_name: str, field_info: Dict[str, Any], value: str) -> bool:
        """ルールベースで発見されたフィールドに値を入力する

        Returns:
            bool: 入力と検証が成功した場合 True、それ以外は False
        """
        selector = field_info.get('selector')
        # RuleBasedAnalyzer は 'input_type' キーに正規化済みの型を格納する。
        # 古い互換のため 'type' (HTML属性) もフォールバックとして参照する。
        input_type = field_info.get('input_type') or field_info.get('type', 'text')
        if not selector:
            self.logger.warning(f"Skipping field {field_name} due to missing selector.")
            return False
        # テキスト系以外（select/checkbox/radio）は値が空でも処理を継続（アルゴリズムや既定動作に委譲）
        if input_type in ["text", "email", "tel", "url", "textarea", "password"]:
            if value is None or not str(value).strip():
                self.logger.warning(f"Skipping field {field_name} due to missing value.")
                return False

        self.logger.info(f"Starting field operation - field: {field_name}, type: {input_type}")
        element = await self.page.query_selector(selector)
        if not element:
            self.logger.warning(f"Rule-based element not found: {selector}")
            return False

        try:
            input_success = await self._fill_element(element, value, input_type, field_name, field_info)
            if input_success:
                # 短時間待機（設定優先。必要時は検証NGで再入力が走る）
                try:
                    await self.page.wait_for_timeout(self._post_input_delay_ms)
                except Exception:
                    pass
                verification_success = await self._verify_field_input(element, field_name, input_type, value)
                if verification_success:
                    self.logger.info(f"Field operation completed successfully - {field_name}")
                    return True
                else:
                    self.logger.warning(f"Field input verification failed - {field_name}")
                    return False
            else:
                self.logger.error(f"Field operation failed - {field_name}")
                return False
        except Exception as e:
            self.logger.error(f"Error in field operation - {field_name}: {e}")
            return False

    async def _fill_element(self, element: ElementHandle, value: str, input_type: str, field_name: str, field_info: Optional[Dict[str, Any]] = None) -> bool:
        """要素の型に応じて入力処理を振り分ける"""
        tag_name = await element.evaluate('el => el.tagName.toLowerCase()')
        type_attr = await element.get_attribute('type')

        if input_type in ["text", "email", "tel", "url", "textarea", "password"]:
            return await self._fill_text_like(element, value, tag_name)
        elif input_type == "select":
            return await self._fill_select(element, value, field_name, field_info=field_info)
        elif input_type == "checkbox":
            return await self._fill_checkbox(element, value)
        elif input_type == "radio":
            return await self._fill_radio(element)
        else:
            self.logger.warning(f"Unknown input type '{input_type}' for field {field_name}, attempting text fill.")
            return await self._fill_text_like(element, value, tag_name)

    async def _fill_text_like(self, element: ElementHandle, value: str, tag_name: str) -> bool:
        """テキスト系要素への入力"""
        await element.fill(str(value))
        return True

    async def _fill_select(self, element: ElementHandle, value: str, field_name: str = "", field_info: Optional[Dict[str, Any]] = None) -> bool:
        """Select要素の選択（安全ログ + 3段階アルゴリズム対応）"""
        auto_action = (field_info or {}).get('auto_action') if isinstance(field_info, dict) else None
        # 1) auto_action に応じた処理
        try:
            if auto_action == 'select_index':
                idx = (field_info or {}).get('selected_index')
                if isinstance(idx, int) and idx >= 0:
                    await element.select_option(index=idx)
                    self.logger.debug(f"Select '{field_name}' chosen by index {idx}")
                    return True
            elif auto_action == 'select_by_algorithm':
                return await self._select_by_keyword_algorithm(element, field_name)
        except Exception as e:
            self.logger.debug(f"Auto-action select failed for '{field_name}': {e}")

        # 2) 値/ラベル指定（値は出力しない）
        try:
            await element.select_option(value=str(value))
            self.logger.debug(f"Select '{field_name}' chosen by value (redacted)")
            return True
        except PlaywrightTimeoutError:
            try:
                await element.select_option(label=str(value))
                self.logger.debug(f"Select '{field_name}' chosen by label (redacted)")
                return True
            except Exception as e:
                # 値の内容はログしない
                self.logger.debug(f"Select choose by value/label failed for '{field_name}': {e}")
                # 3) 最後にアルゴリズムへ委譲
                return await self._select_by_keyword_algorithm(element, field_name)

    async def _select_by_keyword_algorithm(self, element: ElementHandle, field_name: str = "") -> bool:
        """3段階選択アルゴリズム（優先: 営業/提案/メール → その他系 → 最後の非空）"""
        try:
            options = await element.query_selector_all('option')
            if not options:
                return False

            pri1 = ["営業", "提案", "メール", "contact", "inquiry", "問合せ", "お問い合わせ"]
            pri2 = ["その他", "other", "該当なし", "該当しない", "not applicable", "n/a"]

            # 文字列配列化（evaluateで一括取得して往復を削減）
            try:
                opt_data = await element.evaluate(
                    "el => Array.from(el.options).map(o => ({text: (o.textContent||'').trim(), value: (o.value||'').trim()}))"
                )
            except Exception:
                # フォールバック（遅いが確実）
                opt_data = []
                for opt in options:
                    try:
                        opt_data.append({
                            'text': (await opt.text_content()) or '',
                            'value': (await opt.get_attribute('value')) or ''
                        })
                    except Exception:
                        opt_data.append({'text': '', 'value': ''})

            texts = [d.get('text','') for d in opt_data]
            values = [d.get('value','') for d in opt_data]

            def _last_match(keys, seq):
                idxs = [i for i, t in enumerate(seq) if any(k.lower() in (t or '').lower() for k in keys)]
                return idxs[-1] if idxs else None

            # Stage 1: 優先キーワード
            idx = _last_match(pri1, texts)
            if idx is None:
                idx = _last_match(pri2, texts)
            if idx is not None:
                await element.select_option(index=idx)
                self.logger.debug(f"Select '{field_name}' keyword algorithm -> index {idx}")
                return True

            # Stage 2: 最後のオプション（空プレースホルダーの場合は採用しない）
            if len(texts) > 0:
                last_index = len(texts) - 1
                last_text = (texts[last_index] or '').strip()
                last_value = (values[last_index] or '').strip()
                if last_text or last_value:
                    await element.select_option(index=last_index)
                    self.logger.debug(f"Select '{field_name}' fallback -> last option index {last_index}")
                    return True

            # Stage 3: 最初の非空オプション
            placeholder_tokens = [
                '選択してください', '選択して下さい', 'お選びください', 'お選び下さい',
                'please select', 'select', 'choose', '未選択', '未定'
            ]
            for i in range(len(texts)):
                ot = (texts[i] or '').strip(); ov = (values[i] or '').strip()
                if not (ov or ot):
                    continue
                if any(tok in ot.lower() for tok in placeholder_tokens):
                    continue
                await element.select_option(index=i)
                self.logger.debug(f"Select '{field_name}' fallback -> first non-empty index {i}")
                return True

            # すべて空
            return False
        except Exception as e:
            self.logger.debug(f"Keyword algorithm error for '{field_name}': {e}")
            return False

    async def _fill_checkbox(self, element: ElementHandle, value: str) -> bool:
        """Checkbox要素の選択（可視化・短い待機・JSフォールバック強化）"""
        should_be_checked = str(value).lower() not in ['false', '0', '', 'no']
        # 事前に可視・安定化
        try:
            try:
                await element.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                await element.wait_for_element_state('visible', timeout=3000)
            except Exception:
                pass
        except Exception:
            pass
        try:
            # 1) Playwright の操作（タイムアウト短縮）
            if should_be_checked:
                await element.check(timeout=5000)
            else:
                await element.uncheck(timeout=5000)
            if (await element.is_checked()) == should_be_checked:
                return True
        except Exception as e:
            self.logger.debug(f"Primary checkbox action failed, trying fallbacks: {e}")

        # 2) for属性のlabelクリックを試す
        try:
            el_id = await element.get_attribute('id')
            if el_id:
                label_selector = f'label[for="{el_id}"]'
                label = await self.page.query_selector(label_selector)
                if label:
                    try:
                        await label.scroll_into_view_if_needed()
                    except Exception:
                        pass
                    await label.click()
                    if (await element.is_checked()) == should_be_checked:
                        return True
        except Exception as e:
            self.logger.debug(f"Label(for=) click fallback failed: {e}")

        # 3) 親labelクリック（inputがlabel内にあるケース）
        try:
            parent_is_label = await element.evaluate("el => el.closest('label') !== null")
            if parent_is_label:
                await element.evaluate("el => el.closest('label').scrollIntoView({block:'center'})")
                await element.evaluate("el => el.closest('label').click()")
                if (await element.is_checked()) == should_be_checked:
                    return True
        except Exception as e:
            self.logger.debug(f"Closest(label) click fallback failed: {e}")

        # 4) 最終フォールバック: JSでcheckedを書き換え、input/changeイベントを発火
        try:
            await element.evaluate(
                "(el, should) => { el.checked = !!should; el.dispatchEvent(new Event('input', { bubbles: true })); el.dispatchEvent(new Event('change', { bubbles: true })); }",
                should_be_checked
            )
            await self.page.wait_for_timeout(100)
            if (await element.is_checked()) == should_be_checked:
                return True
        except Exception as e:
            self.logger.debug(f"JS set checked fallback failed: {e}")

        return False

    async def _fill_radio(self, element: ElementHandle) -> bool:
        """Radioボタンの選択"""
        await element.check() # Radioはcheck()で良い
        return True

    async def _verify_field_input(self, element: ElementHandle, field_name: str, input_type: str, expected_value: str) -> bool:
        """フィールド入力が正しく行われたか検証する"""
        try:
            if input_type in ["checkbox", "radio"]:
                is_checked = await element.is_checked()
                expected_checked = str(expected_value).lower() not in ['false', '0', '', 'no']
                return is_checked == expected_checked

            actual_value = await element.input_value()
            return expected_value in actual_value
        except Exception as e:
            self.logger.warning(f"Input verification error for {field_name}: {e}")
            return False
