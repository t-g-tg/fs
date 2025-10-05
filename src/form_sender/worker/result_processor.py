"""
結果処理モジュール

フォーム送信結果の処理とデータベースへの書き込みを管理
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from playwright.async_api import Page

from ..security.logger import SecurityLogger
from ..utils.secure_logger import get_secure_logger
from ..analyzer.success_judge import SuccessJudge

logger = get_secure_logger(__name__)
security_logger = SecurityLogger()


class ResultProcessor:
    """送信結果の処理を管理するクラス"""

    def __init__(self, page: Page = None):
        self.page = page
        self.response_data = {}
        self.success_judge = SuccessJudge()

    def setup_response_listener(self, pre_submit_url: str) -> Dict[str, Any]:
        """レスポンスリスナーの設定"""
        self.response_data = {
            "network_calls": [],
            "status_codes": [],
            "error_messages": None,
        }

        def handle_response(response) -> None:
            try:
                url = response.url
                status = response.status
                
                # URLの変更を記録
                if url != pre_submit_url:
                    if not any(call.get("url") == url for call in self.response_data["network_calls"]):
                        self.response_data["network_calls"].append({
                            "url": url,
                            "status": status,
                            "timestamp": time.time()
                        })
                
                # ステータスコードを記録
                if status not in self.response_data["status_codes"]:
                    self.response_data["status_codes"].append(status)
                    
            except Exception as e:
                logger.debug(f"レスポンス処理中のエラー: {e}")

        if self.page:
            self.page.on("response", handle_response)
            return handle_response
        
        return None

    def cleanup_response_listener(self, response_listener) -> None:
        """レスポンスリスナーのクリーンアップ"""
        try:
            if self.page and response_listener:
                self.page.remove_listener("response", response_listener)
                logger.debug("レスポンスリスナーをクリーンアップしました")
        except Exception as e:
            logger.debug(f"レスポンスリスナーのクリーンアップ中にエラー: {e}")

    async def execute_four_stage_judgment(
        self,
        pre_submit_state: Dict[str, Any],
        mutation_result: Dict[str, Any],
        response_data: Dict[str, Any],
    ) -> Dict[str, bool]:
        """4段階の成功判定を実行"""
        try:
            # ページのテキストとHTMLを取得
            page_text = ""
            page_content = ""
            
            if self.page:
                try:
                    page_text = await self.page.text_content("body") or ""
                    page_content = await self.page.content() or ""
                except Exception as e:
                    logger.debug(f"ページコンテンツ取得エラー: {e}")

            # 各段階の判定を実行
            failure_check = await self._check_failure_keywords(page_text, page_content)
            success_check = await self._check_success_keywords(page_text, page_content)
            http_check = await self._check_http_response(response_data)
            state_check = await self._check_state_changes(pre_submit_state, mutation_result)

            return {
                "failure_keywords": failure_check,
                "success_keywords": success_check,
                "http_response": http_check,
                "state_changes": state_check,
            }

        except Exception as e:
            logger.error(f"4段階判定の実行中にエラー: {e}")
            return {
                "failure_keywords": False,
                "success_keywords": False,
                "http_response": False,
                "state_changes": False,
            }

    async def _check_failure_keywords(self, page_text: str, page_content: str) -> bool:
        """失敗キーワードのチェック"""
        failure_keywords = [
            "エラー", "失敗", "error", "fail",
            "必須項目", "入力してください", "required",
            "不正", "invalid", "無効"
        ]
        
        combined_text = (page_text + " " + page_content).lower()
        for keyword in failure_keywords:
            if keyword.lower() in combined_text:
                logger.debug(f"失敗キーワード検出: {keyword}")
                return True
        
        return False

    async def _check_success_keywords(self, page_text: str, page_content: str) -> bool:
        """成功キーワードのチェック"""
        success_keywords = [
            "ありがとうございます", "送信完了", "送信しました",
            "thank you", "success", "completed",
            "受付", "確認", "完了"
        ]
        
        combined_text = (page_text + " " + page_content).lower()
        for keyword in success_keywords:
            if keyword.lower() in combined_text:
                logger.debug(f"成功キーワード検出: {keyword}")
                return True
        
        return False

    async def _check_http_response(self, response_data: Dict[str, Any]) -> bool:
        """HTTPレスポンスのチェック"""
        if not response_data.get("status_codes"):
            return False
        
        status_codes = response_data["status_codes"]
        
        # 成功ステータスコード
        success_codes = [200, 201, 202, 204, 301, 302, 303, 307, 308]
        
        for code in status_codes:
            if code in success_codes:
                logger.debug(f"成功HTTPステータス検出: {code}")
                return True
        
        # エラーステータスコードのチェック
        error_codes = [400, 401, 403, 404, 500, 502, 503, 504]
        has_error = any(code in error_codes for code in status_codes)
        
        if has_error:
            logger.debug(f"エラーHTTPステータス検出: {status_codes}")
            
        return not has_error

    async def _check_state_changes(
        self, pre_submit_state: Dict[str, Any], mutation_result: Dict[str, Any]
    ) -> bool:
        """状態変更のチェック"""
        try:
            # URL変更のチェック
            if pre_submit_state.get("url") != mutation_result.get("final_url"):
                logger.debug("URL変更を検出")
                return True
            
            # DOM変更のチェック
            if await self._evaluate_dom_changes(mutation_result):
                return True
            
            # フォーム状態変更のチェック
            post_state = await self._capture_page_state()
            if await self._evaluate_form_state_changes(pre_submit_state, post_state):
                return True
            
            return False
            
        except Exception as e:
            logger.debug(f"状態変更チェック中にエラー: {e}")
            return False

    async def _evaluate_dom_changes(self, mutation_result: Dict[str, Any]) -> bool:
        """DOM変更の評価"""
        significant_changes = mutation_result.get("significant_changes", 0)
        total_mutations = mutation_result.get("total_mutations", 0)
        
        # 重要な変更が一定数以上ある場合
        if significant_changes >= 3:
            logger.debug(f"重要なDOM変更を検出: {significant_changes}件")
            return True
        
        # 全体の変更が多い場合
        if total_mutations >= 10:
            logger.debug(f"多数のDOM変更を検出: {total_mutations}件")
            return True
        
        return False

    async def _evaluate_form_state_changes(
        self, pre_state: Dict[str, Any], post_state: Dict[str, Any]
    ) -> bool:
        """フォーム状態変更の評価"""
        try:
            # フォーム要素数の変化
            pre_forms = len(pre_state.get("forms", []))
            post_forms = len(post_state.get("forms", []))
            
            if pre_forms != post_forms:
                logger.debug(f"フォーム数の変化: {pre_forms} → {post_forms}")
                return True
            
            # 入力フィールドの変化
            pre_inputs = pre_state.get("visible_inputs", 0)
            post_inputs = post_state.get("visible_inputs", 0)
            
            if abs(pre_inputs - post_inputs) > 3:
                logger.debug(f"入力フィールド数の変化: {pre_inputs} → {post_inputs}")
                return True
            
            return False
            
        except Exception as e:
            logger.debug(f"フォーム状態評価中にエラー: {e}")
            return False

    async def _capture_page_state(self) -> Dict[str, Any]:
        """ページの状態をキャプチャ"""
        if not self.page:
            return {}
        
        try:
            state = {
                "url": self.page.url,
                "title": await self.page.title(),
                "forms": [],
                "visible_inputs": 0,
                "timestamp": time.time()
            }
            
            # フォーム情報の収集
            forms = await self.page.query_selector_all("form")
            for form in forms:
                if await form.is_visible():
                    form_data = {
                        "action": await form.get_attribute("action"),
                        "method": await form.get_attribute("method"),
                    }
                    state["forms"].append(form_data)
            
            # 表示されている入力要素のカウント
            input_selectors = ["input:visible", "textarea:visible", "select:visible"]
            for selector in input_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    state["visible_inputs"] += len(elements)
                except Exception:
                    pass
            
            return state
            
        except Exception as e:
            logger.debug(f"ページ状態キャプチャ中にエラー: {e}")
            return {}

    async def wait_for_submission_response_with_mutation(self) -> Dict[str, Any]:
        """送信レスポンスとDOM変更を待機"""
        if not self.page:
            return {"significant_changes": 0, "total_mutations": 0}
        
        try:
            # DOM変更の監視を開始
            mutation_result = await self._monitor_dynamic_changes(timeout_seconds=10)
            
            # 追加の待機
            await asyncio.sleep(2)
            
            return mutation_result
            
        except Exception as e:
            logger.error(f"送信レスポンス待機中にエラー: {e}")
            return {"significant_changes": 0, "total_mutations": 0}

    async def _monitor_dynamic_changes(self, timeout_seconds: Optional[float] = None) -> Dict[str, Any]:
        """動的な変更を監視"""
        if not self.page:
            return {"significant_changes": 0, "total_mutations": 0}
        
        timeout_seconds = timeout_seconds or 10
        start_time = time.time()
        
        mutation_count = 0
        significant_changes = 0
        
        try:
            # JavaScript によるDOM監視
            await self.page.evaluate("""
                () => {
                    window.__mutationCount = 0;
                    window.__significantChanges = 0;
                    
                    const observer = new MutationObserver((mutations) => {
                        window.__mutationCount += mutations.length;
                        
                        mutations.forEach(mutation => {
                            // 重要な変更の検出
                            if (mutation.type === 'childList' && mutation.addedNodes.length > 0) {
                                mutation.addedNodes.forEach(node => {
                                    if (node.nodeType === Node.ELEMENT_NODE) {
                                        const tagName = node.tagName ? node.tagName.toLowerCase() : '';
                                        if (['div', 'section', 'article', 'form', 'main'].includes(tagName)) {
                                            window.__significantChanges++;
                                        }
                                    }
                                });
                            }
                        });
                    });
                    
                    observer.observe(document.body, {
                        childList: true,
                        subtree: true,
                        attributes: true,
                        attributeOldValue: true
                    });
                    
                    window.__mutationObserver = observer;
                }
            """)
            
            # タイムアウトまで待機
            while time.time() - start_time < timeout_seconds:
                await asyncio.sleep(0.5)
                
                # 変更数を取得
                result = await self.page.evaluate("""
                    () => ({
                        mutations: window.__mutationCount || 0,
                        significant: window.__significantChanges || 0
                    })
                """)
                
                mutation_count = result.get("mutations", 0)
                significant_changes = result.get("significant", 0)
                
                # 十分な変更が検出されたら早期終了
                if significant_changes >= 5 or mutation_count >= 20:
                    break
            
            # 監視を停止
            await self.page.evaluate("""
                () => {
                    if (window.__mutationObserver) {
                        window.__mutationObserver.disconnect();
                    }
                }
            """)
            
        except Exception as e:
            logger.debug(f"動的変更監視中にエラー: {e}")
        
        return {
            "total_mutations": mutation_count,
            "significant_changes": significant_changes,
            "monitoring_duration": time.time() - start_time,
        }

    def process_submission_result(
        self,
        judgment_results: Dict[str, bool],
        mutation_result: Dict[str, Any],
        response_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """送信結果を処理して最終的な結果を生成"""
        # 成功判定のスコア計算
        success_score = 0
        if not judgment_results.get("failure_keywords"):
            success_score += 25
        if judgment_results.get("success_keywords"):
            success_score += 25
        if judgment_results.get("http_response"):
            success_score += 25
        if judgment_results.get("state_changes"):
            success_score += 25
        
        # 最終判定
        is_success = success_score >= 50
        
        # 結果の構築
        result = {
            "success": is_success,
            "success_score": success_score,
            "judgment_details": judgment_results,
            "mutation_summary": {
                "total_mutations": mutation_result.get("total_mutations", 0),
                "significant_changes": mutation_result.get("significant_changes", 0),
            },
            "response_summary": {
                "status_codes": response_data.get("status_codes", []),
                "network_calls": len(response_data.get("network_calls", [])),
            },
            "timestamp": datetime.now(timezone(timedelta(hours=9))).isoformat(),
        }
        
        return result

    def log_submission_results(self, response_data: Dict[str, Any]) -> None:
        """送信結果のログ出力"""
        if response_data.get("status_codes"):
            logger.info(f"HTTPステータスコード: {response_data['status_codes']}")
        
        if response_data.get("network_calls"):
            logger.info(f"ネットワーク呼び出し数: {len(response_data['network_calls'])}")