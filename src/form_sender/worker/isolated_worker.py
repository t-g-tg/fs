"""
独立型ワーカープロセス（Supabase除去版）

マルチプロセス環境でフォーム送信処理を実行する独立型ワーカー
データベースアクセスは行わず、プロセス間通信のみで動作
"""

import asyncio
import json
import logging
import multiprocessing as mp
import os
import queue
import re
import signal
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, List

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError, Page

# 設定とユーティリティ
from config.manager import (
    get_form_sender_config,
    get_retry_config_for,
    get_worker_config,
    get_choice_priority_config,
)
from config.manager import get_privacy_consent_config
from ..detection.bot_detector import BotDetectionSystem
from ..detection.constants import BOT_DETECTION_KEYWORDS
from ..detection.pattern_matcher import FormDetectionPatternMatcher
from ..template.company_processor import CompanyPlaceholderAnalyzer
from ..control.recovery_manager import AutoRecoveryManager
from ..communication.queue_manager import QueueManager, WorkerResult, WorkerTask, ResultStatus, TaskType
from .input_handler import FormInputHandler
from ..utils.error_classifier import ErrorClassifier
from ..analyzer.rule_based_analyzer import RuleBasedAnalyzer
from ..analyzer.success_judge import SuccessJudge
from ..utils.data_mapper import ClientDataMapper
from ..browser.manager import BrowserManager
from ..utils.button_config import (
    get_button_keywords_config,
    get_fallback_selectors,
    get_exclude_keywords,
)
from ..utils.privacy_consent_handler import PrivacyConsentHandler
from ..security.log_sanitizer import LogSanitizer


logger = logging.getLogger(__name__)


class IsolatedFormWorker:
    """独立型フォーム送信ワーカー（プロセス分離版）"""

    def __init__(self, worker_id: int, headless: bool = None):
        """
        初期化

        Args:
            worker_id: ワーカープロセスID
            headless: ブラウザヘッドレスモード (None=環境自動判定, True=強制ヘッドレス, False=強制GUI)
        """
        self.worker_id = worker_id
        self.is_running = False
        self.should_stop = False
        
        # 設定読み込み
        try:
            form_sender_config = get_form_sender_config()
            worker_config = get_worker_config()
            self.config = {
                "timeout_settings": form_sender_config.get("timeout_settings", {}),
                "text_processing": form_sender_config.get("text_processing", {}),
                "state_change_judgment": form_sender_config.get("state_change_judgment", {}),
                "worker_config": worker_config,
            }
        except Exception as e:
            logger.warning(f"設定ファイル読み込み失敗、デフォルト値使用: {e}")
            self.config = {"timeout_settings": {}}

        # Playwright関連
        self.browser_manager = BrowserManager(worker_id, headless, self.config)
        self.page: Optional[Page] = None

        # フォーム処理コンポーネント
        self.bot_detector = BotDetectionSystem()
        self.recovery_manager = AutoRecoveryManager()
        self.pattern_matcher = FormDetectionPatternMatcher()

        # パフォーマンス最適化
        self._selector_cache = {}
        self._cache_max_age = 30  # 秒
        self._last_cache_clear = time.time()

        # 統計情報
        self.stats = {"processed": 0, "success": 0, "failed": 0, "errors": 0, "start_time": time.time()}

        logger.info(f"IsolatedFormWorker {worker_id} initialized")
        # 文字列サニタイザ（機微情報抑止用）
        try:
            self._content_sanitizer = LogSanitizer()
        except Exception:
            self._content_sanitizer = None

    def _evaluate_prohibition_detection(self, sp: Dict[str, Any]) -> (bool, Dict[str, Any]):
        """営業禁止検出の早期中断要否を判定（設定値で閾値を調整可能）。"""
        if not isinstance(sp, dict):
            return False, {
                'level': 'none',
                'confidence_level': 'none',
                'confidence_score': 0.0,
                'matches_count': 0,
            }
        matches = sp.get('matches') or []
        level = (sp.get('prohibition_level') or sp.get('detection_method') or 'detected')
        level_l = str(level).lower()
        conf_level = str(sp.get('confidence_level') or '').lower()
        try:
            conf_score = float(sp.get('confidence_score') or 0.0)
        except Exception:
            conf_score = 0.0
        # 0..100 にクランプ
        if conf_score < 0:
            conf_score = 0.0
        if conf_score > 100:
            conf_score = 100.0
        # 件数（summary優先）
        try:
            matches_count = int(sp.get('summary', {}).get('total_matches'))
        except Exception:
            matches_count = len(matches)

        # 設定から閾値を取得（無ければデフォルト）
        try:
            det = (self.config.get('worker_config') or {}).get('detectors', {}).get('prohibition', {})
            lvl_min = str(det.get('early_abort', {}).get('min_level', 'moderate')).lower()
            conf_lvl_min = str(det.get('early_abort', {}).get('min_confidence_level', 'high')).lower()
            score_min = float(det.get('early_abort', {}).get('min_score', 80))
            matches_min = int(det.get('early_abort', {}).get('min_matches', 2))
        except Exception:
            lvl_min, conf_lvl_min, score_min, matches_min = 'moderate', 'high', 80.0, 2

        # 早期中断判定（レベルは序数比較: weak < mild < moderate < strict）
        order = {'weak': 0, 'mild': 1, 'moderate': 2, 'strict': 3}
        lvl_min_idx = order.get(lvl_min, 2)
        level_idx = order.get(level_l, order.get('moderate', 2))

        should_abort = False
        if level_idx >= lvl_min_idx:
            should_abort = True
        elif conf_level == conf_lvl_min or conf_score >= score_min:
            should_abort = True
        elif matches_count >= matches_min:
            should_abort = True

        summary = {
            'level': level,
            'confidence_level': conf_level or None,
            'confidence_score': conf_score if conf_score > 0 else None,
            'matches_count': matches_count,
        }
        return should_abort, summary

    def _determine_http_status(self, response_analysis: Dict[str, Any]) -> Optional[int]:
        """HTTPステータスを優先順位で決定する: 429 > 403 > 5xx > その他（最後）"""
        try:
            errs = response_analysis.get('error_responses') or []
            if not isinstance(errs, list) or not errs:
                return None
            statuses = [e.get('status') for e in errs if isinstance(e, dict) and isinstance(e.get('status'), int)]
            if not statuses:
                return None
            if any(s == 429 for s in statuses):
                return 429
            if any(s == 403 for s in statuses):
                return 403
            for s in statuses:
                if 500 <= s < 600:
                    return s
            return statuses[-1]
        except Exception:
            return None

    def _is_page_valid(self) -> bool:
        """ページが有効かどうかを確認する"""
        if not self.page:
            return False
        try:
            return not self.page.is_closed()
        except Exception:
            return False

    async def initialize(self) -> bool:
        """Playwrightブラウザの初期化"""
        return await self.browser_manager.launch()
    
    async def _check_shutdown_requested(self) -> bool:
        """
        SHUTDOWNタスクが要求されているかチェック
        
        Returns:
            bool: SHUTDOWNが要求されている場合True
        """
        if not hasattr(self, '_task_queue') or self._task_queue is None:
            return False
        
        try:
            # ノンブロッキングでタスクをチェック
            task_data = self._task_queue.get_nowait()
            if task_data.get("task_type") == TaskType.SHUTDOWN.value:
                logger.info(f"Worker {self.worker_id}: SHUTDOWN task detected during processing")
                # SHUTDOWNタスクをキューに戻す（他のチェックでも検出できるように）
                self._task_queue.put(task_data)
                return True
            else:
                # 他のタスクなのでキューに戻す
                self._task_queue.put(task_data)
                return False
        except queue.Empty:
            return False
        except Exception as e:
            logger.warning(f"Worker {self.worker_id}: Error checking shutdown: {e}")
            return False
    
    def _create_shutdown_result(self, record_id: int, start_time: float) -> WorkerResult:
        """
        SHUTDOWN要求時の結果オブジェクトを作成
        
        Args:
            record_id: レコードID
            start_time: 処理開始時間
            
        Returns:
            WorkerResult: SHUTDOWN処理結果
        """
        return WorkerResult(
            task_id="shutdown_requested",
            record_id=record_id,
            status=ResultStatus.WORKER_SHUTDOWN,
            error_type="SHUTDOWN_REQUESTED",
            processing_time=time.time() - start_time,
            timestamp=time.time(),
            data={
                "shutdown_reason": "Test batch size limit reached"
            }
        )

    async def process_company_task(self, task_data: Dict[str, Any], task_queue=None) -> WorkerResult:
        """
        企業処理タスクを実行（SHUTDOWN監視機能付き）

        Args:
            task_data: タスクデータ
            task_queue: タスクキュー（SHUTDOWN監視用、オプション）

        Returns:
            WorkerResult: 処理結果
        """
        # SHUTDOWNタスク監視機能
        self._task_queue = task_queue
        task = WorkerTask.from_dict(task_data)
        start_time = time.time()

        try:
            # 企業データを取得
            company_data = task.company_data
            client_data = task.client_data
            record_id = company_data.get("id")

            logger.info(f"Worker {self.worker_id}: Processing record_id {record_id} using RuleBasedAnalyzer real-time analysis")

            # フォーム送信処理を実行（RuleBasedAnalyzerリアルタイム解析）
            result = await self._process_single_company_isolated(company_data, client_data, task.targeting_id)

            # 処理時間計算
            processing_time = time.time() - start_time

            # 統計更新
            self.stats["processed"] += 1
            if result.get("status") == "success":
                self.stats["success"] += 1
            else:
                self.stats["failed"] += 1

            # WorkerResult作成
            worker_result = WorkerResult(
                task_id=task.task_id,
                worker_id=self.worker_id,
                status=ResultStatus.SUCCESS if result.get("status") == "success" else ResultStatus.FAILED,
                record_id=record_id,
                error_type=result.get("error_type"),
                error_message=result.get("error_message"),
                instruction_valid_updated=result.get("instruction_valid_updated", False),
                bot_protection_detected=result.get("bot_protection_detected", False),
                processing_time=processing_time,
                additional_data=result.get("additional_data") if isinstance(result, dict) else None,
            )

            logger.info(
                f"Worker {self.worker_id}: Company {record_id} completed: {'success' if result.get('status') == 'success' else 'failed'} (time: {processing_time:.2f}s)"
            )
            return worker_result

        except Exception as e:
            self.stats["errors"] += 1
            processing_time = time.time() - start_time

            logger.error(f"Worker {self.worker_id}: Task processing error: {e}")

            # record_idを安全に取得
            try:
                record_id = task.company_data.get("id") if hasattr(task, "company_data") and task.company_data else None
            except:
                record_id = None

            return WorkerResult(
                task_id=task.task_id,
                worker_id=self.worker_id,
                status=ResultStatus.ERROR,
                record_id=record_id,
                error_message=str(e),
                processing_time=processing_time,
            )

    async def _process_single_company_isolated(
        self, company: Dict[str, Any], client_data: Dict[str, Any], targeting_id: int
    ) -> Dict[str, Any]:
        """
        単一企業の処理（独立版 - データベースアクセスなし）
        """
        record_id = company.get("id")
        form_url = company.get("form_url")

        # 基本検証（form_urlのみ必須）
        if not form_url:
            return {
                "record_id": record_id,
                "status": "failed",
                "error_type": "URL",
                "error_message": "Missing form_url",
                "instruction_valid_updated": True,
            }

        # 自動復旧付き処理実行
        return await self._process_with_auto_recovery_isolated(company, client_data, targeting_id)

    async def _process_with_auto_recovery_isolated(
        self, company: Dict[str, Any], client_data: Dict[str, Any], targeting_id: int
    ) -> Dict[str, Any]:
        """自動復旧機能付き企業処理（独立版）"""
        record_id = company.get("id")
        form_url = company.get("form_url")

        # リトライ設定
        try:
            retry_config = get_retry_config_for("form_analysis")
            max_retries = retry_config["max_retries"]
        except Exception as e:
            logger.warning(f"リトライ設定読み込み失敗、デフォルト値(3)使用: {e}")
            max_retries = 3

        retry_count = 0
        start_time = time.time()
        max_processing_time = 30  # 最大処理時間（秒）
        # 全体ウォッチドッグ（1社あたりのハードタイムアウト）
        # まれにブラウザ/ページ操作が戻らずハングするケースを強制ブレークする
        # 優先順: 1) form_sender_multi_process.task_timeout（秒）→ 2) pre_processing_max（ms）→ 3) 180秒
        hard_timeout = None
        try:
            from config.manager import get_worker_config
            _cfg = get_worker_config()
            t = _cfg.get('form_sender_multi_process', {}).get('task_timeout', None)
            if t is not None:
                # 文字列/数値いずれでも安全に解釈し、正値のみ採用
                st = str(t).strip()
                hard_timeout_candidate = int(st)
                if hard_timeout_candidate > 0:
                    hard_timeout = hard_timeout_candidate
        except (ValueError, TypeError, KeyError) as e:
            logger.debug(f"task_timeout config unavailable: {e}")
            hard_timeout = None
        # フォールバック: pre_processing_max(ms) → 秒換算
        if not hard_timeout or hard_timeout <= 0:
            try:
                _ms_val = (self.config or {}).get('timeout_settings', {}).get('pre_processing_max', None)
                if _ms_val is not None:
                    _ms = int(str(_ms_val).strip())
                    if _ms > 0:
                        hard_timeout = max(30, int(_ms / 1000))
            except (ValueError, TypeError, KeyError) as e:
                logger.debug(f"pre_processing_max config unavailable: {e}")
        # 最終フォールバック
        if not hard_timeout or hard_timeout <= 0:
            hard_timeout = 180

        while retry_count <= max_retries:
            try:
                # 内部ステップの asyncio.TimeoutError はそのまま伝播させると
                # 外側の wait_for と区別が付かないため、内側発生分は一旦センチネルに包んで再送出する。
                class _InnerStepTimeoutError(Exception):
                    pass

                async def _core_with_capture():
                    try:
                        return await self._execute_single_company_core_isolated(company, client_data, targeting_id)
                    except asyncio.TimeoutError as ie:
                        # 内部ステップのタイムアウト → 自動復旧判定のため通常エラー経路へ流す
                        raise _InnerStepTimeoutError(str(ie)) from ie

                # 全体ウォッチドッグ。ここでの asyncio.TimeoutError はハードタイムアウトのみ。
                result = await asyncio.wait_for(_core_with_capture(), timeout=hard_timeout)
                
                # 成功時は復旧カウントリセット
                if result.get("status") == "success":
                    self.recovery_manager.reset_recovery_count()

                return result

            except asyncio.TimeoutError:
                logger.error(
                    f"Worker {self.worker_id}: Company {record_id} processing timed out after {hard_timeout}s"
                )
                # ページ/ブラウザのクリーンアップ（状態不整合の可能性が高い）
                try:
                    if self.page:
                        await self.page.close()
                except Exception:
                    pass
                try:
                    await self.browser_manager.close()
                except Exception:
                    pass
                # 次のタスクに備えてブラウザを再起動（失敗しても続行）
                try:
                    await asyncio.sleep(0.5)
                    await self.browser_manager.launch()
                except Exception:
                    logger.warning(
                        f"Worker {self.worker_id}: Browser relaunch failed after timeout"
                    )
                return {
                    "record_id": record_id,
                    "status": "failed",
                    "error_type": "TIMEOUT",
                    "error_message": f"Hard timeout: processing exceeded {hard_timeout}s",
                    "instruction_valid_updated": ErrorClassifier.should_update_instruction_valid("TIMEOUT"),
                }
            except _InnerStepTimeoutError as e_inner:
                # 内部ステップのタイムアウトは従来どおり分類＋自動復旧の経路へ
                error_message = f"Inner step timeout: {str(e_inner)}"
                logger.error(f"Worker {self.worker_id}: Company {record_id} processing error: {error_message}")

                error_context = {
                    "error_location": "company_processing",
                    "error_message": error_message,
                    "page_url": form_url,
                    "is_timeout": True,
                    "is_bot_detected": False,
                }
                error_type = ErrorClassifier.classify_error_type(error_context)
                result = {
                    "record_id": record_id,
                    "status": "failed",
                    "error_type": error_type,
                    "error_message": error_message,
                    "instruction_valid_updated": ErrorClassifier.should_update_instruction_valid(error_type),
                }
                # 復旧可能かチェック
                if ErrorClassifier.is_recoverable_error(error_type, error_message):
                    if self.recovery_manager.can_attempt_recovery():
                        elapsed_time = time.time() - start_time
                        if elapsed_time > max_processing_time:
                            logger.warning(
                                f"Worker {self.worker_id}: Processing time limit exceeded for record_id {record_id}: {elapsed_time:.1f}s"
                            )
                            result["error_type"] = "TIMEOUT"
                            return result
                        logger.info(f"Worker {self.worker_id}: Attempting auto-recovery for record_id {record_id}")
                        self.recovery_manager.mark_recovery_attempt()
                        recovery_success = await self._attempt_recovery_isolated(error_type, error_message)
                        if recovery_success:
                            retry_count += 1
                            logger.info(
                                f"Worker {self.worker_id}: Recovery successful for record_id {record_id}, retrying... (attempt {retry_count}/{max_retries})"
                            )
                            continue
                        else:
                            logger.warning(f"Worker {self.worker_id}: Recovery failed for record_id {record_id}")
                return result
            except Exception as e:
                error_message = str(e)
                logger.error(f"Worker {self.worker_id}: Company {record_id} processing error: {error_message}")

                # エラー分類
                error_context = {
                    "error_location": "company_processing",
                    "error_message": error_message,
                    "page_url": form_url,
                    "is_timeout": "timeout" in error_message.lower(),
                    "is_bot_detected": any(
                        keyword in error_message.lower() for keyword in ["recaptcha", "cloudflare", "bot"]
                    ),
                }

                error_type = ErrorClassifier.classify_error_type(error_context)

                result = {
                    "record_id": record_id,
                    "status": "failed",
                    "error_type": error_type,
                    "error_message": error_message,
                    "instruction_valid_updated": ErrorClassifier.should_update_instruction_valid(error_type),
                }

                # Bot検知の場合
                if "bot" in error_message.lower() or "recaptcha" in error_message.lower():
                    result["bot_protection_detected"] = True

                # 分類補助用の軽量コンテキストを付与
                try:
                    classify_ctx = {
                        "stage": "company_processing",
                        "is_timeout": bool(error_context.get("is_timeout")),
                        "is_bot_detected": bool(error_context.get("is_bot_detected")),
                        "primary_error_type": error_type,
                    }
                    result["additional_data"] = {"classify_context": classify_ctx}
                except Exception:
                    pass

                # 復旧可能かチェック
                if ErrorClassifier.is_recoverable_error(error_type, error_message):
                    if self.recovery_manager.can_attempt_recovery():
                        # 時間制限チェック
                        elapsed_time = time.time() - start_time
                        if elapsed_time > max_processing_time:
                            logger.warning(
                                f"Worker {self.worker_id}: Processing time limit exceeded for record_id {record_id}: {elapsed_time:.1f}s"
                            )
                            result["error_type"] = "TIMEOUT"
                            return result

                        # 復旧処理実行
                        logger.info(f"Worker {self.worker_id}: Attempting auto-recovery for record_id {record_id}")
                        self.recovery_manager.mark_recovery_attempt()

                        recovery_success = await self._attempt_recovery_isolated(error_type, error_message)
                        if recovery_success:
                            retry_count += 1
                            logger.info(
                                f"Worker {self.worker_id}: Recovery successful for record_id {record_id}, retrying... (attempt {retry_count}/{max_retries})"
                            )
                            continue
                        else:
                            logger.warning(f"Worker {self.worker_id}: Recovery failed for record_id {record_id}")

                return result

        # 最大リトライ数到達
        return {
            "record_id": record_id,
            "status": "failed",
            "error_type": "RETRY_EXCEEDED",
            "error_message": f"Max retries ({max_retries}) exceeded",
        }

    async def _execute_single_company_core_isolated(
        self, company: Dict[str, Any], client_data: Dict[str, Any], targeting_id: int
    ) -> Dict[str, Any]:
        """企業処理のコア実装（独立版）"""
        record_id = company.get("id")
        form_url = company.get("form_url")

        try:
            # Step 1: 指示書処理（常にルールベース）
            expanded_instruction = await self._process_instruction_isolated(company, client_data)
            if "error" in expanded_instruction:
                return expanded_instruction

            # Step 2: ブラウザページ初期化とアクセス（リトライ付き・GUI優先）
            max_retries = 2
            for attempt in range(max_retries + 1):
                try:
                    self.page = await self.browser_manager.create_new_page(form_url)
                    logger.info(f"Worker {self.worker_id}: Page access successful on attempt {attempt + 1}")
                    break
                except Exception as e:
                    error_msg = f"Page access error: {str(e)}"
                    # 接続系の一時障害（GUIで起きやすい）は、同一モードでの再起動で対処
                    conn_issue = any(k in error_msg for k in ["Target closed", "Connection closed", "Browser connection lost"]) or \
                                 any(k in error_msg.lower() for k in ["target page", "connection closed", "browser connection lost"])
                    if attempt < max_retries and conn_issue:
                        try:
                            logger.warning(f"Worker {self.worker_id}: Browser connection issue on attempt {attempt + 1}, reinitializing browser (same mode)...")
                            await self.browser_manager.close()
                            await asyncio.sleep(1.0)
                            if await self.browser_manager.launch():
                                logger.info(f"Worker {self.worker_id}: Browser reinitialized successfully")
                                continue
                            else:
                                logger.error(f"Worker {self.worker_id}: Browser reinitialization failed")
                        except Exception as reinit_error:
                            logger.error(f"Worker {self.worker_id}: Browser reinitialization error: {reinit_error}")
                    
                    # 最終試行でも失敗した場合
                    if attempt >= max_retries:
                        error_context = {
                            'error_message': error_msg,
                            'error_location': 'page_access',
                            'page_url': form_url,
                            'is_timeout': 'timeout' in error_msg.lower(),
                            'is_bot_detected': any(k in error_msg.lower() for k in BOT_DETECTION_KEYWORDS),
                        }
                        error_type = ErrorClassifier.classify_error_type(error_context)
                        return {
                            "error": True,
                            "record_id": record_id,
                            "status": "failed",
                            "error_type": error_type,
                            "error_message": f"Failed after {max_retries + 1} attempts: {error_msg}",
                            "instruction_valid_updated": ErrorClassifier.should_update_instruction_valid(error_type),
                        }

            # Step 3: Bot検知チェック
            bot_check_result = await self._check_bot_detection_isolated(record_id)
            if "error" in bot_check_result:
                return bot_check_result

            # Step 4: フォーム入力実行（RuleBasedAnalyzer統一）
            form_input_result = await self._execute_form_input_isolated(
                None, record_id, True, company, client_data
            )
            if "error" in form_input_result:
                return form_input_result

            # Step 5: フォーム送信と結果判定
            submit_result = await self._execute_form_submission_isolated(
                None, record_id, True
            )

            return submit_result

        finally:
            if self.page:
                try:
                    await self.page.close()
                except Exception as e:
                    logger.warning(f"Failed to close page for record_id {record_id}: {e}")
                self.page = None

    # ===== small helpers =====
    def _get_dom_context(self):
        """現在のDOMコンテキスト（iframe対応）を取得"""
        try:
            dom_ctx = getattr(self, '_dom_context', None)
            return dom_ctx or self.page
        except Exception:
            return self.page

    async def _process_instruction_isolated(
        self, company: Dict[str, Any], client_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """ルールベース解析によるフォーム処理"""
        record_id = company.get("id")
        logger.info(f"Worker {self.worker_id}: Using RuleBasedAnalyzer for record_id {record_id}")
        return {
            "status": "success",
            "instruction": None,
            "record_id": record_id,
            "use_rule_based": True,
        }

    async def shutdown(self):
        """ワーカーをシャットダウンする"""
        if not self.is_running:
            return

        logger.info(f"Worker {self.worker_id}: Shutting down...")
        self.is_running = False
        self.should_stop = True

        # ブラウザを閉じる
        await self.browser_manager.close()

        end_time = time.time()
        uptime = end_time - self.stats["start_time"]
        logger.info(f"Worker {self.worker_id} shutdown complete. Uptime: {uptime:.2f}s")
        logger.info(f"Final Stats: {self.stats}")

    async def _ensure_dynamic_form_ready(self) -> None:
        """HubSpot等の動的フォーム読み込みに備えた段階的待機（軽量版）"""
        try:
            # 既にフォームがあれば何もしない
            form_count = await self.page.evaluate("document.querySelectorAll('form').length")
            has_hubspot = await self.page.evaluate(
                """
                () => Array.from(document.querySelectorAll('script')).some(s => s.src && (s.src.includes('hsforms.net') || s.src.includes('hubspot')))
                """
            )
            if form_count > 0 and not has_hubspot:
                return

            # 最大3段階の待機と再チェック
            for attempt in range(3):
                try:
                    await self.page.wait_for_load_state('networkidle', timeout=8000)
                except Exception:
                    pass

                form_count = await self.page.evaluate("document.querySelectorAll('form').length")
                if form_count > 0:
                    return

                # HubSpot用の追加シグナル
                hubspot_selectors = [
                    '.hbspt-form form',
                    'form[id^="hsForm_"]',
                    'div[data-hs-forms-root] form',
                    '.hs-form',
                    'div[id^="hbspt-form-"] form'
                ]
                for sel in hubspot_selectors:
                    try:
                        cnt = await self.page.evaluate(f"document.querySelectorAll('{sel}').length")
                        if cnt > 0:
                            return
                    except Exception:
                        continue

                # HubSpot iframeのロードを待機
                try:
                    iframe_cnt = await self.page.evaluate("document.querySelectorAll('iframe.hs-form-iframe').length")
                    if iframe_cnt > 0:
                        await asyncio.sleep(3)
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"Worker {self.worker_id}: Dynamic form readiness check warning: {e}")

    async def _select_target_frame_for_analysis(self) -> Optional[Page]:
        """メインページにフォームが無い場合に、フォームを含むiframeを選択"""
        try:
            frames = self.page.frames
            for frame in frames:
                if frame == self.page.main_frame:
                    continue
                try:
                    forms = await frame.query_selector_all('form')
                    if len(forms) > 0:
                        return frame
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"Worker {self.worker_id}: iframe selection warning: {e}")
        return None

    async def _check_bot_detection_isolated(self, record_id: int) -> Dict[str, Any]:
        """Bot検知チェック（独立版）"""
        try:
            is_bot_detected, bot_type = await self.bot_detector.detect_bot_protection(self.page)

            if is_bot_detected:
                error_msg = f"Bot protection detected: {bot_type}" if bot_type else "Bot protection detected"
                logger.warning(f"Worker {self.worker_id}: {error_msg} for record_id {record_id}")
                return {
                    "error": True,
                    "record_id": record_id,
                    "status": "failed",
                    "error_type": "BOT_DETECTED",
                    "error_message": error_msg,
                    "bot_protection_detected": True,
                }

            return {"status": "success"}

        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Bot detection check error: {e}")
            return {"status": "success"}  # Bot検知エラーは処理続行

    async def _execute_form_input_isolated(
        self,
        instruction: Optional[Dict[str, Any]],  # 互換のため受け取るが未使用
        record_id: int,
        use_rule_based: bool = True,
        company: Optional[Dict[str, Any]] = None,
        client_data: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """フォーム入力実行（RuleBasedAnalyzerのみ使用）"""
        try:
            # RuleBasedAnalyzerで実行
            return await self._execute_rule_based_form_input(record_id, company, client_data)
        except Exception as e:
            error_msg = f"Form input error: {str(e)}"
            logger.error(f"Worker {self.worker_id}: {error_msg}")
            error_type = ErrorClassifier.classify_form_input_error(error_message=error_msg)
            return {
                "error": True,
                "record_id": record_id,
                "status": "failed",
                "error_type": error_type,
                "error_message": error_msg,
                "instruction_valid_updated": ErrorClassifier.should_update_instruction_valid(error_type),
            }

    async def _execute_rule_based_form_input(
        self, record_id: int, company: Dict[str, Any], client_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """ルールベースフォーム入力実行"""
        try:
            
            
            # 追加入力用にクライアントデータと初回入力セレクタを保持
            self._current_client_data = client_data
            self._initial_filled_selectors = set()
            logger.info(f"Worker {self.worker_id}: Starting rule-based form analysis for record_id {record_id}")
            await self._ensure_dynamic_form_ready()

            target_frame = await self._select_target_frame_for_analysis()
            # Analyzerと入力・送信はいずれも同一DOMコンテキストで実行する（iframe対応）
            self._dom_context = target_frame or self.page

            analyzer = RuleBasedAnalyzer(self._dom_context)
            analysis_result = await analyzer.analyze_form(client_data)
            self._current_analysis_result = analysis_result

            if not analysis_result or not analysis_result.get('success', True):
                error_message = analysis_result.get('error', 'Form analysis failed')
                logger.warning(f"Worker {self.worker_id}: Form analysis failed: {error_message}")
                return {"error": True, "record_id": record_id, "status": "failed", "error_type": "ANALYSIS_FAILED", "error_message": error_message}

            # 営業禁止検出（Analyzerの結果を用いた早期中断）
            try:
                sp = analysis_result.get('sales_prohibition') or {}
                has_prohibition = bool(sp.get('has_prohibition') or sp.get('prohibition_detected'))
                if has_prohibition:
                    should_abort, summary = self._evaluate_prohibition_detection(sp)
                    if should_abort:
                        logger.warning(
                            f"Worker {self.worker_id}: Sales prohibition detected (record_id={record_id}, matches={summary.get('matches_count')}, level={summary.get('level')}, conf={summary.get('confidence_level')}/{summary.get('confidence_score')})"
                        )
                        return {
                            "error": True,
                            "record_id": record_id,
                            "status": "failed",
                            "error_type": "PROHIBITION_DETECTED",
                            "error_message": "Sales prohibition detected",
                            "additional_data": {
                                "classify_context": {
                                    "stage": "pre_submission_check",
                                    "primary_error_type": "PROHIBITION_DETECTED",
                                    "is_bot_detected": False,
                                },
                                "prohibition_summary": {
                                    "detected": True,
                                    **summary,
                                    "detection_source": "RuleBasedAnalyzer",
                                },
                            },
                        }
                    else:
                        logger.info(
                            f"Worker {self.worker_id}: sales prohibition signals detected but below early-abort threshold (record_id={record_id}, level={summary.get('level')}, conf={summary.get('confidence_level')}/{summary.get('confidence_score')}, matches={summary.get('matches_count')})"
                        )
            except Exception as _e:
                logger.debug(f"Worker {self.worker_id}: sales prohibition early-check skipped: {_e}")

            # 送信前バリデーション: 『お問い合わせ本文（message）』がマッピングされていない場合は送信回避
            try:
                vr = analysis_result.get('validation_result') if isinstance(analysis_result, dict) else None
                fm = analysis_result.get('field_mapping', {}) if isinstance(analysis_result, dict) else {}
                se = analysis_result.get('special_elements') if isinstance(analysis_result, dict) else None
                # DOMにtextareaが存在するか（粗いヒューリスティクス）
                dom_textareas_count = 0
                try:
                    if isinstance(se, dict) and isinstance(se.get('textareas'), list):
                        dom_textareas_count = len(se.get('textareas') or [])
                except Exception:
                    dom_textareas_count = 0
                issues = (vr or {}).get('issues', []) if isinstance(vr, dict) else []
                # AnalysisValidator は contact_form で 'お問い合わせ本文' 欠落を issues に追加する
                message_missing = any("Required field 'お問い合わせ本文' is missing" in str(i) for i in (issues or []))
                # contact_form の厳格判定は AnalysisValidator に委譲する
                if message_missing:
                    # 分岐: DOMにメッセージ欄が無い（textarea不在）→ NO_MESSAGE_AREA
                    #      DOMに何らかのtextareaがあるのに未マッピング → MAPPING（アルゴリズム起因の可能性）
                    detected_type = 'NO_MESSAGE_AREA' if dom_textareas_count == 0 else 'MAPPING'
                    logger.warning(
                        f"Worker {self.worker_id}: Message field missing/not mapped; type={detected_type}, skip submission for record_id {record_id}"
                    )
                    return {
                        "error": True,
                        "record_id": record_id,
                        "status": "failed",
                        # ランナーの分類器と整合する代表コードを採用（詳細はadditional_dataに格納）
                        "error_type": detected_type,
                        "error_message": (
                            "No message area (textarea) found in form"
                            if detected_type == 'NO_MESSAGE_AREA'
                            else "Required field 'お問い合わせ本文' is missing"
                        ),
                        "additional_data": {
                            "classify_context": {
                                "stage": "pre_submission_validation",
                                "primary_error_type": detected_type,
                                "is_bot_detected": False,
                            },
                            # 解析時点の簡易コンテキスト（安全な範囲のみ）
                            "validation_issues": issues[:10] if isinstance(issues, list) else [],
                            "field_mapping_keys": list((fm or {}).keys())[:30],
                            "detected_dom_textareas_count": dom_textareas_count,
                        },
                    }
            except Exception as _val_e:
                # バリデーションで問題があっても送信強行はしない方針のため、ここで例外は握りつぶし
                logger.debug(f"Worker {self.worker_id}: pre-submission validation skipped due to error: {_val_e}")

            # 1) まずは Analyzer の input_assignments（自動処理含む）を優先して入力
            input_assignments = analysis_result.get('input_assignments', {})
            form_mapping = analysis_result.get('field_mapping', {})
            if not input_assignments and not form_mapping:
                logger.warning(f"Worker {self.worker_id}: No form fields found during rule-based analysis")
                return {"error": True, "record_id": record_id, "status": "failed", "error_type": "NO_FORM_FOUND", "error_message": "No form fields detected"}

            await self._perform_dynamic_content_loading()

            post_delay = 200
            try:
                post_delay = int(self.config.get('timeout_settings', {}).get('post_input_delay_ms', 200))
            except Exception:
                post_delay = 200
            input_handler = FormInputHandler(self._dom_context, self.worker_id, post_delay)
            filled_fields = 0

            if input_assignments:
                logger.info(f"Worker {self.worker_id}: Processing {len(input_assignments)} assigned inputs (mapping + auto-handled)")
                for field_name, assign in input_assignments.items():
                    if await self._check_shutdown_requested():
                        return {"error": True, "record_id": record_id, "status": "cancelled", "error_type": "SHUTDOWN_REQUESTED"}
                    try:
                        selector = assign.get('selector')
                        input_type = assign.get('input_type', 'text')
                        value = assign.get('value', '')
                        # 入力ハンドラは field_info から selector/type を参照するため整形
                        field_info = {
                            'selector': selector,
                            'input_type': input_type,
                            'type': input_type,
                            'auto_action': assign.get('auto_action'),
                            'selected_index': assign.get('selected_index')
                        }
                        # チェックボックス/ラジオは値が空でも操作対象
                        if (value is not None and str(value).strip() != '') or input_type in ['checkbox', 'radio']:
                            success = await input_handler.fill_rule_based_field(field_name, field_info, value)
                            if success:
                                filled_fields += 1
                                try:
                                    if selector:
                                        self._initial_filled_selectors.add(selector)
                                except Exception:
                                    pass
                            else:
                                logger.warning(f"Worker {self.worker_id}: Field fill verification failed - {field_name}")
                        else:
                            logger.warning(f"Worker {self.worker_id}: No valid value for field {field_name} - skipping")
                    except Exception as field_error:
                        logger.error(f"Worker {self.worker_id}: Error filling assigned field {field_name}: {field_error}")
                        continue
            else:
                # フォールバック: 旧ロジック（フィールド名から動的に値生成）
                logger.info(f"Worker {self.worker_id}: Processing {len(form_mapping)} form fields")
                for field_name, field_info in form_mapping.items():
                    if await self._check_shutdown_requested():
                        return {"error": True, "record_id": record_id, "status": "cancelled", "error_type": "SHUTDOWN_REQUESTED"}
                    try:
                        value = ClientDataMapper.get_value_for_rule_based_field(field_name, client_data)
                        if value is not None and str(value).strip():
                            success = await input_handler.fill_rule_based_field(field_name, field_info, value)
                            if success:
                                filled_fields += 1
                                try:
                                    selector = field_info.get('selector')
                                    if selector:
                                        self._initial_filled_selectors.add(selector)
                                except Exception:
                                    pass
                            else:
                                logger.warning(f"Worker {self.worker_id}: Field fill verification failed - {field_name}")
                        else:
                            logger.warning(f"Worker {self.worker_id}: No valid value for field {field_name} - skipping")
                    except Exception as field_error:
                        logger.error(f"Worker {self.worker_id}: Error filling rule-based field {field_name}: {field_error}")
                        continue

            if filled_fields == 0:
                return {"error": True, "record_id": record_id, "status": "failed", "error_type": "NO_FIELDS_FILLED", "error_message": "No fields were successfully filled"}

            # 送信ボタンの有効化が遅延するケースに備えて、わずかに待機（計画: 200ms）
            try:
                post_delay = 200
                try:
                    post_delay = int(self.config.get('timeout_settings', {}).get('post_input_delay_ms', 200))
                except Exception:
                    post_delay = 200
                await self.page.wait_for_timeout(post_delay)
            except Exception:
                pass

            logger.info(f"Worker {self.worker_id}: All rule-based form fields completed successfully ({filled_fields} fields)")
            return {"status": "success", "record_id": record_id}

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Worker {self.worker_id}: Rule-based form input error: {error_msg}")
            return {"error": True, "record_id": record_id, "status": "failed", "error_type": "RULE_BASED_ERROR", "error_message": error_msg}

    async def _submit_rule_based_form(self) -> Dict[str, Any]:
        """ルールベースフォーム送信（analysis_result活用版）"""
        try:
            # ページ状態の確認
            if not self._is_page_valid():
                return {
                    "success": False,
                    "error_message": "Page is not available or closed",
                    "has_url_change": False,
                    "page_content": "",
                    "submit_selector": ""
                }
            # 分析結果からsubmit_buttons情報を取得
            submit_buttons = []
            if hasattr(self, '_current_analysis_result') and self._current_analysis_result:
                submit_buttons = self._current_analysis_result.get('submit_buttons', [])
                logger.debug(f"Worker {self.worker_id}: Found {len(submit_buttons)} submit buttons from analysis result")
            
            # 分析結果のsubmit_buttons情報を最優先で使用
            submit_selectors = []
            if submit_buttons:
                for button_info in submit_buttons:
                    selector = button_info.get('selector')
                    if selector:
                        submit_selectors.append(selector)
                        logger.debug(f"Worker {self.worker_id}: Using analyzed submit button - selector: {selector}, text: '{button_info.get('text', '')}'")
            
            # フォールバック: 設定駆動の動的検索
            if not submit_selectors:
                logger.debug(f"Worker {self.worker_id}: No submit buttons from analysis, using fallback selectors")
                keywords_cfg = get_button_keywords_config()
                fallback_cfg = get_fallback_selectors()

                submit_selectors = []
                # 1) ベース
                submit_selectors.extend(fallback_cfg.get("primary", []))
                # 2) キーワード（primary/secondary/confirmation を全て候補に上げる）
                keyset = set(keywords_cfg.get("primary", [])) | set(keywords_cfg.get("secondary", [])) | set(
                    keywords_cfg.get("confirmation", [])
                )
                for k in keyset:
                    k_escaped = k.replace('"', '\\"')
                    submit_selectors.append(f'button:has-text("{k_escaped}")')
                    submit_selectors.append(f'[role="button"]:has-text("{k_escaped}")')
                    submit_selectors.append(f'input[value*="{k_escaped}"]')
                # 3) セカンダリ/属性系
                submit_selectors.extend(fallback_cfg.get("secondary", []))
                submit_selectors.extend(fallback_cfg.get("by_attributes", []))
            
            submit_element = None
            used_selector = ""
            for selector in submit_selectors:
                try:
                    dom_ctx = getattr(self, '_dom_context', self.page)
                    submit_element = await dom_ctx.query_selector(selector)
                    if submit_element:
                        used_selector = selector
                        # 送信ボタンの詳細情報を取得
                        try:
                            button_text = await submit_element.inner_text()
                            button_value = await submit_element.get_attribute("value")
                            button_type = await submit_element.get_attribute("type")
                            is_visible = await submit_element.is_visible()
                            is_enabled = await submit_element.is_enabled()
                            
                            logger.debug(f"Worker {self.worker_id}: Submit button details:")
                            logger.debug(f"  - Selector: {selector}")
                            logger.debug(f"  - Text: '{button_text}'")
                            logger.debug(f"  - Value: '{button_value}'")
                            logger.debug(f"  - Type: '{button_type}'")
                            logger.debug(f"  - Visible: {is_visible}")
                            logger.debug(f"  - Enabled: {is_enabled}")
                            
                            # 除外語（戻る/キャンセル/リセット/検索等）の除外
                            merged_text = (button_text or button_value or "").strip()
                            if merged_text:
                                low = merged_text.lower()
                                if any(x in low for x in [k.lower() for k in get_exclude_keywords()]):
                                    logger.debug(
                                        f"Worker {self.worker_id}: Excluded button by keyword: '{merged_text[:20]}'"
                                    )
                                    # 次の候補へ
                                    continue

                            # ボタンタイプを判定（確認ボタンか送信ボタンか）
                            element_text = merged_text
                            button_category = await self._determine_button_type(element_text)
                            logger.debug(f"Worker {self.worker_id}: Button category determined as: {button_category}")
                            
                            # ボタンが無効なら短時間だけ有効化を待機（必須入力の反映待ち）
                            if not is_enabled:
                                try:
                                    await self.page.wait_for_function(
                                        "(el) => el && !el.disabled && el.getAttribute('aria-disabled') !== 'true'",
                                        submit_element,
                                        timeout=7000
                                    )
                                    is_enabled = await submit_element.is_enabled()
                                    logger.debug(f"Worker {self.worker_id}: Submit button enabled after wait: {is_enabled}")
                                except Exception:
                                    logger.warning(f"Worker {self.worker_id}: Submit button remained disabled before click")
                        
                        except Exception as detail_error:
                            logger.warning(f"Worker {self.worker_id}: Could not get button details: {detail_error}")
                            button_category = "unknown"
                        
                        logger.debug(f"Worker {self.worker_id}: Found submit button with selector: {selector}")
                        break
                except Exception:
                    continue
            
            if not submit_element:
                logger.warning(f"Worker {self.worker_id}: No submit button found with rule-based search")
                # 付加情報: 可能ならページ内容を短く取得（Bot検出補助）
                page_snippet = ""
                try:
                    dom_ctx = getattr(self, '_dom_context', self.page)
                    # 大きなページでも安全に先頭のみ取得（ブラウザ側で切り詰め）
                    page_snippet = await asyncio.wait_for(
                        dom_ctx.evaluate("document.documentElement.outerHTML.slice(0, 1000)"),
                        timeout=5,
                    )
                except Exception as e:
                    logger.debug(f"Worker {self.worker_id}: page snippet acquisition skipped: {e}")

                # Bot保護（reCAPTCHA/Cloudflare等）の厳格検知を一度試す
                try:
                    is_bot_detected, bot_type = await self.bot_detector.detect_bot_protection(getattr(self, '_dom_context', self.page))
                except Exception as e:
                    logger.warning(f"Worker {self.worker_id}: Bot detection check failed in no-submit path: {e}")
                    is_bot_detected, bot_type = (False, None)

                if is_bot_detected:
                    err = f"Bot protection detected (no submit found): {bot_type}" if bot_type else "Bot protection detected (no submit found)"
                    return {
                        "success": False,
                        "error_message": err,
                        "has_url_change": False,
                        "page_content": page_snippet,
                        "submit_selector": "",
                        "bot_protection_detected": True,
                    }

                return {
                    "success": False,
                    "error_message": "Submit button not found",
                    "has_url_change": False,
                    "page_content": page_snippet,
                    "submit_selector": ""
                }
            
            # 送信前のURLを記録
            dom_ctx = getattr(self, '_dom_context', self.page)
            pre_submit_url = dom_ctx.url
            logger.debug(f"Worker {self.worker_id}: Pre-submit URL: ***URL_REDACTED***")
            # SuccessJudge 初期化（送信前に実施）
            try:
                sj = SuccessJudge(dom_ctx)
                await sj.initialize_before_submission()
            except Exception as e:
                logger.warning(f"Worker {self.worker_id}: SuccessJudge pre-initialize failed: {e}")
            
            # 送信ボタンが無効のままなら送信を中止
            try:
                if submit_element and not await submit_element.is_enabled():
                    # reCAPTCHA などの Bot 保護がある場合は強制有効化を行わない
                    guard_present = False
                    try:
                        guard_present = bool(
                            await dom_ctx.query_selector('.g-recaptcha, .grecaptcha-badge, [name="g-recaptcha-response"]')
                        )
                    except Exception:
                        guard_present = False
                    if not guard_present:
                        # 最終フォールバック: disabled属性を外してみる（フロント側のUIバグ回避）
                        logger.warning(f"Worker {self.worker_id}: Submit button is disabled; trying to force-enable")
                        try:
                            await submit_element.evaluate(
                                "el => { el.disabled = false; el.removeAttribute('disabled'); el.classList.remove('disabled'); }"
                            )
                            await asyncio.sleep(0.2)
                        except Exception as e:
                            logger.debug(f"Worker {self.worker_id}: Force-enable evaluate failed: {e}")
                    # 再確認
                    try:
                        if not await submit_element.is_enabled():
                            logger.warning(f"Worker {self.worker_id}: Submit button still disabled; aborting click")
                            # disabled で送信不能な場合、Bot保護を確認（reCAPTCHA等）
                            try:
                                is_bot, bot_type = await self.bot_detector.detect_bot_protection(dom_ctx)
                            except Exception:
                                is_bot, bot_type = (False, None)
                            return {
                                "success": False,
                                "error_message": "Submit button disabled",
                                "has_url_change": False,
                                "page_content": "",
                                "submit_selector": used_selector,
                                "bot_protection_detected": bool(is_bot)
                            }
                    except Exception:
                        logger.warning(f"Worker {self.worker_id}: Submit button state re-check failed; aborting")
                        return {
                            "success": False,
                            "error_message": "Submit button disabled",
                            "has_url_change": False,
                            "page_content": "",
                            "submit_selector": used_selector
                        }
            except Exception:
                pass

            # クリック直前の状態安定化（race条件緩和）
            try:
                await dom_ctx.wait_for_function(
                    "(sel) => { const el = document.querySelector(sel); return !!el && !el.disabled && el.offsetParent !== null; }",
                    arg=used_selector,
                    timeout=3000,
                )
            except Exception:
                pass

            # フォーム送信実行
            try:
                await submit_element.click()
                logger.debug(f"Worker {self.worker_id}: Submit button click executed successfully")
            except Exception as click_error:
                logger.error(f"Worker {self.worker_id}: Submit button click failed: {click_error}")
                # クリック失敗時に Bot 保護を追加検査（UI/DOMベース）
                try:
                    is_bot, bot_type = await self.bot_detector.detect_bot_protection(dom_ctx)
                except Exception:
                    is_bot, bot_type = (False, None)
                return {
                    "success": False,
                    "error_message": f"Submit click failed: {str(click_error)}",
                    "has_url_change": False,
                    "page_content": "",
                    "submit_selector": used_selector,
                    "bot_protection_detected": bool(is_bot)
                }
            
            # 確認ボタンの場合は確認ページ処理を実行
            if 'button_category' in locals() and button_category == "confirmation":
                logger.debug(f"Worker {self.worker_id}: Confirmation button detected, handling confirmation page pattern")
                confirm_result = await self._handle_confirmation_page_pattern()
                # SuccessJudge で最終確認
                try:
                    sj_result = await sj.judge_submission_success(timeout=15)
                    confirm_result['judgment'] = sj_result
                    # SuccessJudge を最終判定のSOTとして採用
                    confirm_result['success'] = bool(sj_result.get('success'))
                except Exception as e:
                    logger.warning(f"Worker {self.worker_id}: SuccessJudge post-confirm failed: {e}")
                return confirm_result
            
            # 直接送信パターンの場合は従来の処理を継続
            
            logger.debug(f"Worker {self.worker_id}: Submit button clicked, starting wait sequence...")
            
            # Phase 1: 明示的に3秒待機（送信処理とページレスポンスを確保）
            await asyncio.sleep(3.0)
            logger.debug(f"Worker {self.worker_id}: Initial 3-second wait completed")
            
            # Phase 2: ネットワークアイドル待機（追加のページ変化を待機）
            try:
                await dom_ctx.wait_for_load_state('networkidle', timeout=10000)
                logger.debug(f"Worker {self.worker_id}: Network idle state reached")
            except Exception as e:
                # ネットワークアイドル待機失敗は警告レベル
                logger.warning(f"Worker {self.worker_id}: Network idle wait failed: {e}, continuing...")
            
            # Phase 3: テキスト表示完了待機（1秒追加）
            await asyncio.sleep(1.0)
            logger.debug(f"Worker {self.worker_id}: Final text rendering wait completed")
            
            # 送信後のURLを確認
            post_submit_url = dom_ctx.url
            logger.debug(f"Worker {self.worker_id}: Post-submit URL: ***URL_REDACTED***")
            
            has_url_change = pre_submit_url != post_submit_url
            # SuccessJudge による最終判定（URL変化有無に関わらず実行）
            try:
                sj_result = await sj.judge_submission_success(timeout=15)
                try:
                    page_content = await dom_ctx.content()
                except Exception:
                    page_content = ""
                if sj_result.get('success'):
                    logger.info(f"Worker {self.worker_id}: SuccessJudge passed: {sj_result.get('stage_name')}")
                    return {
                        "success": True,
                        "has_url_change": has_url_change,
                        "page_content": page_content[:1000] if page_content else "",
                        "submit_selector": used_selector,
                        "judgment": sj_result
                    }
                else:
                    # ここから未入力検出→追加入力→1回だけリトライ
                    logger.info(f"Worker {self.worker_id}: SuccessJudge failed: {sj_result.get('stage_name')} - {sj_result.get('message')}")
                    try:
                        from ..utils.invalid_field_inspector import detect_invalid_required_fields
                        invalids = await detect_invalid_required_fields(dom_ctx)
                    except Exception:
                        invalids = []

                    # 初回入力済みは除外
                    try:
                        already = getattr(self, '_initial_filled_selectors', set()) or set()
                        invalids = [f for f in invalids if f.get('selector') not in already]
                    except Exception:
                        pass

                    if not invalids:
                        return {
                            "success": False,
                            "error_message": sj_result.get('message', 'Submission verification failed'),
                            "has_url_change": has_url_change,
                            "page_content": page_content[:1000] if page_content else "",
                            "submit_selector": used_selector,
                            "bot_protection_detected": bool(sj_result.get('details', {}).get('bot_protection_detected', False)),
                            "judgment": sj_result
                        }

                    # 詳細ログは環境変数で制御（CLIで伝搬）
                    show_retry_logs = (os.getenv("SHOW_RETRY_LOGS", "").lower() in ["1","true","yes","on"])
                    if show_retry_logs:
                        logger.info(f"Worker {self.worker_id}: Retry filling {len(invalids)} invalid fields")
                    else:
                        logger.info(f"Worker {self.worker_id}: Retry due to missing required fields")

                    # 追加入力実行
                    from ..analyzer.field_combination_manager import FieldCombinationManager
                    fcm = FieldCombinationManager()
                    input_handler = FormInputHandler(dom_ctx, self.worker_id)
                    client_blob = getattr(self, '_current_client_data', {}) or {}

                    def _gen_value(itype: str, hint: str, meta: dict):
                        hint_l = (hint or '').lower()
                        name = (meta.get('name') or '').lower()
                        _id = (meta.get('id') or '').lower()
                        cls = (meta.get('class') or '').lower()
                        blob = " ".join([hint_l, name, _id, cls])
                        def has(tokens):
                            return any(t in blob for t in tokens)
                        if itype == 'email' or has(['email','e-mail','メール']):
                            return fcm.get_field_value_for_type('メールアドレス','single', client_blob) or ''
                        if itype == 'tel' or has(['tel','phone','電話']):
                            return fcm.get_field_value_for_type('電話番号','single', client_blob) or ''
                        if itype == 'textarea' or has(['お問い合わせ','問合せ','内容','本文','メッセージ','message']):
                            tgt = client_blob.get('targeting', {}) if isinstance(client_blob, dict) else {}
                            return (tgt.get('message') or '')
                        if has(['件名','subject']):
                            tgt = client_blob.get('targeting', {}) if isinstance(client_blob, dict) else {}
                            return (tgt.get('subject') or 'お問い合わせ')
                        if has(['会社','法人','社名','company','corp']):
                            return fcm.get_field_value_for_type('会社名','single', client_blob) or ''
                        if has(['住所','address']):
                            return fcm.get_field_value_for_type('住所','single', client_blob) or ''
                        if has(['郵便','〒','zip']):
                            return fcm.get_field_value_for_type('郵便番号','single', client_blob) or ''
                        return ''

                    # 優先度設定の読み込み（失敗時はデフォルトにフォールバック）
                    try:
                        choice_cfg = get_choice_priority_config()
                    except Exception:
                        choice_cfg = {
                            'checkbox': {
                                'primary_keywords': ['営業','提案','メール'],
                                'secondary_keywords': ['その他','一般','other','該当なし'],
                                'privacy_keywords': ['プライバシー','privacy','個人情報','利用規約','terms'],
                                'agree_tokens': ['同意','agree','承諾']
                            },
                            'radio': {
                                'primary_keywords': ['営業','提案','メール'],
                                'secondary_keywords': ['その他','一般','other','該当なし']
                            }
                        }

                    def _choose_priority_index(texts: list, pri1: list, pri2: list, pri3: list = None) -> int:
                        def last_match(keys):
                            cand = [i for i, t in enumerate(texts) if any(str(k).lower() in (t or '').lower() for k in (keys or []))]
                            return cand[-1] if cand else None
                        idx = last_match(pri1)
                        if idx is not None:
                            return idx
                        idx = last_match(pri2)
                        if idx is not None:
                            return idx
                        idx = last_match(pri3 or [])
                        if idx is not None:
                            return idx
                        return max(0, len(texts) - 1)

                    def _is_privacy_like(text: str) -> bool:
                        tl = (text or '').lower()
                        for kw in (choice_cfg.get('checkbox', {}).get('privacy_keywords') or []):
                            if str(kw).lower() in tl:
                                return True
                        return False

                    def _has_agree_token(text: str) -> bool:
                        tl = (text or '').lower()
                        for kw in (choice_cfg.get('checkbox', {}).get('agree_tokens') or []):
                            if str(kw).lower() in tl:
                                return True
                        return False

                    filled_ok = 0
                    filled_categories = []
                    try:
                        checkbox_invalids = [e for e in invalids if e.get('input_type') == 'checkbox']
                        other_invalids = [e for e in invalids if e.get('input_type') != 'checkbox']

                        # checkbox: name > id > class を用いたグルーピング
                        groups = {}
                        for ent in checkbox_invalids:
                            meta = ent.get('meta') or {}
                            key = (meta.get('name') or meta.get('id') or meta.get('class') or f"cb:{ent.get('selector')}")
                            groups.setdefault(key, []).append(ent)

                        pri1 = choice_cfg.get('checkbox', {}).get('primary_keywords', [])
                        pri2 = choice_cfg.get('checkbox', {}).get('secondary_keywords', [])
                        pri3 = choice_cfg.get('checkbox', {}).get('tertiary_keywords', ['問い合わせ','問合'])

                        # privacy negative tokens (skip selecting marketing/newsletter)
                        try:
                            consent_cfg = get_privacy_consent_config()
                            negative_tokens = [str(x).lower() for x in (consent_cfg.get('keywords', {}).get('negative', []) or [])]
                        except Exception:
                            negative_tokens = [s.lower() for s in ["メルマガ","newsletter","配信","案内","広告","キャンペーン"]]

                        for _, ents in groups.items():
                            # hint優先、無ければ name/id/class を評価対象に
                            texts = []
                            for ent in ents:
                                meta = ent.get('meta') or {}
                                base = ent.get('hint') or ''
                                if not base:
                                    base = ' '.join([meta.get('name',''), meta.get('id',''), meta.get('class','')])
                                texts.append(base)

                            is_privacy_group = any(_is_privacy_like(t) for t in texts)
                            # 複数必須（同一グループで複数が未入力）の場合の処理方針
                            select_all = bool(choice_cfg.get('checkbox', {}).get('select_all_when_group_required', True))
                            max_sel = int(choice_cfg.get('checkbox', {}).get('max_group_select', 8) or 8)
                            target_indices = []

                            if select_all and len(ents) > 1:
                                # 全要素を選択（ただしprivacyのnegativeトークンは除外）
                                for i, t in enumerate(texts):
                                    tl = (t or '').lower()
                                    if is_privacy_group and any(neg in tl for neg in negative_tokens):
                                        continue
                                    target_indices.append(i)
                            else:
                                # 単一選択（従来ルール）
                                if is_privacy_group:
                                    agree_hits = [i for i, t in enumerate(texts) if _has_agree_token(t) and not any(neg in (t or '').lower() for neg in negative_tokens)]
                                    if agree_hits:
                                        target_indices = [agree_hits[0]]
                                    else:
                                        target_indices = [_choose_priority_index(texts, pri1, pri2, pri3)]
                                else:
                                    target_indices = [_choose_priority_index(texts, pri1, pri2, pri3)]

                            # 上限と重複排除
                            target_indices = list(dict.fromkeys(target_indices))[:max(1, max_sel)]

                            for idx in target_indices:
                                try:
                                    ent = ents[idx]
                                except Exception:
                                    ent = ents[-1]
                                sel = ent.get('selector')
                                if not sel:
                                    continue
                                field_info = {'selector': sel, 'input_type': 'checkbox', 'type': 'checkbox'}
                                ok = await input_handler.fill_rule_based_field('retry', field_info, True)
                                if ok:
                                    filled_ok += 1
                                    filled_categories.append('checkbox')
                                    if show_retry_logs:
                                        logger.debug("retry-filled checkbox via priority rule")

                        # その他（radio/select/text/textareaなど）は既存フォールバック
                        for ent in other_invalids:
                            try:
                                sel = ent.get('selector')
                                itype = ent.get('input_type','text')
                                val = _gen_value(itype, ent.get('hint') or '', ent.get('meta') or {})
                                if not val and itype == 'select':
                                    fo = ent.get('select_first_option') or {}
                                    val = fo.get('value') or fo.get('text') or ''
                                    if not val:
                                        continue
                                if not val and itype in ['radio']:
                                    val = True
                                if (not val or (isinstance(val, str) and val.strip() == '')) and itype in ['text','textarea']:
                                    val = 'ー'
                                if not sel:
                                    continue
                                field_info = { 'selector': sel, 'input_type': itype, 'type': itype }
                                ok = await input_handler.fill_rule_based_field('retry', field_info, val)
                                if ok:
                                    filled_ok += 1
                                    filled_categories.append(itype)
                                    if show_retry_logs:
                                        logger.debug(f"retry-filled {itype} via standard rule")
                            except Exception as e:
                                if show_retry_logs:
                                    logger.debug(f"retry-fill error: {e}")
                                continue
                    except Exception:
                        if show_retry_logs:
                            logger.debug("priority checkbox retry flow error")
                        # フォールバック: 旧ロジック
                        for ent in invalids:
                            try:
                                sel = ent.get('selector')
                                itype = ent.get('input_type','text')
                                val = _gen_value(itype, ent.get('hint') or '', ent.get('meta') or {})
                                if not val and itype == 'select':
                                    fo = ent.get('select_first_option') or {}
                                    val = fo.get('value') or fo.get('text') or ''
                                    if not val:
                                        continue
                                if not val and itype in ['checkbox','radio']:
                                    val = True
                                if (not val or (isinstance(val, str) and val.strip() == '')) and itype in ['text','textarea']:
                                    val = 'ー'
                                if not sel:
                                    continue
                                field_info = { 'selector': sel, 'input_type': itype, 'type': itype }
                                ok = await input_handler.fill_rule_based_field('retry', field_info, val)
                                if ok:
                                    filled_ok += 1
                                    filled_categories.append(itype)
                            except Exception:
                                continue

                    # 1回だけリトライ送信
                    try:
                        await dom_ctx.click(used_selector)
                        await asyncio.sleep(2.0)
                        try:
                            await dom_ctx.wait_for_load_state('networkidle', timeout=8000)
                        except Exception:
                            pass
                    except Exception:
                        pass

                    # 再判定
                    try:
                        sj2 = SuccessJudge(dom_ctx)
                        await sj2.initialize_before_submission()
                        sj2_result = await sj2.judge_submission_success(timeout=15)
                    except Exception:
                        sj2_result = {"success": False, "message": "judge unavailable"}

                    retry_meta = {
                        "attempted": True,
                        "reason": "missing_required_fields",
                        "invalid_count": len(invalids),
                        "filled_count": filled_ok,
                        "filled_categories": list({c for c in filled_categories}),
                        "result": "success" if sj2_result.get('success') else "failure"
                    }

                    if sj2_result.get('success'):
                        return {
                            "success": True,
                            "has_url_change": pre_submit_url != dom_ctx.url,
                            "page_content": "",
                            "submit_selector": used_selector,
                            "judgment": sj2_result,
                            "additional_data": {"retry": retry_meta}
                        }
                    else:
                        # 失敗として返す
                        return {
                            "success": False,
                            "error_message": sj2_result.get('message', sj_result.get('message','Submission verification failed')),
                            "has_url_change": pre_submit_url != dom_ctx.url,
                            "page_content": page_content[:1000] if page_content else "",
                            "submit_selector": used_selector,
                            "bot_protection_detected": bool(sj2_result.get('details', {}).get('bot_protection_detected', False)),
                            "judgment": sj2_result,
                            "additional_data": {"retry": retry_meta}
                        }
            except Exception as e:
                logger.warning(f"Worker {self.worker_id}: SuccessJudge exception: {e}")
                # 最低限のフォールバック（URL変化を成功扱い）
                return {
                    "success": has_url_change,
                    "error_message": None if has_url_change else "No page navigation after form submission",
                    "has_url_change": has_url_change,
                    "page_content": "",
                    "submit_selector": used_selector
                }
                
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Rule-based form submission error: {e}")
            return {
                "success": False,
                "error_message": f"Rule-based submission error: {str(e)}",
                "has_url_change": False,
                "page_content": "",
                "submit_selector": ""
            }
    
    async def _determine_button_type(self, element_text: str) -> str:
        """ボタンタイプを判定する（確認ボタン→送信ボタンの順で判定）
        
        Args:
            element_text: ボタン要素全体のテキスト
        
        Returns:
            'confirmation': 確認ボタン  
            'submit': 送信ボタン
            'unknown': 不明
        """
        if not element_text:
            return "unknown"
        
        element_text_lower = element_text.lower()

        keywords_config = get_button_keywords_config()

        # 1. まず確認ボタンかどうかを判定（優先）
        for keyword in keywords_config.get("confirmation", ["確認", "次", "review", "confirm", "進む"]):
            if keyword.lower() in element_text_lower:
                return "confirmation"

        # 2. 送信ボタンかどうかを判定
        primary = keywords_config.get("primary", ["送信", "送る", "submit", "send"])  # type: ignore
        secondary = keywords_config.get("secondary", ["完了", "complete", "確定", "実行", "登録"])  # type: ignore
        for keyword in list(primary) + list(secondary):
            if keyword.lower() in element_text_lower:
                return "submit"
        
        return "unknown"
    
    async def _handle_confirmation_page_pattern(self) -> Dict[str, Any]:
        """確認ページ経由パターンの処理"""
        try:
            logger.debug(f"Worker {self.worker_id}: Handling confirmation page pattern")
            
            # 確認ページの読み込み待機
            await asyncio.sleep(3.0)
            
            try:
                await self.page.wait_for_load_state('networkidle', timeout=8000)
            except Exception:
                pass  # タイムアウトしても続行
            
            # 確認ページ遷移後のDOMコンテキスト（iframeなど）を再特定
            try:
                target_frame = await self._select_target_frame_for_analysis()
                if target_frame:
                    self._dom_context = target_frame
                else:
                    # P1: 確認画面に iframe が無い場合は page に戻す（入力画面で使っていた旧iframeはdetach済みの可能性がある）
                    self._dom_context = self.page
            except Exception as e:
                logger.debug(f"Worker {self.worker_id}: Confirmation frame reselect skipped: {e}")
                # 安全側: 例外時も page を採用
                self._dom_context = self.page
            
            # 確認ページで最終送信ボタンを探して実行
            return await self._find_and_submit_final_button()
            
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Confirmation page handling error: {e}")
            return {
                "success": False,
                "error_message": f"Confirmation page handling error: {str(e)}",
                "has_url_change": False,
                "page_content": "",
                "submit_selector": ""
            }
    
    async def _find_and_submit_final_button(self) -> Dict[str, Any]:
        """確認ページで最終送信ボタンを見つけて実行（網羅強化＋同意ON＋フォールバック）"""
        try:
            FINAL_SUBMIT_EXTRA_WAIT_MAX_MS = 20000  # 過剰待機抑止の上限（設定は config で別途検証）
            dom_ctx = self._get_dom_context()
            pre_submit_url = dom_ctx.url if dom_ctx else (self.page.url if self.page else "")

            # 送信ボタンのキーワード（設定 + デフォルト）
            kw_cfg = get_button_keywords_config()
            # デフォルトは button_config 側で付与されるため union は不要
            final_keywords = list(dict.fromkeys(kw_cfg.get("final", [])))

            # SuccessJudge 初期化（送信前の状態を記録）
            from ..analyzer.success_judge import SuccessJudge
            sj = SuccessJudge(dom_ctx)
            try:
                await sj.initialize_before_submission()
            except Exception as e:
                logger.warning(f"Worker {self.worker_id}: SuccessJudge pre-initialize failed on confirmation: {e}")

            async def _find_button_by_keyword(keyword: str):
                """最終送信ボタン探索（同一 form 内優先 + 除外語フィルタ）"""
                async def _is_excluded(el) -> bool:
                    try:
                        text_parts = []
                        try:
                            t = await el.inner_text()
                            if t:
                                text_parts.append(t)
                        except Exception:
                            pass
                        try:
                            v = await el.get_attribute("value")
                            if v:
                                text_parts.append(v)
                        except Exception:
                            pass
                        try:
                            a = await el.get_attribute("aria-label")
                            if a:
                                text_parts.append(a)
                        except Exception:
                            pass
                        txt = " ".join(text_parts).lower()
                        excludes = [s.lower() for s in get_exclude_keywords()]
                        return any(x in txt for x in excludes)
                    except Exception:
                        return False

                # 1) form スコープ内の role=button（アクセシブルネーム）
                try:
                    form_loc = dom_ctx.locator("form")
                    loc = form_loc.get_by_role("button", name=re.compile(keyword, re.IGNORECASE))
                    if await loc.count():
                        el = loc.first
                        if await el.is_visible() and not await _is_excluded(el):
                            return el, f"form>>role=button[name~={keyword}]"
                except Exception as e:
                    logger.debug(f"Worker {self.worker_id}: role(form) search failed: {e}")

                # 2) form 内の button/input でテキスト一致
                selectors = [
                    f'form button[type="submit"]:has-text("{keyword}")',
                    f'form button:has-text("{keyword}")',
                    f'form input[type="submit"][value*="{keyword}"]',
                    f'form input[value*="{keyword}"]',
                ]
                for selector in selectors:
                    try:
                        el = await dom_ctx.query_selector(selector)
                        if el and await el.is_visible() and not await _is_excluded(el):
                            return el, selector
                    except Exception as e:
                        logger.debug(f"Worker {self.worker_id}: selector(form) search failed: {selector} / {e}")
                        continue

                # 3) form 内の anchor role=button
                selectors2 = [
                    f'form a[role="button"]:has-text("{keyword}")',
                    f'form [role="button"]:has-text("{keyword}")',
                ]
                for selector in selectors2:
                    try:
                        el = await dom_ctx.query_selector(selector)
                        if el and await el.is_visible() and not await _is_excluded(el):
                            return el, selector
                    except Exception as e:
                        logger.debug(f"Worker {self.worker_id}: selector2(form) search failed: {selector} / {e}")
                        continue

                # 4) フォールバック: グローバル role=button だが、closest('form') がある場合のみ
                try:
                    loc2 = dom_ctx.get_by_role("button", name=re.compile(keyword, re.IGNORECASE))
                    if await loc2.count():
                        el = loc2.first
                        try:
                            within_form = await el.evaluate("el => !!el.closest('form')")
                        except Exception:
                            within_form = False
                        if within_form and await el.is_visible() and not await _is_excluded(el):
                            return el, f"role=button[name~={keyword}] (within_form)"
                except Exception as e:
                    logger.debug(f"Worker {self.worker_id}: role(global) fallback failed: {e}")

                # 5) 画像ボタン/onclick（form 内のみに限定）
                selectors3 = [
                    f'form input[type="image"][alt*="{keyword}"]',
                    f'form button[onclick*="submit"]:has-text("{keyword}")',
                ]
                for selector in selectors3:
                    try:
                        el = await dom_ctx.query_selector(selector)
                        if el and await el.is_visible() and not await _is_excluded(el):
                            return el, selector
                    except Exception as e:
                        logger.debug(f"Worker {self.worker_id}: image/onclick(form) search failed: {selector} / {e}")
                        continue

                return None, None

            found = None
            used_selector = ""
            for kw in final_keywords:
                el, sel = await _find_button_by_keyword(kw)
                if el:
                    found = el
                    used_selector = sel or ""
                    break

            if not found:
                logger.warning(f"Worker {self.worker_id}: No final submit button found on confirmation page")
                return {
                    "success": False,
                    "error_message": "Final submit button not found on confirmation page",
                    "has_url_change": False,
                    "page_content": "",
                    "submit_selector": "",
                    "final_url": self.page.url if hasattr(self, 'page') and self.page else "",
                    "original_url": pre_submit_url,
                }

            # 同意チェック（送信ボタン近傍）
            try:
                await PrivacyConsentHandler.ensure_near_button(dom_ctx, found, context_hint="final-submit")
            except Exception as _consent_err:
                logger.debug(f"Worker {self.worker_id}: Privacy consent ensure near final failed: {_consent_err}")

            # クリック実行フォールバック
            async def _click_with_fallback(el):
                try:
                    await el.scroll_into_view_if_needed()
                except Exception:
                    pass
                try:
                    if await el.is_enabled():
                        await el.click()
                        return True
                except Exception:
                    pass
                try:
                    await el.evaluate("el => el.click()")
                    return True
                except Exception:
                    pass
                try:
                    await el.evaluate("el => { const f = el.closest('form'); if (f && f.requestSubmit) f.requestSubmit(el); else if (f) f.submit(); }")
                    return True
                except Exception:
                    pass
                try:
                    if self.page:
                        await el.focus()
                        await self.page.keyboard.press("Enter")
                        return True
                except Exception:
                    pass
                return False

            # 確認ダイアログの自動承認（クリーンアップ可）
            dialog_task = None
            try:
                if self.page:
                    dialog_task = asyncio.create_task(self.page.wait_for_event('dialog'))
                    async def _auto_accept_dialog():
                        try:
                            d = await asyncio.wait_for(dialog_task, timeout=5)
                            await d.accept()
                        except Exception as e:
                            logger.debug(f"Worker {self.worker_id}: dialog wait/accept skipped: {e}")
                    asyncio.create_task(_auto_accept_dialog())
            except Exception as e:
                logger.debug(f"Worker {self.worker_id}: setup dialog auto-accept failed: {e}")

            clicked = await _click_with_fallback(found)
            if not clicked:
                return {
                    "success": False,
                    "error_message": "Final submit click failed",
                    "has_url_change": False,
                    "page_content": "",
                    "submit_selector": used_selector,
                    "final_url": self.page.url if hasattr(self, 'page') and self.page else "",
                    "original_url": pre_submit_url,
                }

            # 送信後待機（余裕値は設定から）
            # 追加待機（0〜上限にクランプ）
            try:
                wc = self.config.get("worker_config") or {}
                fs = wc.get("final_submit") or {}
                extra_ms = int(fs.get("confirmation_extra_wait_ms", 2000))
            except Exception:
                extra_ms = 2000
            extra_ms = max(0, min(extra_ms, FINAL_SUBMIT_EXTRA_WAIT_MAX_MS))
            await asyncio.sleep(3.0 + extra_ms / 1000.0)
            try:
                await dom_ctx.wait_for_load_state('networkidle', timeout=12000)
            except Exception:
                pass

            # 使わなかった dialog 待機タスクを安全にキャンセル
            try:
                if dialog_task and not dialog_task.done():
                    dialog_task.cancel()
            except Exception:
                pass

            try:
                sj_result = await sj.judge_submission_success(timeout=20)
                try:
                    page_content = await dom_ctx.content()
                except Exception:
                    page_content = ""
                return {
                    "success": bool(sj_result.get("success")),
                    "error_message": None if sj_result.get("success") else sj_result.get("message", "Submission verification failed"),
                    "has_url_change": pre_submit_url != (dom_ctx.url if hasattr(dom_ctx, 'url') else (self.page.url if self.page else pre_submit_url)),
                    "page_content": page_content[:1000] if page_content else "",
                    "submit_selector": used_selector,
                    "bot_protection_detected": bool(sj_result.get('details', {}).get('bot_protection_detected', False)),
                    "judgment": sj_result,
                    "final_url": (dom_ctx.url if hasattr(dom_ctx, 'url') else (self.page.url if self.page else "")),
                    "original_url": pre_submit_url,
                }
            except Exception as e:
                logger.warning(f"Worker {self.worker_id}: SuccessJudge post-final failed: {e}")
                return {
                    "success": False,
                    "error_message": "Submission verification failed",
                    "has_url_change": pre_submit_url != (dom_ctx.url if hasattr(dom_ctx, 'url') else (self.page.url if self.page else pre_submit_url)),
                    "page_content": "",
                    "submit_selector": used_selector,
                    "final_url": (dom_ctx.url if hasattr(dom_ctx, 'url') else (self.page.url if self.page else "")),
                    "original_url": pre_submit_url,
                }
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error finding final submit button: {e}")
            try:
                dom_ctx = getattr(self, '_dom_context', self.page)
                final_url = dom_ctx.url if dom_ctx else (self.page.url if self.page else "")
            except Exception:
                final_url = self.page.url if hasattr(self, 'page') and self.page else ""
            return {
                "success": False,
                "error_message": f"Final button search error: {str(e)}",
                "has_url_change": False,
                "page_content": "",
                "submit_selector": "",
                "final_url": final_url,
                "original_url": pre_submit_url if 'pre_submit_url' in locals() else "",
            }
    
    # _analyze_final_submission_result: SuccessJudgeでの統一判定に置き換え済み（削除）

    async def _execute_form_submission_isolated(
        self, instruction: Optional[Dict[str, Any]], record_id: int, use_rule_based: bool = False
    ) -> Dict[str, Any]:
        """フォーム送信と結果判定（統合実装版）"""
        try:
            logger.debug(f"Worker {self.worker_id}: Starting form submission")

            # 統合された送信処理を使用
            submit_result = await self._submit_rule_based_form()

            if submit_result.get("success"):
                logger.info(f"Worker {self.worker_id}: Form submission successful")
                success_payload = {
                    "record_id": record_id,
                    "status": "success",
                    "submitted_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
                }
                if submit_result.get('additional_data'):
                    success_payload['additional_data'] = submit_result['additional_data']
                return success_payload
            else:
                error_message = submit_result.get("error_message", "Form submission failed")
                logger.warning(f"Worker {self.worker_id}: Form submission failed: {error_message}")

                # SuccessJudgeの判定やページ内容を使って詳細分類を強化
                judgment = submit_result.get("judgment") or {}
                details = judgment.get('details') or {}
                primary_error = details.get('primary_error_type') or ''

                page_content = submit_result.get("page_content", "")
                has_url_change = bool(submit_result.get("has_url_change"))
                submit_selector = submit_result.get("submit_selector", "")

                # 1) SuccessJudgeの分類を優先マッピング
                mapped_from_judgment = None
                try:
                    if isinstance(primary_error, str) and primary_error:
                        if primary_error.startswith('必須項目未入力'):
                            mapped_from_judgment = 'MAPPING'
                        elif primary_error.startswith('メール形式エラー'):
                            mapped_from_judgment = 'VALIDATION_FORMAT'
                        elif primary_error.startswith('reCAPTCHA'):
                            mapped_from_judgment = 'BOT_DETECTED'
                        elif primary_error.startswith('営業お断り'):
                            mapped_from_judgment = 'PROHIBITION_DETECTED'
                        elif primary_error.startswith('システムエラー'):
                            mapped_from_judgment = 'SYSTEM'
                except Exception:
                    mapped_from_judgment = None

                # 2) ページ内容と組み合わせた詳細分類
                if not mapped_from_judgment:
                    try:
                        error_type = ErrorClassifier.classify_form_submission_error(
                            error_message=error_message,
                            has_url_change=has_url_change,
                            page_content=page_content,
                            submit_selector=submit_selector,
                        )
                    except Exception:
                        error_type = ErrorClassifier.classify_error_type({'error_message': error_message, 'stage': 'submission'})
                else:
                    error_type = mapped_from_judgment

                result_dict = {
                    "error": True,
                    "record_id": record_id,
                    "status": "failed",
                    "error_type": error_type,
                    "error_message": error_message,
                }
                # Bot保護検出を伝搬
                try:
                    result_dict["bot_protection_detected"] = bool(submit_result.get("bot_protection_detected", False))
                except Exception:
                    pass

                # 分類補助用の詳細コンテキストを付与
                try:
                    details = details or {}
                    resp = details.get('response_analysis') or {}
                    http_status = self._determine_http_status(resp)

                    classify_ctx = {
                        "stage": judgment.get('stage') if isinstance(judgment, dict) else None,
                        "has_url_change": bool(has_url_change),
                        "primary_error_type": primary_error,
                        "has_error_responses": bool(resp.get('has_error_responses')) if isinstance(resp, dict) else None,
                        "has_redirects": bool(resp.get('has_redirects')) if isinstance(resp, dict) else None,
                        "http_status": http_status,
                        # ページ本文は短いスニペットのみ（DB保存前にサニタイズし、ログには出さない）
                        "page_content_snippet": (
                            (self._content_sanitizer.sanitize_string(page_content[:600]) if self._content_sanitizer else "")
                            if isinstance(page_content, str) else ""
                        ),
                        # 追加: Bot検出フラグを分類コンテキストに明示含める
                        "is_bot_detected": bool(submit_result.get("bot_protection_detected", False)),
                    }

                    # 既存 additional_data（例: retry メタ）と安全にマージ
                    add: Dict[str, Any] = {}
                    if isinstance(submit_result, dict) and isinstance(submit_result.get('additional_data'), dict):
                        add.update(submit_result['additional_data'])
                    # classify_context は必ず保持
                    add['classify_context'] = classify_ctx
                    result_dict['additional_data'] = add
                except Exception:
                    pass

                return result_dict
        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Form submission execution error: {e}")
            return {
                "error": True,
                "record_id": record_id,
                "status": "failed",
                "error_type": "SUBMISSION_ERROR",
                "error_message": str(e),
            }

    async def _perform_dynamic_content_loading(self):
        """動的コンテンツの読み込み待機"""
        try:
            # JavaScript実行完了まで待機
            await asyncio.sleep(1.5)

            # ネットワークアイドル状態まで待機
            try:
                await self.page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass  # タイムアウトしても続行

        except Exception as e:
            logger.warning(f"Worker {self.worker_id}: Dynamic content loading warning: {e}")

    

    async def _fill_form_field_isolated(self, field_name: str, field_config: Dict[str, Any]) -> None:
        """フォームフィールドへの入力実行（独立版）"""
        try:
            selector = field_config.get("selector")
            input_type = field_config.get("input_type", "text")
            value = field_config.get("value", "")

            if not selector:
                logger.warning(f"Worker {self.worker_id}: No selector for field ***FIELD_REDACTED***")
                return

            # 要素の待機
            try:
                await self.page.wait_for_selector(selector, timeout=5000)
            except Exception:
                logger.warning(f"Worker {self.worker_id}: Element ***SELECTOR_REDACTED*** not found for ***FIELD_REDACTED***")
                return

            # 入力タイプに応じた処理
            if input_type in ["text", "email", "tel", "url"]:
                await self.page.fill(selector, str(value))
            elif input_type == "textarea":
                await self.page.fill(selector, str(value))
            elif input_type == "select":
                await self.page.select_option(selector, str(value))
            elif input_type == "checkbox":
                if value:
                    await self.page.check(selector)
                else:
                    await self.page.uncheck(selector)
            elif input_type == "radio":
                await self.page.click(selector)
            else:
                # デフォルトはテキスト入力
                await self.page.fill(selector, str(value))

            # 短い待機（UI反応時間）
            await asyncio.sleep(0.3)

        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error filling field ***FIELD_REDACTED***: {e}")
            raise

    async def _fill_form_field_isolated_detailed(self, field_name: str, field_config: Dict[str, Any]) -> None:
        """フォームフィールドへの入力実行（詳細エラー情報版）"""
        try:
            selector = field_config.get("selector")
            input_type = field_config.get("input_type", "text")
            value = field_config.get("value", "")

            if not selector:
                raise Exception(f"No selector provided for field ***FIELD_REDACTED***")

            # 要素の待機（より詳細なエラー情報付き）
            try:
                await self.page.wait_for_selector(selector, timeout=5000)
            except Exception as e:
                raise Exception(f"Element ***SELECTOR_REDACTED*** not found for ***FIELD_REDACTED***: {str(e)}")

            # 入力前に要素の状態をチェック
            try:
                element = await self.page.query_selector(selector)
                if not element:
                    raise Exception(f"Element ***SELECTOR_REDACTED*** exists but not queryable for ***FIELD_REDACTED***")
                
                # 要素の可視性と有効性をチェック
                is_visible = await element.is_visible()
                is_enabled = await element.is_enabled()
                
                if not is_visible:
                    logger.warning(f"Worker {self.worker_id}: Element ***SELECTOR_REDACTED*** is not visible for ***FIELD_REDACTED***")
                if not is_enabled:
                    logger.warning(f"Worker {self.worker_id}: Element ***SELECTOR_REDACTED*** is not enabled for ***FIELD_REDACTED***")
                    
            except Exception as e:
                logger.warning(f"Worker {self.worker_id}: Element state check failed for ***FIELD_REDACTED***: {e}")

            # 入力タイプに応じた処理（エラー詳細化）
            try:
                if input_type in ["text", "email", "tel", "url"]:
                    await self.page.fill(selector, str(value))
                elif input_type == "textarea":
                    await self.page.fill(selector, str(value))
                elif input_type == "select":
                    await self.page.select_option(selector, str(value))
                elif input_type == "checkbox":
                    if value:
                        await self.page.check(selector)
                    else:
                        await self.page.uncheck(selector)
                elif input_type == "radio":
                    await self.page.click(selector)
                else:
                    # デフォルトはテキスト入力
                    await self.page.fill(selector, str(value))
            except Exception as e:
                # 入力タイプ固有のエラーメッセージを生成
                if "Cannot type text into input[type=" in str(e):
                    raise Exception(f"Input type mismatch for field ***FIELD_REDACTED***: Cannot type text into {input_type} field - {str(e)}")
                elif "timeout" in str(e).lower():
                    raise Exception(f"Element interaction timeout for field ***FIELD_REDACTED***: {str(e)}")
                elif "not found" in str(e).lower():
                    raise Exception(f"Element ***SELECTOR_REDACTED*** not found during input for ***FIELD_REDACTED***: {str(e)}")
                else:
                    raise Exception(f"Error filling field ***FIELD_REDACTED*** (type: {input_type}): {str(e)}")

            # 短い待機（UI反応時間）
            await asyncio.sleep(0.3)

        except Exception as e:
            # すでに詳細なメッセージが設定されている場合はそのまま、そうでなければ基本情報を追加
            error_msg = str(e)
            if not any(keyword in error_msg for keyword in ["FIELD_REDACTED", "SELECTOR_REDACTED", input_type]):
                error_msg = f"Field ***FIELD_REDACTED*** (selector: ***SELECTOR_REDACTED***, type: {input_type}): {error_msg}"
            logger.error(f"Worker {self.worker_id}: {error_msg}")
            raise Exception(error_msg)

    async def _submit_form_isolated(self, submit_config: Dict[str, Any]) -> bool:
        """フォーム送信実行（成功判定改善版）"""
        try:
            selector = submit_config.get("selector")
            if not selector:
                logger.warning(f"Worker {self.worker_id}: No submit button selector provided")
                return False

            # 送信ボタンの待機
            try:
                await self.page.wait_for_selector(selector, timeout=5000)
            except Exception:
                logger.warning(f"Worker {self.worker_id}: Submit button ***SELECTOR_REDACTED*** not found")
                return False

            # SuccessJudge 準備（送信前の状態を記録）
            from ..analyzer.success_judge import SuccessJudge
            sj = SuccessJudge(self.page)
            try:
                await sj.initialize_before_submission()
            except Exception as e:
                logger.warning(f"Worker {self.worker_id}: SuccessJudge pre-initialize failed (bool): {e}")

            # 送信ボタンクリック
            await self.page.click(selector)

            # 送信後の状態変化を待機
            await asyncio.sleep(2)

            # 送信後のBot検知チェック
            is_bot_detected, bot_type = await self.bot_detector.detect_bot_protection(self.page)
            if is_bot_detected:
                logger.warning(f"Worker {self.worker_id}: Bot protection detected after submit: {bot_type}")
                return False

            # 成功判定（SuccessJudgeに集約）
            try:
                result = await sj.judge_submission_success(timeout=15)
                return bool(result.get("success"))
            except Exception as e:
                logger.warning(f"Worker {self.worker_id}: SuccessJudge exception (bool): {e}")
                return False

        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Error during form submission: {e}")
            return False

    async def _submit_form_isolated_detailed(self, submit_config: Dict[str, Any]) -> Dict[str, Any]:
        """フォーム送信実行（詳細情報返却版）"""
        try:
            selector = submit_config.get("selector")
            if not selector:
                error_msg = "No submit button selector provided"
                logger.warning(f"Worker {self.worker_id}: {error_msg}")
                return {
                    "success": False,
                    "error_message": error_msg,
                    "has_url_change": False,
                    "page_content": "",
                    "submit_selector": ""
                }

            # 送信ボタンの待機
            try:
                await self.page.wait_for_selector(selector, timeout=5000)
            except Exception as e:
                error_msg = f"Submit button ***SELECTOR_REDACTED*** not found"
                logger.warning(f"Worker {self.worker_id}: {error_msg}")
                return {
                    "success": False,
                    "error_message": error_msg,
                    "has_url_change": False,
                    "page_content": "",
                    "submit_selector": selector
                }

            # SuccessJudge 準備（送信前の状態を記録）
            from ..analyzer.success_judge import SuccessJudge
            sj = SuccessJudge(self.page)
            try:
                await sj.initialize_before_submission()
            except Exception as e:
                logger.warning(f"Worker {self.worker_id}: SuccessJudge pre-initialize failed (detailed): {e}")

            # 送信ボタンクリック
            await self.page.click(selector)

            # 送信後の状態変化を待機
            await asyncio.sleep(2)

            # 送信後のBot検知チェック
            is_bot_detected, bot_type = await self.bot_detector.detect_bot_protection(self.page)
            if is_bot_detected:
                error_msg = f"Bot protection detected after submit: {bot_type}" if bot_type else "Bot protection detected after submit"
                logger.warning(f"Worker {self.worker_id}: {error_msg}")
                return {
                    "success": False,
                    "error_message": error_msg,
                    "has_url_change": False,
                    "page_content": "",
                    "submit_selector": selector
                }

            # 成功判定（SuccessJudgeに集約）
            try:
                sj_result = await sj.judge_submission_success(timeout=15)
                try:
                    page_content = await asyncio.wait_for(self.page.content(), timeout=10)
                except Exception:
                    page_content = ""
                return {
                    "success": bool(sj_result.get("success")),
                    "error_message": None if sj_result.get("success") else sj_result.get("message", "Submission verification failed"),
                    "has_url_change": True if sj_result.get('details', {}).get('url_change_type') else False,
                    "page_content": page_content,
                    "submit_selector": selector,
                    "judgment": sj_result
                }
            except Exception as e:
                error_msg = f"SuccessJudge exception: {str(e)}"
                logger.warning(f"Worker {self.worker_id}: {error_msg}")
                try:
                    page_content = await asyncio.wait_for(self.page.content(), timeout=5)
                except Exception:
                    page_content = ""
                return {
                    "success": False,
                    "error_message": error_msg,
                    "has_url_change": False,
                    "page_content": page_content,
                    "submit_selector": selector
                }

        except Exception as e:
            error_msg = f"Error during form submission: {str(e)}"
            logger.error(f"Worker {self.worker_id}: {error_msg}")
            
            # 例外時もできるだけ詳細情報を返す
            try:
                page_content = await asyncio.wait_for(self.page.content(), timeout=5) if self.page else ""
            except:
                page_content = ""
            
            return {
                "success": False,
                "error_message": error_msg,
                "has_url_change": False,
                "page_content": page_content,
                "submit_selector": submit_config.get("selector", "")
            }

    # _analyze_page_content_for_success: SuccessJudgeでの統一判定に置き換え済み（削除）

    async def _check_form_disappearance(self) -> bool:
        """
        元のフォームが消失したかチェック（成功の間接的指標）
        
        Returns:
            bool: フォームが消失したかどうか
        """
        try:
            # 一般的なフォーム要素をチェック
            form_selectors = [
                "form", "input[type='submit']", "button[type='submit']",
                "input[type='text']", "textarea", "select"
            ]
            
            form_elements_count = 0
            for selector in form_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    form_elements_count += len(elements)
                except Exception:
                    continue
            
            # フォーム要素が著しく少なくなった場合は成功の可能性
            return form_elements_count < 2
            
        except Exception as e:
            logger.debug(f"Worker {self.worker_id}: Error checking form disappearance: {e}")
            return False


    async def _attempt_recovery_isolated(self, error_type: str, error_message: str) -> bool:
        """復旧処理実行（独立版・更新版）"""
        try:
            if error_type == "TIMEOUT":
                # タイムアウト復旧: ページを閉じて待機
                if self.page:
                    await self.page.close()
                    self.page = None
                await asyncio.sleep(2)
                return True

            elif error_type == "ACCESS":
                # アクセスエラーの詳細分析とリカバリ
                if any(
                    keyword in error_message.lower()
                    for keyword in ["closed", "target page", "browser has been closed", "context has been closed"]
                ):
                    # ブラウザクラッシュの場合は完全再初期化
                    logger.warning(f"Worker {self.worker_id}: Detected browser crash, performing full reinitialization")
                    return await self._reinitialize_browser()
                else:
                    # 通常のアクセスエラーは短時間待機
                    await asyncio.sleep(1)
                    return True

            elif error_type in ["ELEMENT_EXTERNAL", "INPUT_EXTERNAL"]:
                # 外部要因による要素/入力エラー復旧: ページリフレッシュ
                if self.page:
                    await self.page.reload(timeout=10000)
                    await asyncio.sleep(1)
                return True

            elif error_type == "SYSTEM":
                # システムエラー復旧: 短時間待機
                await asyncio.sleep(1)
                return True

            return False

        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Recovery attempt error: {e}")
            # 復旧失敗時は完全リソースクリーンアップ
            try:
                if self.page:
                    await self.page.close()
                    self.page = None
                # browser_contextはbrowser_managerが管理
            except:
                pass  # クリーンアップエラーは無視
            return False

    async def _reinitialize_browser(self) -> bool:
        """
        ブラウザ完全再初期化（クラッシュ対応）

        Returns:
            bool: 再初期化が成功したかどうか
        """
        try:
            logger.warning(f"Worker {self.worker_id}: Starting browser reinitialization due to crash")

            # Step 1: 既存リソースの強制クリーンアップ
            cleanup_tasks = []

            # Page を非同期で閉じる
            if self.page:
                try:
                    cleanup_tasks.append(asyncio.create_task(asyncio.wait_for(self.page.close(), timeout=3)))
                except Exception:
                    pass  # エラーは無視
                finally:
                    self.page = None

            # Browser関連リソースはbrowser_managerが管理
            # browser_managerの適切なクリーンアップを実行
            try:
                cleanup_tasks.append(asyncio.create_task(asyncio.wait_for(self.browser_manager.close(), timeout=10)))
            except Exception:
                pass

            # Playwright を非同期で停止（このattributeが存在する場合のみ）
            if hasattr(self, 'playwright') and self.playwright:
                try:
                    cleanup_tasks.append(asyncio.create_task(asyncio.wait_for(self.playwright.stop(), timeout=3)))
                except Exception:
                    pass
                finally:
                    self.playwright = None

            # 全クリーンアップタスクを並列実行（タイムアウト付き）
            if cleanup_tasks:
                try:
                    await asyncio.wait_for(asyncio.gather(*cleanup_tasks, return_exceptions=True), timeout=10)
                except asyncio.TimeoutError:
                    logger.warning(f"Worker {self.worker_id}: Browser cleanup timeout during reinitialization")

            # Step 2: 少し待機してプロセス完全終了を確保
            await asyncio.sleep(2)

            # Step 3: 新しいブラウザを初期化
            logger.info(f"Worker {self.worker_id}: Reinitializing new browser instance")
            reinit_success = await self.initialize()

            if reinit_success:
                logger.info(f"Worker {self.worker_id}: Browser reinitialization completed successfully")
                return True
            else:
                logger.error(f"Worker {self.worker_id}: Browser reinitialization failed")
                return False

        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Critical error during browser reinitialization: {e}")
            return False

    async def cleanup(self):
        """リソースクリーンアップ（強化版）"""
        cleanup_errors = []

        try:
            # Page cleanup with timeout
            if self.page:
                try:
                    await asyncio.wait_for(self.page.close(), timeout=5)
                    logger.debug(f"Worker {self.worker_id}: Page closed successfully")
                except asyncio.TimeoutError:
                    logger.warning(f"Worker {self.worker_id}: Page close timeout, forcing closure")
                except Exception as e:
                    cleanup_errors.append(f"Page cleanup: {e}")
                finally:
                    self.page = None

            # Browser cleanup はbrowser_managerが管理
            try:
                await asyncio.wait_for(self.browser_manager.close(), timeout=10)
                logger.debug(f"Worker {self.worker_id}: Browser manager closed successfully")
            except asyncio.TimeoutError:
                logger.warning(f"Worker {self.worker_id}: Browser manager close timeout, forcing closure")
            except Exception as e:
                cleanup_errors.append(f"Browser manager cleanup: {e}")

            # Playwright cleanup with timeout（属性が存在する場合のみ）
            if hasattr(self, 'playwright') and self.playwright:
                try:
                    await asyncio.wait_for(self.playwright.stop(), timeout=5)
                    logger.debug(f"Worker {self.worker_id}: Playwright stopped successfully")
                except asyncio.TimeoutError:
                    logger.warning(f"Worker {self.worker_id}: Playwright stop timeout, forcing stop")
                except Exception as e:
                    cleanup_errors.append(f"Playwright cleanup: {e}")
                finally:
                    self.playwright = None

            # 統計情報のクリア
            self.stats = {}
            self._selector_cache.clear()

            if cleanup_errors:
                logger.warning(f"Worker {self.worker_id}: Cleanup completed with errors: {cleanup_errors}")
            else:
                logger.info(f"Worker {self.worker_id}: All resources cleaned up successfully")

        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Critical cleanup error: {e}")
            # 強制的にリソース参照をクリア
            self.page = None
            # browser と playwright は browser_manager が管理

    def get_stats(self) -> Dict[str, Any]:
        """統計情報を取得"""
        elapsed_time = time.time() - self.stats["start_time"]
        return {
            "worker_id": self.worker_id,
            "elapsed_time": elapsed_time,
            "processed": self.stats["processed"],
            "success": self.stats["success"],
            "failed": self.stats["failed"],
            "errors": self.stats["errors"],
            "success_rate": self.stats["success"] / max(self.stats["processed"], 1) * 100,
        }


def worker_process_main(worker_id: int, task_queue: mp.Queue, result_queue: mp.Queue, headless: bool = None):
    """
    ワーカープロセスのメイン関数

    Args:
        worker_id: ワーカーID
        task_queue: タスクキュー
        result_queue: 結果キュー
        headless: ブラウザヘッドレスモード (None=環境自動判定, True=強制ヘッドレス, False=強制GUI)
    """
    # ログフィルタ（マッピング関連のINFO/DEBUG抑制）を最初に適用
    try:
        from form_sender.security.log_filters import MappingLogFilter
        quiet_env = os.getenv('QUIET_MAPPING_LOGS', '')
        quiet_env_flag = True if quiet_env == '' else quiet_env.lower() in ['1', 'true', 'yes', 'on']
        if quiet_env_flag:
            _filter = MappingLogFilter()
            _root = logging.getLogger()
            _root.addFilter(_filter)
            for _h in _root.handlers:
                _h.addFilter(_filter)
            logging.getLogger(__name__).info(
                f"Worker {worker_id}: Mapping logs set to QUIET (INFO/DEBUG suppressed)"
            )
    except Exception as _e:
        logging.getLogger(__name__).warning(f"Worker {worker_id}: Failed to configure mapping-log filter: {_e}")

    # プロセス名設定
    import setproctitle

    setproctitle.setproctitle(f"form-sender-worker-{worker_id}")

    # ログレベル設定
    logger.setLevel(logging.INFO)

    # グレースフル終了制御
    shutdown_event = asyncio.Event()
    current_task_completion = asyncio.Event()
    current_task_completion.set()  # 初期状態では完了

    # シグナルハンドラ設定（グレースフル終了対応）
    def signal_handler(signum, frame):
        logger.info(f"Worker {worker_id}: Received signal {signum}, initiating graceful shutdown...")
        shutdown_event.set()

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # ワーカーインスタンス
    worker = None

    async def shutdown_worker():
        """ワーカー終了処理（強制クリーンアップ対応）"""
        nonlocal worker

        if worker:
            try:
                # 通常のクリーンアップを試行
                await asyncio.wait_for(worker.cleanup(), timeout=15)
                logger.info(f"Worker {worker_id}: Normal cleanup completed")
            except asyncio.TimeoutError:
                logger.warning(f"Worker {worker_id}: Cleanup timeout, performing force cleanup")
                await force_cleanup_playwright_resources(worker)
            except Exception as e:
                logger.error(f"Worker {worker_id}: Cleanup error: {e}")
                await force_cleanup_playwright_resources(worker)

        # 終了通知
        shutdown_result = WorkerResult(
            task_id=f"shutdown_{worker_id}", worker_id=worker_id, status=ResultStatus.WORKER_SHUTDOWN
        )
        try:
            result_queue.put(shutdown_result.to_dict(), timeout=1)
        except:
            pass

    async def force_cleanup_playwright_resources(worker_instance):
        """強制Playwrightリソースクリーンアップ"""
        try:
            # 強制的にリソース参照をクリア
            if hasattr(worker_instance, "page"):
                worker_instance.page = None
            if hasattr(worker_instance, "browser"):
                worker_instance.browser = None
            if hasattr(worker_instance, "playwright"):
                worker_instance.playwright = None

            # システムレベルでChromiumプロセスをクリーンアップ
            import psutil
            import os

            current_pid = os.getpid()

            try:
                current_process = psutil.Process(current_pid)
                children = current_process.children(recursive=True)

                for child in children:
                    if "chrom" in child.name().lower():
                        logger.info(f"Worker {worker_id}: Force killing Chromium process {child.pid}")
                        child.kill()

            except Exception as ps_e:
                logger.warning(f"Worker {worker_id}: Could not cleanup child processes: {ps_e}")

        except Exception as e:
            logger.error(f"Worker {worker_id}: Force cleanup error: {e}")

    async def worker_main_loop():
        """ワーカーメインループ（グレースフル終了対応）"""
        nonlocal worker

        try:
            # ワーカー初期化
            worker = IsolatedFormWorker(worker_id, headless)

            # Playwright初期化
            if not await worker.initialize():
                logger.error(f"Worker {worker_id}: Failed to initialize")
                return

            # 準備完了通知
            ready_result = WorkerResult(
                task_id=f"ready_{worker_id}", worker_id=worker_id, status=ResultStatus.WORKER_READY
            )
            result_queue.put(ready_result.to_dict())

            logger.info(f"Worker {worker_id}: Ready and waiting for tasks")

            # ハートビート送信タスクを開始
            heartbeat_task = asyncio.create_task(heartbeat_sender())

            # タスク処理ループ（グレースフル終了対応 + 即時停止機能）
            while not worker.should_stop and not shutdown_event.is_set():
                try:
                    # 【重要】 タスク取得前のSHUTDOWNチェック
                    try:
                        # ノンブロッキングでSHUTDOWNタスクがあるかチェック
                        peek_task = task_queue.get_nowait()
                        if peek_task.get("task_type") == TaskType.SHUTDOWN.value:
                            logger.info(f"Worker {worker_id}: SHUTDOWN task detected before task acquisition - immediate stop")
                            break
                        # SHUTDOWNでなければ、このタスクを処理対象とする
                        task_data = peek_task
                    except queue.Empty:
                        # タスクなし、短時間待機してチェック
                        await asyncio.sleep(0.1)
                        continue

                    # グレースフル終了チェック（タスク取得後）
                    if shutdown_event.is_set():
                        logger.info(f"Worker {worker_id}: Graceful shutdown requested, finishing current task")
                        # 現在のタスクをキューに戻す
                        task_queue.put(task_data)
                        break

                    # 現在のタスク処理中であることを示す
                    current_task_completion.clear()

                    try:
                        # 企業処理タスク実行（SHUTDOWN監視付き）
                        result = await worker.process_company_task(task_data, task_queue)

                        # 結果送信
                        result_queue.put(result.to_dict())

                    finally:
                        # タスク完了を通知
                        current_task_completion.set()

                except Exception as e:
                    logger.error(f"Worker {worker_id}: Task processing error: {e}")
                    current_task_completion.set()  # エラー時も完了扱い
                    continue

            # グレースフル終了の場合、現在のタスク完了を待機
            if shutdown_event.is_set():
                logger.info(f"Worker {worker_id}: Waiting for current task completion...")
                await asyncio.wait_for(current_task_completion.wait(), timeout=30)
                logger.info(f"Worker {worker_id}: Current task completed, proceeding with shutdown")

            # ハートビートタスク終了
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

        except Exception as e:
            logger.error(f"Worker {worker_id}: Fatal error: {e}")
        finally:
            await shutdown_worker()

    async def heartbeat_sender():
        """独立したハートビート送信タスク"""
        heartbeat_interval = 30  # デフォルト30秒

        while not shutdown_event.is_set():
            try:
                # ハートビート送信
                heartbeat_result = WorkerResult(
                    task_id=f"heartbeat_{worker_id}_{int(time.time())}",
                    worker_id=worker_id,
                    status=ResultStatus.WORKER_READY,
                )
                result_queue.put(heartbeat_result.to_dict(), timeout=1)

                # 次回まで待機
                await asyncio.sleep(heartbeat_interval)

            except Exception as e:
                logger.warning(f"Worker {worker_id}: Heartbeat error: {e}")
                await asyncio.sleep(5)  # エラー時は短時間待機

    # asyncioでメインループ実行
    try:
        asyncio.run(worker_main_loop())
    except KeyboardInterrupt:
        logger.info(f"Worker {worker_id}: Interrupted by user")
    except Exception as e:
        logger.error(f"Worker {worker_id}: Unexpected error: {e}")

    logger.info(f"Worker {worker_id}: Process terminated")
