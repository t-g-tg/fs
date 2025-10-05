"""
フォーム送信ワーカー本体

メインのフォーム送信処理ロジック
"""

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime, timezone, timedelta
# Pathは未使用のため削除（過去互換コメントを除去）
from typing import Dict, Any, List, Optional, Tuple, Union, Callable

try:
    from playwright.async_api import ElementHandle
except ImportError:
    ElementHandle = Any  # フォールバック型

# より具体的な型定義
FieldConfigType = Dict[str, Union[str, bool, Any]]
ResponseDataType = Dict[str, Union[None, int, List[Dict[str, Union[str, int, float]]], List[float]]]
TimeoutConfigType = Dict[str, int]
ButtonKeywordsConfigType = Dict[str, List[str]]

from playwright.async_api import async_playwright, Browser, Page, TimeoutError as PlaywrightTimeoutError
from supabase import create_client, Client

from ..control.recovery_manager import AutoRecoveryManager
from ..security.logger import SecurityLogger
# 旧instruction_jsonベースのテンプレート処理は廃止（ルールベース解析に統一）
from ..detection.bot_detector import BotDetectionSystem
from ..utils.error_classifier import ErrorClassifier
from ..utils.secure_logger import get_secure_logger, secure_log_decorator
from ..utils.performance_monitor import PerformanceMonitor
from ..utils.button_config import get_button_keywords_config as _shared_get_button_keywords_config
from ..analyzer.success_judge import SuccessJudge
from ..utils.cookie_handler import CookieConsentHandler
from ..utils.privacy_consent_handler import PrivacyConsentHandler
from config.manager import get_form_sender_config, get_retry_config_for, get_database_config


def _load_config_file(filename: str) -> Dict[str, Any]:
    """設定ファイルを読み込む（エラーハンドリング強化版）"""
    try:
        config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "config", filename
        )

        if not os.path.exists(config_path):
            logger.warning(f"設定ファイル {filename} が見つかりません: {config_path}")
            return _get_default_config(filename)

        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
            logger.debug(f"設定ファイル {filename} を正常に読み込みました")
            return config
    except json.JSONDecodeError as e:
        logger.error(f"設定ファイル {filename} の JSON 形式が不正です: {e}")
        return _get_default_config(filename)
    except Exception as e:
        logger.error(f"設定ファイル {filename} の読み込みに失敗: {e}")
        return _get_default_config(filename)


def _get_default_config(filename: str) -> Dict[str, Any]:
    """設定ファイルのデフォルト値を提供"""
    defaults = {
        "timeouts.json": {
            "timeouts": {
                "page_load_wait": 3000,
                "ajax_processing_wait": 3000,
                "form_submission_wait": 5000,
                "confirmation_page_wait": 3000,
                "dom_mutation_wait": 10000,
                "auto_exit_fallback": 10000,
            }
        },
        "button_keywords.json": {
            "submit_button_keywords": {
                "primary": ["送信", "送る", "submit", "send"],
                "secondary": ["完了", "complete", "確定", "confirm", "実行", "execute", "登録", "register"],
                "confirmation": ["確認", "次", "review", "confirm", "進む"],
            }
        },
    }
    return defaults.get(filename, {})


def _get_timeout_config() -> TimeoutConfigType:
    """タイムアウト設定を取得"""
    config = _load_config_file("timeouts.json")
    return config.get(
        "timeouts",
        {
            "page_load_wait": 3000,
            "ajax_processing_wait": 3000,
            "form_submission_wait": 5000,
            "confirmation_page_wait": 3000,
            "dom_mutation_wait": 10000,
            "auto_exit_fallback": 10000,
        },
    )


def _get_button_keywords_config() -> ButtonKeywordsConfigType:
    """ボタンキーワード設定を取得（共通ユーティリティへ委譲）"""
    return _shared_get_button_keywords_config()  # type: ignore


# ログレベル本番環境最適化
# セキュアロガーのセットアップ  
_raw_logger = logging.getLogger(__name__)
logger = get_secure_logger(__name__)


# 本番環境判定とログレベル最適化
def _should_log_detailed() -> bool:
    """詳細ログを出力すべきかを判定（本番環境最適化）"""
    env_mode = os.getenv("ENVIRONMENT", "production").lower()
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    return env_mode in ["development", "debug", "test"] or log_level in ["DEBUG"]


# ログレベル優先度判定
def _log_info_optimized(message: str, always_log: bool = False):
    """本番環境最適化版のINFOログ出力"""
    if always_log or _should_log_detailed():
        logger.info(message)
    else:
        logger.debug(message)  # 本番環境ではDEBUGレベルに降格


class KeywordMatcher:
    """キーワード検索の高速化クラス（正規表現事前コンパイル）"""

    def __init__(self, keywords: List[str]):
        """キーワードリストから正規表現パターンを事前コンパイル"""
        import re

        try:
            if not keywords:
                self.pattern = None
                return

            # 特殊文字をエスケープしてパターンを作成
            escaped_keywords = [re.escape(keyword) for keyword in keywords]
            pattern_string = "|".join(escaped_keywords)
            self.pattern = re.compile(pattern_string, re.IGNORECASE)

        except Exception as e:
            logger.error(f"Error compiling keyword pattern: {e}")
            self.pattern = None

    def match(self, text: str) -> bool:
        """テキスト内でのキーワードマッチング（高速）"""
        try:
            if not self.pattern or not text:
                return False
            return bool(self.pattern.search(text))
        except Exception as e:
            logger.error(f"Error during keyword matching: {e}")
            return False

    def find_match(self, text: str) -> Optional[str]:
        """最初にマッチしたキーワードを返す"""
        try:
            if not self.pattern or not text:
                return None
            match = self.pattern.search(text)
            if match:
                return match.group(0)
            return None
        except Exception as e:
            logger.error(f"Error finding keyword match: {e}")
            return None

    def find_matches(self, text: str) -> List[str]:
        """マッチしたキーワードのリストを返す"""
        try:
            if not self.pattern or not text:
                return []
            matches = self.pattern.findall(text)
            return list(set(matches))  # 重複除去
        except Exception as e:
            logger.error(f"Error finding keyword matches: {e}")
            return []


class FormSenderWorker:
    """フォーム送信ワーカー（GitHub Actions版・完全版）"""

    def __init__(self):
        self.playwright = None
        self.browser = None
        self.page = None
        self.supabase = None
        self.results = []
        self.errors = []
        self.recovery_manager = AutoRecoveryManager()

        # DOM検索結果のキャッシュ（パフォーマンス最適化）
        self._selector_cache = {}
        self._cache_max_age = 30  # 秒
        import time

        self._last_cache_clear = time.time()
        
        # パフォーマンス監視システム
        self.performance_monitor = PerformanceMonitor(
            warning_memory_mb=1024,
            critical_memory_mb=2048, 
            warning_cpu_percent=80.0,
            critical_cpu_percent=95.0,
            monitoring_interval=30.0,
            log_callback=lambda msg, data: logger.warning(msg, data)
        )

        # 設定ファイルから設定を読み込み
        try:
            form_sender_config = get_form_sender_config()
            self.timeout_settings = form_sender_config["timeout_settings"]
            self.text_processing = form_sender_config.get("text_processing", {})
            self.state_change_config = form_sender_config.get("state_change_judgment", {})
        except Exception as e:
            logger.warning(f"設定ファイルの読み込みに失敗しました、デフォルト値を使用します: {e}")
            # フォールバック用のデフォルト設定
            self.timeout_settings = {
                "page_load": 15000,
                "element_wait": 15000,
                "click_timeout": 5000,
                "input_timeout": 5000,
                "pre_processing_max": 30000,
                "dynamic_message_wait": 15000,
            }
            # テキスト処理のデフォルト設定
            self.text_processing = {
                "escape_sequences": {"\\n": "\n", "\\t": "\t", "\\r": "\r", "\\\\": "\\", '\\"': '"', "\\'": "'"},
                "target_fields": ["message", "subject"],
                "enable_nested_processing": True,
                "max_nesting_depth": 1,
            }

        # FORM_SENDER.md 4.3.3節準拠のキーワード設定
        # 文脈を考慮した失敗キーワード（単語単体を削除し、フレーズベースに変更）
        self.failure_keywords = [
            # 日本語失敗キーワード（基本）
            "送信に失敗",
            "送信できません",
            "送信エラー",
            "エラーが発生",
            "不正な入力",
            "入力してください",
            # 日本語失敗キーワード（拡充）
            "送信できませんでした",
            "送信が失敗",
            "必須項目です",
            "入力必須です",
            "未入力です",
            "選択してください",
            "正しく入力してください",
            "形式が正しくありません",
            "システムエラー",
            "サーバーエラー",
            "通信エラー",
            "アクセスできません",
            "許可されていません",
            "無効なアクセス",
            "ご入力ください",
            # 英語失敗キーワード（文脈付きフレーズ）
            "submission failed",
            "submission error",
            "form error",
            "validation error",
            "field required",
            "field is required",
            "is required",
            "please enter",
            "please select",
            "please fill",
            "invalid format",
            "invalid input",
            "format error",
            "connection failed",
            "server error",
            "system error",
            "access denied",
            "not allowed",
        ]

        self.success_keywords = [
            # 日本語成功キーワード（基本）
            "送信完了",
            "送信されました",
            "ありがとうございます",
            "受付完了",
            "お問い合わせを受け付けました",
            # 日本語成功キーワード（拡充）
            "お申込みありがとうございます",
            "資料請求ありがとうございます",
            "ご連絡ありがとうございます",
            "申込み完了",
            "登録完了",
            "受付いたしました",
            "承りました",
            "お預かりしました",
            "確認メールを送信",
            "後日ご連絡",
            "担当者から連絡",
            "申し込み完了",
            # 英語成功キーワード（基本）
            "thank",
            "sent",
            "success",
            "submitted",
            # 英語成功キーワード（拡充）
            "application received",
            "request submitted",
            "we will contact you",
            "confirmation sent",
            "thank you",
            "completed",
            "received",
            "application sent",
        ]

        self.error_classifications = {
            "INSTRUCTION": "instruction_json解析・プレースホルダ展開エラー",
            "ELEMENT": "フォーム要素検出・セレクタエラー",
            "INPUT": "フォーム入力・値設定エラー",
            "SUBMISSION": "フォーム送信・ネットワーク通信エラー",
            "BOT_DETECTION": "reCAPTCHA・Cloudflare等のBot検知エラー",
            "TIMEOUT": "タイムアウトエラー（サーバー応答・要素読み込み遅延）",
            "SYSTEM": "上記以外のシステムエラー・予期しないエラー",
        }

        # キーワードマッチャーの初期化（パフォーマンス最適化）
        self.failure_matcher = KeywordMatcher(self.failure_keywords)
        self.success_matcher = KeywordMatcher(self.success_keywords)
        _log_info_optimized(
            f"Keyword matchers initialized with {len(self.failure_keywords)} failure and {len(self.success_keywords)} success patterns"
        )

    async def initialize(self):
        """PlaywrightブラウザとSupabaseを初期化"""
        try:
            # Supabase初期化
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

            if not supabase_url or not supabase_key:
                raise ValueError("Supabase credentials not found in environment variables")

            self.supabase = create_client(supabase_url, supabase_key)
            logger.info("Supabase client initialized successfully")

            # Playwrightブラウザ初期化
            self.playwright = await async_playwright().start()

            # 環境変数でheadlessモードを制御（デフォルトは本番用のTrue）
            headless_mode = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true"
            _log_info_optimized(f"Playwright headless mode: {headless_mode}")

            self.browser = await self.playwright.chromium.launch(
                headless=headless_mode,
                args=[
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-web-security",
                ],
            )
            logger.info("Browser initialized successfully")
            
            # パフォーマンス監視開始
            await self.performance_monitor.start_monitoring()
            logger.info("Performance monitoring started")

        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            raise

    async def cleanup(self) -> None:
        """リソースクリーンアップ（安全性向上版）"""
        # ページクリーンアップ
        if hasattr(self, "page") and self.page:
            try:
                await self.page.close()
            except Exception as page_error:
                logger.warning(f"Page cleanup error: {page_error}")

        # ブラウザクリーンアップ
        if hasattr(self, "browser") and self.browser:
            try:
                await self.browser.close()
            except Exception as browser_error:
                logger.warning(f"Browser cleanup error: {browser_error}")

        # Playwrightクリーンアップ
        if hasattr(self, "playwright") and self.playwright:
            try:
                await self.playwright.stop()
            except Exception as playwright_error:
                logger.warning(f"Playwright cleanup error: {playwright_error}")
        
        # パフォーマンス監視停止
        try:
            await self.performance_monitor.stop_monitoring()
            logger.info("Performance monitoring stopped")
        except Exception as monitor_error:
            logger.warning(f"Performance monitor cleanup error: {monitor_error}")

    def get_client_data(self, config_file_path: str, targeting_id: int) -> Dict[str, Any]:
        """設定ファイルからクライアントデータを読み込み（2シート構造保持版）"""
        try:
            # 文字エンコーディングを検出して安全にファイルを読み込み
            config_data = self._safe_read_json_file(config_file_path)

            # 改行コードのデコード処理を追加
            config_data = self._decode_newlines_in_config(config_data)

            # 2シート構造の場合はそのまま保持、フラット構造の場合は下位互換性を維持
            if "targeting" in config_data and "client" in config_data:
                # Gas側の2シート構造をそのまま保持
                result_data = dict(config_data)
                # targeting_idを確実に設定（2シート構造）
                result_data["targeting"]["id"] = targeting_id
                SecurityLogger.safe_log_info(
                    f"Client data retrieved (2-sheet structure) for targeting_id: {targeting_id}"
                )
            else:
                # 下位互換性：フラット構造の場合は従来どおり2シート構造に変換
                unified_data = {"client": config_data.get("client", {}), "targeting": config_data.get("targeting", {})}
                # targeting_idを確実に設定
                unified_data["targeting"]["id"] = targeting_id
                result_data = unified_data
                SecurityLogger.safe_log_info(
                    f"Client data retrieved (flat structure converted) for targeting_id: {targeting_id}"
                )

            return result_data

        except Exception as e:
            logger.error(f"Error retrieving client data from config file: {e}")
            raise

    def _get_config_value(
        self, key: str, default: Any, expected_type: type, config_source: str = "state_change_config"
    ) -> Any:
        """設定値の型安全取得（型チェック付き）"""
        try:
            config_dict = getattr(self, config_source, {})
            value = config_dict.get(key, default)

            if not isinstance(value, expected_type):
                logger.warning(
                    f"Invalid config type for {key}: expected {expected_type.__name__}, got {type(value).__name__}, using default"
                )
                return default

            # 数値型の場合の範囲チェック
            if expected_type in (int, float):
                if expected_type == int and value < 0:
                    logger.warning(f"Invalid config value for {key}: {value} (negative integer), using default")
                    return default
                elif expected_type == float and (value < 0 or value > 1):
                    logger.warning(f"Invalid config value for {key}: {value} (out of range 0-1), using default")
                    return default

            # リスト型の場合の内容チェック
            if expected_type == list:
                if not all(isinstance(item, str) for item in value):
                    logger.warning(f"Invalid config list content for {key}: contains non-string items, using default")
                    return default
                if len(value) == 0:
                    logger.warning(f"Empty config list for {key}: using default")
                    return default

            return value

        except Exception as e:
            logger.error(f"Error retrieving config value for {key}: {e}, using default")
            return default

    def _validate_javascript_content(self, script_content: str, is_internal_script: bool = False) -> bool:
        """JavaScript実行内容の包括的セキュリティ検証（強化版）"""
        try:
            # 内部生成スクリプトは信頼済みとしてスキップ
            if is_internal_script:
                return True

            # Phase 1: 重大な脅威パターンの検出
            critical_patterns = [
                # コード実行関連（高リスク）
                r"eval\s*\(",
                r"Function\s*\(",
                r"setTimeout\s*\([^)]*[\"'][^\"']*[\"']\s*,",  # 文字列実行のsetTimeout
                r"setInterval\s*\([^)]*[\"'][^\"']*[\"']\s*,", # 文字列実行のsetInterval
                
                # 外部通信関連（高リスク）
                r"fetch\s*\(",
                r"XMLHttpRequest",
                r"navigator\.sendBeacon",
                r"WebSocket\s*\(",
                r"EventSource\s*\(",
                
                # DOM改ざん関連（中リスク）
                r"document\.write",
                r"innerHTML\s*=.*[<>]",  # HTML挿入の可能性
                r"outerHTML\s*=.*[<>]", # HTML挿入の可能性
                r"document\.head\.appendChild",
                r"document\.body\.appendChild",
                
                # データアクセス関連（中リスク）
                r"localStorage\s*\[",
                r"sessionStorage\s*\[",
                r"document\.cookie\s*=",
                r"window\.location\s*=",
                r"location\.href\s*=",
            ]

            # Phase 2: 疑わしいパターンの検出
            suspicious_patterns = [
                # 難読化の可能性
                r"\\x[0-9a-fA-F]{2}",      # 16進エスケープ
                r"\\u[0-9a-fA-F]{4}",      # Unicode エスケープ
                r"String\.fromCharCode",    # 文字コードからの文字列生成
                r"atob\s*\(",              # Base64デコード
                r"btoa\s*\(",              # Base64エンコード
                
                # プロトタイプ汚染の可能性
                r"__proto__",
                r"constructor\.prototype",
                r"Object\.prototype",
                
                # その他の危険な操作
                r"window\[",               # 動的プロパティアクセス
                r"this\[",                 # 動的プロパティアクセス
                r"arguments\[",            # 引数の動的アクセス
            ]

            import re

            # 重大な脅威の検出
            for pattern in critical_patterns:
                matches = re.findall(pattern, script_content, re.IGNORECASE)
                if matches:
                    logger.error(f"CRITICAL: Dangerous JavaScript pattern detected: {pattern} - Matches: {matches}")
                    return False

            # 疑わしいパターンの検出（複数検出で拒否）
            suspicious_count = 0
            detected_suspicious = []
            for pattern in suspicious_patterns:
                matches = re.findall(pattern, script_content, re.IGNORECASE)
                if matches:
                    suspicious_count += len(matches)
                    detected_suspicious.append(f"{pattern}: {len(matches)} matches")
            
            if suspicious_count >= 3:  # 疑わしいパターンが3つ以上で拒否
                logger.warning(f"SUSPICIOUS: Multiple suspicious patterns detected ({suspicious_count}): {detected_suspicious}")
                return False

            # Phase 3: コンテンツ制約の検証
            # スクリプトサイズの制限（8KB）
            if len(script_content) > 8192:
                logger.warning(f"JavaScript content too large: {len(script_content)} bytes (limit: 8KB)")
                return False

            # 改行数の制限（複雑すぎるスクリプトを拒否）
            line_count = script_content.count('\n') + 1
            if line_count > 100:
                logger.warning(f"JavaScript too complex: {line_count} lines (limit: 100)")
                return False

            # Phase 4: 構文チェック（基本レベル）
            # 括弧の対応チェック
            paren_count = script_content.count('(') - script_content.count(')')
            brace_count = script_content.count('{') - script_content.count('}')
            bracket_count = script_content.count('[') - script_content.count(']')
            
            if paren_count != 0 or brace_count != 0 or bracket_count != 0:
                logger.warning(f"JavaScript syntax error: Unbalanced brackets (paren:{paren_count}, brace:{brace_count}, bracket:{bracket_count})")
                return False

            logger.debug(f"JavaScript validation passed: {len(script_content)} bytes, {line_count} lines, {suspicious_count} suspicious patterns")
            return True

        except Exception as e:
            logger.error(f"Error validating JavaScript content: {e}")
            return False

    def _safe_read_json_file(self, file_path: str) -> Dict[str, Any]:
        """文字エンコーディング検出付きJSON読み込み"""
        try:
            import chardet

            encoding_detection_available = True
        except ImportError:
            logger.warning("chardet library not available, using fallback encoding detection")
            encoding_detection_available = False

        try:
            # まずUTF-8で読み込みを試行（最も一般的）
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    config_data = json.loads(content)
                    _log_info_optimized(f"Configuration file loaded: {file_path}")
                    return config_data
            except UnicodeDecodeError:
                pass  # UTF-8 reading failed, trying fallback encodings

            # UTF-8で読めない場合はエンコーディング検出
            with open(file_path, "rb") as f:
                raw_data = f.read()

                if encoding_detection_available:
                    detected = chardet.detect(raw_data)
                    encoding = detected.get("encoding", "utf-8")
                    confidence = detected.get("confidence", 0.0)

                    pass  # Encoding detected, proceeding with read

                    # 検出されたエンコーディングで再読み込み
                    try:
                        content = raw_data.decode(encoding)
                        config_data = json.loads(content)
                        _log_info_optimized(f"Successfully read file with detected encoding {encoding}: {file_path}")
                        return config_data
                    except (UnicodeDecodeError, json.JSONDecodeError) as e:
                        logger.warning(f"Failed to read with detected encoding {encoding}: {e}")
                else:
                    pass  # Using fallback encodings

            # 最終手段: 複数のエンコーディングを試行
            fallback_encodings = ["utf-8", "shift_jis", "euc-jp", "iso-2022-jp", "cp932", "latin1"]
            for enc in fallback_encodings:
                try:
                    content = raw_data.decode(enc)
                    config_data = json.loads(content)
                    logger.warning(f"File read with fallback encoding {enc}: {file_path}")
                    return config_data
                except (UnicodeDecodeError, json.JSONDecodeError):
                    continue

            # すべて失敗した場合
            raise ValueError(f"Unable to read file {file_path} with any supported encoding")

        except FileNotFoundError:
            raise FileNotFoundError(f"Config file not found: {file_path}")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON in file {file_path}: {e.msg}", e.doc, e.pos)
        except Exception as e:
            logger.error(f"Unexpected error reading file {file_path}: {e}")
            raise

    def _decode_newlines_in_config(self, config_data: Dict[str, Any]) -> Dict[str, Any]:
        """設定データ内の改行コードをデコードする（最適化版）"""
        try:
            # 設定ファイルから対象フィールドを取得
            target_fields = self.text_processing.get("target_fields", ["message", "subject"])
            enable_nested = self.text_processing.get("enable_nested_processing", True)
            max_depth = self.text_processing.get("max_nesting_depth", 1)
            processed_fields = []

            # ルートレベルの対象フィールドを処理
            for field in target_fields:
                if field in config_data and isinstance(config_data[field], str):
                    original_value = config_data[field]
                    decoded_value = self._decode_string_escapes(original_value)
                    config_data[field] = decoded_value
                    processed_fields.append(field)
                    pass  # Field decoded successfully

            # ネストした辞書内の対象フィールドを処理（設定に従う）
            if enable_nested and max_depth > 0:
                for key, value in config_data.items():
                    if isinstance(value, dict):
                        for field in target_fields:
                            if field in value and isinstance(value[field], str):
                                original_value = value[field]
                                decoded_value = self._decode_string_escapes(original_value)
                                value[field] = decoded_value
                                processed_fields.append(f"{key}.{field}")
                                pass  # Nested field decoded successfully

            if processed_fields:
                _log_info_optimized(f"Newline decoding completed for fields: {', '.join(processed_fields)}")
            else:
                pass  # No target fields for decoding

            return config_data

        except Exception as e:
            logger.error(f"Error decoding newlines in config: {e}")
            return config_data

    def _decode_string_escapes(self, text: str) -> str:
        """文字列のエスケープシーケンスをデコードする（設定ベース）"""
        try:
            # 設定ファイルからエスケープシーケンスを取得
            escape_sequences = self.text_processing.get(
                "escape_sequences", {"\\n": "\n", "\\t": "\t", "\\r": "\r", "\\\\": "\\", '\\"': '"', "\\'": "'"}
            )

            decoded = text
            # 設定に基づいてエスケープシーケンスをデコード
            for escape_seq, actual_char in escape_sequences.items():
                decoded = decoded.replace(escape_seq, actual_char)

            return decoded
        except Exception as e:
            logger.warning(f"Error decoding string escapes: {e}, returning original text")
            return text

    async def _perform_dynamic_content_loading(self):
        """動的コンテンツの読み込み待機（FORM_SENDER.md 4.1節準拠）"""
        try:
            # Step 1: 基本的なページ読み込み待機
            await self.page.wait_for_load_state("domcontentloaded", timeout=15000)
            await self.page.wait_for_load_state("load", timeout=15000)

            # Step 2: JavaScript実行完了待機
            timeout_config = _get_timeout_config()
            await asyncio.sleep(timeout_config.get("ajax_processing_wait", 3000) / 1000)  # Ajax処理の完了を待つ

            # Step 3: 段階的スクロールによる動的読み込み（SPA対応強化）
            await self._perform_staged_scrolling()

            # Step 4: フォーム要素の出現確認
            form_elements = await self.page.query_selector_all('form, input, textarea, select, button[type="submit"]')
            if form_elements:
                _log_info_optimized(f"Form elements detected: {len(form_elements)} elements found")
                return True

            logger.warning("No form elements detected after dynamic content loading")
            return False

        except Exception as e:
            logger.error(f"Error in dynamic content loading: {e}")
            return False

    async def _perform_staged_scrolling(self):
        """段階的スクロールによる動的読み込み"""
        try:
            # 3段階スクロール: 33% → 66% → 100%
            scroll_positions = [0.33, 0.66, 1.0]

            for position in scroll_positions:
                # スクロール実行
                await self.page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {position})")

                # 各段階での待機（lazy loading要素の出現を待つ）
                timeout_config = _get_timeout_config()
                await asyncio.sleep(timeout_config.get("ajax_processing_wait", 3000) / 1000)

                # フォーム要素の出現チェック
                form_elements = await self.page.query_selector_all("form, input, textarea")
                if form_elements:
                    pass  # Form elements found at this scroll position
                    return  # 早期終了

        except Exception as e:
            logger.warning(f"Error in staged scrolling: {e}")

    async def _fill_form_field(self, field_name: str, field_config: FieldConfigType) -> None:
        """フォームフィールドへの入力実行（柔軟性向上版）"""
        try:
            # 1. フィールド設定の検証
            selector, input_type, value, required = self._validate_field_config(field_name, field_config)
            if selector is None:  # スキップ対象
                return

            # 2. セレクタの解決（フォールバック機能付き）
            final_selector = await self._resolve_field_selector(field_name, selector, input_type, required)
            if final_selector is None:  # 要素が見つからない
                return

            # 3. 入力実行
            await self._execute_field_input(field_name, final_selector, input_type, value)
            logger.debug(f"Field {field_name} successfully processed")

        except Exception as e:
            logger.error(f"Error filling field {field_name}: {e}")
            raise

    def _validate_field_config(
        self, field_name: str, field_config: FieldConfigType
    ) -> Tuple[Optional[str], str, Union[str, bool], bool]:
        """フィールド設定の検証"""
        selector = field_config.get("selector")
        input_type = field_config.get("input_type", "text")
        value = field_config.get("value", "")
        required = field_config.get("required", False)

        if not selector:
            if required:
                raise Exception(f"Required field {field_name} has no selector")
            logger.warning(f"Optional field {field_name} has no selector, skipping")
            return None, input_type, value, required  # スキップ対象

        return selector, input_type, value, required

    async def _resolve_field_selector(
        self, field_name: str, selector: str, input_type: str, required: bool
    ) -> Optional[str]:
        """セレクタの解決（フォールバック機能付き）"""
        try:
            # 通常セレクタでの要素検索
            await self.page.wait_for_selector(selector, timeout=5000)
            logger.debug(f"Field {field_name} found with selector: {selector}")
            return selector
        except PlaywrightTimeoutError:
            # フォールバック処理
            fallback_selector = await self._handle_selector_fallback(field_name, selector, input_type)
            if fallback_selector:
                return fallback_selector

            # 最終的に見つからない場合の処理
            if required:
                raise Exception(f"Required field {field_name} not found: {selector}")
            else:
                logger.warning(f"Optional field {field_name} not found: {selector}")
                return None

    async def _handle_selector_fallback(
        self, field_name: str, original_selector: str, input_type: str
    ) -> Optional[str]:
        """セレクタのフォールバック処理"""
        # 特定のフィールドタイプに対してフォールバック処理を実行
        if "ご希望の連絡方法" in original_selector and input_type == "checkbox":
            fallback_selectors = ["input[name='userData[ご希望の連絡方法][]']", "input[name*='連絡方法']", "input[name*='contact']"]

            for fallback in fallback_selectors:
                try:
                    elements_count = await self.page.locator(fallback).count()
                    if elements_count > 0:
                        # nth-of-type(2)の場合は2番目の要素を選択
                        if ":nth-of-type(2)" in original_selector:
                            if elements_count >= 2:
                                final_selector = f"{fallback}:nth-of-type(2)"
                                logger.info(f"Field {field_name} found with fallback: {final_selector}")
                                return final_selector
                        else:
                            # 最初の要素を選択
                            final_selector = f"{fallback}:first-of-type"
                            logger.info(f"Field {field_name} found with fallback: {final_selector}")
                            return final_selector
                except:
                    continue

        # 汎用的なフォールバック処理
        return await self._generic_selector_fallback(field_name, original_selector, input_type)

    async def _generic_selector_fallback(
        self, field_name: str, original_selector: str, input_type: str
    ) -> Optional[str]:
        """汎用的なセレクタフォールバック処理（強化版）"""
        fallback_patterns = []

        # フィールド名から推測されるパターンを生成
        field_variations = self._generate_field_variations(field_name)

        # セレクタタイプ別のフォールバック
        if "name=" in original_selector:
            # name属性ベース
            for variation in field_variations:
                fallback_patterns.extend(
                    [
                        f"input[name='{variation}']",
                        f"input[name*='{variation}']",
                        f"textarea[name='{variation}']",
                        f"select[name='{variation}']",
                        f"[name='{variation}']",
                    ]
                )

        if "id=" in original_selector:
            # id属性ベース
            for variation in field_variations:
                fallback_patterns.extend(
                    [f"#{variation}", f"input#{variation}", f"[id='{variation}']", f"[id*='{variation}']"]
                )

        # class属性ベース
        for variation in field_variations:
            fallback_patterns.extend([f".{variation}", f"input.{variation}", f"[class*='{variation}']"])

        # placeholder属性ベース
        for variation in field_variations:
            fallback_patterns.extend([f"input[placeholder*='{variation}']", f"textarea[placeholder*='{variation}']"])

        # セレクタを試行
        for pattern in fallback_patterns:
            try:
                count = await self.page.locator(pattern).count()
                if count > 0:
                    logger.info(f"Field {field_name} found with fallback selector: {pattern}")
                    return pattern
            except Exception:
                continue

        # 最後のフォールバック：input type別
        type_selectors = {
            "text": 'input[type="text"]:not([name]):not([id])',
            "email": 'input[type="email"], input[type="mail"]',
            "tel": 'input[type="tel"]',
            "textarea": "textarea:not([name]):not([id])",
            "select": "select:not([name]):not([id])",
            "checkbox": 'input[type="checkbox"]:not([name]):not([id])',
            "radio": 'input[type="radio"]:not([name]):not([id])',
        }

        if input_type in type_selectors:
            try:
                count = await self.page.locator(type_selectors[input_type]).count()
                if count > 0:
                    logger.info(f"Field {field_name} found with type-based fallback: {type_selectors[input_type]}")
                    return type_selectors[input_type]
            except Exception:
                pass

        return None

    def _generate_field_variations(self, field_name: str) -> list[str]:
        """フィールド名のバリエーションを生成"""
        variations = [field_name]

        # 英日変換マップ
        translations = {
            "name": ["name", "お名前", "氏名", "nama", "fullname"],
            "email": ["email", "メール", "mail", "e-mail", "eメール"],
            "phone": ["phone", "電話", "tel", "telephone", "電話番号"],
            "company": ["company", "会社", "会社名", "corp", "corporation"],
            "message": ["message", "メッセージ", "内容", "content", "inquiry"],
            "privacy": ["privacy", "プライバシー", "個人情報", "consent", "同意"],
            "contact": ["contact", "連絡", "renraku"],
        }

        field_lower = field_name.lower()
        for eng, variations_list in translations.items():
            if eng in field_lower:
                variations.extend(variations_list)
                break

        # 一般的な変換パターン
        variations.append(field_name.replace("_", "-"))
        variations.append(field_name.replace("-", "_"))
        variations.append(field_name.replace(" ", ""))
        variations.append(field_name.replace(" ", "_"))

        # 重複を除去して返す
        return list(set(variations))

    async def _execute_field_input(
        self, field_name: str, selector: str, input_type: str, value: Union[str, bool]
    ) -> None:
        """入力タイプ別の実際の入力処理"""
        if input_type in ["text", "email", "tel"]:
            await self.page.fill(selector, str(value))
            logger.debug(f"Field {field_name} filled with text: {value}")
        elif input_type == "textarea":
            await self.page.fill(selector, str(value))
            logger.debug(f"Field {field_name} filled with textarea: {value}")
        elif input_type == "select":
            await self.page.select_option(selector, str(value))
            logger.debug(f"Field {field_name} selected option (value redacted)")
        elif input_type == "checkbox":
            await self._handle_checkbox_input(field_name, selector, value)
        elif input_type == "radio":
            await self.page.check(selector)
            logger.debug(f"Field {field_name} radio selected")

    async def _handle_checkbox_input(self, field_name: str, selector: str, value: Union[str, bool]) -> None:
        """チェックボックス専用の入力処理"""
        if isinstance(value, bool):
            if value:
                await self.page.check(selector)
                logger.debug(f"Field {field_name} checked")
            else:
                await self.page.uncheck(selector)
                logger.debug(f"Field {field_name} unchecked")
        elif str(value).lower() in ["true", "1", "yes", "on"]:
            await self.page.check(selector)
            logger.debug(f"Field {field_name} checked")
        else:
            await self.page.uncheck(selector)
            logger.debug(f"Field {field_name} unchecked")

    async def _submit_form(self, submit_config: Dict[str, Any]) -> bool:
        """フォーム送信実行と3段階判定による成功判定（2段階フォーム対応版）

        FORM_SENDER.md 4.3節に準拠した送信処理。直接送信と確認ページ経由の
        2つのパターンを自動判定し、適切な処理フローを実行する。

        Args:
            submit_config: 送信ボタン設定（selector, method等）

        Returns:
            bool: 送信成功の場合True、失敗の場合False

        Raises:
            Exception: 送信処理中の予期しないエラー
        """
        try:
            # 1. 送信ボタンの準備
            final_selector = await self._prepare_submit_button(submit_config)
            if not final_selector:
                return False

            # 2. プライバシー同意チェックの強制（マッピングと独立）
            try:
                if final_selector:
                    btn = self.page.locator(final_selector)
                    await PrivacyConsentHandler.ensure_near_button(self.page, btn, context_hint="submit")
            except Exception as _consent_err:
                logger.debug(f"Privacy consent ensure near submit failed (continue): {_consent_err}")

            # 3. 送信前の状態記録
            pre_submit_state = await self._capture_page_state()

            # 4. SuccessJudgeの送信前初期化
            await self.success_judge.initialize_before_submission()

            # 5. レスポンス監視の設定
            response_data = self._setup_response_listener(self.page.url)

            try:
                # 6. 送信パターンの判定と実行
                return await self._handle_submission_patterns(final_selector, response_data, pre_submit_state)
            finally:
                # 7. リスナークリーンアップ
                self._cleanup_response_listener(response_data.get("listener"))

        except Exception as e:
            logger.error(f"Error in form submission: {e}")
            return False

    async def _prepare_submit_button(self, submit_config: Dict[str, Any]) -> Optional[str]:
        """送信ボタンの準備（候補優先 + セレクタ検出とフォールバック）"""
        # 解析結果から供給された候補を優先
        selector_candidates = submit_config.get("selector_candidates") or []
        if selector_candidates:
            logger.info("Trying analyzer-provided submit selector candidates first")
            for cand in selector_candidates:
                try:
                    await self.page.wait_for_selector(cand, timeout=self.timeout_settings.get("element_wait", 15000))
                    logger.debug(f"Submit button found via analyzer candidate: {cand}")
                    return cand
                except Exception:
                    continue

        selector = submit_config.get("selector")

        # セレクタがnullまたは空の場合はデフォルトセレクタを使用
        if not selector:
            logger.info("No explicit submit selector; using default/fallback selectors")
            selector = 'button[type="submit"], input[type="submit"]'

        # 設定ベースのタイムアウト制御
        button_timeout = self.timeout_settings.get("element_wait", 15000)

        # 送信ボタンの存在確認（フォールバック機能付き）
        try:
            await self.page.wait_for_selector(selector, timeout=button_timeout)
            logger.debug(f"Submit button found with selector: {selector}")
            return selector
        except PlaywrightTimeoutError:
            logger.warning(f"Submit button not found with primary selector: {selector}")
            return await self._try_fallback_selectors()

    async def _try_fallback_selectors(self) -> Optional[str]:
        """フォールバックセレクタの試行"""
        fallback_selectors = [
            'button[type="submit"]',
            'input[type="submit"]',
            'button:has-text("送信")',
            'button:has-text("Submit")',
            "form button:first-of-type",
            "#submit",
            ".submit",
        ]

        for fallback_selector in fallback_selectors:
            try:
                await self.page.wait_for_selector(fallback_selector, timeout=3000)
                logger.debug(f"Submit button found with fallback selector: {fallback_selector}")
                return fallback_selector
            except PlaywrightTimeoutError:
                continue

        logger.error("Submit button not found with any selector")
        return None

    async def _wait_until_clickable(self, selector: str, timeout_ms: int) -> bool:
        """指定セレクタの要素が可視・有効になり、クリック可能になるまで待機する"""
        import time
        deadline = time.time() + (timeout_ms / 1000)
        locator = self.page.locator(selector).first

        # 要素がDOMに現れるまで待機
        try:
            await locator.wait_for(state="attached", timeout=timeout_ms)
        except Exception:
            return False

        while time.time() < deadline:
            try:
                await locator.scroll_into_view_if_needed()
            except Exception:
                pass
            try:
                visible = await locator.is_visible()
                enabled = await locator.is_enabled()
                # 一部サイトは disabled 属性のトグルで管理しているため属性も確認
                try:
                    has_disabled_attr = bool(await locator.get_attribute("disabled"))
                except Exception:
                    has_disabled_attr = False

                if visible and enabled and not has_disabled_attr:
                    return True
            except Exception:
                # 存在しない/取得失敗 → 短い待機後に再試行
                pass
            await asyncio.sleep(0.2)
        return False

    async def _wait_until_element_clickable(self, element, timeout_ms: int) -> bool:
        """ElementHandleがクリック可能になるまで待機する（可視・有効・disabled属性なし）"""
        import time
        deadline = time.time() + (timeout_ms / 1000)
        try:
            await element.wait_for_element_state("visible", timeout=timeout_ms)
        except Exception:
            return False
        while time.time() < deadline:
            try:
                try:
                    await element.scroll_into_view_if_needed()
                except Exception:
                    pass
                visible = await element.is_visible()
                enabled = await element.is_enabled()
                try:
                    has_disabled_attr = bool(await element.get_attribute("disabled"))
                except Exception:
                    has_disabled_attr = False
                if visible and enabled and not has_disabled_attr:
                    return True
            except Exception:
                pass
            await asyncio.sleep(0.2)
        return False

    def _setup_response_listener(self, pre_submit_url: str) -> ResponseDataType:
        """HTTPレスポンス監視の設定"""
        response_data = {
            "status_code": None,
            "redirects": [],
            "post_requests": [],
            "response_times": [],
            "listener": None,
        }

        import time

        start_time = time.time()

        def handle_response(response) -> None:
            response_time = time.time() - start_time

            # POSTリクエストまたは関連URL
            if response.request.method == "POST" or response.url.startswith(
                pre_submit_url[: pre_submit_url.rfind("/") + 1]
            ):
                response_data["status_code"] = response.status
                response_data["response_times"].append(response_time)

                if response.request.method == "POST":
                    response_data["post_requests"].append(
                        {"status": response.status, "url": response.url, "time": response_time}
                    )

                if 300 <= response.status < 400:
                    response_data["redirects"].append(
                        {"status": response.status, "url": response.url, "time": response_time}
                    )

        response_data["listener"] = handle_response
        self.page.on("response", handle_response)

        return response_data

    async def _handle_submission_patterns(
        self, final_selector: str, response_data: Dict[str, Any], pre_submit_state: Dict[str, Any]
    ) -> bool:
        """送信パターンの判定と実行"""
        click_timeout = self.timeout_settings.get("click_timeout", 5000)

        try:
            # ボタンテキストを取得してパターン判定（要素全体を解析）
            button_element_text = await self._get_button_element_text(final_selector)
            button_type = await self._determine_button_type(button_element_text)

            logger.info(f"Button element text: '{button_element_text}', button_type: {button_type}")

            # 事前にクリック可能になるまで待機（disabled解除待ち等）
            try:
                clickable = await self._wait_until_clickable(final_selector, max(click_timeout, 5000))
                if not clickable:
                    logger.warning("Submit button did not become clickable within timeout; trying click anyway")
            except Exception as _:
                pass

            # ボタン押下（JavaScript実行フォールバック付き）
            success = await self._execute_submit_button_click(final_selector, click_timeout)
            if not success:
                logger.error(f"Submit button click failed for selector: {final_selector}")
                return False

            # 確認ボタンの場合は確認ページ経由パターンを実行
            if button_type == "confirmation":
                return await self._handle_confirmation_page_pattern(response_data, pre_submit_state, final_selector)

            # 直接送信パターン：動的コンテンツとレスポンスの監視
            mutation_result = await self._wait_for_submission_response_with_mutation()

            # レスポンス監視結果のログ出力
            self._log_submission_results(response_data)

            # 4段階判定システムによる成功判定
            return await self._execute_four_stage_judgment(response_data, pre_submit_state, mutation_result)

        except asyncio.TimeoutError:
            logger.error(f"Submit button click timeout after {click_timeout}ms")
            return False
        except Exception as click_error:
            logger.error(f"Error during submit button click: {click_error}")
            return False

    def _log_submission_results(self, response_data: Dict[str, Any]) -> None:
        """送信結果のログ出力（最小限）"""
        if response_data["post_requests"]:
            _log_info_optimized(f"Form submission detected: {len(response_data['post_requests'])} requests")
        if response_data["redirects"]:
            _log_info_optimized(f"Redirects detected: {len(response_data['redirects'])}")

    def _cleanup_response_listener(self, response_listener) -> None:
        """レスポンスリスナーのクリーンアップ"""
        if response_listener and hasattr(self, "page") and self.page:
            try:
                self.page.remove_listener("response", response_listener)
            except Exception as cleanup_error:
                logger.warning(f"Error during response listener cleanup: {cleanup_error}")

    def _clear_selector_cache_if_needed(self) -> None:
        """必要に応じてセレクタキャッシュをクリア"""
        import time

        current_time = time.time()
        if current_time - self._last_cache_clear > self._cache_max_age:
            self._selector_cache.clear()
            self._last_cache_clear = current_time
            logger.debug("Selector cache cleared due to age")

    async def _cached_query_selector_all(self, selector: str) -> List[Any]:
        """キャッシュ機能付きのquery_selector_all（冗長なDOM検索の排除）"""
        cache_key = f"selector_{selector}_{self.page.url}"

        # キャッシュのクリアをチェック
        self._clear_selector_cache_if_needed()

        # キャッシュから取得を試行
        if cache_key in self._selector_cache:
            cached_result = self._selector_cache[cache_key]
            logger.debug(f"Using cached result for selector: {selector}")
            return cached_result

        # キャッシュにない場合は実際にDOM検索を実行
        try:
            elements = await self.page.query_selector_all(selector)
            # 結果をキャッシュに保存（最大100件まで）
            if len(self._selector_cache) < 100:
                self._selector_cache[cache_key] = elements
                logger.debug(f"Cached result for selector: {selector}")
            return elements
        except Exception as e:
            logger.warning(f"Error in cached query selector: {e}")
            return []

    # 自動復旧機能付き企業処理
    async def process_single_company(
        self, company: Dict[str, Any], client_data: Dict[str, Any], targeting_id: int
    ) -> Dict[str, Any]:
        """単一企業の処理（自動復旧機能付き）"""
        record_id = company.get("id")
        form_url = company.get("form_url")

        # 旧互換: URL欠落時は従来の分類（INSTRUCTION）を維持して下流互換を確保
        if not form_url:
            return {
                "record_id": record_id,
                "status": "failed",
                "error_type": "INSTRUCTION",
                "submitted_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
                "instruction_valid_updated": True,
            }

        # 自動復旧機能付きで処理実行
        return await self._process_with_auto_recovery(company, client_data, targeting_id)

    async def _process_with_auto_recovery(
        self, company: Dict[str, Any], client_data: Dict[str, Any], targeting_id: int
    ) -> Dict[str, Any]:
        """自動復旧機能付き企業処理（無限ループ防止版）"""
        record_id = company.get("id")
        form_url = company.get("form_url")

        last_result = None

        # リトライ設定を設定ファイルから取得
        try:
            retry_config = get_retry_config_for("form_analysis")
            max_retries = retry_config["max_retries"]
        except Exception as e:
            logger.warning(f"リトライ設定の読み込みに失敗、デフォルト値(3)を使用: {e}")
            max_retries = 3
        retry_count = 0
        start_time = time.time()
        max_processing_time = 30  # 最大処理時間（秒）

        # メイン処理 + 復旧試行ループ（安全装置付き）
        while retry_count <= max_retries:
            try:
                result = await self._execute_single_company_core(company, client_data, targeting_id)

                # 成功時は復旧カウントをリセット
                if result.get("status") == "success":
                    self.recovery_manager.reset_recovery_count()

                return result

            except Exception as e:
                error_message = str(e)
                logger.error(f"Company {record_id} processing error: {error_message}")

                # エラーの分類
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

                last_result = {
                    "record_id": record_id,
                    "status": "failed",
                    "error_type": error_type,
                    "submitted_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
                    "instruction_valid_updated": ErrorClassifier.should_update_instruction_valid(error_type),
                }

                # 復旧可能なエラーかチェック
                if ErrorClassifier.is_recoverable_error(error_type, error_message):
                    if self.recovery_manager.can_attempt_recovery():
                        logger.info(
                            f"Attempting auto-recovery for company {record_id}"
                        )  # Recovery attempts always logged
                        self.recovery_manager.mark_recovery_attempt()

                        # 時間制限チェック
                        elapsed_time = time.time() - start_time
                        if elapsed_time > max_processing_time:
                            logger.warning(
                                f"Processing time limit exceeded for company {record_id}: {elapsed_time:.1f}s"
                            )
                            last_result["error_type"] = "TIMEOUT"
                            return last_result

                        # 復旧処理実行
                        recovery_success = await self._attempt_recovery(error_type, error_message)
                        if recovery_success:
                            retry_count += 1  # リトライカウンター更新
                            logger.info(
                                f"Recovery successful for company {record_id}, retrying... (attempt {retry_count}/{max_retries})"
                            )
                            continue  # リトライ
                        else:
                            logger.warning(f"Recovery failed for company {record_id}")
                    else:
                        logger.warning(f"Cannot attempt recovery for company {record_id}")

                # 復旧不可能または復旧失敗の場合は結果を返す
                break

        # ループ終了（最大リトライ数到達またはタイムアウト）
        if last_result:
            logger.error(f"Company {record_id} processing failed after {retry_count} attempts")
            return last_result
        else:
            # 予期しない状況（通常は発生しない）
            return {
                "record_id": record_id,
                "status": "failed",
                "error_type": "SYSTEM",
                "submitted_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
                "instruction_valid_updated": False,
            }

    def _is_recoverable_error(self, error_type: str, error_message: str) -> bool:
        """復旧可能なエラーかどうか判定（更新版）"""
        # 復旧可能なエラータイプ（新しい分類に対応）
        recoverable_types = ["SYSTEM", "TIMEOUT", "ELEMENT_EXTERNAL", "INPUT_EXTERNAL", "ACCESS"]

        if error_type not in recoverable_types:
            return False

        # 特定のエラーメッセージパターンは復旧不可能
        non_recoverable_patterns = ["instruction_valid", "placeholder", "json decode", "invalid selector", "malformed"]

        if any(pattern in error_message.lower() for pattern in non_recoverable_patterns):
            return False

        return True

    async def _attempt_recovery(self, error_type: str, error_message: str) -> bool:
        """復旧処理を実行（更新版）"""
        try:
            logger.info(f"Starting recovery for error type: {error_type}")

            if error_type == "SYSTEM":
                # システムエラーの復旧
                return await self._recover_from_system_error()

            elif error_type == "TIMEOUT":
                # タイムアウトエラーの復旧
                return await self._recover_from_timeout_error()

            elif error_type in ["ELEMENT_EXTERNAL", "INPUT_EXTERNAL"]:
                # 外部要因による要素/入力エラーの復旧
                return await self._recover_from_external_error()

            elif error_type == "ACCESS":
                # アクセスエラーの復旧
                return await self._recover_from_access_error()

            return False

        except Exception as e:
            logger.error(f"Recovery process failed: {e}")
            return False

    async def _recover_from_system_error(self) -> bool:
        """システムエラーからの復旧"""
        try:
            logger.info("Attempting system error recovery...")

            # ブラウザの完全再初期化
            await self._reinitialize_browser()

            # 短い待機
            timeout_config = _get_timeout_config()
            await asyncio.sleep(timeout_config.get("form_submission_wait", 5000) / 1000)

            logger.info("System error recovery completed")
            return True

        except Exception as e:
            logger.error(f"System error recovery failed: {e}")
            return False

    async def _recover_from_timeout_error(self) -> bool:
        """タイムアウトエラーからの復旧"""
        try:
            logger.info("Attempting timeout error recovery...")

            # タイムアウト設定を増加
            original_settings = self.timeout_settings.copy()
            self.timeout_settings = {k: v * 1.5 for k, v in self.timeout_settings.items()}

            # ページクリーンアップ
            if self.page:
                await self.page.close()
                self.page = None

            # 待機時間を増加
            timeout_config = _get_timeout_config()
            await asyncio.sleep(timeout_config.get("dom_mutation_wait", 10000) / 1000)

            logger.info("Timeout error recovery completed")
            return True

        except Exception as e:
            logger.error(f"Timeout error recovery failed: {e}")
            # タイムアウト設定を元に戻す
            self.timeout_settings = original_settings
            return False

    async def _recover_from_element_error(self) -> bool:
        """要素検出エラーからの復旧"""
        try:
            logger.info("Attempting element error recovery...")

            # ページをリフレッシュ
            if self.page:
                await self.page.reload()
                timeout_config = _get_timeout_config()
                await asyncio.sleep(timeout_config.get("page_load_wait", 3000) / 1000)

            logger.info("Element error recovery completed")
            return True

        except Exception as e:
            logger.error(f"Element error recovery failed: {e}")
            return False

    async def _recover_from_external_error(self) -> bool:
        """外部要因による要素/入力エラーからの復旧"""
        try:
            logger.info("Attempting external error recovery (page refresh)...")

            # ページをリフレッシュして動的コンテンツを再読み込み
            if self.page:
                await self.page.reload(timeout=10000)
                # 動的コンテンツの読み込み待機
                await asyncio.sleep(2)
                try:
                    await self.page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass  # タイムアウトしても続行

            logger.info("External error recovery completed")
            return True

        except Exception as e:
            logger.error(f"External error recovery failed: {e}")
            return False

    async def _recover_from_access_error(self) -> bool:
        """アクセスエラーからの復旧"""
        try:
            logger.info("Attempting access error recovery (short wait)...")

            # 短時間待機してリトライ
            await asyncio.sleep(1)

            logger.info("Access error recovery completed")
            return True

        except Exception as e:
            logger.error(f"Access error recovery failed: {e}")
            return False

    async def _reinitialize_browser(self):
        """ブラウザの完全再初期化"""
        try:
            # 既存リソースをクリーンアップ
            if self.page:
                await self.page.close()
                self.page = None
            if self.browser:
                await self.browser.close()
                self.browser = None
            if self.playwright:
                await self.playwright.stop()
                self.playwright = None

            # 新しいブラウザセッションを開始
            self.playwright = await async_playwright().start()

            # 環境変数でheadlessモードを制御（デフォルトは本番用のTrue）
            headless_mode = os.getenv("PLAYWRIGHT_HEADLESS", "true").lower() == "true"

            self.browser = await self.playwright.chromium.launch(
                headless=headless_mode, args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"]
            )

            logger.info("Browser reinitialized successfully")

        except Exception as e:
            logger.error(f"Browser reinitialization failed: {e}")
            raise

    async def _execute_single_company_core(
        self, company: Dict[str, Any], client_data: Dict[str, Any], targeting_id: int
    ) -> Dict[str, Any]:
        """企業処理のコア実装（責任分離版）"""
        record_id = company.get("id")

        try:
            # Step 1: 旧instruction_json処理は廃止。ルールベースのみで実行。
            expanded_instruction = {"instruction": {}}  # 後方互換のため空dictを渡す

            # Step 2: ブラウザページ初期化とアクセス
            access_result = await self._initialize_and_access_page(company)
            if "error" in access_result:
                return access_result

            # Step 3: Bot検知チェック
            bot_check_result = await self._check_bot_detection(record_id)
            if "error" in bot_check_result:
                return bot_check_result

            # Step 4: フォーム入力実行
            form_input_result = await self._execute_form_input(expanded_instruction["instruction"], record_id)
            if "error" in form_input_result:
                return form_input_result

            # Step 5: フォーム送信と結果判定
            submit_result = await self._execute_form_submission(expanded_instruction["instruction"], record_id)

            await self.page.close()
            return submit_result

        except Exception as e:
            if self.page:
                await self.page.close()
            raise

    async def _process_instruction(self, company: Dict[str, Any], client_data: Dict[str, Any]) -> Dict[str, Any]:
        """[互換用] instruction_json処理は廃止。ルールベース統一に伴い空の指示を返す。"""
        return {"instruction": {}}

    async def _initialize_and_access_page(self, company: Dict[str, Any]) -> Dict[str, Any]:
        """ブラウザページの初期化とアクセス"""
        form_url = company.get("form_url")

        # ページ作成と設定
        self.page = await self.browser.new_page()
        await self.page.set_viewport_size({"width": 1920, "height": 1080})
        await self.page.set_extra_http_headers({"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"})

        # Cookieブロッカーは成功率低下の恐れがあるため未使用（2025-09-04 方針）

        # SuccessJudgeの初期化
        self.success_judge = SuccessJudge(self.page)

        # ページアクセス
        await self.page.goto(form_url, timeout=self.timeout_settings["page_load"])

        # 動的コンテンツ対応の短時間待機
        timeout_config = _get_timeout_config()
        await asyncio.sleep(timeout_config.get("ajax_processing_wait", 3000) / 1000)

        # Cookie同意バナーの自動処理
        await CookieConsentHandler.handle(self.page)

        return {"status": "success"}

    async def _check_bot_detection(self, record_id: int) -> Dict[str, Any]:
        """Bot検知チェック"""
        is_bot_detected, bot_type = await BotDetectionSystem.detect_bot_protection(self.page)
        if is_bot_detected:
            await self.page.close()
            return {
                "error": True,
                "record_id": record_id,
                "status": "failed",
                "error_type": "BOT_DETECTION",
                "submitted_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
                "instruction_valid_updated": False,
                "bot_protection_detected": True,
            }

        return {"status": "success"}

    async def _execute_form_input(self, expanded_instruction: Dict[str, Any], record_id: int) -> Dict[str, Any]:
        """
        フォーム入力実行 - ルールベースシステム版
        instruction_jsonに依存せず、動的にフォーム要素を解析・入力
        """
        logger.info(f"Starting rule-based form input for record_id: {record_id}")
        
        try:
            # ルールベース解析システムの初期化
            from ..analyzer.rule_based_analyzer import RuleBasedAnalyzer
            
            analyzer = RuleBasedAnalyzer(self.page)
            
            # クライアントデータの準備
            client_data = self.client_data if hasattr(self, 'client_data') else {}
            
            # 動的コンテンツ読み込み待機
            await self._perform_dynamic_content_loading()
            
            # Phase 1: フォーム全体の解析実行
            logger.info("Executing comprehensive form analysis...")
            analysis_result = await analyzer.analyze_form(client_data)
            
            if not analysis_result.get('success', False):
                error_msg = analysis_result.get('error', 'Form analysis failed')
                logger.error(f"Form analysis failed: {error_msg}")
                return {
                    "error": True,
                    "record_id": record_id,
                    "status": "failed",
                    "error_type": "FORM_ANALYSIS_FAILED",
                    "submitted_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
                    "instruction_valid_updated": False,
                    "analysis_error": error_msg
                }
            
            # 解析結果の詳細ログ
            summary = analysis_result.get('analysis_summary', {})
            logger.info(f"Form analysis completed: {summary.get('mapping_coverage', 'N/A')} coverage, "
                       f"{summary.get('mapped_fields', 0)} mapped fields, "
                       f"{summary.get('auto_handled_fields', 0)} auto-handled fields")

            # 送信ボタン候補を保存（instruction_json 廃止に伴う後方互換代替）
            try:
                submit_buttons = analysis_result.get('submit_buttons', []) or []
                # selector のみ抽出し順序を保持（検出順＝優先度）
                self._submit_button_candidates = [b.get('selector') for b in submit_buttons if b.get('selector')]
                logger.info(
                    f"Detected {len(self._submit_button_candidates)} submit button candidates via analyzer"
                )
            except Exception as submit_err:
                self._submit_button_candidates = []
                logger.warning(f"Failed to capture analyzer submit buttons: {submit_err}")
            
            # Phase 2: ルールベース入力実行
            input_assignments = analysis_result.get('input_assignments', {})
            if not input_assignments:
                logger.warning("No input assignments found from form analysis")
                return {
                    "error": True,
                    "record_id": record_id,
                    "status": "failed", 
                    "error_type": "NO_INPUT_ASSIGNMENTS",
                    "submitted_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
                    "instruction_valid_updated": False
                }
            
            # 入力実行結果の追跡
            input_results = {
                'successful_inputs': 0,
                'failed_inputs': 0,
                'skipped_inputs': 0,
                'field_results': {}
            }
            
            # 各フィールドへの入力実行
            for field_name, input_config in input_assignments.items():
                try:
                    input_result = await self._execute_rule_based_input(field_name, input_config)
                    input_results['field_results'][field_name] = input_result
                    
                    if input_result['success']:
                        input_results['successful_inputs'] += 1
                        logger.debug(f"Successfully input field: {field_name}")
                    elif input_result.get('skipped', False):
                        input_results['skipped_inputs'] += 1
                        logger.debug(f"Skipped field: {field_name} - {input_result.get('reason', '')}")
                    else:
                        input_results['failed_inputs'] += 1
                        logger.warning(f"Failed to input field: {field_name} - {input_result.get('error', '')}")
                        
                        # 必須フィールドの失敗は全体エラーとする
                        if input_config.get('required', False):
                            logger.error(f"Required field '{field_name}' input failed, aborting")
                            return {
                                "error": True,
                                "record_id": record_id,
                                "status": "failed",
                                "error_type": "REQUIRED_FIELD_ERROR", 
                                "submitted_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
                                "instruction_valid_updated": False,
                                "failed_field": field_name,
                                "field_error": input_result.get('error', '')
                            }
                            
                except Exception as field_error:
                    logger.error(f"Exception during input for field '{field_name}': {field_error}")
                    input_results['failed_inputs'] += 1
                    input_results['field_results'][field_name] = {
                        'success': False,
                        'error': str(field_error),
                        'exception': True
                    }
                    
                    # 必須フィールドの例外は全体エラーとする
                    if input_config.get('required', False):
                        return {
                            "error": True,
                            "record_id": record_id,
                            "status": "failed",
                            "error_type": "REQUIRED_FIELD_EXCEPTION",
                            "submitted_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
                            "instruction_valid_updated": False,
                            "failed_field": field_name,
                            "field_exception": str(field_error)
                        }
            
            # 入力結果の評価
            total_fields = len(input_assignments)
            success_rate = input_results['successful_inputs'] / total_fields if total_fields > 0 else 0
            
            logger.info(f"Rule-based form input completed: "
                       f"{input_results['successful_inputs']}/{total_fields} successful "
                       f"({success_rate:.1%}), {input_results['failed_inputs']} failed, "
                       f"{input_results['skipped_inputs']} skipped")
            
            # 最低成功率チェック（50%未満は失敗とする）
            if success_rate < 0.5 and input_results['successful_inputs'] < 3:
                logger.error(f"Input success rate too low: {success_rate:.1%}")
                return {
                    "error": True,
                    "record_id": record_id,
                    "status": "failed",
                    "error_type": "LOW_INPUT_SUCCESS_RATE",
                    "submitted_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
                    "instruction_valid_updated": False,
                    "success_rate": success_rate,
                    "input_results": input_results
                }
            
            # 成功
            return {
                "status": "success",
                "record_id": record_id,
                "input_results": input_results,
                "analysis_summary": summary,
                "rule_based": True
            }
            
        except Exception as e:
            logger.error(f"Rule-based form input error: {e}", exc_info=True)
            return {
                "error": True,
                "record_id": record_id,
                "status": "failed",
                "error_type": "RULE_BASED_INPUT_ERROR",
                "submitted_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
                "instruction_valid_updated": False,
                "exception": str(e)
            }
    
    async def _execute_rule_based_input(self, field_name: str, input_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        ルールベース入力の実行
        各フィールドタイプに応じた適切な入力処理
        """
        try:
            selector = input_config.get('selector', '')
            input_type = input_config.get('input_type', 'text')
            value = input_config.get('value', '')
            auto_action = input_config.get('auto_action', None)
            
            if not selector:
                return {
                    'success': False,
                    'error': 'No selector provided',
                    'skipped': True,
                    'reason': 'missing_selector'
                }
            
            # 要素の存在確認
            try:
                element = self.page.locator(selector)
                await element.wait_for(state='visible', timeout=5000)
            except PlaywrightTimeoutError:
                logger.debug(f"Element not found or not visible for field '{field_name}': {selector}")
                return {
                    'success': False,
                    'error': 'Element not found or not visible',
                    'skipped': True,
                    'reason': 'element_not_visible',
                    'selector': selector
                }
            
            # 入力タイプ別処理
            if input_type == 'text' or input_type == 'email' or input_type == 'tel' or input_type == 'url' or input_type == 'number':
                return await self._execute_text_input(element, value, field_name, input_type)
                
            elif input_type == 'textarea':
                return await self._execute_textarea_input(element, value, field_name)
                
            elif input_type == 'select':
                return await self._execute_select_input(element, value, field_name, auto_action, input_config.get('selected_index'))
                
            elif input_type == 'checkbox':
                return await self._execute_checkbox_input(element, value, field_name, auto_action)
                
            elif input_type == 'radio':
                return await self._execute_radio_input(element, value, field_name, auto_action)
            
            else:
                logger.warning(f"Unknown input type '{input_type}' for field '{field_name}'")
                return {
                    'success': False,
                    'error': f'Unknown input type: {input_type}',
                    'skipped': True,
                    'reason': 'unknown_input_type'
                }
                
        except Exception as e:
            logger.error(f"Error executing rule-based input for field '{field_name}': {e}")
            return {
                'success': False,
                'error': str(e),
                'exception': True
            }
    
    async def _execute_text_input(self, element: "Locator", value: str, field_name: str, input_type: str) -> Dict[str, Any]:
        """テキスト系入力の実行"""
        try:
            if not value:
                logger.debug(f"No value provided for text field '{field_name}', skipping")
                return {
                    'success': True,
                    'skipped': True,
                    'reason': 'no_value_provided'
                }
            
            # 既存の値をクリア
            await element.clear()
            
            # 値を入力
            await element.fill(str(value))
            
            # 入力確認
            filled_value = await element.input_value()
            input_successful = filled_value == str(value)
            
            if input_successful:
                logger.debug(f"Successfully filled {input_type} field '{field_name}' - ***VALUE_REDACTED***")
                return {
                    'success': True,
                    'input_type': input_type,
                    'filled_value': '***VALUE_REDACTED***'  # 個人情報保護
                }
            else:
                logger.warning(f"Input verification failed for field '{field_name}' - ***VALUES_REDACTED***")
                return {
                    'success': False,
                    'error': 'Input verification failed',
                    'expected_value': '***VALUE_REDACTED***',
                    'actual_value': '***VALUE_REDACTED***'
                }
                
        except Exception as e:
            logger.error(f"Error filling text field '{field_name}': {e}")
            return {
                'success': False,
                'error': str(e),
                'input_type': input_type
            }
    
    async def _execute_textarea_input(self, element: "Locator", value: str, field_name: str) -> Dict[str, Any]:
        """textarea入力の実行"""
        try:
            if not value:
                logger.debug(f"No value provided for textarea field '{field_name}', skipping")
                return {
                    'success': True,
                    'skipped': True,
                    'reason': 'no_value_provided'
                }
            
            # 既存の値をクリア
            await element.clear()
            
            # 値を入力
            await element.fill(str(value))
            
            # 入力確認
            filled_value = await element.input_value()
            input_successful = filled_value == str(value)
            
            if input_successful:
                logger.debug(f"Successfully filled textarea field '{field_name}' with {len(str(value))} characters")
                return {
                    'success': True,
                    'input_type': 'textarea',
                    'filled_length': len(filled_value)
                }
            else:
                logger.warning(f"Textarea input verification failed for field '{field_name}'")
                return {
                    'success': False,
                    'error': 'Textarea input verification failed',
                    'expected_length': len(str(value)),
                    'actual_length': len(filled_value)
                }
                
        except Exception as e:
            logger.error(f"Error filling textarea field '{field_name}': {e}")
            return {
                'success': False,
                'error': str(e),
                'input_type': 'textarea'
            }
    
    async def _execute_select_input(self, element: "Locator", value: str, field_name: str, auto_action: str, selected_index: Optional[int] = None) -> Dict[str, Any]:
        """select要素入力の実行"""
        try:
            # オプションの取得
            options = element.locator('option')
            option_count = await options.count()
            
            if option_count == 0:
                return {
                    'success': False,
                    'error': 'No options found in select element',
                    'input_type': 'select'
                }
            
            # auto_actionに基づく選択
            if auto_action == 'select_index' and isinstance(selected_index, int) and selected_index >= 0:
                try:
                    await element.select_option(index=selected_index)
                    logger.debug(f"Selected index {selected_index} for select field '{field_name}'")
                    return {
                        'success': True,
                        'input_type': 'select',
                        'selected_option': 'by_index',
                        'selected_index': selected_index,
                        'total_options': option_count
                    }
                except Exception as e:
                    logger.warning(f"Failed to select index for field '{field_name}': {e}")

            if auto_action == 'select_by_algorithm':
                # 3段階フォールバックアルゴリズムを即適用
                return await self._execute_select_fallback(element, options, option_count, field_name)

            if auto_action == 'select_last':
                # 最後の選択肢を選択（参考リポジトリの戦略）
                try:
                    last_option_index = option_count - 1
                    last_option = options.nth(last_option_index)
                    last_option_value = await last_option.get_attribute('value') or ''
                    
                    await element.select_option(index=last_option_index)
                    
                    logger.debug(f"Selected last option (index {last_option_index}) for select field '{field_name}'")
                    return {
                        'success': True,
                        'input_type': 'select',
                        'selected_option': 'last',
                        'selected_index': last_option_index,
                        'selected_value': '***VALUE_REDACTED***',
                        'total_options': option_count
                    }
                except Exception as e:
                    logger.warning(f"Failed to select last option for field '{field_name}': {e}")
            
            # 値による選択を試行（値はログに出さない）
            if value:
                try:
                    # まず値で試行
                    await element.select_option(value=str(value))
                    logger.debug(f"Selected option by value for select field '{field_name}' (redacted)")
                    return {
                        'success': True,
                        'input_type': 'select',
                        'selected_option': 'by_value',
                        'selected_value': '***VALUE_REDACTED***',
                        'total_options': option_count
                    }
                except:
                    try:
                        # テキストで試行
                        await element.select_option(label=str(value))
                        logger.debug(f"Selected option by label for select field '{field_name}' (redacted)")
                        return {
                            'success': True,
                            'input_type': 'select',
                            'selected_option': 'by_label',
                            'selected_value': '***VALUE_REDACTED***',
                            'total_options': option_count
                        }
                    except:
                        # 部分一致で試行
                        for i in range(option_count):
                            try:
                                option = options.nth(i)
                                option_text = await option.text_content() or ''
                                option_value = await option.get_attribute('value') or ''
                                
                                if (str(value).lower() in option_text.lower() or 
                                    str(value).lower() in option_value.lower()):
                                    await element.select_option(index=i)
                                    logger.debug(f"Selected option by partial match for select field '{field_name}' (redacted)")
                                    return {
                                        'success': True,
                                        'input_type': 'select',
                                        'selected_option': 'by_partial_match',
                                        'selected_index': i,
                                        'selected_text': '***VALUE_REDACTED***',
                                        'total_options': option_count
                                    }
                            except:
                                continue

            # 3段階フォールバックシステム（参考リポジトリの手法）
            return await self._execute_select_fallback(element, options, option_count, field_name)
            
        except Exception as e:
            logger.error(f"Error handling select field '{field_name}': {e}")
            return {
                'success': False,
                'error': str(e),
                'input_type': 'select'
            }

    async def _execute_select_fallback(self, element, options, option_count: int, field_name: str) -> Dict[str, Any]:
        """Select要素の3段階フォールバックシステム（参考リポジトリ準拠）"""
        
        # Stage 1: "その他"系オプション優先選択（参考リポジトリの詳細パターン）
        preferred_options = [
            # 基本的な「その他」パターン
            "その他", "一般", "上記にない", "該当なし", "該当しない", "なし", "無し",
            "当てはまらない", "あてはまらない", "不明", "未定", "未選択", 
            "選択してください", "選択して下さい", "お選びください", "お選び下さい",
            
            # 英語パターン
            "other", "others", "none", "not listed", "not applicable", "n/a",
            "please select", "select", "unknown", "undecided", "not specified",
            "no answer", "not sure", "misc", "miscellaneous",
            
            # 数値・記号パターン  
            "0", "-", "--", "---", "99", "999", "9999",
            "選択肢0", "option0", "default", "empty"
        ]
        try:
            for i in range(option_count):
                option = options.nth(i)
                option_text = (await option.text_content() or '').lower().strip()
                option_value = (await option.get_attribute('value') or '').lower().strip()
                
                # 優先オプションパターンマッチング（完全一致→部分一致の2段階）
                for preferred in preferred_options:
                    # Step 1: 完全一致（最高精度）
                    if (option_text == preferred.lower() or 
                        option_value == preferred.lower() or
                        option_text.strip() == preferred.lower().strip()):
                        await element.select_option(index=i)
                        logger.info(f"Selected preferred option (exact match) (index {i}) for select field '{field_name}'")
                        return {
                            'success': True,
                            'input_type': 'select',
                            'selected_option': 'preferred_exact',
                            'selected_index': i,
                            'selected_text': '***VALUE_REDACTED***',
                            'match_pattern': preferred,
                            'total_options': option_count
                        }
                    
                # Step 2: 部分一致（高精度）
                for preferred in preferred_options:
                    if (preferred.lower() in option_text or 
                        preferred.lower() in option_value):
                        # ただし、数値パターン（"0", "99"等）は完全一致のみ
                        if preferred in ["0", "99", "999", "9999"] and option_text != preferred.lower():
                            continue
                            
                        await element.select_option(index=i)
                        logger.info(f"Selected preferred option (partial match) (index {i}) for select field '{field_name}'")
                        return {
                            'success': True,
                            'input_type': 'select',
                            'selected_option': 'preferred_partial',
                            'selected_index': i,
                            'selected_text': '***VALUE_REDACTED***',
                            'match_pattern': preferred,
                            'total_options': option_count
                        }
        except Exception as e:
            logger.debug(f"Preferred option selection failed for field '{field_name}': {e}")
        
        # Stage 2: 最終オプション強制選択（参考リポジトリの確実な手法）
        try:
            last_index = option_count - 1
            if last_index > 0:  # 最低でも2つのオプションがある場合のみ
                await element.select_option(index=last_index)
                last_option = options.nth(last_index)
                last_option_text = await last_option.text_content() or ''
                last_option_value = await last_option.get_attribute('value') or ''
                
                logger.info(f"Selected last option (index {last_index}) for select field '{field_name}'")
                return {
                    'success': True,
                    'input_type': 'select',
                    'selected_option': 'last_option',
                    'selected_index': last_index,
                    'selected_text': '***VALUE_REDACTED***',
                    'selected_value': '***VALUE_REDACTED***',
                    'total_options': option_count
                }
        except Exception as e:
            logger.debug(f"Last option selection failed for field '{field_name}': {e}")
        
        # Stage 3: 最初の非空オプション選択（最後のフォールバック）
        try:
            for i in range(option_count):
                option = options.nth(i)
                option_value = await option.get_attribute('value') or ''
                option_text = await option.text_content() or ''
                
                if option_value.strip() and option_text.strip():
                    await element.select_option(index=i)
                    logger.warning(f"Selected fallback option (index {i}) for select field '{field_name}'")
                    return {
                        'success': True,
                        'input_type': 'select',
                        'selected_option': 'fallback',
                        'selected_index': i,
                        'selected_value': '***VALUE_REDACTED***',
                        'selected_text': '***VALUE_REDACTED***',
                        'total_options': option_count
                    }
        except Exception as e:
            logger.warning(f"All fallback attempts failed for field '{field_name}': {e}")
        
        # 全ての選択試行が失敗
        return {
            'success': False,
            'error': 'All selection attempts failed (3-stage fallback)',
            'input_type': 'select',
            'total_options': option_count
        }
    
    async def _execute_checkbox_input(self, element: "Locator", value: Any, field_name: str, auto_action: str) -> Dict[str, Any]:
        """checkbox入力の実行（JavaScript実行フォールバック付き）"""
        try:
            # 現在のチェック状態を確認
            current_checked = await element.is_checked()
            
            # auto_actionまたはvalueに基づいてチェック状態を決定
            should_check = False
            if auto_action == 'check':
                should_check = True
            elif isinstance(value, bool):
                should_check = value
            elif str(value).lower() in ['true', '1', 'yes', 'on']:
                should_check = True
            
            # チェック状態の変更が必要な場合のみ実行
            if should_check and not current_checked:
                success = await self._execute_checkbox_action(element, 'check', field_name)
                return {
                    'success': success,
                    'input_type': 'checkbox',
                    'action': 'checked',
                    'previous_state': current_checked,
                    'new_state': True
                }
            elif not should_check and current_checked:
                success = await self._execute_checkbox_action(element, 'uncheck', field_name)
                return {
                    'success': success,
                    'input_type': 'checkbox',
                    'action': 'unchecked',
                    'previous_state': current_checked,
                    'new_state': False
                }
            else:
                logger.debug(f"Checkbox field '{field_name}' already in desired state")
                return {
                    'success': True,
                    'input_type': 'checkbox',
                    'action': 'no_change',
                    'state': current_checked
                }
                
        except Exception as e:
            logger.error(f"Error handling checkbox field '{field_name}': {e}")
            return {
                'success': False,
                'error': str(e),
                'input_type': 'checkbox'
            }

    async def _execute_checkbox_action(self, element: "Locator", action: str, field_name: str) -> bool:
        """チェックボックスの確実な操作（JavaScript実行フォールバック付き）"""
        try:
            # Phase 1: 通常のPlaywright操作を試行
            if action == 'check':
                await element.check()
            else:
                await element.uncheck()
                
            # 操作成功の確認
            await asyncio.sleep(0.1)  # 短時間待機
            actual_state = await element.is_checked()
            expected_state = (action == 'check')
            
            if actual_state == expected_state:
                logger.debug(f"Checkbox '{field_name}' {action} successful (standard method)")
                return True
            else:
                logger.warning(f"Checkbox '{field_name}' {action} verification failed, trying JavaScript fallback")
                
        except Exception as e:
            logger.warning(f"Standard checkbox {action} failed for '{field_name}': {e}, trying JavaScript fallback")
        
        # Phase 2: JavaScript実行によるフォールバック（参考リポジトリの手法）
        try:
            # JavaScript経由でクリック実行（検知回避）
            await element.evaluate("element => element.click()")
            
            # JavaScript操作の結果確認
            await asyncio.sleep(0.2)
            actual_state = await element.is_checked()
            expected_state = (action == 'check')
            
            if actual_state == expected_state:
                logger.info(f"Checkbox '{field_name}' {action} successful (JavaScript fallback)")
                return True
            else:
                logger.error(f"JavaScript fallback also failed for checkbox '{field_name}'")
                return False
                
        except Exception as js_error:
            logger.error(f"JavaScript fallback failed for checkbox '{field_name}': {js_error}")
            return False
    
    async def _execute_radio_input(self, element: "Locator", value: Any, field_name: str, auto_action: str) -> Dict[str, Any]:
        """radio入力の実行（JavaScript実行フォールバック付き）"""
        try:
            # ラジオボタンの確実な選択
            success = await self._execute_radio_action(element, field_name)
            
            # auto_actionに基づいてレスポンス調整
            if auto_action == 'select_first':
                logger.debug(f"Selected radio button field '{field_name}' (first option)")
                return {
                    'success': success,
                    'input_type': 'radio',
                    'action': 'selected_first',
                    'auto_action': True
                }
            else:
                # 通常のラジオボタン選択
                logger.debug(f"Selected radio button field '{field_name}'")
                return {
                    'success': success,
                    'input_type': 'radio',
                    'action': 'selected',
                    'auto_action': False
                }
                
        except Exception as e:
            logger.error(f"Error handling radio field '{field_name}': {e}")
            return {
                'success': False,
                'error': str(e),
                'input_type': 'radio'
            }

    async def _execute_radio_action(self, element: "Locator", field_name: str) -> bool:
        """ラジオボタンの確実な選択（JavaScript実行フォールバック付き）"""
        try:
            # Phase 1: 通常のPlaywright操作を試行
            await element.check()
                
            # 選択成功の確認
            await asyncio.sleep(0.1)  # 短時間待機
            is_selected = await element.is_checked()
            
            if is_selected:
                logger.debug(f"Radio '{field_name}' selection successful (standard method)")
                return True
            else:
                logger.warning(f"Radio '{field_name}' selection verification failed, trying JavaScript fallback")
                
        except Exception as e:
            logger.warning(f"Standard radio selection failed for '{field_name}': {e}, trying JavaScript fallback")
        
        # Phase 2: JavaScript実行によるフォールバック（参考リポジトリの手法）
        try:
            # JavaScript経由でクリック実行（検知回避）
            await element.evaluate("element => element.click()")
            
            # JavaScript操作の結果確認
            await asyncio.sleep(0.2)
            is_selected = await element.is_checked()
            
            if is_selected:
                logger.info(f"Radio '{field_name}' selection successful (JavaScript fallback)")
                return True
            else:
                logger.error(f"JavaScript fallback also failed for radio '{field_name}'")
                return False
                
        except Exception as js_error:
            logger.error(f"JavaScript fallback failed for radio '{field_name}': {js_error}")
            return False

    async def _execute_form_submission(self, _unused_expanded_instruction: Dict[str, Any], record_id: int) -> Dict[str, Any]:
        """フォーム送信と結果判定（instruction_json非依存）"""
        # ルールベース解析で検出した候補を優先的に使用
        submit_cfg: Dict[str, Any] = {}
        try:
            candidates = getattr(self, "_submit_button_candidates", []) or []
            if candidates:
                submit_cfg["selector_candidates"] = candidates
                logger.info(f"Using {len(candidates)} analyzer submit candidates for submission")
        except Exception:
            pass

        submit_result = await self._submit_form(submit_cfg)

        return {
            "record_id": record_id,
            "success": submit_result,
            "error_type": None if submit_result else "SUBMIT_ERROR",
            "submitted_at": datetime.now(timezone(timedelta(hours=9))).strftime("%Y-%m-%dT%H:%M:%S+09:00"),
            "instruction_valid_updated": False,
        }

    # _removed_classify_error_type: 旧メソッドは未使用のため削除

    # _process_company_placeholders_in_instruction: instruction_json廃止に伴い不要のため削除

    async def _wait_for_submission_response_with_mutation(self) -> Dict[str, Any]:
        """送信後の動的コンテンツ監視（設定ベース）- MutationObserver結果を返す"""
        try:
            # 設定ファイルからタイムアウトを取得
            wait_time = self.timeout_settings.get("dynamic_message_wait", 15000) / 1000  # msからsに変換

            # 基本待機（初期レスポンス待ち）
            await asyncio.sleep(2)

            # MutationObserverで動的変更を監視
            mutation_result = await self._monitor_dynamic_changes(wait_time - 2)

            # ポップアップ・モーダルを監視
            await self._detect_popups_and_modals()

            pass  # Response monitoring completed

            return mutation_result

        except Exception as e:
            logger.warning(f"Error in submission response monitoring: {e}")
            return {"detected": False, "changes": []}

    async def _execute_four_stage_judgment(
        self, response_data: Dict[str, Any], pre_submit_state: Dict[str, Any], mutation_result: Dict[str, Any]
    ) -> bool:
        """
        6段階送信成功判定システム（ListersForm参考システム + Playwright対応）
        1. URL変更判定 (90-95% accuracy)
        2. 成功メッセージ判定 (85-90% accuracy)
        3. フォーム消失判定 (80-85% accuracy)
        4. 兄弟要素解析判定 (75-80% accuracy)
        5. エラーパターン判定 (70-75% accuracy)
        6. 失敗パターン判定 (65-70% accuracy)
        """
        try:
            # 新しい6段階判定システムを実行
            judgment_result = await self.success_judge.judge_submission_success(timeout=15)
            
            # 判定結果のログ出力
            stage_name = judgment_result.get('stage_name', 'Unknown')
            stage = judgment_result.get('stage', 0)
            confidence = judgment_result.get('confidence', 0.0)
            message = judgment_result.get('message', '')
            
            if judgment_result['success']:
                logger.info(f"Submission SUCCESS - Stage {stage}: {stage_name} (confidence: {confidence:.2f}) - {message}")
            else:
                logger.info(f"Submission FAILED - Stage {stage}: {stage_name} (confidence: {confidence:.2f}) - {message}")
            
            # 詳細情報をデバッグログに出力
            details = judgment_result.get('details', {})
            if details:
                logger.debug(f"Judgment details: {details}")
                
            return judgment_result['success']
            
        except Exception as e:
            logger.error(f"Error in 6-stage judgment system: {e}")
            return False

    async def _check_failure_keywords(self, page_text: str, page_content: str) -> bool:
        """段階1: 失敗キーワード判定（最優先判定） - パフォーマンス最適化版"""
        try:
            # パフォーマンス最適化: 事前コンパイル済みパターンで一括検索
            text_match = self.failure_matcher.find_match(page_text)
            content_match = self.failure_matcher.find_match(page_content)

            if text_match or content_match:
                matched_keyword = (text_match or content_match or '').lower()

                # 成功語が同時に強くヒットしている場合は失敗とみなさない（偽陽性抑止）
                try:
                    if self.success_matcher.find_match(page_text) or self.success_matcher.find_match(page_content):
                        return False
                except Exception:
                    pass

                # エラー文脈ヒント（DOMの属性・クラス）
                error_context_hints = [
                    'aria-invalid="true"', 'class="error', 'class="alert', 'class="is-error', 'class="invalid',
                    '[role="alert"]', 'data-error', 'data-valmsg-for'
                ]
                has_error_context = any(hint in (page_content or '').lower() for hint in error_context_hints)

                # Bot/キャプチャ系は単独でも強いシグナル
                is_bot_like = any(tok in matched_keyword for tok in ['captcha', 'recaptcha', 'not a robot'])

                if is_bot_like or has_error_context:
                    logger.info("Failure keyword detected with strong context")  # No sensitive detail
                    return True
                # それ以外は早期段階では失敗としない（SuccessJudgeで再評価）
                return False

            return False

        except Exception as e:
            logger.error(f"Error checking failure keywords: {e}")
            return False

    async def _check_success_keywords(self, page_text: str, page_content: str) -> bool:
        """段階2: 成功キーワード判定 - パフォーマンス最適化版"""
        try:
            # パフォーマンス最適化: 事前コンパイル済みパターンで一括検索
            text_match = self.success_matcher.find_match(page_text)
            content_match = self.success_matcher.find_match(page_content)

            if text_match or content_match:
                matched_keyword = text_match or content_match
                logger.info(f"Success keyword detected: {matched_keyword}")  # Critical detection always logged
                return True

            return False

        except Exception as e:
            logger.error(f"Error checking success keywords: {e}")
            return False

    async def _check_http_response(self, response_data: Dict[str, Any]) -> bool:
        """段階4: HTTPレスポンス判定（技術的判定）"""
        try:
            status_code = response_data.get("status_code")
            redirects = response_data.get("redirects", [])

            # リダイレクトが発生した場合は成功
            if redirects or (status_code and 300 <= status_code < 400):
                logger.info(f"HTTP redirect detected (status: {status_code}) - success")
                return True

            # 200番台レスポンスも成功と判定（送信ボタン押下後のため）
            if status_code and 200 <= status_code < 300:
                logger.info(f"HTTP 200-series response after form submission - success (status: {status_code})")
                return True

            # リクエストが発生しなかった場合は判定不能
            if status_code is None:
                pass  # No HTTP request detected
                return None

            # 400番台、500番台は失敗
            logger.info(f"HTTP response indicates failure (status: {status_code})")
            return False

        except Exception as e:
            logger.error(f"Error checking HTTP response: {e}")
            return None

    async def _check_state_changes(self, pre_submit_state: Dict[str, Any], mutation_result: Dict[str, Any]) -> bool:
        """段階3: 状態変化総合判定（高精度・競合状態回避・順次実行）"""
        try:
            # 送信後の状態を取得
            post_submit_state = await self._capture_page_state()

            # 順次実行で評価（競合状態回避・優先順位明確化）
            # A. DOM構造変化の評価（最高優先度）
            try:
                dom_result = await self._evaluate_dom_changes(mutation_result)
                if dom_result is not None:
                    logger.info(f"Stage 3A: DOM changes judgment - {'successful' if dom_result else 'failed'}")
                    return dom_result
            except Exception as dom_error:
                logger.warning(f"Stage 3A: DOM changes evaluation failed - {dom_error}")

            # B. フォーム要素状態変化の評価（高優先度）
            try:
                form_result = await self._evaluate_form_state_changes(pre_submit_state, post_submit_state)
                if form_result is not None:
                    logger.info(f"Stage 3B: Form state changes judgment - {'successful' if form_result else 'failed'}")
                    return form_result
            except Exception as form_error:
                logger.warning(f"Stage 3B: Form state changes evaluation failed - {form_error}")

            # C. ページメタ情報変化の評価（中優先度）
            try:
                meta_result = await self._evaluate_meta_changes(pre_submit_state, post_submit_state)
                if meta_result is not None:
                    logger.info(
                        f"Stage 3C: Meta information changes judgment - {'successful' if meta_result else 'failed'}"
                    )
                    return meta_result
            except Exception as meta_error:
                logger.warning(f"Stage 3C: Meta information changes evaluation failed - {meta_error}")

            # D. URL変化詳細評価（低優先度）
            try:
                url_result = await self._evaluate_url_changes(pre_submit_state, post_submit_state)
                if url_result is not None:
                    logger.info(f"Stage 3D: URL changes judgment - {'successful' if url_result else 'failed'}")
                    return url_result
            except Exception as url_error:
                logger.warning(f"Stage 3D: URL changes evaluation failed - {url_error}")

            # すべて判定不能
            logger.debug("Stage 3: All evaluations returned indeterminate results")
            return None

        except Exception as e:
            logger.error(f"Error in state changes judgment: {e}")
            return None

    async def _evaluate_dom_changes(self, mutation_result: Dict[str, Any]) -> bool:
        """DOM構造変化の評価（段階3A）"""
        try:
            if not mutation_result.get("detected"):
                return None

            changes = mutation_result.get("changes", [])
            if not changes:
                return None

            # 大量の変更は成功の兆候（ページ再構築）
            threshold = self._get_config_value("dom_change_threshold", 5, int)
            if len(changes) >= threshold:
                logger.info(f"Significant DOM changes detected: {len(changes)} mutations (threshold: {threshold})")
                return True

            # 少量の変更（エラーメッセージ表示など）は失敗の可能性
            if len(changes) <= 2:
                return None  # 判定不能

            return None  # 中程度の変更は判定不能

        except Exception as e:
            logger.error(f"Error evaluating DOM changes: {e}")
            return None

    async def _evaluate_form_state_changes(self, pre_state: Dict[str, Any], post_state: Dict[str, Any]) -> bool:
        """フォーム要素状態変化の評価（段階3B）"""
        try:
            pre_buttons = pre_state.get("submit_buttons", {})
            post_buttons = post_state.get("submit_buttons", {})

            # 送信ボタンが無効化されたかチェック
            for button_id in pre_buttons:
                if button_id in post_buttons:
                    pre_disabled = pre_buttons[button_id].get("disabled", False)
                    post_disabled = post_buttons[button_id].get("disabled", False)

                    # 有効から無効に変化した場合は成功の兆候
                    if not pre_disabled and post_disabled:
                        logger.debug(f"Submit button became disabled - likely success")
                        return True

            # フォーム要素のクリア状態をチェック
            pre_forms = pre_state.get("form_elements", {})
            post_forms = post_state.get("form_elements", {})

            cleared_fields = 0
            total_fields = 0

            for form_id in pre_forms:
                if form_id in post_forms:
                    pre_inputs = pre_forms[form_id]
                    post_inputs = post_forms[form_id]

                    for input_id in pre_inputs:
                        if input_id in post_inputs:
                            total_fields += 1
                            pre_value = pre_inputs[input_id].get("value", "")
                            post_value = post_inputs[input_id].get("value", "")

                            # 値があったフィールドがクリアされた場合
                            if pre_value and not post_value:
                                cleared_fields += 1

            # フィールドの指定比率以上がクリアされた場合は成功の兆候
            clear_ratio = self._get_config_value("form_clear_ratio", 0.5, float)
            if total_fields > 0 and cleared_fields >= total_fields * clear_ratio:
                logger.info(
                    f"Form fields cleared: {cleared_fields}/{total_fields} (ratio: {clear_ratio}) - likely success"
                )
                return True

            return None  # 判定不能

        except Exception as e:
            logger.error(f"Error evaluating form state changes: {e}")
            return None

    async def _evaluate_meta_changes(self, pre_state: Dict[str, Any], post_state: Dict[str, Any]) -> bool:
        """ページメタ情報変化の評価（段階3C）"""
        try:
            pre_title = pre_state.get("title", "").lower()
            post_title = post_state.get("title", "").lower()

            # タイトルが変更された場合
            if pre_title != post_title:
                # 成功キーワードが含まれる場合
                success_patterns = self._get_config_value(
                    "success_title_patterns", ["完了", "成功", "ありがとう", "thank", "complete", "success", "受付"], list
                )
                if any(pattern in post_title for pattern in success_patterns):
                    logger.info(f"Success pattern in title change: '{post_title}'")
                    return True

                # 失敗キーワードが含まれる場合
                failure_patterns = self._get_config_value(
                    "failure_title_patterns", ["エラー", "失敗", "無効", "error", "failed", "invalid"], list
                )
                if any(pattern in post_title for pattern in failure_patterns):
                    logger.info(f"Failure pattern in title change: '{post_title}'")
                    return False

            return None  # 判定不能

        except Exception as e:
            logger.error(f"Error evaluating meta changes: {e}")
            return None

    async def _evaluate_url_changes(self, pre_state: Dict[str, Any], post_state: Dict[str, Any]) -> bool:
        """URL変化詳細評価（段階3D）"""
        try:
            pre_url = pre_state.get("url", "").lower()
            post_url = post_state.get("url", "").lower()

            # URL変化がない場合
            if pre_url == post_url:
                return None

            # 成功URLパターンをチェック
            success_patterns = self._get_config_value(
                "success_url_patterns",
                [
                    "/thanks",
                    "/thank-you",
                    "/complete",
                    "/completed",
                    "/done",
                    "/submitted",
                    "/success",
                    "/confirm",
                    "/confirmation",
                    "/kanryou",
                    "/uketsuke",
                    "/arigatou",
                ],
                list,
            )

            for pattern in success_patterns:
                if pattern in post_url:
                    logger.info(f"Success URL pattern detected: {pattern}")
                    return True

            # 失敗URLパターンをチェック
            failure_patterns = self._get_config_value(
                "failure_url_patterns", ["/error", "/failed", "/failure", "/invalid"], list
            )

            for pattern in failure_patterns:
                if pattern in post_url:
                    logger.info(f"Failure URL pattern detected: {pattern}")
                    return False

            # クエリパラメータの変化をチェック
            from urllib.parse import urlparse, parse_qs

            try:
                pre_parsed = urlparse(pre_url)
                post_parsed = urlparse(post_url)

                pre_params = parse_qs(pre_parsed.query)
                post_params = parse_qs(post_parsed.query)

                # 成功パラメータの追加
                success_params = self._get_config_value(
                    "success_query_params", ["success", "completed", "submitted"], list
                )
                for param in success_params:
                    if param not in pre_params and param in post_params:
                        logger.info(f"Success parameter added: {param}")
                        return True

                # 失敗パラメータの追加
                failure_params = self._get_config_value("failure_query_params", ["error", "failed", "invalid"], list)
                for param in failure_params:
                    if param not in pre_params and param in post_params:
                        logger.info(f"Failure parameter added: {param}")
                        return False

            except Exception:
                pass

            return None  # 判定不能

        except Exception as e:
            logger.error(f"Error evaluating URL changes: {e}")
            return None

    async def _capture_page_state(self) -> Dict[str, Any]:
        """送信前のページ状態を詳細にキャプチャ（段階4判定用）"""
        try:
            state = {
                "url": self.page.url,
                "title": await self.page.title(),
                "form_elements": {},
                "submit_buttons": {},
                "meta_description": None,
            }

            # フォーム要素の状態を記録
            try:
                forms = await self.page.query_selector_all("form")
                for i, form in enumerate(forms):
                    form_id = f"form_{i}"

                    # 入力フィールドの状態
                    inputs = await form.query_selector_all("input, textarea, select")
                    input_states = {}
                    for j, input_elem in enumerate(inputs):
                        try:
                            input_id = f"input_{j}"
                            input_type = await input_elem.get_attribute("type") or "text"
                            is_disabled = await input_elem.is_disabled()
                            is_visible = await input_elem.is_visible()
                            value = await input_elem.input_value() if input_type != "file" else ""

                            input_states[input_id] = {
                                "type": input_type,
                                "disabled": is_disabled,
                                "visible": is_visible,
                                "value": value[:100] if value else "",  # 最初の100文字のみ
                            }
                        except Exception:
                            continue

                    state["form_elements"][form_id] = input_states
            except Exception:
                pass

            # 送信ボタンの状態を記録
            try:
                buttons = await self.page.query_selector_all(
                    'button[type="submit"], input[type="submit"], button:has-text("送信"), button:has-text("Submit")'
                )
                for i, button in enumerate(buttons):
                    try:
                        button_id = f"button_{i}"
                        is_disabled = await button.is_disabled()
                        is_visible = await button.is_visible()

                        state["submit_buttons"][button_id] = {"disabled": is_disabled, "visible": is_visible}
                    except Exception:
                        continue
            except Exception:
                pass

            # メタ情報を記録
            try:
                meta_desc = await self.page.query_selector('meta[name="description"]')
                if meta_desc:
                    state["meta_description"] = await meta_desc.get_attribute("content")
            except Exception:
                pass

            return state

        except Exception as e:
            logger.warning(f"Error capturing page state: {e}")
            return {"url": "", "title": "", "form_elements": {}, "submit_buttons": {}, "meta_description": None}

    async def _monitor_dynamic_changes(self, timeout_seconds: Optional[float] = None) -> Dict[str, Any]:
        """動的DOM変更のMutationObserver監視（セキュリティ強化版）"""
        try:
            # 設定ファイルからタイムアウト値を取得（フォールバック付き）
            if timeout_seconds is None:
                timeout_seconds = self.timeout_settings.get("dom_monitoring", 8000) / 1000

            # 実行時間をより厳格に制限
            max_timeout = 10.0  # 最大10秒
            if timeout_seconds > max_timeout:
                logger.warning(f"DOM monitoring timeout too long: {timeout_seconds}s, limiting to {max_timeout}s")
                timeout_seconds = max_timeout

            timeout_ms = int(timeout_seconds * 1000)

            # JavaScript実行の安全性を考慮したスクリプト
            mutation_script = f"""
            (() => {{
                try {{
                    return new Promise((resolve, reject) => {{
                        let resolved = false;
                        const safeResolve = (data) => {{
                            if (!resolved) {{
                                resolved = true;
                                resolve(data);
                            }}
                        }};
                        
                        const observer = new MutationObserver((mutations) => {{
                            try {{
                                const changes = mutations.map(mutation => ({{
                                    type: mutation.type,
                                    target: mutation.target ? mutation.target.tagName : 'UNKNOWN',
                                    addedNodes: mutation.addedNodes ? Array.from(mutation.addedNodes).length : 0,
                                    removedNodes: mutation.removedNodes ? Array.from(mutation.removedNodes).length : 0
                                }}));
                                
                                if (changes.length > 0) {{
                                    observer.disconnect();
                                    safeResolve({{ detected: true, changes }});
                                }}
                            }} catch (error) {{
                                observer.disconnect();
                                safeResolve({{ detected: false, changes: [], error: error.message }});
                            }}
                        }});
                        
                        try {{
                            observer.observe(document.body || document.documentElement, {{
                                childList: true,
                                subtree: true,
                                attributes: true,
                                attributeFilter: ['class', 'style', 'hidden']
                            }});
                        }} catch (observeError) {{
                            safeResolve({{ detected: false, changes: [], error: observeError.message }});
                            return;
                        }}
                        
                        setTimeout(() => {{
                            observer.disconnect();
                            safeResolve({{ detected: false, changes: [] }});
                        }}, {timeout_ms});
                    }});
                }} catch (error) {{
                    return {{ detected: false, changes: [], error: error.message }};
                }}
            }})()
            """

            # JavaScript実行前のセキュリティ検証（内部スクリプトフラグ使用）
            if not self._validate_javascript_content(mutation_script, is_internal_script=True):
                logger.error("JavaScript content failed security validation")
                return {"detected": False, "changes": [], "security_error": True}

            # JavaScript実行タイムアウトの設定（より厳格に）
            js_timeout = min(self.timeout_settings.get("javascript_execution", 10000), 8000)  # 最大8秒

            # JavaScript実行（セキュリティ強化・タイムアウト制御付き）
            result = await asyncio.wait_for(self.page.evaluate(mutation_script), timeout=js_timeout / 1000)

            # 結果の検証とログ出力
            if isinstance(result, dict):
                if result.get("error"):
                    logger.warning(f"JavaScript execution warning: {result['error']}")

                if result.get("detected"):
                    logger.info(f"Dynamic changes detected: {len(result.get('changes', []))} mutations")
                else:
                    pass  # No dynamic changes detected

                return result
            else:
                logger.warning("Unexpected result type from JavaScript execution")
                return {"detected": False, "changes": []}

        except asyncio.TimeoutError:
            logger.error(f"DOM monitoring timeout after {timeout_seconds}s")
            return {"detected": False, "changes": [], "timeout": True}
        except Exception as e:
            logger.error(f"Error in dynamic change monitoring: {e}")
            return {"detected": False, "changes": [], "error": str(e)}

    async def _handle_confirmation_page_pattern(
        self, response_data: Dict[str, Any], pre_submit_state: Dict[str, Any], original_button_selector: str
    ) -> bool:
        """確認ページ経由パターンの処理（FORM_SENDER.md 4.3.2節準拠）"""
        try:
            logger.info("Handling confirmation page pattern")

            # 2. ネットワークリクエスト監視による分岐処理
            status_code = response_data.get("status_code")
            redirects = response_data.get("redirects", [])

            if redirects or (status_code and 300 <= status_code < 400):
                # 300番台レスポンス：リダイレクト発生
                logger.info("Redirect detected, waiting for confirmation page load")
                timeout_config = _get_timeout_config()
                await asyncio.sleep(timeout_config.get("page_load_wait", 3000) / 1000)  # ページ読み込み待機
                try:
                    await self.page.wait_for_load_state("networkidle", timeout=timeout_config.get("page_load", 15000))
                except Exception:
                    pass
                return await self._find_and_submit_final_button()

            elif status_code and (400 <= status_code < 500 or 500 <= status_code < 600):
                # 400・500番台レスポンス：確認ページ遷移失敗
                logger.error(f"Confirmation page transition failed with status {status_code}")
                return False

            else:
                # 200番台レスポンスまたはリクエストなし：詳細検証実行
                return await self._verify_confirmation_page_transition(original_button_selector)

        except Exception as e:
            logger.error(f"Error in confirmation page pattern handling: {e}")
            return False

    async def _verify_confirmation_page_transition(self, original_button_selector: str) -> bool:
        """確認ページ遷移の詳細検証（FORM_SENDER.md 4.3.2節準拠）"""
        try:
            # Ajax処理の完了を待つ
            timeout_config = _get_timeout_config()
            await asyncio.sleep(timeout_config.get("ajax_processing_wait", 3000) / 1000)
            try:
                await self.page.wait_for_load_state("networkidle", timeout=timeout_config.get("page_load", 15000))
            except Exception:
                pass

            # 確認ボタン同一性チェック
            try:
                original_button_exists = await self.page.locator(original_button_selector).count() > 0
                if original_button_exists:
                    # 同じボタン要素が存在：確認ページへの遷移失敗
                    logger.error("Same confirm button still exists, transition to confirmation page failed")
                    return False
                else:
                    # 同じボタン要素が存在しない：確認ページに遷移成功
                    logger.info("Confirm button no longer exists, successfully transitioned to confirmation page")
                    return await self._find_and_submit_final_button()
            except Exception as check_error:
                logger.error(f"Error checking original button existence: {check_error}")
                # エラー時は遷移成功と仮定して最終送信を試行
                return await self._find_and_submit_final_button()

        except Exception as e:
            logger.error(f"Error in confirmation page transition verification: {e}")
            return False

    async def _find_and_submit_final_button(self) -> bool:
        """確認ページでの最終送信ボタン検出と送信実行（キーワードベース検索）

        2段階フォームの確認ページにおいて、最終送信ボタンを検出し送信処理を実行する。
        優先セレクタによる早期検出とキーワードマッチングを組み合わせて高い成功率を実現。

        Returns:
            bool: 送信成功の場合True、失敗の場合False

        Raises:
            Exception: 送信処理中の予期しないエラー
        """
        try:
            # 1. 確認ページ要素分析（参考リポジトリの手法）
            confirmation_analysis = await self._analyze_confirmation_page_inputs()
            if confirmation_analysis.get('has_editable_inputs'):
                logger.info(f"Confirmation page has editable inputs: {confirmation_analysis.get('input_count', 0)}")
                # 必要に応じて再入力処理を実行
                await self._handle_confirmation_page_inputs(confirmation_analysis)
            
            # 2. ボタン要素の取得
            all_buttons = await self._get_all_visible_buttons()
            if not all_buttons:
                return False

            # 3. 送信ボタンの検出
            button_result = await self._select_submit_button_by_keywords(all_buttons)
            if not button_result:
                await self._log_debug_button_info(all_buttons)
                return False

            final_button_element, matched_keyword = button_result

            # 4. 最終送信の実行
            return await self._execute_final_submission(final_button_element, matched_keyword)

        except Exception as e:
            logger.error(f"Error in final button submission: {e}")
            return False

    async def _get_all_visible_buttons(self) -> List[ElementHandle]:
        """すべての表示可能なボタン要素を効率的に取得する（リソース管理強化版）

        優先セレクタを使用した早期検出により、全要素検索を回避して
        パフォーマンスを最適化。表示・有効状態の要素のみを返却。

        Returns:
            List[ElementHandle]: 表示可能で有効なボタン要素のリスト

        Raises:
            Exception: DOM操作エラー
        """
        buttons = []
        try:
            # 優先セレクタによる早期検出の試行
            priority_selectors = [
                'button[type="submit"]',
                'input[type="submit"]',
                'button:has-text("送信")',
                'button:has-text("確認")',
                'button:has-text("Submit")',
            ]

            # まず優先セレクタで検索
            for selector in priority_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    for element in elements:
                        try:
                            if await element.is_visible() and await element.is_enabled():
                                buttons.append(element)
                                if len(buttons) >= 3:  # 効率化：最初の3個で十分
                                    return buttons
                        except Exception as elem_error:
                            logger.debug(f"Error checking element visibility/enable state: {elem_error}")
                            # 無効な要素は破棄して次に進む
                            continue
                except Exception as selector_error:
                    logger.debug(f"Error with selector {selector}: {selector_error}")
                    continue

            # 優先セレクタで見つからない場合のみ全検索
            if not buttons:
                try:
                    all_elements = await self.page.query_selector_all(
                        'button, input[type="submit"], input[type="button"]'
                    )
                    for element in all_elements:
                        try:
                            if await element.is_visible() and await element.is_enabled():
                                buttons.append(element)
                        except Exception as elem_error:
                            logger.debug(f"Error checking fallback element: {elem_error}")
                            continue
                except Exception as fallback_error:
                    logger.warning(f"Error in fallback button search: {fallback_error}")

            return buttons
        except Exception as e:
            logger.error(f"Error getting visible buttons: {e}")
            return []
        finally:
            # 大量の要素を処理した場合のメモリ使用量を報告
            if len(buttons) > 10:
                logger.debug(f"Large number of buttons found: {len(buttons)}, consider performance optimization")

    async def _extract_button_texts(self, button: ElementHandle) -> List[str]:
        """ボタンの各種属性から包括的にテキストを取得する"""
        button_texts = []

        # 属性名と取得メソッドのマッピング
        attributes = {
            "inner_text": lambda: button.inner_text(),
            "value": lambda: button.get_attribute("value"),
            "title": lambda: button.get_attribute("title"),
            "aria-label": lambda: button.get_attribute("aria-label"),
        }

        for attr_name, getter in attributes.items():
            try:
                text = await getter()
                if text and text.strip():
                    button_texts.append(text.strip())
            except Exception as e:
                logger.debug(f"Error getting {attr_name}: {e}")

        return button_texts

    async def _select_submit_button_by_keywords(
        self, buttons: List[ElementHandle]
    ) -> Optional[Tuple[ElementHandle, str]]:
        """キーワードマッチングによる送信ボタン選択（フォールバック強化版）"""
        keywords_config = _get_button_keywords_config()
        submit_keywords = keywords_config["primary"] + keywords_config["secondary"]

        # フォールバック用のキーワードパターンを追加
        fallback_keywords = ["送", "信", "submit", "send", "完了", "登録", "実行", "次", "next", "ok", "yes", "決定", "確定"]

        for button in buttons:
            try:
                # ボタンテキストを包括的に取得（要素全体の情報を収集）
                button_texts = await self._extract_button_texts(button)
                combined_text = " ".join(button_texts).lower()

                # 正規化：空白文字を削除したバージョンも作成
                normalized_text = "".join(combined_text.split())

                # Phase 1: 標準キーワード検索（優先順位順）
                for keyword in submit_keywords:
                    keyword_lower = keyword.lower()
                    if keyword_lower in combined_text or keyword_lower in normalized_text:
                        logger.debug(
                            f"Final submit button found with keyword '{keyword}': text='{combined_text[:100]}...'"
                        )
                        return (button, keyword)

                # Phase 2: 部分マッチ検索（「送　信」→「送」「信」で検索）
                for keyword in submit_keywords:
                    keyword_chars = list(keyword.lower())
                    if len(keyword_chars) >= 2 and all(char in normalized_text for char in keyword_chars):
                        logger.debug(
                            f"Final submit button found with partial match '{keyword}': text='{combined_text[:100]}...'"
                        )
                        return (button, keyword)

            except Exception as button_error:
                logger.debug(f"Error checking button: {button_error}")
                continue

        # Phase 3: フォールバックキーワード検索
        for button in buttons:
            try:
                button_texts = await self._extract_button_texts(button)
                combined_text = " ".join(button_texts).lower()
                normalized_text = "".join(combined_text.split())

                for keyword in fallback_keywords:
                    if keyword in combined_text or keyword in normalized_text:
                        logger.debug(
                            f"Final submit button found with fallback keyword '{keyword}': text='{combined_text[:100]}...'"
                        )
                        return (button, keyword)

            except Exception as button_error:
                continue

        return None

    async def _log_debug_button_info(self, buttons: List[ElementHandle]) -> None:
        """デバッグ用：ページ上のボタン情報を詳細ログ出力（強化版）"""
        logger.error("Final submit button not found on confirmation page using keyword search")
        logger.info("Available buttons on page:")

        for i, button in enumerate(buttons[:5]):  # 5個まで表示（パフォーマンス最適化）
            try:
                is_visible = await button.is_visible()
                inner_text = await button.inner_text()
                value_attr = await button.get_attribute("value") or ""
                tag_name = await button.evaluate("el => el.tagName")

                logger.info(
                    f"  Button {i+1}: tag={tag_name}, visible={is_visible}, text='{inner_text}', value='{value_attr}'"
                )

            except Exception as e:
                logger.info(f"  Button {i+1}: Error getting info - {e}")

    async def _log_field_error_details(self, field_name: str, field_config: dict, error: Exception) -> None:
        """フィールド入力エラーの詳細ログ出力"""
        try:
            logger.warning(f"Failed to fill field {field_name}: {error}")
            logger.info(f"Field config: {field_config}")

            # 現在のページ状態を記録
            current_url = self.page.url
            page_title = await self.page.title()
            logger.info(f"Page context: Title='{page_title}'")

            # セレクタが見つかるかチェック
            selector = field_config.get("selector", "")
            if selector:
                try:
                    count = await self.page.locator(selector).count()
                    logger.info(f"Selector '{selector}' found {count} elements")

                    if count > 0:
                        # 最初の要素の詳細情報を取得
                        element = self.page.locator(selector).first
                        is_visible = await element.is_visible()
                        is_enabled = await element.is_enabled()
                        tag_name = await element.evaluate("el => el.tagName")
                        element_type = await element.get_attribute("type") or ""

                        logger.info(
                            f"Element details: tag={tag_name}, type='{element_type}', visible={is_visible}, enabled={is_enabled}"
                        )

                except Exception as selector_check_error:
                    logger.info(f"Error checking selector: {selector_check_error}")

        except Exception as log_error:
            logger.debug(f"Error in field error logging: {log_error}")

    async def _execute_final_submission(self, button_element: ElementHandle, matched_keyword: str) -> bool:
        """最終送信ボタンの押下と結果監視"""
        # 送信前の状態を記録
        pre_submit_state = await self._capture_page_state()
        pre_submit_url = self.page.url

        # HTTPレスポンス監視の準備
        response_data = await self._setup_response_monitoring(pre_submit_url)

        try:
            # 最終送信ボタン押下（JavaScript実行フォールバック付き）
            success = await self._execute_element_click(button_element, f"final submit button (keyword: '{matched_keyword}')")
            if not success:
                logger.error(f"Final submit button click failed for keyword: '{matched_keyword}'")
                return False
            
            logger.debug(f"Final submit button clicked (keyword: '{matched_keyword}')")

            # 送信後の結果監視
            mutation_result = await self._wait_for_submission_response_with_mutation()

            # 4段階判定システムによる成功判定
            return await self._execute_four_stage_judgment(response_data, pre_submit_state, mutation_result)

        finally:
            # レスポンスリスナーをクリーンアップ
            try:
                self.page.remove_listener("response", response_data["handler"])
            except:
                pass

    async def _setup_response_monitoring(self, pre_submit_url: str) -> Dict[str, Any]:
        """HTTPレスポンス監視の準備"""
        response_data = {
            "status_code": None,
            "redirects": [],
            "post_requests": [],
            "response_times": [],
            "handler": None,
        }

        import time

        start_time = time.time()

        def handle_response(response) -> None:
            response_time = time.time() - start_time

            if response.request.method == "POST" or response.url.startswith(
                pre_submit_url[: pre_submit_url.rfind("/") + 1]
            ):
                response_data["status_code"] = response.status
                response_data["response_times"].append(response_time)

                if response.request.method == "POST":
                    response_data["post_requests"].append(
                        {"status": response.status, "url": response.url, "time": response_time}
                    )

                if 300 <= response.status < 400:
                    response_data["redirects"].append(
                        {"status": response.status, "url": response.url, "time": response_time}
                    )

        response_data["handler"] = handle_response
        self.page.on("response", handle_response)

        return response_data

    async def _get_button_element_text(self, selector: str) -> str:
        """ボタン要素全体の文字列を取得する（要素全体のテキスト内容を抽出）"""
        try:
            element = self.page.locator(selector).first

            # 要素全体の文字列情報を取得（HTML含む）
            element_info = await element.evaluate(
                """
                el => {
                    // 要素のすべてのテキスト情報を収集
                    const texts = [];
                    
                    // タグ名
                    texts.push(el.tagName.toLowerCase());
                    
                    // すべての属性値
                    for (const attr of el.attributes) {
                        if (attr.value) texts.push(attr.value);
                    }
                    
                    // innerText（表示テキスト）
                    if (el.innerText) texts.push(el.innerText);
                    
                    // textContent（非表示テキスト含む）
                    if (el.textContent && el.textContent !== el.innerText) {
                        texts.push(el.textContent);
                    }
                    
                    // 子要素のテキストも収集
                    const descendants = el.querySelectorAll('*');
                    for (const desc of descendants) {
                        if (desc.innerText) texts.push(desc.innerText);
                        for (const attr of desc.attributes) {
                            if (attr.value) texts.push(attr.value);
                        }
                    }
                    
                    return texts.filter(text => text && text.trim()).join(' ').trim();
                }
            """
            )

            return element_info

        except Exception as e:
            logger.debug(f"Error getting button element text for {selector}: {e}")
            return ""

    async def _get_button_text(self, selector: str) -> str:
        """下位互換のため既存メソッドを残す"""
        return await self._get_button_element_text(selector)

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
        keywords_config = _get_button_keywords_config()

        # 1. まず確認ボタンかどうかを判定（優先）
        confirmation_keywords = keywords_config.get("confirmation", ["確認", "次", "review", "confirm", "進む"])
        for keyword in confirmation_keywords:
            if keyword.lower() in element_text_lower:
                return "confirmation"

        # 2. 確認ボタンでない場合、送信ボタンかどうかを判定
        primary_keywords = keywords_config.get("primary", ["送信", "送る", "submit", "send"])
        secondary_keywords = keywords_config.get(
            "secondary", ["完了", "complete", "確定", "実行", "execute", "登録", "register"]
        )

        # primary → secondary の順で検索
        all_submit_keywords = primary_keywords + secondary_keywords
        for keyword in all_submit_keywords:
            if keyword.lower() in element_text_lower:
                return "submit"

        return "unknown"

    async def _detect_popups_and_modals(self) -> Dict[str, Any]:
        """ポップアップ・モーダルの検出（FORM_SENDER.md 4.3.4節準拠）"""
        try:
            popup_selectors = [
                ".modal",
                ".popup",
                ".dialog",
                ".notification",
                ".alert",
                ".toast",
                ".message-box",
                '[role="dialog"]',
                '[role="alert"]',
                '[role="alertdialog"]',
                "[data-modal]",
                ".sweetalert",
                ".swal",
                ".fancybox",
                ".lightbox",
            ]

            detected_popups = []

            for selector in popup_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    for element in elements:
                        # 表示状態をチェック
                        is_visible = await element.is_visible()
                        if is_visible:
                            text_content = await element.inner_text()
                            if text_content.strip():
                                detected_popups.append(
                                    {
                                        "selector": selector,
                                        "text": text_content[:200],  # 最初の200文字のみ
                                        "element_tag": await element.tag_name(),
                                    }
                                )
                                pass  # Popup detected
                except Exception as selector_error:
                    # 個別セレクターのエラーは無視して続行
                    continue

            return {"detected": len(detected_popups) > 0, "popups": detected_popups, "count": len(detected_popups)}

        except Exception as e:
            logger.error(f"Error in popup detection: {e}")
            return {"detected": False, "popups": [], "count": 0}

    async def _execute_submit_button_click(self, selector: str, timeout_ms: int) -> bool:
        """送信ボタンの確実なクリック（JavaScript実行フォールバック付き）"""
        try:
            # クリック前にスクロール/可視化
            try:
                loc = self.page.locator(selector).first
                await loc.scroll_into_view_if_needed()
                # 一瞬待ってレイアウト安定を待つ
                await asyncio.sleep(0.05)
            except Exception:
                pass

            # Phase 1: 通常のPlaywright操作を試行
            await asyncio.wait_for(self.page.click(selector), timeout=timeout_ms / 1000)
            logger.debug(f"Submit button click successful (standard method): {selector}")
            return True
            
        except Exception as e:
            logger.warning(f"Standard submit button click failed for '{selector}': {e}, trying JavaScript fallback")
        
        # Phase 2: JavaScript実行によるフォールバック（参考リポジトリの手法）
        try:
            # セレクタから要素を取得してJavaScript実行
            await self.page.evaluate(f"""
                const element = document.querySelector('{selector}');
                if (element) {{
                    element.click();
                }} else {{
                    throw new Error('Element not found for JavaScript click');
                }}
            """)
            
            logger.debug(f"Submit button click successful (JavaScript fallback): {selector}")
            return True
            
        except Exception as js_error:
            logger.error(f"JavaScript fallback also failed for submit button '{selector}': {js_error}")
            return False

    async def _execute_element_click(self, element, description: str) -> bool:
        """要素の確実なクリック（JavaScript実行フォールバック付き）"""
        try:
            # クリック前にスクロール/可視化 + enable待ち
            try:
                await self._wait_until_element_clickable(element, self.timeout_settings.get("click_timeout", 5000))
            except Exception:
                pass

            # Phase 1: 通常のPlaywright操作を試行
            await element.click()
            logger.debug(f"Element click successful (standard method): {description}")
            return True
            
        except Exception as e:
            logger.warning(f"Standard element click failed for '{description}': {e}, trying JavaScript fallback")
        
        # Phase 2: JavaScript実行によるフォールバック（参考リポジトリの手法）
        try:
            # ElementHandle経由でJavaScript実行
            await element.evaluate("element => element.click()")
            logger.info(f"Element click successful (JavaScript fallback): {description}")
            return True
            
        except Exception as js_error:
            logger.error(f"JavaScript fallback also failed for element '{description}': {js_error}")
            return False

    async def _analyze_confirmation_page_inputs(self) -> Dict[str, Any]:
        """確認ページでの入力可能項目の分析（参考リポジトリの手法）"""
        try:
            # 編集可能な入力要素を検出
            editable_inputs = []
            
            # 1. テキスト入力フィールド（readonlyでないもの）
            text_inputs = await self.page.query_selector_all('input[type="text"], input:not([type]), textarea')
            for input_elem in text_inputs:
                try:
                    is_readonly = await input_elem.get_attribute('readonly')
                    is_disabled = await input_elem.get_attribute('disabled')
                    is_visible = await input_elem.is_visible()
                    
                    if not is_readonly and not is_disabled and is_visible:
                        name = await input_elem.get_attribute('name') or ''
                        id_attr = await input_elem.get_attribute('id') or ''
                        placeholder = await input_elem.get_attribute('placeholder') or ''
                        
                        editable_inputs.append({
                            'type': 'text',
                            'element': input_elem,
                            'name': name,
                            'id': id_attr,
                            'placeholder': placeholder
                        })
                except Exception as e:
                    logger.debug(f"Error analyzing input element: {e}")
                    continue
            
            # 2. セレクト要素
            select_elements = await self.page.query_selector_all('select')
            for select_elem in select_elements:
                try:
                    is_disabled = await select_elem.get_attribute('disabled')
                    is_visible = await select_elem.is_visible()
                    
                    if not is_disabled and is_visible:
                        name = await select_elem.get_attribute('name') or ''
                        id_attr = await select_elem.get_attribute('id') or ''
                        
                        editable_inputs.append({
                            'type': 'select',
                            'element': select_elem,
                            'name': name,
                            'id': id_attr
                        })
                except Exception as e:
                    logger.debug(f"Error analyzing select element: {e}")
                    continue
            
            # 3. チェックボックス・ラジオボタン
            checkboxes_radios = await self.page.query_selector_all('input[type="checkbox"], input[type="radio"]')
            for cb_radio in checkboxes_radios:
                try:
                    is_disabled = await cb_radio.get_attribute('disabled')
                    is_visible = await cb_radio.is_visible()
                    
                    if not is_disabled and is_visible:
                        input_type = await cb_radio.get_attribute('type')
                        name = await cb_radio.get_attribute('name') or ''
                        
                        editable_inputs.append({
                            'type': input_type,
                            'element': cb_radio,
                            'name': name
                        })
                except Exception as e:
                    logger.debug(f"Error analyzing checkbox/radio element: {e}")
                    continue
            
            return {
                'has_editable_inputs': len(editable_inputs) > 0,
                'input_count': len(editable_inputs),
                'editable_inputs': editable_inputs,
                'analysis_success': True
            }
            
        except Exception as e:
            logger.error(f"Error analyzing confirmation page inputs: {e}")
            return {
                'has_editable_inputs': False,
                'input_count': 0,
                'editable_inputs': [],
                'analysis_success': False,
                'error': str(e)
            }

    async def _handle_confirmation_page_inputs(self, analysis: Dict[str, Any]) -> None:
        """確認ページでの入力処理（基本的な対応のみ）"""
        try:
            editable_inputs = analysis.get('editable_inputs', [])
            
            for input_info in editable_inputs:
                try:
                    element = input_info['element']
                    input_type = input_info['type']
                    name = input_info.get('name', '')
                    
                    # 基本的な自動対応のみ実装
                    if input_type == 'checkbox':
                        # チェックボックスは自動でチェック
                        is_checked = await element.is_checked()
                        if not is_checked:
                            await self._execute_element_click(element, f"confirmation checkbox: {name}")
                            logger.debug(f"Checked confirmation page checkbox: {name}")
                    
                    elif input_type == 'radio':
                        # ラジオボタンは最初の選択肢を選択
                        is_checked = await element.is_checked()
                        if not is_checked:
                            await self._execute_element_click(element, f"confirmation radio: {name}")
                            logger.debug(f"Selected confirmation page radio: {name}")
                    
                    elif input_type == 'select':
                        # セレクト要素は最後のオプション選択
                        options = await element.locator('option').all()
                        if len(options) > 1:
                            await element.select_option(index=len(options) - 1)
                            logger.debug(f"Selected last option for confirmation page select: {name}")
                    
                    # テキスト入力は通常、確認ページでは必要ないのでスキップ
                    
                except Exception as e:
                    logger.debug(f"Error handling confirmation input {input_info.get('name', '')}: {e}")
                    continue
            
            logger.info(f"Processed {len(editable_inputs)} confirmation page inputs")
            
        except Exception as e:
            logger.warning(f"Error handling confirmation page inputs: {e}")
