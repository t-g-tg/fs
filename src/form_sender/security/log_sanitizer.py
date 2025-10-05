"""
ログサニタイゼーション

機密情報のログ出力を防止し、安全なログ記録を実現する
"""

import re
import logging
from functools import lru_cache
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


@lru_cache(maxsize=128)
def get_compiled_pattern(pattern_str: str):
    """LRUキャッシュ付き正規表現コンパイル（パフォーマンス最適化）"""
    try:
        return re.compile(pattern_str)
    except re.error as e:
        logger.warning(f"Failed to compile cached regex pattern '{pattern_str}': {e}")
        return None


class LogSanitizer:
    """ログサニタイゼーションクラス"""

    def __init__(self):
        """初期化"""
        # 機密情報パターン（正規表現）- 企業識別情報強化版
        self.sensitive_patterns = [
            # API キー / トークン
            (r'(?i)(api[_-]?key|token|secret|password)\s*[=:]\s*["\']?([a-zA-Z0-9_-]{8,})["\']?', r"\1=***REDACTED***"),
            
            # URL全体をサニタイズ（完全にマスク）
            (r"https?://[^\s]+", r"***URL_REDACTED***"),
            
            # URL中のクレデンシャル（上記パターンでカバーされるが、念のため残す）
            (r"https?://[^:]+:([^@]+)@", r"https://***:***REDACTED***@"),
            
            # 企業名パターン（日本語企業名の完全マスク）
            (r'(?i)(company[_-]?name|企業名|会社名)\s*[=:\-]\s*["\']?([^"\'\s\n,}{]+)["\']?', r'\1=***COMPANY_NAME_REDACTED***'),
            
            # 株式会社・有限会社等を含む企業名
            (r'(株式会社|有限会社|合同会社|合資会社|合名会社|一般社団法人|公益社団法人|一般財団法人|公益財団法人|特定非営利活動法人|NPO法人)[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF\w\s]{1,50}', r'***COMPANY_REDACTED***'),
            
            # 企業名らしき文字列（カタカナ・漢字混在）
            (r'[\u30A0-\u30FF\u4E00-\u9FAF]{2,}[\u30A0-\u30FF\u4E00-\u9FAF\w\s]*(?:株式会社|有限会社|会社|法人|コーポレーション|Corp|Inc|Ltd|Co\.)', r'***COMPANY_REDACTED***'),
            
            # フォームURL関連の完全マスク
            (r'(?i)(form[_-]?url|page[_-]?url|target[_-]?url)\s*[=:\-]\s*["\']?([^"\'\s\n,}{]+)["\']?', r'\1=***FORM_URL_REDACTED***'),
            
            # メールアドレス（完全マスク - GitHub Actions環境では特に重要）
            (r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', r'***EMAIL_REDACTED***'),
            
            # 電話番号（日本の電話番号パターン）
            (r"\b(0\d{1,4}-?\d{1,4}-?\d{3,4})\b", r"***-***-****"),
            
            # 住所（大まかな住所パターン）
            (r"[都道府県市区町村]{1,3}[0-9一二三四五六七八九十\-−]{1,10}", r"***住所***"),
            
            # クレジットカード番号
            (r"\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b", r"****-****-****-****"),
            
            # SQL中のパスワード
            (r"(?i)(password\s*=\s*['\"])([^'\"]+)(['\"])", r"\1***REDACTED***\3"),
            
            # JSON中の機密データ（企業情報拡張版）
            (r'(?i)("(?:password|secret|token|key|company_name|form_url|company_url)"\s*:\s*")([^"]+)(")', r"\1***REDACTED***\3"),
            
            # ログメッセージ中の企業名参照
            (r'(?i)(企業|company|会社)[^\s]*[:\s]*([^\s\n,}{]+)', r'\1: ***COMPANY_REDACTED***'),
            
            # 日本語の人名（姓名パターン - 拡張）
            (r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]{1,4}\s*[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]{1,4}(?=\s*(さん|様|氏|殿))', r'***NAME_REDACTED***'),
            
            # 一般的な日本語人名パターン（2-4文字の姓・名）
            (r'(?:田中|佐藤|鈴木|高橋|渡辺|伊藤|山本|中村|小林|加藤|吉田|山田|太郎|花子|一郎|次郎)[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]*', r'***NAME_REDACTED***'),
            
            # 問い合わせ内容・メッセージパターン
            (r'(?i)(message|inquiry|comment|content|body|text|問い合わせ|メッセージ|本文|内容)\s*[=:]\s*["\']?([^"\']{50,})["\']?', r'\1=***MESSAGE_REDACTED***'),
            
            # 個人情報を含む可能性のある長いテキスト（30文字以上の日本語文、技術用語除く）
            # Note: この処理はsanitize_stringでintelligent処理に切り替わります
        ]
        
        # 正規表現パターンをコンパイル済みオブジェクトとしてキャッシュ（パフォーマンス最適化）
        self._compiled_patterns = []
        for pattern, replacement in self.sensitive_patterns:
            try:
                compiled_pattern = re.compile(pattern)
                self._compiled_patterns.append((compiled_pattern, replacement))
            except re.error as e:
                logger.warning(f"Failed to compile regex pattern '{pattern}': {e}")

        # 特定の値を完全にマスクするキーワード（クライアント情報強化版）
        self.mask_completely = [
            "SUPABASE_SERVICE_ROLE_KEY", "SUPABASE_URL", 
            "targeting_id", "TARGETING_ID",
            "company_name", "COMPANY_NAME", "企業名", "会社名",
            "form_url", "FORM_URL", "company_url", "COMPANY_URL",
            "page_url", "PAGE_URL", "target_url", "TARGET_URL",
            # クライアント個人情報
            "name", "NAME", "氏名", "名前", "お名前",
            "email", "EMAIL", "メール", "メールアドレス",
            "message", "MESSAGE", "メッセージ", "問い合わせ", "本文", "内容",
            "phone", "PHONE", "電話", "電話番号", "TEL",
            "address", "ADDRESS", "住所", "所在地"
        ]

        # URL中の機密パラメータ
        self.sensitive_url_params = ["token", "key", "secret", "password", "auth"]
        
        # GitHub Actions環境での追加マスキング設定
        import os
        self.is_github_actions = os.getenv('GITHUB_ACTIONS', '').lower() == 'true'
        if self.is_github_actions:
            # CI/CD環境では更に厳格なマスキングを適用
            self.github_actions_patterns = [
                # record_id以外の数値IDも完全マスク
                (r'(?i)(id|ID)\s*[=:]\s*(\d+)', r'\1=***ID_REDACTED***'),
                # 企業名パターン（簡素化版）- 法人格後に日本語が続く場合のみマスク
                (r'(株式会社|有限会社|合同会社|合資会社|合名会社|一般社団法人|公益社団法人|一般財団法人|公益財団法人|特定非営利活動法人|NPO法人|会社|法人|コーポレーション|Corp|Inc|Ltd|Co)[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF\w\s]{1,50}', r'***COMPANY_REDACTED***'),
                # 英語の企業名らしき大文字単語
                (r'\b[A-Z][a-zA-Z]{2,}\s+(?:Corp|Inc|Ltd|LLC|Co|Company|Corporation|Limited)\b', r'***COMPANY_REDACTED***'),
                # ドメイン名の完全マスク
                (r'\b[a-zA-Z0-9-]+\.(com|co\.jp|jp|net|org|info|biz)[^\s]*', r'***DOMAIN_REDACTED***'),
            ]
            self.sensitive_patterns.extend(self.github_actions_patterns)
            
            # GitHub Actions専用パターンもコンパイル
            for pattern, replacement in self.github_actions_patterns:
                try:
                    compiled_pattern = re.compile(pattern)
                    self._compiled_patterns.append((compiled_pattern, replacement))
                except re.error as e:
                    logger.warning(f"Failed to compile GitHub Actions regex pattern '{pattern}': {e}")

    def sanitize_string(self, text: str) -> str:
        """
        文字列から機密情報を除去（2段階フィルタリング最適化版）

        Args:
            text: サニタイズ対象の文字列

        Returns:
            str: サニタイズ済み文字列
        """
        if not isinstance(text, str):
            return str(text)

        # 2段階フィルタリング最適化：基本パターンマッチング
        # 機密情報の可能性が高い場合のみ詳細処理を実行
        if not self._has_sensitive_content(text):
            return text

        sanitized = text

        # コンパイル済みパターンでサニタイズ（パフォーマンス最適化）
        for compiled_pattern, replacement in self._compiled_patterns:
            try:
                sanitized = compiled_pattern.sub(replacement, sanitized)
            except Exception as e:
                logger.warning(f"Error applying compiled sanitization pattern: {e}")
                continue

        # 長文日本語の知的サニタイズ（技術用語保護）
        sanitized = self._intelligent_sanitize_long_text(sanitized)

        return sanitized

    @lru_cache(maxsize=256)  
    def _has_sensitive_content(self, text: str) -> bool:
        """
        高速事前フィルタリング：機密情報が含まれる可能性を判定
        簡素なキーワード検索で高速スクリーニング
        
        Args:
            text: チェック対象の文字列
            
        Returns:
            bool: 機密情報を含む可能性がある場合True
        """
        if len(text) < 5:  # 短すぎる文字列はスキップ
            return False
            
        # 高速キーワードマッチング（正規表現を使わない）
        sensitive_keywords = [
            'password', 'secret', 'token', 'key', 'api',
            'https://', 'http://', '@', '.com', '.jp',
            '株式会社', '有限会社', 'Corp', 'Inc', 'Ltd',
            'company_name', 'form_url', 'email'
        ]
        
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in sensitive_keywords)

    @lru_cache(maxsize=128)
    def _contains_technical_terms(self, text: str) -> bool:
        """
        技術用語・エラーメッセージ検出（デバッグ情報保護用）
        
        Args:
            text: チェック対象の文字列
            
        Returns:
            bool: 技術用語やエラーメッセージが含まれる場合True
        """
        technical_keywords = [
            # エラー関連
            'error', 'exception', 'timeout', 'failed', 'failure', 'traceback',
            'エラー', '例外', 'タイムアウト', '失敗', 'エラー発生',
            
            # 処理関連  
            'processing', 'connection', 'response', 'request', 'status',
            '処理', '接続', '応答', 'リクエスト', 'ステータス', '実行',
            
            # 成功・完了関連
            'success', 'completed', 'finished', 'done', 'ok',
            '成功', '完了', '終了', 'OK', '正常',
            
            # システム関連
            'worker', 'process', 'thread', 'queue', 'buffer',
            'ワーカー', 'プロセス', 'キュー', 'バッファ',
            
            # ログレベル
            'debug', 'info', 'warning', 'critical',
            
            # 技術識別子
            'id:', 'pid:', 'task_id:', 'record_id:'
        ]
        
        text_lower = text.lower()
        return any(keyword in text_lower for keyword in technical_keywords)

    def _intelligent_sanitize_long_text(self, text: str) -> str:
        """
        知的長文サニタイズ：技術用語を含むテキストは保護
        
        Args:
            text: サニタイズ対象の文字列
            
        Returns:
            str: 知的サニタイズ済みの文字列
        """
        import re
        
        # 30文字以上の日本語文を検出
        long_japanese_pattern = r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF\s]{30,}'
        matches = re.finditer(long_japanese_pattern, text)
        
        result = text
        offset = 0
        
        for match in matches:
            matched_text = match.group()
            
            # 技術用語・エラーメッセージが含まれるかチェック
            if self._contains_technical_terms(matched_text):
                # 技術用語を含む場合：部分マスクのみ（デバッグ情報保護）
                # 重要な情報は残しつつ、一部を見えなくする
                if len(matched_text) > 50:
                    # 前半と後半を残して中央部をマスク
                    preserved = matched_text[:20] + '***[PARTIAL_REDACTED]***' + matched_text[-15:]
                else:
                    # 短めの場合はより軽いマスク
                    preserved = matched_text[:15] + '***[INFO_REDACTED]***' + matched_text[-10:]
            else:
                # 技術用語を含まない長文：完全マスク（従来通り）
                preserved = '***TEXT_REDACTED***'
            
            # テキスト置換（オフセット調整）
            start_pos = match.start() + offset
            end_pos = match.end() + offset
            result = result[:start_pos] + preserved + result[end_pos:]
            offset += len(preserved) - len(matched_text)
        
        return result
    
    def sanitize_for_github_actions(self, text: str) -> str:
        """
        GitHub Actions環境専用の強化サニタイゼーション
        record_id以外のすべての識別情報をマスク
        
        Args:
            text: サニタイゼーション対象の文字列
            
        Returns:
            str: 強化サニタイゼーション済み文字列
        """
        if not isinstance(text, str):
            return str(text)
        
        # 基本サニタイゼーションを適用
        sanitized = self.sanitize_string(text)
        
        # GitHub Actions環境でのみ追加処理
        if self.is_github_actions:
            # record_idのみを保護しながら他のIDをマスク
            # まずrecord_idを一時的に保護
            import re
            record_id_matches = re.findall(r'(record_id[:\s=]*\d+)', sanitized, re.IGNORECASE)
            protected_record_ids = {}
            
            for i, match in enumerate(record_id_matches):
                placeholder = f"__PROTECTED_RECORD_ID_{i}__"
                protected_record_ids[placeholder] = match
                sanitized = sanitized.replace(match, placeholder, 1)
            
            # 追加のGitHub Actions専用マスキング
            # あらゆるID（record_id以外）をマスク
            sanitized = re.sub(r'(?<!record_)(id|ID)\s*[=:]\s*(\d+)', r'\1=***ID_REDACTED***', sanitized)
            
            # 3文字以上の日本語文字列（企業名の可能性）を全てマスク
            sanitized = re.sub(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]{3,}', r'***JP_TEXT***', sanitized)
            
            # 保護したrecord_idを復元
            for placeholder, original in protected_record_ids.items():
                sanitized = sanitized.replace(placeholder, original)
        
        return sanitized
    
    def safe_log_for_github_actions(self, message: str, record_id: int = None) -> str:
        """
        GitHub Actions環境での安全なログメッセージ生成
        record_idのみを含む匿名化されたメッセージを作成
        
        Args:
            message: 基本メッセージ
            record_id: 許可されるrecord_id
            
        Returns:
            str: 安全なログメッセージ
        """
        # record_id以外の識別情報をすべて除去
        safe_message = self.sanitize_for_github_actions(message)
        
        # record_idが指定されている場合は明示的に追加
        if record_id is not None:
            safe_message = f"[record_id: {record_id}] {safe_message}"
        
        return safe_message

    def sanitize_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        辞書から機密情報を除去

        Args:
            data: サニタイズ対象の辞書

        Returns:
            Dict[str, Any]: サニタイズ済み辞書
        """
        if not isinstance(data, dict):
            return data

        sanitized = {}

        for key, value in data.items():
            # キー名チェック（完全マスク）
            if any(sensitive in key.upper() for sensitive in self.mask_completely):
                sanitized[key] = "***REDACTED***"
                continue

            # 値の再帰的サニタイズ
            if isinstance(value, dict):
                sanitized[key] = self.sanitize_dict(value)
            elif isinstance(value, list):
                sanitized[key] = self.sanitize_list(value)
            elif isinstance(value, str):
                sanitized[key] = self.sanitize_string(value)
            else:
                sanitized[key] = value

        return sanitized

    def sanitize_list(self, data: List[Any]) -> List[Any]:
        """
        リストから機密情報を除去

        Args:
            data: サニタイズ対象のリスト

        Returns:
            List[Any]: サニタイズ済みリスト
        """
        if not isinstance(data, list):
            return data

        sanitized = []

        for item in data:
            if isinstance(item, dict):
                sanitized.append(self.sanitize_dict(item))
            elif isinstance(item, list):
                sanitized.append(self.sanitize_list(item))
            elif isinstance(item, str):
                sanitized.append(self.sanitize_string(item))
            else:
                sanitized.append(item)

        return sanitized

    def sanitize_url(self, url: str) -> str:
        """
        URLから機密パラメータを除去

        Args:
            url: サニタイズ対象のURL

        Returns:
            str: サニタイズ済みURL
        """
        if not isinstance(url, str) or "?" not in url:
            return self.sanitize_string(url)

        base_url, query_string = url.split("?", 1)

        # クエリパラメータを解析
        params = []
        for param in query_string.split("&"):
            if "=" in param:
                key, value = param.split("=", 1)
                if any(sensitive in key.lower() for sensitive in self.sensitive_url_params):
                    params.append(f"{key}=***REDACTED***")
                else:
                    params.append(f"{key}={value}")
            else:
                params.append(param)

        sanitized_url = f"{base_url}?{'&'.join(params)}"
        return self.sanitize_string(sanitized_url)

    def sanitize_log_record(self, record: logging.LogRecord) -> None:
        """
        LogRecordの内容をサニタイズ

        Args:
            record: ログレコード
        """
        try:
            # メッセージのサニタイズ
            if hasattr(record, "getMessage"):
                original_msg = record.getMessage()
                record.msg = self.sanitize_string(str(original_msg))
                record.args = ()

            # 追加属性のサニタイズ
            if hasattr(record, "__dict__"):
                for attr_name, attr_value in record.__dict__.items():
                    if attr_name.startswith("_") or attr_name in [
                        "name",
                        "levelno",
                        "levelname",
                        "pathname",
                        "filename",
                        "module",
                        "lineno",
                        "funcName",
                        "created",
                        "msecs",
                        "relativeCreated",
                        "thread",
                        "threadName",
                        "processName",
                        "process",
                        "stack_info",
                    ]:
                        continue

                    if isinstance(attr_value, str):
                        setattr(record, attr_name, self.sanitize_string(attr_value))
                    elif isinstance(attr_value, dict):
                        setattr(record, attr_name, self.sanitize_dict(attr_value))
                    elif isinstance(attr_value, list):
                        setattr(record, attr_name, self.sanitize_list(attr_value))

        except Exception as e:
            # サニタイズ処理でエラーが発生した場合、元のレコードはそのままにしてエラーログを出力
            logger.warning(f"Error sanitizing log record: {e}")


class SanitizingFormatter(logging.Formatter):
    """サニタイゼーション機能付きフォーマッター"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sanitizer = LogSanitizer()

    def format(self, record: logging.LogRecord) -> str:
        """
        ログレコードをフォーマット（サニタイズ付き）

        Args:
            record: ログレコード

        Returns:
            str: フォーマット済みログメッセージ
        """
        # レコードのコピーを作成（元のレコードを変更しないため）
        record_copy = logging.makeLogRecord(record.__dict__)

        # サニタイズ実行
        self.sanitizer.sanitize_log_record(record_copy)

        # 通常のフォーマット処理
        return super().format(record_copy)


class SanitizingHandler(logging.Handler):
    """サニタイゼーション機能付きハンドラー"""

    def __init__(self, handler: logging.Handler):
        super().__init__()
        self.handler = handler
        self.sanitizer = LogSanitizer()

        # 元のハンドラーの設定を継承
        self.setLevel(handler.level)
        if handler.formatter:
            self.setFormatter(handler.formatter)

    def emit(self, record: logging.LogRecord):
        """
        ログレコードを出力（サニタイズ付き）

        Args:
            record: ログレコード
        """
        try:
            # レコードのコピーを作成
            record_copy = logging.makeLogRecord(record.__dict__)

            # サニタイズ実行
            self.sanitizer.sanitize_log_record(record_copy)

            # 元のハンドラーに委譲
            self.handler.emit(record_copy)

        except Exception as e:
            # サニタイズ処理でエラーが発生した場合、元のレコードをそのまま出力
            logger.warning(f"Error in sanitizing handler: {e}")
            self.handler.emit(record)


# グローバルサニタイザーインスタンス
global_sanitizer = LogSanitizer()


def sanitize_for_log(data: Union[str, Dict, List, Any]) -> Union[str, Dict, List, Any]:
    """
    ログ出力用のデータサニタイゼーション便利関数

    Args:
        data: サニタイズ対象のデータ

    Returns:
        Union[str, Dict, List, Any]: サニタイズ済みデータ
    """
    if isinstance(data, str):
        return global_sanitizer.sanitize_string(data)
    elif isinstance(data, dict):
        return global_sanitizer.sanitize_dict(data)
    elif isinstance(data, list):
        return global_sanitizer.sanitize_list(data)
    else:
        return data


def setup_sanitized_logging(logger_name: Optional[str] = None) -> logging.Logger:
    """
    サニタイゼーション機能付きロガーをセットアップ

    Args:
        logger_name: ロガー名（Noneの場合はルートロガー）

    Returns:
        logging.Logger: 設定済みロガー
    """
    target_logger = logging.getLogger(logger_name)

    # 既存ハンドラーをサニタイゼーション機能付きに置き換え
    original_handlers = target_logger.handlers[:]
    target_logger.handlers.clear()

    for handler in original_handlers:
        sanitizing_handler = SanitizingHandler(handler)
        target_logger.addHandler(sanitizing_handler)

    return target_logger
