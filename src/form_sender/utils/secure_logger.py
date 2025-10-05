"""
セキュアログ管理システム
機密情報の自動マスキングと安全なログ出力を提供
"""

import re
import logging
from typing import Any, Dict, List, Pattern, Optional
from functools import wraps
import json

class SecureLogger:
    """機密情報を自動でマスキングするセキュアなロガー"""
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._setup_sensitive_patterns()
    
    def _setup_sensitive_patterns(self) -> None:
        """機密情報パターンの定義"""
        self.sensitive_patterns = [
            # メールアドレス
            (r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '***@***.***'),
            
            # 電話番号（日本）
            (r'\b0\d{1,4}-\d{1,4}-\d{3,4}\b', '***-****-****'),
            (r'\b0\d{9,11}\b', '***********'),
            
            # パスワード関連
            (r'(password|passwd|pwd|pass)\s*[=:]\s*[\'"]?([^\s\'"]+)[\'"]?', r'\1=***'),
            (r'(token|secret|key|api_key)\s*[=:]\s*[\'"]?([^\s\'"]+)[\'"]?', r'\1=***'),
            
            # URL内の認証情報
            (r'(https?://)[^:]+:[^@]+@', r'\1***:***@'),
            
            # 住所（日本の郵便番号）
            (r'\b\d{3}-\d{4}\b', '***-****'),
            
            # 個人名（カタカナ・ひらがな・漢字の組み合わせ）
            (r'[一-龯ひ-ゖヲ-ヶー]{2,8}\s*[一-龯ひ-ゖヲ-ヶー]{1,8}', '***名前***'),
            
            # 企業名（株式会社、有限会社等を含む）
            (r'(株式会社|有限会社|合同会社|合資会社|合名会社)\s*[^\s\n]{2,20}', r'\1***'),
            
            # IP アドレス
            (r'\b(?:\d{1,3}\.){3}\d{1,3}\b', '***.***.***.**'),
            
            # クレジットカード番号様のパターン
            (r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b', '****-****-****-****'),
            
            # セッションID、トークン様の長い英数字
            (r'\b[A-Za-z0-9]{20,}\b', '***TOKEN***'),
        ]
        
        # コンパイル済み正規表現パターン
        self.compiled_patterns = [
            (re.compile(pattern, re.IGNORECASE), replacement) 
            for pattern, replacement in self.sensitive_patterns
        ]
    
    def _sanitize_message(self, message: str) -> str:
        """メッセージから機密情報を除去"""
        try:
            if not isinstance(message, str):
                message = str(message)
            
            # 追加: LogSanitizer を併用して URL/企業名などを強力にマスク（CIでは特に重要）
            try:
                from form_sender.security.log_sanitizer import LogSanitizer
                sanitizer = LogSanitizer()
                message = sanitizer.sanitize_string(message)
            except Exception:
                pass

            # 各パターンでマスキング
            sanitized = message
            for pattern, replacement in self.compiled_patterns:
                sanitized = pattern.sub(replacement, sanitized)
            
            return sanitized
            
        except Exception as e:
            # サニタイズでエラーが発生した場合は、安全のため全体をマスク
            return f"[LOG_SANITIZATION_ERROR: {type(e).__name__}]"
    
    def _sanitize_extra_data(self, extra_data: Dict[str, Any]) -> Dict[str, Any]:
        """追加データから機密情報を除去"""
        try:
            if not extra_data:
                return {}
            
            sanitized = {}
            for key, value in extra_data.items():
                # キー自体が機密情報の可能性をチェック
                sanitized_key = self._sanitize_message(str(key))
                
                # 値の処理
                if isinstance(value, str):
                    sanitized_value = self._sanitize_message(value)
                elif isinstance(value, dict):
                    sanitized_value = self._sanitize_extra_data(value)
                elif isinstance(value, list):
                    sanitized_value = [
                        self._sanitize_message(str(item)) if isinstance(item, str) 
                        else str(item) for item in value
                    ]
                else:
                    sanitized_value = str(value)
                
                sanitized[sanitized_key] = sanitized_value
            
            return sanitized
            
        except Exception as e:
            return {"sanitization_error": f"{type(e).__name__}"}
    
    def debug(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """デバッグレベルのセキュアログ出力"""
        sanitized_message = self._sanitize_message(message)
        sanitized_extra = self._sanitize_extra_data(extra or {})
        
        if sanitized_extra:
            self.logger.debug(f"{sanitized_message} | Extra: {json.dumps(sanitized_extra, ensure_ascii=False)}")
        else:
            self.logger.debug(sanitized_message)
    
    def info(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """インフォメーションレベルのセキュアログ出力"""
        sanitized_message = self._sanitize_message(message)
        sanitized_extra = self._sanitize_extra_data(extra or {})
        
        if sanitized_extra:
            self.logger.info(f"{sanitized_message} | Extra: {json.dumps(sanitized_extra, ensure_ascii=False)}")
        else:
            self.logger.info(sanitized_message)
    
    def warning(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """警告レベルのセキュアログ出力"""
        sanitized_message = self._sanitize_message(message)
        sanitized_extra = self._sanitize_extra_data(extra or {})
        
        if sanitized_extra:
            self.logger.warning(f"{sanitized_message} | Extra: {json.dumps(sanitized_extra, ensure_ascii=False)}")
        else:
            self.logger.warning(sanitized_message)
    
    def error(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """エラーレベルのセキュアログ出力"""
        sanitized_message = self._sanitize_message(message)
        sanitized_extra = self._sanitize_extra_data(extra or {})
        
        if sanitized_extra:
            self.logger.error(f"{sanitized_message} | Extra: {json.dumps(sanitized_extra, ensure_ascii=False)}")
        else:
            self.logger.error(sanitized_message)
    
    def critical(self, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        """クリティカルレベルのセキュアログ出力"""
        sanitized_message = self._sanitize_message(message)
        sanitized_extra = self._sanitize_extra_data(extra or {})
        
        if sanitized_extra:
            self.logger.critical(f"{sanitized_message} | Extra: {json.dumps(sanitized_extra, ensure_ascii=False)}")
        else:
            self.logger.critical(sanitized_message)


def secure_log_decorator(secure_logger: SecureLogger):
    """関数の実行をセキュアにログ記録するデコレータ"""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            func_name = f"{func.__module__}.{func.__qualname__}"
            
            # 引数から機密情報を除去してログ出力
            safe_args = [str(arg)[:100] + "..." if len(str(arg)) > 100 else str(arg) for arg in args[:3]]  # 最初の3つの引数のみ
            safe_kwargs = {k: str(v)[:100] + "..." if len(str(v)) > 100 else str(v) for k, v in list(kwargs.items())[:3]}  # 最初の3つのキーワード引数のみ
            
            secure_logger.debug(f"Function started: {func_name}", {
                "args_count": len(args),
                "kwargs_keys": list(kwargs.keys()),
                "safe_args": safe_args,
                "safe_kwargs": safe_kwargs
            })
            
            try:
                if 'async' in str(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                    
                secure_logger.debug(f"Function completed: {func_name}")
                return result
                
            except Exception as e:
                secure_logger.error(f"Function failed: {func_name}", {
                    "error_type": type(e).__name__,
                    "error_message": str(e)[:200]  # エラーメッセージも制限
                })
                raise
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            func_name = f"{func.__module__}.{func.__qualname__}"
            
            # 引数から機密情報を除去してログ出力
            safe_args = [str(arg)[:100] + "..." if len(str(arg)) > 100 else str(arg) for arg in args[:3]]
            safe_kwargs = {k: str(v)[:100] + "..." if len(str(v)) > 100 else str(v) for k, v in list(kwargs.items())[:3]}
            
            secure_logger.debug(f"Function started: {func_name}", {
                "args_count": len(args),
                "kwargs_keys": list(kwargs.keys()),
                "safe_args": safe_args,
                "safe_kwargs": safe_kwargs
            })
            
            try:
                result = func(*args, **kwargs)
                secure_logger.debug(f"Function completed: {func_name}")
                return result
                
            except Exception as e:
                secure_logger.error(f"Function failed: {func_name}", {
                    "error_type": type(e).__name__,
                    "error_message": str(e)[:200]
                })
                raise
        
        # 非同期関数かどうかを判定
        import inspect
        if inspect.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper
    
    return decorator


# グローバルセキュアロガーの作成
_default_logger = logging.getLogger(__name__)
secure_logger = SecureLogger(_default_logger)

def get_secure_logger(name: str = None) -> SecureLogger:
    """セキュアロガーのインスタンスを取得"""
    if name:
        logger = logging.getLogger(name)
    else:
        logger = _default_logger
    
    return SecureLogger(logger)
