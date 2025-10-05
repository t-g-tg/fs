"""
セキュリティ強化ロガー

個人情報や機密データのログ出力時マスキング機能を提供
"""

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


class SecurityLogger:
    """セキュリティ強化されたロガー"""
    
    # マスキング対象の機密フィールド
    SENSITIVE_FIELDS = [
        'email_1', 'email_2', 'phone_1', 'phone_2', 'phone_3',
        'last_name', 'first_name', 'last_name_kana', 'first_name_kana',
        'last_name_hiragana', 'first_name_hiragana', 'postal_code_1', 
        'postal_code_2', 'address_1', 'address_2', 'address_3', 'address_4', 
        'address_5', 'form_sender_name', 'position', 'department'
    ]
    
    @classmethod
    def mask_sensitive_data(cls, data: Any) -> Any:
        """機密データをマスキング"""
        if isinstance(data, dict):
            masked = {}
            for key, value in data.items():
                if key.lower() in [field.lower() for field in cls.SENSITIVE_FIELDS]:
                    if isinstance(value, str) and value:
                        # 最初の1文字と最後の1文字以外をマスク
                        if len(value) <= 2:
                            masked[key] = '*' * len(value)
                        else:
                            masked[key] = value[0] + '*' * (len(value) - 2) + value[-1]
                    else:
                        masked[key] = '***MASKED***'
                else:
                    masked[key] = cls.mask_sensitive_data(value)
            return masked
        elif isinstance(data, list):
            return [cls.mask_sensitive_data(item) for item in data]
        elif isinstance(data, str):
            # メールアドレス、電話番号、個人名らしき文字列のマスキング
            if cls._looks_like_email(data):
                return cls._mask_email(data)
            elif cls._looks_like_phone(data):
                return cls._mask_phone(data)
            elif cls._looks_like_japanese_name(data):
                return cls._mask_japanese_name(data)
        return data
    
    @classmethod
    def _looks_like_email(cls, text: str) -> bool:
        """メールアドレスらしき文字列かどうか"""
        return '@' in text and '.' in text
    
    @classmethod
    def _mask_email(cls, email: str) -> str:
        """メールアドレスをマスキング"""
        if '@' not in email:
            return email
        local, domain = email.split('@', 1)
        if len(local) <= 2:
            masked_local = '*' * len(local)
        else:
            masked_local = local[0] + '*' * (len(local) - 2) + local[-1]
        return f"{masked_local}@{domain}"
    
    @classmethod
    def _looks_like_phone(cls, text: str) -> bool:
        """電話番号らしき文字列かどうか"""
        return bool(re.match(r'^[\d\-\(\)\+\s]+$', text)) and len(text) >= 8
    
    @classmethod
    def _mask_phone(cls, phone: str) -> str:
        """電話番号をマスキング"""
        if len(phone) <= 4:
            return '*' * len(phone)
        return phone[:2] + '*' * (len(phone) - 4) + phone[-2:]
    
    @classmethod
    def _looks_like_japanese_name(cls, text: str) -> bool:
        """日本語の名前らしき文字列かどうか"""
        return bool(re.search(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]', text)) and len(text) <= 10
    
    @classmethod
    def _mask_japanese_name(cls, name: str) -> str:
        """日本語名をマスキング"""
        if len(name) <= 1:
            return '*'
        elif len(name) == 2:
            return name[0] + '*'
        else:
            return name[0] + '*' * (len(name) - 2) + name[-1]
    
    @classmethod
    def safe_log_info(cls, message: str, data: Any = None):
        """安全なINFOログ出力"""
        if data is not None:
            masked_data = cls.mask_sensitive_data(data)
            logger.info(f"{message}: {masked_data}")
        else:
            logger.info(message)
    
    @classmethod
    def safe_log_debug(cls, message: str, data: Any = None):
        """安全なDEBUGログ出力"""
        if data is not None:
            masked_data = cls.mask_sensitive_data(data)
            logger.debug(f"{message}: {masked_data}")
        else:
            logger.debug(message)
    
    @classmethod
    def safe_log_warning(cls, message: str, data: Any = None):
        """安全なWARNINGログ出力"""
        if data is not None:
            masked_data = cls.mask_sensitive_data(data)
            logger.warning(f"{message}: {masked_data}")
        else:
            logger.warning(message)
    
    @classmethod
    def safe_log_error(cls, message: str, data: Any = None):
        """安全なERRORログ出力"""
        if data is not None:
            masked_data = cls.mask_sensitive_data(data)
            logger.error(f"{message}: {masked_data}")
        else:
            logger.error(message)