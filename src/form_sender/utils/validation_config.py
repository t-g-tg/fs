"""
設定値検証システム
外部設定ファイルを使用した型安全な設定管理
"""

import json
import os
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Union, List

logger = logging.getLogger(__name__)


class ValidationConfigManager:
    """設定値検証管理クラス"""
    
    def __init__(self, config_file: Optional[str] = None):
        """初期化
        
        Args:
            config_file: 設定ファイルパス（デフォルトは config/validation.json）
        """
        if config_file is None:
            # プロジェクトルートから設定ファイルを探す
            project_root = Path(__file__).parent.parent.parent.parent
            config_file = project_root / "config" / "validation.json"
        
        self.config_file = Path(config_file)
        self.validation_config = self._load_validation_config()
        
    def _load_validation_config(self) -> Dict[str, Any]:
        """設定ファイルを読み込み"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Validation config file not found: {self.config_file}")
            return {}
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in validation config: {e}")
            return {}
    
    def get_supabase_key_validation(self) -> Dict[str, Any]:
        """Supabaseキー検証設定を取得"""
        env_config = self.validation_config.get("environment_variables", {})
        supabase_config = env_config.get("SUPABASE_SERVICE_ROLE_KEY", {})
        
        return {
            'required': supabase_config.get('required', True),
            'min_length': supabase_config.get('min_length', 100),
            'key_prefix': supabase_config.get('key_prefix', 'eyJ'),
            'error_msg': supabase_config.get('error_msg', 'Invalid Supabase service role key')
        }
    
    def get_supabase_url_validation(self) -> Dict[str, Any]:
        """Supabase URL検証設定を取得"""
        env_config = self.validation_config.get("environment_variables", {})
        url_config = env_config.get("SUPABASE_URL", {})
        
        return {
            'required': url_config.get('required', True),
            'min_length': url_config.get('min_length', 20),
            'url_prefix': url_config.get('url_prefix', 'https://'),
            'error_msg': url_config.get('error_msg', 'Invalid Supabase URL')
        }
    
    def get_github_actions_validation(self) -> Dict[str, Any]:
        """GitHub Actions検証設定を取得"""
        env_config = self.validation_config.get("environment_variables", {})
        github_config = env_config.get("GITHUB_ACTIONS", {})
        
        return {
            'required': github_config.get('required', False),
            'allowed_values': github_config.get('allowed_values', ['true', 'false']),
            'error_msg': github_config.get('error_msg', 'GITHUB_ACTIONS must be true or false')
        }
    
    def validate_supabase_key(self, key: str) -> bool:
        """Supabaseキーの検証
        
        Args:
            key: 検証するキー
            
        Returns:
            検証結果
        """
        config = self.get_supabase_key_validation()
        
        if not key and config['required']:
            return False
            
        if len(key) < config['min_length']:
            return False
            
        if not key.startswith(config['key_prefix']):
            return False
            
        return True
    
    def validate_supabase_url(self, url: str) -> bool:
        """Supabase URLの検証
        
        Args:
            url: 検証するURL
            
        Returns:
            検証結果
        """
        config = self.get_supabase_url_validation()
        
        if not url and config['required']:
            return False
            
        if len(url) < config['min_length']:
            return False
            
        if not url.startswith(config['url_prefix']):
            return False
            
        return True
    
    def validate_github_actions_flag(self, value: str) -> bool:
        """GitHub Actions フラグの検証
        
        Args:
            value: 検証する値
            
        Returns:
            検証結果
        """
        config = self.get_github_actions_validation()
        
        if not value and not config['required']:
            return True  # オプショナルで値なしはOK
            
        return value.lower() in config['allowed_values']
    
    def get_form_sender_config(self) -> Dict[str, Any]:
        """Form Sender設定を取得"""
        return self.validation_config.get("form_sender", {
            "max_workers": {"default": 2, "min_value": 1, "max_value": 10},
            "batch_size": {"default": 10, "min_value": 1, "max_value": 100},
            "max_execution_time_hours": {"default": 5, "min_value": 1, "max_value": 12}
        })
    
    def get_security_config(self) -> Dict[str, Any]:
        """セキュリティ設定を取得"""
        return self.validation_config.get("security", {
            "log_sanitization_enabled": {"default": True},
            "github_actions_enhanced_masking": {"default": True}
        })


# シングルトンインスタンス
_validation_manager: Optional[ValidationConfigManager] = None


def get_validation_manager() -> ValidationConfigManager:
    """ValidationConfigManagerのシングルトンインスタンスを取得"""
    global _validation_manager
    if _validation_manager is None:
        _validation_manager = ValidationConfigManager()
    return _validation_manager


def validate_environment_variable(var_name: str, value: str) -> tuple[bool, str]:
    """環境変数の検証
    
    Args:
        var_name: 環境変数名
        value: 値
        
    Returns:
        (検証結果, エラーメッセージ)
    """
    manager = get_validation_manager()
    
    if var_name == "SUPABASE_SERVICE_ROLE_KEY":
        is_valid = manager.validate_supabase_key(value)
        config = manager.get_supabase_key_validation()
        return is_valid, config['error_msg'] if not is_valid else ""
    
    elif var_name == "SUPABASE_URL":
        is_valid = manager.validate_supabase_url(value)
        config = manager.get_supabase_url_validation()
        return is_valid, config['error_msg'] if not is_valid else ""
    
    elif var_name == "GITHUB_ACTIONS":
        is_valid = manager.validate_github_actions_flag(value)
        config = manager.get_github_actions_validation()
        return is_valid, config['error_msg'] if not is_valid else ""
    
    # 未定義の環境変数は常にValid
    return True, ""