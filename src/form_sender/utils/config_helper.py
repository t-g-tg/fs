"""
Configuration Helper Utility

設定取得の共通パターンをユーティリティ化
"""

import logging
from typing import Dict, Any, Optional
from config.manager import get_worker_config

logger = logging.getLogger(__name__)


def get_module_config(
    module_name: str, 
    fallback_key: str = "multi_process",
    config_key: Optional[str] = None
) -> Dict[str, Any]:
    """
    モジュール専用設定を優先取得する共通ユーティリティ
    
    Args:
        module_name: モジュール名 (form_sender, form_finder等)
        fallback_key: フォールバック設定キー (デフォルト: multi_process)  
        config_key: 特定の設定キー (None なら全設定取得)
    
    Returns:
        Dict[str, Any]: 設定辞書
        
    Example:
        # Form-Sender専用設定を取得、フォールバックでmulti_process
        config = get_module_config("form_sender")
        
        # 特定の設定のみ取得
        db_config = get_module_config("form_sender", config_key="db_batch_writing")
    """
    try:
        worker_config = get_worker_config()
        
        # モジュール専用設定キーを構築
        module_config_key = f"{module_name}_multi_process"
        
        # モジュール専用設定を優先
        module_config = worker_config.get(module_config_key, {})
        if module_config:
            logger.info(f"Using {module_config_key} config for {module_name}")
            target_config = module_config
        else:
            # フォールバック設定を使用
            logger.info(f"Using fallback {fallback_key} config for {module_name}")
            target_config = worker_config.get(fallback_key, {})
            
        # 特定の設定キーが指定されている場合
        if config_key:
            return target_config.get(config_key, {})
            
        return target_config
        
    except Exception as e:
        logger.warning(f"Could not load {module_name} config, using empty dict: {e}")
        return {}


def get_form_sender_config(config_key: Optional[str] = None) -> Dict[str, Any]:
    """
    Form-Sender専用設定取得のショートカット
    
    Args:
        config_key: 特定の設定キー (None なら全設定取得)
        
    Returns:
        Dict[str, Any]: Form-Sender設定辞書
    """
    return get_module_config("form_sender", config_key=config_key)


def get_form_finder_config(config_key: Optional[str] = None) -> Dict[str, Any]:
    """
    Form-Finder専用設定取得のショートカット
    
    Args:
        config_key: 特定の設定キー (None なら全設定取得)
        
    Returns:
        Dict[str, Any]: Form-Finder設定辞書
    """
    return get_module_config("form_finder", config_key=config_key)