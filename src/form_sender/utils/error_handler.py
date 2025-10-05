"""
標準化されたエラーハンドリングユーティリティ
"""

import logging
from typing import Any, Dict, Optional, Callable, TypeVar
from pathlib import Path

logger = logging.getLogger(__name__)

T = TypeVar('T')

class ConfigLoadError(Exception):
    """設定読み込みエラー"""
    pass


class StandardErrorHandler:
    """標準化されたエラーハンドリング"""
    
    @staticmethod
    def load_config_with_fallback(
        loader_func: Callable[[], T],
        fallback_value: T,
        config_name: str,
        critical: bool = False
    ) -> T:
        """
        設定読み込みを標準化されたエラーハンドリングで実行
        
        Args:
            loader_func: 設定読み込み関数
            fallback_value: フォールバック値
            config_name: 設定名（ログ用）
            critical: 重要な設定か（Trueの場合は例外を伝播）
            
        Returns:
            T: 読み込まれた設定またはフォールバック値
            
        Raises:
            ConfigLoadError: critical=Trueで読み込み失敗時
        """
        try:
            result = loader_func()
            logger.info(f"Successfully loaded config: {config_name}")
            return result
            
        except FileNotFoundError as e:
            msg = f"Config file not found for {config_name}: {e}"
            if critical:
                logger.error(msg)
                raise ConfigLoadError(msg) from e
            else:
                logger.warning(f"{msg}, using fallback value")
                return fallback_value
                
        except ValueError as e:
            msg = f"Invalid config format for {config_name}: {e}"
            if critical:
                logger.error(msg)
                raise ConfigLoadError(msg) from e
            else:
                logger.warning(f"{msg}, using fallback value")
                return fallback_value
                
        except Exception as e:
            msg = f"Unexpected error loading {config_name}: {e}"
            if critical:
                logger.error(msg)
                raise ConfigLoadError(msg) from e
            else:
                logger.warning(f"{msg}, using fallback value")
                return fallback_value
    
    @staticmethod
    def safe_execute_with_retry(
        func: Callable[[], T],
        max_retries: int = 3,
        retry_delay: float = 1.0,
        operation_name: str = "operation"
    ) -> Optional[T]:
        """
        リトライ付き安全実行
        
        Args:
            func: 実行する関数
            max_retries: 最大リトライ回数
            retry_delay: リトライ間隔（秒）
            operation_name: 操作名（ログ用）
            
        Returns:
            Optional[T]: 実行結果（失敗時はNone）
        """
        import time
        
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                result = func()
                if attempt > 0:
                    logger.info(f"{operation_name} succeeded on attempt {attempt + 1}")
                return result
                
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    logger.warning(f"{operation_name} failed on attempt {attempt + 1}: {e}, retrying in {retry_delay}s")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"{operation_name} failed after {max_retries + 1} attempts: {e}")
                    
        return None
    
    @staticmethod
    def validate_path_safety(file_path: str, allowed_directories: list = None) -> bool:
        """
        パスの安全性検証
        
        Args:
            file_path: 検証するファイルパス
            allowed_directories: 許可されたディレクトリのリスト
            
        Returns:
            bool: 安全な場合True
        """
        try:
            path = Path(file_path).resolve()
            
            # 基本的な安全性チェック
            if '..' in str(path):
                logger.warning(f"Path traversal attempt blocked: {file_path}")
                return False
                
            # 許可されたディレクトリチェック
            if allowed_directories:
                for allowed_dir in allowed_directories:
                    if path.is_relative_to(Path(allowed_dir).resolve()):
                        return True
                logger.warning(f"Path outside allowed directories: {file_path}")
                return False
                
            return True
            
        except Exception as e:
            logger.warning(f"Path validation error: {e}")
            return False


# 便利な関数エイリアス
load_config_safe = StandardErrorHandler.load_config_with_fallback
execute_with_retry = StandardErrorHandler.safe_execute_with_retry
validate_path = StandardErrorHandler.validate_path_safety