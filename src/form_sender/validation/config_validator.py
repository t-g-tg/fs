"""
設定値妥当性検証

worker_config.jsonやその他設定ファイルの値を検証し、
安全な範囲内に収まることを保証
"""

import logging
import os
from typing import Dict, Any, List, Tuple

logger = logging.getLogger(__name__)


class ConfigValidator:
    """設定値妥当性検証クラス"""
    
    # 設定値の制約定義
    CONSTRAINTS = {
        'multi_process': {
            'num_workers': {'min': 1, 'max': 4, 'type': int},
            'github_actions_workers': {'min': 1, 'max': 3, 'type': int},
            'max_workers': {'min': 1, 'max': 4, 'type': int},
            'worker_startup_timeout': {'min': 30, 'max': 300, 'type': int},
            'worker_shutdown_timeout': {'min': 10, 'max': 120, 'type': int},
            'heartbeat_interval': {'min': 10, 'max': 300, 'type': int},
            'health_check_interval': {'min': 30, 'max': 600, 'type': int},
            'batch_size': {'min': 5, 'max': 100, 'type': int},
            'max_pending_tasks': {'min': 10, 'max': 1000, 'type': int},
            'task_timeout': {'min': 60, 'max': 1800, 'type': int},
            'result_collection_timeout': {'min': 10, 'max': 300, 'type': int},
            'memory_per_worker_mb': {'min': 2048, 'max': 8192, 'type': int}
        },
        'worker': {
            'timeout_seconds': {'min': 60, 'max': 1800, 'type': int},
            'max_retries': {'min': 1, 'max': 10, 'type': int}
        },
        'form_sender': {
            'timeout_settings': {
                'page_load': {'min': 5000, 'max': 60000, 'type': int},
                'element_wait': {'min': 5000, 'max': 60000, 'type': int},
                'click_timeout': {'min': 1000, 'max': 30000, 'type': int},
                'input_timeout': {'min': 1000, 'max': 30000, 'type': int},
                'pre_processing_max': {'min': 10000, 'max': 120000, 'type': int},
                'dynamic_message_wait': {'min': 5000, 'max': 60000, 'type': int},
                'dom_monitoring': {'min': 3000, 'max': 30000, 'type': int},
                'javascript_execution': {'min': 3000, 'max': 30000, 'type': int}
            }
        },
        'performance': {
            'memory_limit_mb': {'min': 512, 'max': 4096, 'type': int},
            'cpu_limit_percent': {'min': 10, 'max': 100, 'type': int},
            'network_timeout_seconds': {'min': 10, 'max': 300, 'type': int}
        }
    }
    
    @staticmethod
    def validate_multiprocess_config(config: Dict[str, Any]) -> List[str]:
        """
        マルチプロセス設定の妥当性を検証
        
        Args:
            config: 検証対象の設定辞書
            
        Returns:
            List[str]: エラーメッセージのリスト（エラーなしの場合は空リスト）
        """
        errors = []
        
        try:
            # Form-Sender専用設定を優先、フォールバックで旧multi_process設定
            form_sender_config = config.get('form_sender_multi_process', {})
            if form_sender_config:
                mp_config = form_sender_config
            else:
                mp_config = config.get('multi_process', {})
                
            if not mp_config:
                return ['form_sender_multi_process or multi_process section is missing']
            
            # GitHub Actions環境の特別制約
            is_github_actions = os.getenv('GITHUB_ACTIONS') == 'true'
            
            # 各設定値の検証
            for field_name, constraints in ConfigValidator.CONSTRAINTS['multi_process'].items():
                value = mp_config.get(field_name)
                
                if value is None:
                    errors.append(f"Missing required field: multi_process.{field_name}")
                    continue
                
                # 型チェック
                if not isinstance(value, constraints['type']):
                    try:
                        value = constraints['type'](value)
                        mp_config[field_name] = value  # 型変換して更新
                    except (ValueError, TypeError):
                        errors.append(f"Invalid type for multi_process.{field_name}: expected {constraints['type'].__name__}")
                        continue
                
                # 範囲チェック
                if 'min' in constraints and value < constraints['min']:
                    errors.append(f"multi_process.{field_name} must be >= {constraints['min']}, got {value}")
                
                if 'max' in constraints and value > constraints['max']:
                    errors.append(f"multi_process.{field_name} must be <= {constraints['max']}, got {value}")
            
            # GitHub Actions固有の制約（3ワーカー設計対応）
            if is_github_actions:
                num_workers = mp_config.get('num_workers', 0)
                
                # 3ワーカー設計の具体的検証
                if num_workers == 3:
                    logger.info(f"GitHub Actions: Using optimized 3-worker configuration")
                elif num_workers > 3:
                    errors.append(f"GitHub Actions environment: num_workers should be <= 3 (1 core reserved for orchestrator), got {num_workers}")
                elif num_workers < 2:
                    errors.append(f"GitHub Actions environment: num_workers should be >= 2 for parallel processing, got {num_workers}")
                
                # システムメモリを考慮した正確なメモリ計算
                memory_per_worker = mp_config.get('memory_per_worker_mb', 0)
                system_memory = 3072  # オーケストレーター + システムプロセス: 3GB
                total_memory = (memory_per_worker * num_workers) + system_memory
                
                # メモリ配分の妥当性チェック
                if total_memory > 14336:  # 16GB環境の89%まで許容（11%安全マージン）
                    errors.append(f"GitHub Actions: Total memory usage ({total_memory}MB = {num_workers}×{memory_per_worker}MB + {system_memory}MB system) exceeds safe limit (14336MB/16GB)")
                    
                # Form-Sender/Form-Finder別の推奨メモリ配分チェック
                if memory_per_worker == 3072:
                    logger.info("Memory allocation suitable for Form-Sender (heavy processing)")
                elif memory_per_worker == 2048:
                    logger.info("Memory allocation suitable for Form-Finder (lightweight exploration)")
                elif memory_per_worker > 3072:
                    errors.append(f"GitHub Actions: memory_per_worker ({memory_per_worker}MB) may be excessive for current workloads")
            
            # 相互関係の検証
            num_workers = mp_config.get('num_workers', 0)
            max_workers = mp_config.get('max_workers', 0)
            if max_workers < num_workers:
                errors.append(f"max_workers ({max_workers}) must be >= num_workers ({num_workers})")
            
            batch_size = mp_config.get('batch_size', 0)
            max_pending = mp_config.get('max_pending_tasks', 0)
            if max_pending < batch_size * num_workers * 2:
                errors.append(f"max_pending_tasks ({max_pending}) should be >= batch_size * num_workers * 2 ({batch_size * num_workers * 2})")
        
        except Exception as e:
            errors.append(f"Validation error: {e}")
        
        return errors
    
    @staticmethod
    def validate_timeout_settings(config: Dict[str, Any]) -> List[str]:
        """
        タイムアウト設定の妥当性を検証
        
        Args:
            config: 検証対象の設定辞書
            
        Returns:
            List[str]: エラーメッセージのリスト
        """
        errors = []
        
        try:
            timeout_settings = config.get('form_sender', {}).get('timeout_settings', {})
            if not timeout_settings:
                return ['form_sender.timeout_settings section is missing']
            
            # 各タイムアウト値の検証
            for field_name, constraints in ConfigValidator.CONSTRAINTS['form_sender']['timeout_settings'].items():
                value = timeout_settings.get(field_name)
                
                if value is None:
                    errors.append(f"Missing timeout setting: {field_name}")
                    continue
                
                # 型・範囲チェック
                if not isinstance(value, constraints['type']):
                    try:
                        value = constraints['type'](value)
                        timeout_settings[field_name] = value
                    except (ValueError, TypeError):
                        errors.append(f"Invalid type for timeout_settings.{field_name}")
                        continue
                
                if value < constraints['min'] or value > constraints['max']:
                    errors.append(f"timeout_settings.{field_name} must be between {constraints['min']} and {constraints['max']}")
            
            # タイムアウト値の相互関係チェック
            page_load = timeout_settings.get('page_load', 0)
            element_wait = timeout_settings.get('element_wait', 0)
            pre_processing = timeout_settings.get('pre_processing_max', 0)
            
            if page_load > pre_processing:
                errors.append("page_load timeout should not exceed pre_processing_max timeout")
            
            if element_wait > pre_processing:
                errors.append("element_wait timeout should not exceed pre_processing_max timeout")
        
        except Exception as e:
            errors.append(f"Timeout validation error: {e}")
        
        return errors
    
    @staticmethod
    def validate_full_config(config: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        設定全体の包括的な妥当性検証
        
        Args:
            config: 検証対象の設定辞書
            
        Returns:
            Tuple[bool, List[str]]: (検証成功フラグ, エラーメッセージリスト)
        """
        all_errors = []
        
        # マルチプロセス設定検証
        mp_errors = ConfigValidator.validate_multiprocess_config(config)
        all_errors.extend(mp_errors)
        
        # タイムアウト設定検証
        timeout_errors = ConfigValidator.validate_timeout_settings(config)
        all_errors.extend(timeout_errors)
        
        # ワーカー設定検証
        worker_config = config.get('worker', {})
        for field_name, constraints in ConfigValidator.CONSTRAINTS['worker'].items():
            value = worker_config.get(field_name)
            if value is not None and not (constraints['min'] <= value <= constraints['max']):
                all_errors.append(f"worker.{field_name} must be between {constraints['min']} and {constraints['max']}")
        
        # パフォーマンス設定検証
        perf_config = config.get('performance', {})
        for field_name, constraints in ConfigValidator.CONSTRAINTS['performance'].items():
            value = perf_config.get(field_name)
            if value is not None and not (constraints['min'] <= value <= constraints['max']):
                all_errors.append(f"performance.{field_name} must be between {constraints['min']} and {constraints['max']}")
        
        is_valid = len(all_errors) == 0
        
        if is_valid:
            logger.info("Configuration validation passed successfully")
        else:
            logger.error(f"Configuration validation failed with {len(all_errors)} errors: {all_errors}")
        
        return is_valid, all_errors
    
    @staticmethod
    def get_safe_config_recommendations() -> Dict[str, Any]:
        """
        安全な設定値の推奨値を取得
        
        Returns:
            Dict[str, Any]: 推奨設定値
        """
        is_github_actions = os.getenv('GITHUB_ACTIONS') == 'true'
        
        if is_github_actions:
            return {
                'multi_process': {
                    'num_workers': 2,
                    'github_actions_workers': 2,
                    'max_workers': 2,
                    'batch_size': 10,
                    'memory_per_worker_mb': 4096,
                    'max_pending_tasks': 50
                }
            }
        else:
            return {
                'multi_process': {
                    'num_workers': 2,
                    'github_actions_workers': 2,
                    'max_workers': 3,
                    'batch_size': 15,
                    'memory_per_worker_mb': 4096,
                    'max_pending_tasks': 100
                }
            }