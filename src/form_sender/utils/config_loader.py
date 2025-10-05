"""
設定ローダー
各種設定ファイルを読み込み、型安全な設定オブジェクトを提供
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import timedelta


@dataclass
class MemoryThresholds:
    """メモリ閾値設定"""
    warning_mb: float
    critical_mb: float


@dataclass
class CPUThresholds:
    """CPU閾値設定"""
    warning_percent: float
    critical_percent: float


@dataclass
class MonitoringIntervals:
    """監視間隔設定"""
    base_interval: float
    low_load: float
    normal_load: float
    high_load: float
    critical_load: float
    enable_dynamic: bool


@dataclass
class LoadLevelThresholds:
    """負荷レベル判定閾値"""
    low_utilization_max: float
    normal_utilization_max: float
    high_utilization_max: float


@dataclass
class GCMonitoring:
    """ガベージコレクション監視設定"""
    alert_threshold: int
    monitoring_period: int


@dataclass
class HistoryManagement:
    """履歴管理設定"""
    max_metrics_history: int
    load_level_history_size: int
    min_measurements_for_adjustment: int


@dataclass
class BackpressureLevels:
    """背圧制御レベル設定"""
    level_1_threshold: float
    level_2_threshold: float
    level_3_threshold: float
    level_4_threshold: float


@dataclass
class OverflowBuffer:
    """オーバーフローバッファ設定"""
    max_overflow_size: int
    emergency_file_prefix: str
    cleanup_after_hours: int


@dataclass
class BufferManagement:
    """バッファ管理設定"""
    backpressure_levels: BackpressureLevels
    overflow_buffer: OverflowBuffer


@dataclass
class TerminationVerification:
    """プロセス終了確認設定"""
    max_wait_seconds: int
    check_interval_seconds: float
    zombie_cleanup_enabled: bool
    fd_leak_detection: bool
    proc_status_verification: bool
    sigchld_handler_enabled: bool


@dataclass
class WorkerLimits:
    """ワーカー制限設定"""
    max_workers: int
    worker_timeout_seconds: int
    restart_threshold_failures: int


@dataclass
class WorkerResilienceConfig:
    """ワーカー耐性設定"""
    max_consecutive_failures: int
    max_fatal_error_ratio: float
    retry_delay_seconds: float
    max_retries: int


@dataclass
class WorkerResilienceSettings:
    """全ワーカー耐性設定統合"""
    fetch_detail: WorkerResilienceConfig
    form_sender: WorkerResilienceConfig


@dataclass
class ProcessManagement:
    """プロセス管理設定"""
    termination_verification: TerminationVerification
    worker_limits: WorkerLimits


@dataclass
class PerformanceMonitoringConfig:
    """パフォーマンス監視設定統合クラス"""
    memory_thresholds: MemoryThresholds
    cpu_thresholds: CPUThresholds
    monitoring_intervals: MonitoringIntervals
    load_level_thresholds: LoadLevelThresholds
    gc_monitoring: GCMonitoring
    history_management: HistoryManagement
    buffer_management: BufferManagement
    process_management: ProcessManagement
    worker_resilience: WorkerResilienceSettings


class ConfigLoader:
    """設定ファイルローダー"""
    
    def __init__(self, config_dir: Optional[str] = None):
        if config_dir is None:
            # デフォルトのconfigディレクトリを推定
            current_file = Path(__file__).resolve()
            project_root = current_file.parents[3]  # src/form_sender/utils/config_loader.py から3階層上
            self.config_dir = project_root / "config"
        else:
            self.config_dir = Path(config_dir)
            
        if not self.config_dir.exists():
            raise FileNotFoundError(f"Config directory not found: {self.config_dir}")
    
    def _load_json_config(self, filename: str) -> Dict[str, Any]:
        """JSONファイルから設定を読み込み"""
        config_path = self.config_dir / filename
        
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
            
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {config_path}: {e}")
        except Exception as e:
            raise RuntimeError(f"Failed to load {config_path}: {e}")
    
    def load_performance_monitoring_config(self) -> PerformanceMonitoringConfig:
        """パフォーマンス監視設定を読み込み"""
        config_data = self._load_json_config("performance_monitoring.json")
        
        try:
            # 各セクションを個別にデータクラスに変換
            memory_thresholds = MemoryThresholds(**config_data["memory_thresholds"])
            cpu_thresholds = CPUThresholds(**config_data["cpu_thresholds"])
            
            monitoring_intervals = MonitoringIntervals(**config_data["monitoring_intervals"])
            load_level_thresholds = LoadLevelThresholds(**config_data["load_level_thresholds"])
            gc_monitoring = GCMonitoring(**config_data["gc_monitoring"])
            history_management = HistoryManagement(**config_data["history_management"])
            
            # 複雑なネスト構造の処理
            backpressure_levels = BackpressureLevels(**config_data["buffer_management"]["backpressure_levels"])
            overflow_buffer = OverflowBuffer(**config_data["buffer_management"]["overflow_buffer"])
            buffer_management = BufferManagement(
                backpressure_levels=backpressure_levels,
                overflow_buffer=overflow_buffer
            )
            
            termination_verification = TerminationVerification(**config_data["process_management"]["termination_verification"])
            worker_limits = WorkerLimits(**config_data["process_management"]["worker_limits"])
            process_management = ProcessManagement(
                termination_verification=termination_verification,
                worker_limits=worker_limits
            )
            
            # ワーカー耐性設定
            fetch_detail_resilience = WorkerResilienceConfig(**config_data["worker_resilience"]["fetch_detail"])
            form_sender_resilience = WorkerResilienceConfig(**config_data["worker_resilience"]["form_sender"])
            worker_resilience = WorkerResilienceSettings(
                fetch_detail=fetch_detail_resilience,
                form_sender=form_sender_resilience
            )
            
            return PerformanceMonitoringConfig(
                memory_thresholds=memory_thresholds,
                cpu_thresholds=cpu_thresholds,
                monitoring_intervals=monitoring_intervals,
                load_level_thresholds=load_level_thresholds,
                gc_monitoring=gc_monitoring,
                history_management=history_management,
                buffer_management=buffer_management,
                process_management=process_management,
                worker_resilience=worker_resilience
            )
            
        except KeyError as e:
            raise ValueError(f"Missing required configuration key: {e}")
        except TypeError as e:
            raise ValueError(f"Invalid configuration format: {e}")
    
    def validate_config(self, config: PerformanceMonitoringConfig) -> None:
        """設定値の妥当性検証"""
        # メモリ閾値の検証
        if config.memory_thresholds.warning_mb >= config.memory_thresholds.critical_mb:
            raise ValueError("Memory warning threshold must be less than critical threshold")
            
        # CPU閾値の検証
        if config.cpu_thresholds.warning_percent >= config.cpu_thresholds.critical_percent:
            raise ValueError("CPU warning threshold must be less than critical threshold")
            
        # 監視間隔の検証
        intervals = config.monitoring_intervals
        if intervals.base_interval <= 0:
            raise ValueError("Base monitoring interval must be positive")
            
        # 負荷レベル閾値の検証
        thresholds = config.load_level_thresholds
        if not (0 <= thresholds.low_utilization_max <= thresholds.normal_utilization_max <= thresholds.high_utilization_max <= 1.0):
            raise ValueError("Load level thresholds must be in ascending order between 0.0 and 1.0")
            
        # 背圧レベルの検証
        bp = config.buffer_management.backpressure_levels
        if not (0 <= bp.level_1_threshold <= bp.level_2_threshold <= bp.level_3_threshold <= bp.level_4_threshold <= 1.0):
            raise ValueError("Backpressure level thresholds must be in ascending order between 0.0 and 1.0")


# グローバル設定インスタンス（シングルトンパターン）
_config_loader: Optional[ConfigLoader] = None
_performance_config: Optional[PerformanceMonitoringConfig] = None


def get_config_loader() -> ConfigLoader:
    """設定ローダーのシングルトンインスタンスを取得"""
    global _config_loader
    if _config_loader is None:
        _config_loader = ConfigLoader()
    return _config_loader


def get_performance_monitoring_config() -> PerformanceMonitoringConfig:
    """パフォーマンス監視設定のシングルトンインスタンスを取得"""
    global _performance_config
    if _performance_config is None:
        loader = get_config_loader()
        _performance_config = loader.load_performance_monitoring_config()
        loader.validate_config(_performance_config)
    return _performance_config


def reload_config() -> None:
    """設定を再読み込み"""
    global _config_loader, _performance_config
    _config_loader = None
    _performance_config = None