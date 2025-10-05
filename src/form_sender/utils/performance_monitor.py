"""
パフォーマンス監視システム
メモリ使用量、CPU使用率、処理時間などを監視し、
パフォーマンス問題を早期に検出する
"""

import asyncio
import psutil
import time
import gc
import threading
from collections import deque
from typing import Dict, Any, Optional, Callable, List, Literal
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
import json

from .config_loader import get_performance_monitoring_config, PerformanceMonitoringConfig

@dataclass
class PerformanceMetrics:
    """パフォーマンス指標を格納するデータクラス"""
    timestamp: str
    memory_usage_mb: float
    memory_percent: float
    cpu_percent: float
    process_time: float
    gc_objects: int
    active_threads: int
    
    # プロセス固有情報
    process_id: int
    process_name: str
    
    # 追加指標
    file_descriptors: Optional[int] = None
    network_connections: Optional[int] = None
    context_switches: Optional[int] = None

class PerformanceMonitor:
    """包括的パフォーマンス監視システム"""
    
    def __init__(self, 
                 config: Optional[PerformanceMonitoringConfig] = None,
                 warning_memory_mb: Optional[float] = None,  # 下位互換性のため残存
                 critical_memory_mb: Optional[float] = None,
                 warning_cpu_percent: Optional[float] = None,
                 critical_cpu_percent: Optional[float] = None,
                 monitoring_interval: Optional[float] = None,
                 enable_dynamic_intervals: Optional[bool] = None,
                 log_callback: Optional[Callable[[str, Dict[str, Any]], None]] = None):
        
        # 設定の初期化（config優先、個別パラメータで上書き可能）
        if config is None:
            config = get_performance_monitoring_config()
        self.config = config
        
        # 個別パラメータでの上書き（下位互換性）
        self.warning_memory_mb = warning_memory_mb or config.memory_thresholds.warning_mb
        self.critical_memory_mb = critical_memory_mb or config.memory_thresholds.critical_mb
        self.warning_cpu_percent = warning_cpu_percent or config.cpu_thresholds.warning_percent
        self.critical_cpu_percent = critical_cpu_percent or config.cpu_thresholds.critical_percent
        
        base_interval = monitoring_interval or config.monitoring_intervals.base_interval
        self.base_monitoring_interval = base_interval
        self.current_monitoring_interval = base_interval
        
        self.enable_dynamic_intervals = enable_dynamic_intervals if enable_dynamic_intervals is not None else config.monitoring_intervals.enable_dynamic
        self.log_callback = log_callback
        
        # 動的間隔設定（configから取得）
        intervals = config.monitoring_intervals
        self.interval_thresholds = {
            "low_load": intervals.low_load,
            "normal_load": intervals.normal_load,
            "high_load": intervals.high_load,
            "critical_load": intervals.critical_load
        }
        
        # 負荷レベル履歴（設定から最大サイズを取得）
        self.load_level_history = deque(maxlen=config.history_management.load_level_history_size)
        self.last_interval_adjustment = datetime.now(timezone(timedelta(hours=9)))
        
        # 負荷レベル閾値（configから取得）
        self.load_thresholds = config.load_level_thresholds
        
        # 監視状態
        self.is_monitoring = False
        self.monitor_task: Optional[asyncio.Task] = None
        self.process = psutil.Process()
        
        # メトリクス履歴（設定から最大サイズを取得）
        self.max_history_size = config.history_management.max_metrics_history
        # dequeによる効率的な履歴管理（メモリリーク防止）
        self.metrics_history = deque(maxlen=self.max_history_size)
        
        # アラート状態追跡
        self.alert_states = {
            "memory_warning": False,
            "memory_critical": False,
            "cpu_warning": False,
            "cpu_critical": False,
            "gc_frequency_high": False
        }
        
        # GC統計の追跡（設定から閾値を取得）
        self.last_gc_count = [0, 0, 0]  # gen0, gen1, gen2
        self.gc_alert_threshold = config.gc_monitoring.alert_threshold
        
        # パフォーマンス統計
        self.stats = {
            "total_measurements": 0,
            "memory_warnings": 0,
            "memory_criticals": 0,
            "cpu_warnings": 0,
            "cpu_criticals": 0,
            "max_memory_seen": 0.0,
            "max_cpu_seen": 0.0,
            "interval_adjustments": 0,
            "time_in_low_load": 0.0,
            "time_in_normal_load": 0.0,
            "time_in_high_load": 0.0,
            "time_in_critical_load": 0.0
        }
    
    def get_current_metrics(self) -> PerformanceMetrics:
        """現在のパフォーマンス指標を取得"""
        try:
            # メモリ使用量
            memory_info = self.process.memory_info()
            memory_mb = memory_info.rss / 1024 / 1024
            memory_percent = self.process.memory_percent()
            
            # CPU使用率
            cpu_percent = self.process.cpu_percent()
            
            # プロセス時間
            process_time = sum(self.process.cpu_times())
            
            # ガベージコレクション情報
            gc_objects = len(gc.get_objects())
            
            # スレッド数
            active_threads = threading.active_count()
            
            # 追加のシステム情報
            file_descriptors = None
            network_connections = None
            context_switches = None
            
            try:
                # Unix系システムでのみ利用可能
                file_descriptors = self.process.num_fds()
            except (AttributeError, psutil.AccessDenied):
                pass
            
            try:
                network_connections = len(self.process.connections())
            except (psutil.AccessDenied, psutil.NoSuchProcess):
                pass
            
            try:
                context_switches = self.process.num_ctx_switches().voluntary
            except (AttributeError, psutil.AccessDenied):
                pass
            
            # タイムスタンプ（JST）
            timestamp = datetime.now(timezone(timedelta(hours=9))).isoformat()
            
            metrics = PerformanceMetrics(
                timestamp=timestamp,
                memory_usage_mb=memory_mb,
                memory_percent=memory_percent,
                cpu_percent=cpu_percent,
                process_time=process_time,
                gc_objects=gc_objects,
                active_threads=active_threads,
                process_id=self.process.pid,
                process_name=self.process.name(),
                file_descriptors=file_descriptors,
                network_connections=network_connections,
                context_switches=context_switches
            )
            
            return metrics
            
        except Exception as e:
            # エラーが発生した場合のフォールバック
            timestamp = datetime.now(timezone(timedelta(hours=9))).isoformat()
            return PerformanceMetrics(
                timestamp=timestamp,
                memory_usage_mb=0.0,
                memory_percent=0.0,
                cpu_percent=0.0,
                process_time=0.0,
                gc_objects=0,
                active_threads=0,
                process_id=self.process.pid,
                process_name="unknown"
            )
    
    def _check_alerts(self, metrics: PerformanceMetrics) -> List[Dict[str, Any]]:
        """アラート条件をチェックして通知を生成"""
        alerts = []
        
        # メモリアラート
        if metrics.memory_usage_mb >= self.critical_memory_mb:
            if not self.alert_states["memory_critical"]:
                alerts.append({
                    "type": "CRITICAL",
                    "category": "memory",
                    "message": f"メモリ使用量が危険レベルに達しました: {metrics.memory_usage_mb:.1f}MB (閾値: {self.critical_memory_mb}MB)",
                    "value": metrics.memory_usage_mb,
                    "threshold": self.critical_memory_mb
                })
                self.alert_states["memory_critical"] = True
                self.stats["memory_criticals"] += 1
        elif metrics.memory_usage_mb >= self.warning_memory_mb:
            if not self.alert_states["memory_warning"] and not self.alert_states["memory_critical"]:
                alerts.append({
                    "type": "WARNING",
                    "category": "memory",
                    "message": f"メモリ使用量が警告レベルに達しました: {metrics.memory_usage_mb:.1f}MB (閾値: {self.warning_memory_mb}MB)",
                    "value": metrics.memory_usage_mb,
                    "threshold": self.warning_memory_mb
                })
                self.alert_states["memory_warning"] = True
                self.stats["memory_warnings"] += 1
        else:
            # 正常範囲に戻った場合はアラート状態をリセット
            if self.alert_states["memory_warning"] or self.alert_states["memory_critical"]:
                alerts.append({
                    "type": "INFO",
                    "category": "memory",
                    "message": f"メモリ使用量が正常範囲に戻りました: {metrics.memory_usage_mb:.1f}MB",
                    "value": metrics.memory_usage_mb,
                    "threshold": self.warning_memory_mb
                })
                self.alert_states["memory_warning"] = False
                self.alert_states["memory_critical"] = False
        
        # CPUアラート
        if metrics.cpu_percent >= self.critical_cpu_percent:
            if not self.alert_states["cpu_critical"]:
                alerts.append({
                    "type": "CRITICAL",
                    "category": "cpu",
                    "message": f"CPU使用率が危険レベルに達しました: {metrics.cpu_percent:.1f}% (閾値: {self.critical_cpu_percent}%)",
                    "value": metrics.cpu_percent,
                    "threshold": self.critical_cpu_percent
                })
                self.alert_states["cpu_critical"] = True
                self.stats["cpu_criticals"] += 1
        elif metrics.cpu_percent >= self.warning_cpu_percent:
            if not self.alert_states["cpu_warning"] and not self.alert_states["cpu_critical"]:
                alerts.append({
                    "type": "WARNING", 
                    "category": "cpu",
                    "message": f"CPU使用率が警告レベルに達しました: {metrics.cpu_percent:.1f}% (閾値: {self.warning_cpu_percent}%)",
                    "value": metrics.cpu_percent,
                    "threshold": self.warning_cpu_percent
                })
                self.alert_states["cpu_warning"] = True
                self.stats["cpu_warnings"] += 1
        else:
            # 正常範囲に戻った場合はアラート状態をリセット
            if self.alert_states["cpu_warning"] or self.alert_states["cpu_critical"]:
                alerts.append({
                    "type": "INFO",
                    "category": "cpu", 
                    "message": f"CPU使用率が正常範囲に戻りました: {metrics.cpu_percent:.1f}%",
                    "value": metrics.cpu_percent,
                    "threshold": self.warning_cpu_percent
                })
                self.alert_states["cpu_warning"] = False
                self.alert_states["cpu_critical"] = False
        
        # GCアラート
        current_gc_count = [gc.get_count()[i] for i in range(3)]
        gc_diff = [current_gc_count[i] - self.last_gc_count[i] for i in range(3)]
        
        if sum(gc_diff) >= self.gc_alert_threshold:
            if not self.alert_states["gc_frequency_high"]:
                alerts.append({
                    "type": "WARNING",
                    "category": "gc",
                    "message": f"ガベージコレクションが頻発しています: {sum(gc_diff)}回 (Gen0:{gc_diff[0]}, Gen1:{gc_diff[1]}, Gen2:{gc_diff[2]})",
                    "value": sum(gc_diff),
                    "threshold": self.gc_alert_threshold
                })
                self.alert_states["gc_frequency_high"] = True
        else:
            self.alert_states["gc_frequency_high"] = False
            
        self.last_gc_count = current_gc_count
        
        return alerts
    
    def _calculate_system_load_level(self, metrics: PerformanceMetrics) -> Literal["low", "normal", "high", "critical"]:
        """システム負荷レベルを計算（設定から閾値を取得）"""
        memory_utilization = metrics.memory_usage_mb / self.critical_memory_mb
        cpu_utilization = metrics.cpu_percent / 100.0
        
        # 最大値で負荷レベルを決定
        max_utilization = max(memory_utilization, cpu_utilization)
        
        # 設定から閾値を取得
        thresholds = self.load_thresholds
        
        if max_utilization >= thresholds.high_utilization_max:  # 設定による危険レベル
            return "critical"
        elif max_utilization >= thresholds.normal_utilization_max:  # 設定による高負荷
            return "high" 
        elif max_utilization >= thresholds.low_utilization_max:  # 設定による通常負荷
            return "normal"
        else:  # 設定による低負荷
            return "low"
    
    def _adjust_monitoring_interval(self, current_load: Literal["low", "normal", "high", "critical"]) -> None:
        """負荷レベルに基づいて監視間隔を動的調整"""
        if not self.enable_dynamic_intervals:
            return
            
        # 負荷レベル履歴に追加
        self.load_level_history.append(current_load)
        
        # 設定で指定された最低測定回数後から調整開始
        min_measurements = self.config.history_management.min_measurements_for_adjustment
        if len(self.load_level_history) < min_measurements:
            return
            
        # 過去の負荷レベルで安定性をチェック（最低測定回数を使用）
        recent_levels = list(self.load_level_history)[-min_measurements:]
        if len(set(recent_levels)) > 1:  # 負荷が不安定な場合は調整しない
            return
            
        # 新しい間隔を決定
        target_interval = self.interval_thresholds[f"{current_load}_load"]
        
        # 間隔変更が必要かチェック
        if abs(self.current_monitoring_interval - target_interval) > 1.0:
            old_interval = self.current_monitoring_interval
            self.current_monitoring_interval = target_interval
            self.stats["interval_adjustments"] += 1
            self.last_interval_adjustment = datetime.now(timezone(timedelta(hours=9)))
            
            if self.log_callback:
                self.log_callback(
                    f"監視間隔を動的調整: {old_interval}s → {target_interval}s (負荷レベル: {current_load})",
                    {
                        "old_interval": old_interval,
                        "new_interval": target_interval,
                        "load_level": current_load,
                        "load_history": recent_levels
                    }
                )
    
    def _update_stats(self, metrics: PerformanceMetrics, load_level: str) -> None:
        """統計情報を更新"""
        self.stats["total_measurements"] += 1
        
        if metrics.memory_usage_mb > self.stats["max_memory_seen"]:
            self.stats["max_memory_seen"] = metrics.memory_usage_mb
            
        if metrics.cpu_percent > self.stats["max_cpu_seen"]:
            self.stats["max_cpu_seen"] = metrics.cpu_percent
            
        # 負荷レベル別時間を更新
        load_time_key = f"time_in_{load_level}_load"
        if load_time_key in self.stats:
            self.stats[load_time_key] += self.current_monitoring_interval
    
    async def _monitoring_loop(self) -> None:
        """バックグラウンド監視ループ"""
        while self.is_monitoring:
            try:
                # メトリクス収集
                metrics = self.get_current_metrics()
                
                # 履歴に追加（dequeが自動的にサイズ制限）
                self.metrics_history.append(metrics)
                
                # システム負荷レベル計算
                load_level = self._calculate_system_load_level(metrics)
                
                # 動的間隔調整
                self._adjust_monitoring_interval(load_level)
                
                # アラートチェック
                alerts = self._check_alerts(metrics)
                
                # 統計更新（負荷レベル情報も含む）
                self._update_stats(metrics, load_level)
                
                # ログ出力（コールバックが設定されている場合）
                if self.log_callback and alerts:
                    for alert in alerts:
                        self.log_callback(alert["message"], {
                            "alert_type": alert["type"],
                            "alert_category": alert["category"],
                            "metrics": asdict(metrics),
                            "system_load_level": load_level,
                            "current_interval": self.current_monitoring_interval
                        })
                
                # 現在の監視間隔で待機（動的に調整される）
                await asyncio.sleep(self.current_monitoring_interval)
                
            except Exception as e:
                if self.log_callback:
                    self.log_callback(f"Performance monitoring error: {e}", {
                        "error_type": type(e).__name__
                    })
                await asyncio.sleep(self.current_monitoring_interval)
    
    async def start_monitoring(self) -> None:
        """パフォーマンス監視を開始"""
        if self.is_monitoring:
            return
            
        self.is_monitoring = True
        self.monitor_task = asyncio.create_task(self._monitoring_loop())
        
        if self.log_callback:
            self.log_callback("Performance monitoring started", {
                "warning_memory_mb": self.warning_memory_mb,
                "critical_memory_mb": self.critical_memory_mb,
                "warning_cpu_percent": self.warning_cpu_percent,
                "critical_cpu_percent": self.critical_cpu_percent,
                "base_monitoring_interval": self.base_monitoring_interval,
                "current_monitoring_interval": self.current_monitoring_interval,
                "dynamic_intervals_enabled": self.enable_dynamic_intervals,
                "interval_thresholds": self.interval_thresholds
            })
    
    async def stop_monitoring(self) -> None:
        """パフォーマンス監視を停止"""
        self.is_monitoring = False
        
        if self.monitor_task:
            self.monitor_task.cancel()
            try:
                await self.monitor_task
            except asyncio.CancelledError:
                pass
            self.monitor_task = None
        
        if self.log_callback:
            self.log_callback("Performance monitoring stopped", {
                "total_measurements": self.stats["total_measurements"],
                "max_memory_seen": self.stats["max_memory_seen"],
                "max_cpu_seen": self.stats["max_cpu_seen"],
                "total_alerts": sum([
                    self.stats["memory_warnings"], 
                    self.stats["memory_criticals"],
                    self.stats["cpu_warnings"], 
                    self.stats["cpu_criticals"]
                ])
            })
    
    def get_summary_report(self) -> Dict[str, Any]:
        """サマリーレポートを生成"""
        if not self.metrics_history:
            return {"error": "No metrics available"}
        
        # 最新の指標
        latest_metrics = self.metrics_history[-1]
        
        # 過去の統計
        memory_values = [m.memory_usage_mb for m in self.metrics_history[-10:]]  # 最新10件
        cpu_values = [m.cpu_percent for m in self.metrics_history[-10:]]
        
        return {
            "monitoring_status": "active" if self.is_monitoring else "inactive",
            "latest_metrics": asdict(latest_metrics),
            "recent_averages": {
                "memory_mb": sum(memory_values) / len(memory_values) if memory_values else 0,
                "cpu_percent": sum(cpu_values) / len(cpu_values) if cpu_values else 0
            },
            "alert_statistics": self.stats,
            "alert_states": self.alert_states,
            "metrics_count": len(self.metrics_history),
            "base_monitoring_interval": self.base_monitoring_interval,
            "current_monitoring_interval": self.current_monitoring_interval,
            "dynamic_intervals_enabled": self.enable_dynamic_intervals,
            "load_level_history": list(self.load_level_history),
            "last_interval_adjustment": self.last_interval_adjustment.isoformat() if hasattr(self, 'last_interval_adjustment') else None
        }
    
    def force_gc_and_measure(self) -> Dict[str, Any]:
        """強制ガベージコレクション実行と効果測定"""
        before_metrics = self.get_current_metrics()
        
        # ガベージコレクション実行
        gc.collect()
        
        # 少し待機してから測定
        time.sleep(0.1)
        after_metrics = self.get_current_metrics()
        
        return {
            "gc_executed_at": datetime.now(timezone(timedelta(hours=9))).isoformat(),
            "memory_before_mb": before_metrics.memory_usage_mb,
            "memory_after_mb": after_metrics.memory_usage_mb,
            "memory_freed_mb": before_metrics.memory_usage_mb - after_metrics.memory_usage_mb,
            "objects_before": before_metrics.gc_objects,
            "objects_after": after_metrics.gc_objects,
            "objects_freed": before_metrics.gc_objects - after_metrics.gc_objects
        }
    
    def get_load_analysis_report(self) -> Dict[str, Any]:
        """負荷分析レポートを生成"""
        if not self.metrics_history:
            return {"error": "No metrics available"}
            
        recent_metrics = list(self.metrics_history)[-20:]  # 最新20件で分析
        
        load_levels = []
        memory_trend = []
        cpu_trend = []
        
        for metrics in recent_metrics:
            load_level = self._calculate_system_load_level(metrics)
            load_levels.append(load_level)
            memory_trend.append(metrics.memory_usage_mb)
            cpu_trend.append(metrics.cpu_percent)
        
        # 負荷レベル分布
        load_distribution = {
            "low": load_levels.count("low"),
            "normal": load_levels.count("normal"),
            "high": load_levels.count("high"),
            "critical": load_levels.count("critical")
        }
        
        # トレンド分析
        memory_avg = sum(memory_trend) / len(memory_trend) if memory_trend else 0
        cpu_avg = sum(cpu_trend) / len(cpu_trend) if cpu_trend else 0
        
        return {
            "analysis_period": f"過去{len(recent_metrics)}回の測定",
            "load_distribution": load_distribution,
            "current_load_level": load_levels[-1] if load_levels else "unknown",
            "performance_trends": {
                "memory_avg_mb": round(memory_avg, 2),
                "cpu_avg_percent": round(cpu_avg, 2),
                "memory_trend_direction": "increasing" if memory_trend[-1] > memory_trend[0] else "decreasing" if memory_trend else "stable",
                "cpu_trend_direction": "increasing" if cpu_trend[-1] > cpu_trend[0] else "decreasing" if cpu_trend else "stable"
            },
            "interval_efficiency": {
                "total_adjustments": self.stats.get("interval_adjustments", 0),
                "current_interval": self.current_monitoring_interval,
                "base_interval": self.base_monitoring_interval,
                "efficiency_gain": f"{((self.base_monitoring_interval / self.current_monitoring_interval) - 1) * 100:.1f}%" if self.current_monitoring_interval != self.base_monitoring_interval else "0.0%"
            },
            "load_time_distribution": {
                "low_load_time": self.stats.get("time_in_low_load", 0),
                "normal_load_time": self.stats.get("time_in_normal_load", 0),
                "high_load_time": self.stats.get("time_in_high_load", 0),
                "critical_load_time": self.stats.get("time_in_critical_load", 0)
            }
        }
    
    def reset_dynamic_intervals(self) -> None:
        """動的間隔設定をリセット"""
        self.current_monitoring_interval = self.base_monitoring_interval
        self.load_level_history.clear()
        self.stats["interval_adjustments"] = 0
        
        if self.log_callback:
            self.log_callback("動的監視間隔をリセットしました", {
                "reset_interval": self.base_monitoring_interval,
                "dynamic_enabled": self.enable_dynamic_intervals
            })