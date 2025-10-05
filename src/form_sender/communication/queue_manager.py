"""
マルチプロセス通信管理

オーケストレーターとワーカープロセス間の通信を管理する
"""

import logging
import multiprocessing as mp
import queue
import time
import uuid
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

logger = logging.getLogger(__name__)


class QueueManagerError(Exception):
    """キューマネージャーの基本例外クラス"""
    pass


class QueueOverflowError(QueueManagerError):
    """キューオーバーフロー例外"""
    pass


class WorkerCommunicationError(QueueManagerError):
    """ワーカー通信エラー例外"""
    pass


class TaskType(Enum):
    """タスクタイプ定義"""
    PROCESS_COMPANY = "process_company"
    SHUTDOWN = "shutdown"
    HEARTBEAT = "heartbeat"


class ResultStatus(Enum):
    """結果ステータス定義"""
    SUCCESS = "success"
    FAILED = "failed"
    ERROR = "error"
    PROHIBITION_DETECTED = "prohibition_detected"
    WORKER_READY = "worker_ready"
    WORKER_SHUTDOWN = "worker_shutdown"


@dataclass
class WorkerTask:
    """ワーカータスクのデータ構造"""
    task_id: str
    task_type: TaskType
    company_data: Optional[Dict[str, Any]] = None
    client_data: Optional[Dict[str, Any]] = None
    targeting_id: Optional[int] = None
    worker_id: Optional[int] = None
    # instruction_json削除 - RuleBasedAnalyzerのリアルタイム解析のみを使用
    
    def to_dict(self) -> Dict[str, Any]:
        """辞書形式に変換"""
        result = asdict(self)
        # Enumを文字列に変換
        result['task_type'] = self.task_type.value
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkerTask':
        """辞書から作成"""
        # 文字列をEnumに変換
        data['task_type'] = TaskType(data['task_type'])
        return cls(**data)


@dataclass
class WorkerResult:
    """ワーカー結果のデータ構造"""
    task_id: str
    worker_id: int
    status: ResultStatus
    record_id: Optional[int] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    instruction_valid_updated: bool = False
    bot_protection_detected: bool = False
    processing_time: Optional[float] = None
    additional_data: Optional[Dict[str, Any]] = None  # form_finder等の拡張データ用
    
    def to_dict(self) -> Dict[str, Any]:
        """辞書形式に変換"""
        result = asdict(self)
        # Enumを文字列に変換
        result['status'] = self.status.value
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WorkerResult':
        """辞書から作成"""
        # 文字列をEnumに変換
        data['status'] = ResultStatus(data['status'])
        return cls(**data)


class QueueManager:
    """マルチプロセス通信管理クラス"""
    
    def __init__(self, num_workers: int = 2):
        """
        初期化
        
        Args:
            num_workers: ワーカープロセス数
        """
        self.num_workers = num_workers
        
        # プロセス間通信キュー
        self.task_queue = mp.Queue()
        self.result_queue = mp.Queue()
        
        # タスク管理
        self.pending_tasks = {}  # task_id -> WorkerTask
        self.completed_tasks = {}  # task_id -> WorkerResult
        self.task_counter = 0
        
        # ワーカー状態管理
        self.worker_status = {}  # worker_id -> status
        self.worker_last_heartbeat = {}  # worker_id -> timestamp
        
        # 統計情報
        self.stats = {
            'tasks_sent': 0,
            'results_received': 0,
            'errors': 0,
            'start_time': time.time()
        }
        
        logger.info(f"QueueManager initialized with {num_workers} workers")
    
    def generate_task_id(self) -> str:
        """一意なタスクIDを生成（UUID + カウンターベース）"""
        self.task_counter += 1
        # UUIDの先頭8文字 + タスクカウンターで衝突リスクを排除
        uuid_prefix = uuid.uuid4().hex[:8]
        return f"task_{uuid_prefix}_{self.task_counter}"
    
    def send_task(self, company_data: Dict[str, Any], client_data: Optional[Dict[str, Any]] = None, 
                  targeting_id: Optional[int] = None) -> str:
        """
        企業処理タスクをワーカーに送信
        
        Args:
            company_data: 企業データ
            client_data: クライアントデータ（オプション、Form Finder等では不要）
            targeting_id: ターゲティングID（オプション、Form Finder等では不要）
            
        Returns:
            task_id: 生成されたタスクID
        """
        task_id = self.generate_task_id()
        
        task = WorkerTask(
            task_id=task_id,
            task_type=TaskType.PROCESS_COMPANY,
            company_data=company_data,
            client_data=client_data,
            targeting_id=targeting_id
            # instruction_json削除 - RuleBasedAnalyzerのリアルタイム解析のみを使用
        )
        
        try:
            # タスクを辞書形式でキューに送信
            self.task_queue.put(task.to_dict(), timeout=5)
            self.pending_tasks[task_id] = task
            self.stats['tasks_sent'] += 1
            
            logger.debug(f"Task sent: {task_id} for company {company_data.get('id')}")
            return task_id
            
        except queue.Full:
            logger.error(f"Task queue is full, could not send task {task_id}")
            # より詳細なエラー情報を提供
            queue_size = self.task_queue.qsize() if hasattr(self.task_queue, 'qsize') else 'unknown'
            raise QueueOverflowError(
                f"Task queue overflow: unable to send task {task_id}. "
                f"Current queue size: {queue_size}. "
                f"Consider increasing max_pending_tasks or reducing batch size."
            )
    
    def get_result(self, timeout: Optional[float] = None) -> Optional[WorkerResult]:
        """
        ワーカーからの結果を1件取得
        
        Args:
            timeout: タイムアウト（秒）。Noneの場合はブロックしない
            
        Returns:
            WorkerResult: 結果データ。なければNone
        """
        try:
            if timeout is None:
                # ノンブロッキング取得
                result_data = self.result_queue.get_nowait()
            else:
                result_data = self.result_queue.get(timeout=timeout)
            
            # データ型チェック: 既にWorkerResultオブジェクトの場合はそのまま使用
            if isinstance(result_data, WorkerResult):
                result = result_data
            else:
                # 辞書の場合のみfrom_dictで変換
                result = WorkerResult.from_dict(result_data)
            
            # 統計更新
            self.stats['results_received'] += 1
            
            # ワーカー状態更新
            if result.status == ResultStatus.WORKER_READY:
                self.worker_status[result.worker_id] = 'ready'
                self.worker_last_heartbeat[result.worker_id] = time.time()
                logger.debug(f"Worker {result.worker_id} is ready")
                
            elif result.status == ResultStatus.WORKER_SHUTDOWN:
                self.worker_status[result.worker_id] = 'shutdown'
                logger.info(f"Worker {result.worker_id} has shut down")
                
            else:
                # 処理結果の場合
                task_id = result.task_id
                if task_id in self.pending_tasks:
                    self.completed_tasks[task_id] = result
                    del self.pending_tasks[task_id]
                    
                    if result.status == ResultStatus.ERROR:
                        self.stats['errors'] += 1
                        logger.warning(f"Task {task_id} completed with error: {result.error_message}")
                    else:
                        logger.debug(f"Task {task_id} completed: {result.status.value}")
            
            return result
            
        except queue.Empty:
            return None
        except Exception as e:
            logger.error(f"Unexpected error getting result from queue: {e}")
            # 通信エラーとして分類し、上位レイヤーに通知
            raise WorkerCommunicationError(
                f"Failed to retrieve worker result from queue: {e}"
            )
    
    def get_all_available_results(self) -> List[WorkerResult]:
        """
        利用可能な全ての結果を取得（ノンブロッキング）
        
        Returns:
            List[WorkerResult]: 結果リスト
        """
        results = []
        
        while True:
            result = self.get_result(timeout=None)  # ノンブロッキング
            if result is None:
                break
            results.append(result)
        
        return results
    
    def send_shutdown_signal(self):
        """全ワーカーに終了シグナルを送信"""
        logger.info("Sending shutdown signal to all workers")
        
        for i in range(self.num_workers):
            shutdown_task = WorkerTask(
                task_id=f"shutdown_{i}",
                task_type=TaskType.SHUTDOWN
            )
            
            try:
                self.task_queue.put(shutdown_task.to_dict(), timeout=1)
                logger.debug(f"Shutdown signal sent to worker slot {i}")
            except queue.Full:
                logger.warning(f"Could not send shutdown signal to worker slot {i} - queue full")
    
    def wait_for_workers_shutdown(self, timeout: float = 30) -> bool:
        """
        全ワーカーの終了を待機
        
        Args:
            timeout: タイムアウト（秒）
            
        Returns:
            bool: 全ワーカーが正常終了したかどうか
        """
        logger.info(f"Waiting for {self.num_workers} workers to shutdown...")
        shutdown_count = 0
        start_time = time.time()
        
        while shutdown_count < self.num_workers and (time.time() - start_time) < timeout:
            result = self.get_result(timeout=1)
            if result and result.status == ResultStatus.WORKER_SHUTDOWN:
                shutdown_count += 1
                logger.info(f"Worker {result.worker_id} shutdown confirmed ({shutdown_count}/{self.num_workers})")
        
        if shutdown_count == self.num_workers:
            logger.info("All workers have shut down successfully")
            return True
        else:
            logger.warning(f"Only {shutdown_count}/{self.num_workers} workers shut down within timeout")
            return False
    
    def check_worker_health(self, heartbeat_timeout: float = 120) -> Dict[int, str]:
        """
        ワーカーのヘルス状態をチェック（改善版 - フォーム送信時間を考慮）
        
        Args:
            heartbeat_timeout: ハートビートタイムアウト（秒）- デフォルト120秒に延長
            
        Returns:
            Dict[int, str]: worker_id -> status のマップ
        """
        current_time = time.time()
        health_status = {}
        
        # キューサイズ取得（バックプレッシャー検知用）
        try:
            task_queue_size = self.task_queue.qsize() if hasattr(self.task_queue, 'qsize') else 0
            result_queue_size = self.result_queue.qsize() if hasattr(self.result_queue, 'qsize') else 0
        except:
            task_queue_size = 0
            result_queue_size = 0
        
        # バックプレッシャー検知閾値（調整）
        high_backpressure_threshold = 150  # 100 -> 150
        medium_backpressure_threshold = 70  # 50 -> 70
        
        for worker_id, status in self.worker_status.items():
            last_heartbeat = self.worker_last_heartbeat.get(worker_id, 0)
            heartbeat_age = current_time - last_heartbeat
            
            if status == 'shutdown':
                health_status[worker_id] = 'shutdown'
            elif heartbeat_age > heartbeat_timeout * 2:  # 240秒で完全応答なし判定
                health_status[worker_id] = 'unresponsive'
                logger.warning(f"Worker {worker_id} appears unresponsive (last heartbeat: {heartbeat_age:.1f}s ago)")
            elif heartbeat_age > heartbeat_timeout:  # 120秒で劣化判定
                health_status[worker_id] = 'degraded'
                logger.info(f"Worker {worker_id} degraded (heartbeat age: {heartbeat_age:.1f}s)")
            elif task_queue_size > high_backpressure_threshold:
                health_status[worker_id] = 'high_backpressure'
                logger.warning(f"Worker {worker_id} experiencing high backpressure (queue size: {task_queue_size})")
            elif task_queue_size > medium_backpressure_threshold:
                health_status[worker_id] = 'medium_backpressure'
                logger.info(f"Worker {worker_id} experiencing medium backpressure (queue size: {task_queue_size})")
            elif result_queue_size > high_backpressure_threshold:
                health_status[worker_id] = 'result_backpressure'
                logger.warning(f"Worker {worker_id} result queue backpressure (queue size: {result_queue_size})")
            else:
                health_status[worker_id] = 'healthy'
        
        return health_status
    
    def get_pending_task_count(self) -> int:
        """保留中のタスク数を取得"""
        return len(self.pending_tasks)
    
    def get_stats(self) -> Dict[str, Any]:
        """統計情報を取得"""
        elapsed_time = time.time() - self.stats['start_time']
        
        return {
            'elapsed_time': elapsed_time,
            'tasks_sent': self.stats['tasks_sent'],
            'results_received': self.stats['results_received'],
            'errors': self.stats['errors'],
            'pending_tasks': len(self.pending_tasks),
            'completed_tasks': len(self.completed_tasks),
            'worker_count': self.num_workers,
            'queue_sizes': {
                'task_queue': self.task_queue.qsize() if hasattr(self.task_queue, 'qsize') else 'unknown',
                'result_queue': self.result_queue.qsize() if hasattr(self.result_queue, 'qsize') else 'unknown'
            }
        }
    
    def recover_pending_tasks(self, timeout_seconds: float = 300) -> List[str]:
        """
        長時間pending状態のタスクを検出・回復
        
        Args:
            timeout_seconds: タスクタイムアウト（秒）
            
        Returns:
            List[str]: 回復されたタスクIDのリスト
        """
        current_time = time.time()
        recovered_tasks = []
        
        # 長時間pending状態のタスクを特定
        tasks_to_recover = []
        for task_id, task in self.pending_tasks.items():
            # タスクの送信時刻を推定（task_idから時刻情報を抽出）
            try:
                # task_id形式: task_{uuid}_{counter} の想定
                task_age = current_time - (self.stats['start_time'] + self.task_counter - len(self.pending_tasks))
                if task_age > timeout_seconds:
                    tasks_to_recover.append(task_id)
                    logger.warning(f"Task {task_id} has been pending for {task_age:.1f}s, marking for recovery")
            except:
                # 時刻推定失敗の場合、安全側でタイムアウト扱い
                tasks_to_recover.append(task_id)
        
        # 回復処理：pending状態から削除し、再送信キューに追加
        for task_id in tasks_to_recover:
            if task_id in self.pending_tasks:
                task = self.pending_tasks[task_id]
                # タスクを再送信キューに戻す（新しいIDで）
                new_task_id = self.generate_task_id()
                task['task_id'] = new_task_id
                
                try:
                    self.task_queue.put(task, timeout=1)
                    del self.pending_tasks[task_id]
                    recovered_tasks.append(task_id)
                    logger.info(f"Task {task_id} recovered as {new_task_id}")
                except queue.Full:
                    logger.error(f"Cannot recover task {task_id} - queue full")
        
        if recovered_tasks:
            logger.info(f"Recovered {len(recovered_tasks)} pending tasks")
        
        return recovered_tasks
    
    def get_pending_task_summary(self) -> Dict[str, Any]:
        """
        pending タスクの詳細サマリーを取得
        
        Returns:
            Dict[str, Any]: pending タスクの統計情報
        """
        current_time = time.time()
        
        # 年齢別分布
        age_distribution = {'<1min': 0, '1-5min': 0, '5-10min': 0, '>10min': 0}
        
        for task_id, task in self.pending_tasks.items():
            # 簡易的な年齢推定
            estimated_age = current_time - (self.stats['start_time'] + self.task_counter - len(self.pending_tasks))
            
            if estimated_age < 60:
                age_distribution['<1min'] += 1
            elif estimated_age < 300:
                age_distribution['1-5min'] += 1
            elif estimated_age < 600:
                age_distribution['5-10min'] += 1
            else:
                age_distribution['>10min'] += 1
        
        return {
            'total_pending': len(self.pending_tasks),
            'age_distribution': age_distribution,
            'oldest_task_age_estimate': max(0, current_time - (self.stats['start_time'] + 1)) if self.pending_tasks else 0
        }
    
    def cleanup(self):
        """リソースクリーンアップ（改善版）"""
        logger.info("Cleaning up QueueManager resources")
        
        # pending タスクの状況をログ出力
        if self.pending_tasks:
            logger.warning(f"Cleanup: {len(self.pending_tasks)} tasks still pending")
            summary = self.get_pending_task_summary()
            logger.warning(f"Pending task summary: {summary}")
        
        # キューのクリーンアップ
        try:
            # 残タスクをクリア
            task_count = 0
            while not self.task_queue.empty():
                self.task_queue.get_nowait()
                task_count += 1
            if task_count > 0:
                logger.info(f"Cleared {task_count} remaining tasks from queue")
        except:
            pass
            
        try:
            # 残結果をクリア  
            result_count = 0
            while not self.result_queue.empty():
                self.result_queue.get_nowait()
                result_count += 1
            if result_count > 0:
                logger.info(f"Cleared {result_count} remaining results from queue")
        except:
            pass
        
        # 内部状態をクリア
        self.pending_tasks.clear()
        self.completed_tasks.clear()
        self.worker_status.clear()
        self.worker_last_heartbeat.clear()
        
        logger.info("QueueManager cleanup completed")