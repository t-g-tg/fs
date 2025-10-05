"""
自動復旧マネージャー

システム障害からの自動復旧機能
"""

import logging
import time

logger = logging.getLogger(__name__)


class AutoRecoveryManager:
    """自動復旧機能マネージャー"""
    
    def __init__(self, max_recovery_attempts: int = 3):
        self.max_recovery_attempts = max_recovery_attempts
        self.recovery_count = 0
        self.last_recovery_time = 0
        self.recovery_cooldown = 60  # 復旧試行間の待機時間（秒）
    
    def can_attempt_recovery(self) -> bool:
        """復旧試行が可能かチェック"""
        current_time = time.time()
        if current_time - self.last_recovery_time < self.recovery_cooldown:
            logger.info(f"Recovery cooldown active, waiting {self.recovery_cooldown}s")
            return False
        
        if self.recovery_count >= self.max_recovery_attempts:
            logger.warning(f"Max recovery attempts ({self.max_recovery_attempts}) reached")
            return False
            
        return True
    
    def mark_recovery_attempt(self):
        """復旧試行をマーク"""
        self.recovery_count += 1
        self.last_recovery_time = time.time()
        logger.info(f"Recovery attempt {self.recovery_count}/{self.max_recovery_attempts}")
    
    def reset_recovery_count(self):
        """復旧カウントをリセット（成功時）"""
        if self.recovery_count > 0:
            logger.info(f"Recovery successful, resetting count from {self.recovery_count} to 0")
            self.recovery_count = 0