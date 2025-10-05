"""
フォーム送信成功/失敗判定用パターンマッチャー

パフォーマンス最適化と設定外部化を実装した判定クラス
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class FormDetectionPatternMatcher:
    """フォーム送信結果判定用パターンマッチャー（最適化版）"""

    def __init__(self, config_path: Optional[str] = None):
        """
        初期化
        
        Args:
            config_path: パターン設定ファイルのパス（省略時は標準パスを使用）
        """
        self.config_path = config_path or self._get_default_config_path()
        
        # パフォーマンス最適化：小文字変換を事前実行
        self._success_url_patterns_lower: List[str] = []
        self._success_content_patterns_lower: List[str] = []
        self._error_url_patterns_lower: List[str] = []
        self._error_content_patterns_lower: List[str] = []
        self._acceptable_redirect_patterns_lower: List[str] = []
        
        # 設定読み込み
        self._load_patterns()
        
        logger.info(f"FormDetectionPatternMatcher initialized with {len(self._success_url_patterns_lower)} success URL patterns, "
                   f"{len(self._success_content_patterns_lower)} success content patterns")

    def _get_default_config_path(self) -> str:
        """標準設定ファイルパスを取得"""
        # プロジェクトルートからの相対パス
        current_dir = Path(__file__).parent
        project_root = current_dir.parent.parent.parent
        return str(project_root / "config" / "form_detection_patterns.json")

    def _load_patterns(self) -> None:
        """パターン設定ファイルからパターンを読み込み"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                patterns_config = json.load(f)
            
            # パフォーマンス最適化：事前に小文字化
            self._success_url_patterns_lower = [p.lower() for p in patterns_config.get("success_url_patterns", [])]
            self._success_content_patterns_lower = [p.lower() for p in patterns_config.get("success_content_patterns", [])]
            self._error_url_patterns_lower = [p.lower() for p in patterns_config.get("error_url_patterns", [])]
            self._error_content_patterns_lower = [p.lower() for p in patterns_config.get("error_content_patterns", [])]
            self._acceptable_redirect_patterns_lower = [p.lower() for p in patterns_config.get("acceptable_redirect_patterns", [])]
            
            logger.info(f"Patterns loaded from {self.config_path}")
            
        except FileNotFoundError:
            logger.error(f"Pattern config file not found: {self.config_path}")
            self._use_fallback_patterns()
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in pattern config file: {e}")
            self._use_fallback_patterns()
        except Exception as e:
            logger.error(f"Error loading pattern config: {e}")
            self._use_fallback_patterns()

    def _use_fallback_patterns(self) -> None:
        """フォールバック用の基本パターンを使用"""
        logger.warning("Using fallback patterns due to config load failure")
        
        self._success_url_patterns_lower = [
            "/thanks", "/thank-you", "/complete", "/completed", "/done", 
            "/submitted", "/success", "/confirm", "/confirmation",
            "/kanryou", "/uketsuke", "/arigatou", "完了", "受付", "成功"
        ]
        
        self._success_content_patterns_lower = [
            "送信完了", "受付完了", "ありがとう", "完了しました", "thank you", 
            "submitted", "success", "successfully", "confirmation"
        ]
        
        self._error_url_patterns_lower = [
            "/error", "/404", "/500", "/403", "/failed", "エラー", "失敗"
        ]
        
        self._error_content_patterns_lower = [
            "エラー", "失敗", "error", "failed", "問題が発生", "something went wrong"
        ]
        
        self._acceptable_redirect_patterns_lower = [
            "/login", "/home", "/index", "/dashboard", "/"
        ]

    def is_success_url(self, url: str) -> bool:
        """
        成功URLかどうかの判定（最適化版）
        
        Args:
            url: 判定対象URL
            
        Returns:
            bool: 成功URLかどうか
        """
        if not url:
            return False
            
        url_lower = url.lower()
        return any(pattern in url_lower for pattern in self._success_url_patterns_lower)

    def is_error_url(self, url: str) -> bool:
        """
        エラーURLかどうかの判定（最適化版）
        
        Args:
            url: 判定対象URL
            
        Returns:
            bool: エラーURLかどうか
        """
        if not url:
            return False
            
        url_lower = url.lower()
        return any(pattern in url_lower for pattern in self._error_url_patterns_lower)

    def is_acceptable_redirect(self, url: str) -> bool:
        """
        許可可能なリダイレクト先URLかどうかの判定
        
        Args:
            url: 判定対象URL
            
        Returns:
            bool: 許可可能なリダイレクト先かどうか
        """
        if not url:
            return False
            
        url_lower = url.lower()
        return any(pattern in url_lower for pattern in self._acceptable_redirect_patterns_lower)

    def contains_success_indicators(self, content: str) -> bool:
        """
        成功指標がコンテンツに含まれているかの判定（最適化版）
        
        Args:
            content: 判定対象コンテンツ
            
        Returns:
            bool: 成功指標が含まれているかどうか
        """
        if not content:
            return False
            
        content_lower = content.lower()
        return any(pattern in content_lower for pattern in self._success_content_patterns_lower)

    def contains_error_indicators(self, content: str) -> bool:
        """
        エラー指標がコンテンツに含まれているかの判定（最適化版）
        
        Args:
            content: 判定対象コンテンツ
            
        Returns:
            bool: エラー指標が含まれているかどうか
        """
        if not content:
            return False
            
        content_lower = content.lower()
        return any(pattern in content_lower for pattern in self._error_content_patterns_lower)

    def get_pattern_stats(self) -> Dict[str, int]:
        """パターン統計情報を取得（デバッグ用）"""
        return {
            "success_url_patterns": len(self._success_url_patterns_lower),
            "success_content_patterns": len(self._success_content_patterns_lower),
            "error_url_patterns": len(self._error_url_patterns_lower),
            "error_content_patterns": len(self._error_content_patterns_lower),
            "acceptable_redirect_patterns": len(self._acceptable_redirect_patterns_lower)
        }

    def reload_patterns(self) -> bool:
        """
        パターンを再読み込み（運用時の設定変更対応）
        
        Returns:
            bool: 再読み込みが成功したかどうか
        """
        try:
            self._load_patterns()
            logger.info("Patterns reloaded successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to reload patterns: {e}")
            return False