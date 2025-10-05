"""
ログ監査システム
GitHub Actions環境でのログ出力前自動スキャン機能
企業識別情報漏洩の完全防止
"""

import re
import os
import logging
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

class ViolationLevel(Enum):
    """セキュリティ違反レベル"""
    CRITICAL = "critical"  # 即座にブロック
    HIGH = "high"         # 警告付きでブロック
    MEDIUM = "medium"     # 警告のみ
    LOW = "low"           # 情報のみ

@dataclass
class SecurityViolation:
    """セキュリティ違反情報"""
    level: ViolationLevel
    violation_type: str
    matched_pattern: str
    matched_text: str
    position: Tuple[int, int]  # (start, end)
    suggestion: str

class LogAuditor:
    """ログ出力前の自動セキュリティ監査システム"""
    
    def __init__(self):
        self.is_github_actions = os.getenv('GITHUB_ACTIONS', '').lower() == 'true'
        self.is_production = os.getenv('ENVIRONMENT', '').lower() == 'production'
        
        # GitHub Actions環境では最も厳格な監査を実行
        self.strict_mode = self.is_github_actions or self.is_production
        
        self._setup_violation_patterns()
    
    def _setup_violation_patterns(self):
        """セキュリティ違反パターンの定義"""
        
        # CRITICAL: 絶対に出力してはいけない情報
        self.critical_patterns = [
            # 企業名（日本語）
            (r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]{3,}(?:株式会社|有限会社|会社|法人|コーポレーション|Corp|Inc|Ltd|Co\.)', 
             "日本語企業名が検出されました"),
            
            # URL（完全）
            (r'https?://[^\s]+', "URLが検出されました"),
            
            # メールアドレス（完全）
            (r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', "メールアドレスが検出されました"),
            
            # API Key/Secret
            (r'(?i)(api[_-]?key|secret|token|password)\s*[=:]\s*["\']?([a-zA-Z0-9_-]{8,})["\']?', 
             "API Key/Secretが検出されました"),
        ]
        
        # HIGH: GitHub Actions環境では絶対に出力不可
        self.high_patterns = [
            # 企業名フィールド
            (r'(?i)(company[_-]?name|企業名|会社名)\s*[=:\-]\s*["\']?([^"\'\s\n,}{]+)["\']?', 
             "企業名フィールドが検出されました"),
            
            # 英語企業名
            (r'\b[A-Z][a-zA-Z]{2,}\s+(?:Corp|Inc|Ltd|LLC|Co|Company|Corporation|Limited)\b', 
             "英語企業名が検出されました"),
            
            # ドメイン名
            (r'\b[a-zA-Z0-9-]+\.(com|co\.jp|jp|net|org|info|biz)[^\s]*', 
             "ドメイン名が検出されました"),
             
            # クライアント個人情報フィールド
            (r'(?i)(name|email|message|phone|address|氏名|名前|メール|問い合わせ|電話|住所)\s*[=:\-]\s*["\']?([^"\'\s\n,}{]{3,})["\']?',
             "クライアント個人情報フィールドが検出されました"),
        ]
        
        # MEDIUM: 注意が必要な情報
        self.medium_patterns = [
            # 3文字以上の日本語（企業名の可能性）
            (r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF]{3,}', 
             "日本語文字列が検出されました（企業名の可能性）"),
            
            # ID（record_id以外）
            (r'(?<!record_)(id|ID)\s*[=:]\s*(\d+)', 
             "ID情報が検出されました（record_id以外）"),
        ]
        
        # LOW: 情報として記録
        self.low_patterns = [
            # 電話番号
            (r'\b0\d{1,4}-?\d{1,4}-?\d{3,4}\b', 
             "電話番号らしき文字列が検出されました"),
        ]
    
    def audit_log_message(self, message: str) -> Tuple[bool, List[SecurityViolation]]:
        """
        ログメッセージのセキュリティ監査
        
        Args:
            message: 監査対象のログメッセージ
            
        Returns:
            Tuple[bool, List[SecurityViolation]]: (出力許可フラグ, 違反リスト)
        """
        if not isinstance(message, str):
            message = str(message)
        
        violations = []
        
        # CRITICAL違反チェック
        for pattern, description in self.critical_patterns:
            for match in re.finditer(pattern, message, re.IGNORECASE):
                violations.append(SecurityViolation(
                    level=ViolationLevel.CRITICAL,
                    violation_type="CRITICAL_INFO_DISCLOSURE",
                    matched_pattern=pattern,
                    matched_text=match.group(),
                    position=(match.start(), match.end()),
                    suggestion=f"{description} - 完全にマスクが必要です"
                ))
        
        # HIGH違反チェック（GitHub Actions環境でのみCRITICAL扱い）
        for pattern, description in self.high_patterns:
            for match in re.finditer(pattern, message, re.IGNORECASE):
                level = ViolationLevel.CRITICAL if self.strict_mode else ViolationLevel.HIGH
                violations.append(SecurityViolation(
                    level=level,
                    violation_type="HIGH_RISK_DISCLOSURE",
                    matched_pattern=pattern,
                    matched_text=match.group(),
                    position=(match.start(), match.end()),
                    suggestion=f"{description} - GitHub Actions環境では出力禁止"
                ))
        
        # MEDIUM違反チェック
        if self.strict_mode:  # GitHub Actions環境でのみチェック
            for pattern, description in self.medium_patterns:
                for match in re.finditer(pattern, message, re.IGNORECASE):
                    violations.append(SecurityViolation(
                        level=ViolationLevel.MEDIUM,
                        violation_type="MEDIUM_RISK_DISCLOSURE",
                        matched_pattern=pattern,
                        matched_text=match.group(),
                        position=(match.start(), match.end()),
                        suggestion=f"{description} - 注意が必要"
                    ))
        
        # LOW違反チェック（情報収集目的）
        for pattern, description in self.low_patterns:
            for match in re.finditer(pattern, message, re.IGNORECASE):
                violations.append(SecurityViolation(
                    level=ViolationLevel.LOW,
                    violation_type="LOW_RISK_DISCLOSURE",
                    matched_pattern=pattern,
                    matched_text=match.group(),
                    position=(match.start(), match.end()),
                    suggestion=f"{description} - 情報として記録"
                ))
        
        # 出力許可判定
        critical_violations = [v for v in violations if v.level == ViolationLevel.CRITICAL]
        allow_output = len(critical_violations) == 0
        
        return allow_output, violations
    
    def safe_sanitize_message(self, message: str) -> str:
        """
        安全なメッセージサニタイゼーション
        record_idのみを保持して他をマスク
        
        Args:
            message: サニタイゼーション対象メッセージ
            
        Returns:
            str: 安全化されたメッセージ
        """
        if not isinstance(message, str):
            message = str(message)
        
        # record_idを一時保護（GitHub Actions環境では特に重要）
        record_ids = re.findall(r'(record_id[:\s=]*\d+)', message, re.IGNORECASE)
        protected_ids = {}
        
        for i, record_id in enumerate(record_ids):
            placeholder = f"__PROTECTED_RECORD_ID_{i}__"
            protected_ids[placeholder] = record_id
            message = message.replace(record_id, placeholder, 1)
        
        # GitHub Actions環境では、より厳格な事前マスキングを実行
        if self.strict_mode:
            # 長いテキスト（個人情報含む可能性）を事前マスキング
            message = re.sub(r'[\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF\s]{30,}', '***LONG_TEXT_REDACTED***', message)
            # 任意の文字列値（3文字以上）を事前マスキング  
            message = re.sub(r'["\'][\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAFa-zA-Z0-9@._-]{3,}["\']', '"***VALUE_REDACTED***"', message)
        
        # 全パターンでマスキング
        for pattern, _ in self.critical_patterns + self.high_patterns:
            message = re.sub(pattern, "***REDACTED***", message, flags=re.IGNORECASE)
        
        if self.strict_mode:
            for pattern, _ in self.medium_patterns:
                message = re.sub(pattern, "***MASKED***", message, flags=re.IGNORECASE)
        
        # record_idを復元
        for placeholder, original in protected_ids.items():
            message = message.replace(placeholder, original)
        
        return message
    
    def generate_audit_report(self, violations: List[SecurityViolation]) -> Dict[str, Any]:
        """監査レポートの生成"""
        violation_summary = {
            "critical": len([v for v in violations if v.level == ViolationLevel.CRITICAL]),
            "high": len([v for v in violations if v.level == ViolationLevel.HIGH]),
            "medium": len([v for v in violations if v.level == ViolationLevel.MEDIUM]),
            "low": len([v for v in violations if v.level == ViolationLevel.LOW])
        }
        
        violation_types = {}
        for violation in violations:
            if violation.violation_type not in violation_types:
                violation_types[violation.violation_type] = 0
            violation_types[violation.violation_type] += 1
        
        return {
            "audit_timestamp": os.getenv('GITHUB_RUN_ID', 'local_run'),
            "environment": "github_actions" if self.is_github_actions else "local",
            "strict_mode": self.strict_mode,
            "total_violations": len(violations),
            "violation_summary": violation_summary,
            "violation_types": violation_types,
            "suggestions": [v.suggestion for v in violations if v.level in [ViolationLevel.CRITICAL, ViolationLevel.HIGH]]
        }

# グローバルインスタンス
_global_auditor = LogAuditor()

def audit_before_log(message: str) -> Tuple[bool, str, List[SecurityViolation]]:
    """
    ログ出力前の監査実行（便利関数）
    
    Args:
        message: 監査対象メッセージ
        
    Returns:
        Tuple[bool, str, List[SecurityViolation]]: (許可フラグ, 安全化メッセージ, 違反リスト)
    """
    allow_output, violations = _global_auditor.audit_log_message(message)
    
    if not allow_output:
        # 出力が許可されない場合は安全化
        safe_message = _global_auditor.safe_sanitize_message(message)
        return False, safe_message, violations
    
    return True, message, violations

def get_audit_report(violations: List[SecurityViolation]) -> Dict[str, Any]:
    """監査レポートの取得（便利関数）"""
    return _global_auditor.generate_audit_report(violations)