"""
安全なログ出力関数
開発者が簡単に安全なログを出力するためのヘルパー関数群
GitHub Actions環境でのクライアント個人情報完全保護
"""

import logging
from typing import Any, Optional, Dict, List
from .log_auditor import audit_before_log, get_audit_report
from .log_sanitizer import LogSanitizer

# グローバル設定
_sanitizer = LogSanitizer()
_logger = logging.getLogger(__name__)

def safe_log_debug(message: str, extra: Optional[Dict[str, Any]] = None, record_id: Optional[str] = None) -> None:
    """
    安全なDEBUGログ出力
    
    Args:
        message: ログメッセージ
        extra: 追加情報（自動的にサニタイズされる）
        record_id: 処理対象のrecord_id（保護される）
    """
    _safe_log_with_level(logging.DEBUG, message, extra, record_id)

def safe_log_info(message: str, extra: Optional[Dict[str, Any]] = None, record_id: Optional[str] = None) -> None:
    """
    安全なINFOログ出力
    
    Args:
        message: ログメッセージ
        extra: 追加情報（自動的にサニタイズされる）
        record_id: 処理対象のrecord_id（保護される）
    """
    _safe_log_with_level(logging.INFO, message, extra, record_id)

def safe_log_warning(message: str, extra: Optional[Dict[str, Any]] = None, record_id: Optional[str] = None) -> None:
    """
    安全なWARNINGログ出力
    
    Args:
        message: ログメッセージ
        extra: 追加情報（自動的にサニタイズされる）
        record_id: 処理対象のrecord_id（保護される）
    """
    _safe_log_with_level(logging.WARNING, message, extra, record_id)

def safe_log_error(message: str, extra: Optional[Dict[str, Any]] = None, record_id: Optional[str] = None, exc_info: bool = False) -> None:
    """
    安全なERRORログ出力
    
    Args:
        message: ログメッセージ
        extra: 追加情報（自動的にサニタイズされる）
        record_id: 処理対象のrecord_id（保護される）
        exc_info: 例外情報を含めるかどうか
    """
    _safe_log_with_level(logging.ERROR, message, extra, record_id, exc_info)

def safe_log_field_operation(operation: str, field_name: str, field_type: str, record_id: Optional[str] = None, success: bool = True) -> None:
    """
    フィールド操作の安全なログ出力（値は一切含まない）
    
    Args:
        operation: 操作種別（input, select, etc.）
        field_name: フィールド名
        field_type: フィールドタイプ（text, email, etc.）
        record_id: 処理対象のrecord_id
        success: 操作成功フラグ
    """
    status = "SUCCESS" if success else "FAILED"
    safe_message = f"Field operation: {operation} - field_name: {field_name} - field_type: {field_type} - status: {status}"
    
    if record_id:
        safe_message += f" - record_id: {record_id}"
    
    level = logging.DEBUG if success else logging.WARNING
    _safe_log_with_level(level, safe_message)

def safe_log_form_submission(record_id: str, form_url: str = "***URL_REDACTED***", success: bool = True, field_count: int = 0) -> None:
    """
    フォーム送信結果の安全なログ出力
    
    Args:
        record_id: 処理対象のrecord_id
        form_url: フォームURL（自動的にマスクされる）
        success: 送信成功フラグ
        field_count: 処理したフィールド数
    """
    status = "SUCCESS" if success else "FAILED"
    safe_message = f"Form submission: status: {status} - fields_processed: {field_count} - record_id: {record_id}"
    
    level = logging.INFO if success else logging.ERROR
    _safe_log_with_level(level, safe_message)

def safe_log_validation_result(field_name: str, field_type: str, validation_passed: bool, record_id: Optional[str] = None) -> None:
    """
    バリデーション結果の安全なログ出力（値は一切含まない）
    
    Args:
        field_name: フィールド名
        field_type: フィールドタイプ
        validation_passed: バリデーション結果
        record_id: 処理対象のrecord_id
    """
    status = "PASSED" if validation_passed else "FAILED"
    safe_message = f"Validation: field_name: {field_name} - field_type: {field_type} - status: {status}"
    
    if record_id:
        safe_message += f" - record_id: {record_id}"
    
    level = logging.DEBUG if validation_passed else logging.WARNING
    _safe_log_with_level(level, safe_message)

def _safe_log_with_level(level: int, message: str, extra: Optional[Dict[str, Any]] = None, record_id: Optional[str] = None, exc_info: bool = False) -> None:
    """
    内部関数：指定されたレベルで安全なログ出力を実行
    
    Args:
        level: ログレベル
        message: ログメッセージ
        extra: 追加情報
        record_id: 処理対象のrecord_id
        exc_info: 例外情報を含めるかどうか
    """
    # record_idを含む完全なメッセージを構築
    full_message = message
    if record_id:
        full_message = f"{message} - record_id: {record_id}"
    
    # 追加情報をメッセージに統合（安全化）
    if extra:
        sanitized_extra = _sanitizer.sanitize(str(extra))
        full_message += f" - extra: {sanitized_extra}"
    
    # 監査実行
    allow_output, safe_message, violations = audit_before_log(full_message)
    
    # 違反がある場合は監査レポートを出力
    if violations:
        violation_report = get_audit_report(violations)
        _logger.warning(f"Log audit detected violations: {len(violations)} issues found")
        
        # CRITICAL違反がある場合は詳細を記録（値は含まない）
        critical_violations = [v for v in violations if v.level.value == "critical"]
        if critical_violations:
            _logger.error(f"CRITICAL log violations detected: {len(critical_violations)} critical issues - message blocked")
    
    # 安全なメッセージでログ出力
    logger = logging.getLogger('form_sender')
    logger.log(level, safe_message, exc_info=exc_info)

def create_safe_field_result(field_name: str, field_type: str, success: bool, error_message: Optional[str] = None) -> Dict[str, Any]:
    """
    フィールド処理結果の安全な辞書作成（値は一切含まない）
    
    Args:
        field_name: フィールド名
        field_type: フィールドタイプ
        success: 成功フラグ
        error_message: エラーメッセージ（サニタイズされる）
        
    Returns:
        Dict[str, Any]: 安全な結果辞書
    """
    result = {
        'field_name': field_name,
        'field_type': field_type,
        'success': success,
        'filled_value': '***VALUE_REDACTED***'  # 個人情報保護
    }
    
    if not success and error_message:
        # エラーメッセージも安全化
        result['error'] = _sanitizer.sanitize(error_message)
    
    return result

def get_safe_processing_summary(total_fields: int, successful_fields: int, failed_fields: int, record_id: str) -> str:
    """
    処理サマリの安全な文字列生成
    
    Args:
        total_fields: 総フィールド数
        successful_fields: 成功フィールド数
        failed_fields: 失敗フィールド数
        record_id: 処理対象のrecord_id
        
    Returns:
        str: 安全なサマリ文字列
    """
    return (f"Processing summary: total_fields: {total_fields} - "
            f"successful: {successful_fields} - failed: {failed_fields} - "
            f"record_id: {record_id}")