"""
指示書テンプレート処理

プレースホルダ変数の展開機能を提供
"""

import logging
import re
from typing import Dict, Any

logger = logging.getLogger(__name__)


class InstructionTemplateProcessor:
    """指示書テンプレート処理クラス（プレースホルダ変数展開）"""
    
    def __init__(self, client_data: Dict[str, Any]):
        self.client_data = client_data
    
    def expand_placeholders(self, instruction_json: Dict[str, Any]) -> Dict[str, Any]:
        """指示書内のプレースホルダ変数を実際のデータに展開"""
        return self._process_value(instruction_json)
    
    def _process_value(self, value: Any) -> Any:
        """再帰的に値を処理してプレースホルダを展開"""
        if isinstance(value, str):
            return self._expand_string_placeholders(value)
        elif isinstance(value, dict):
            return {k: self._process_value(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [self._process_value(item) for item in value]
        else:
            return value
    
    def _expand_string_placeholders(self, text: str) -> str:
        """文字列内のプレースホルダを展開"""
        # {client.field}や{targeting.field}の形式のプレースホルダを検出
        placeholder_pattern = r'\{([^}]+)\}'
        
        def replace_placeholder(match):
            placeholder = match.group(1)
            return self._get_placeholder_value(placeholder)
        
        return re.sub(placeholder_pattern, replace_placeholder, text)
    
    def _get_placeholder_value(self, placeholder: str) -> str:
        """プレースホルダから実際の値を取得（FORM_SENDER.md 1.3.2節準拠）"""
        try:
            if '.' in placeholder:
                parts = placeholder.split('.')
                if len(parts) == 2:
                    # 通常のフィールド（client.company_name、client.email_1等）
                    table, field = parts
                    if table in self.client_data and field in self.client_data[table]:
                        value = self.client_data[table][field]
                        return str(value) if value is not None else ''
            
            logger.warning(f"Unknown placeholder: {placeholder}")
            return f"{{{placeholder}}}"  # 元のプレースホルダを返す
        
        except Exception as e:
            logger.error(f"Error processing placeholder {placeholder}: {e}")
            return f"{{{placeholder}}}"