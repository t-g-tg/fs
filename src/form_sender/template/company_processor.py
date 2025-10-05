"""
企業固有プレースホルダー処理

targeting.message内の企業固有プレースホルダー（{representative}など）を
企業データに置換する機能を提供
"""

import logging
import re
from typing import Dict, Any, Set, List

logger = logging.getLogger(__name__)


class CompanyPlaceholderAnalyzer:
    """企業固有プレースホルダー解析クラス"""
    
    @staticmethod
    def extract_company_placeholders(text: str) -> Set[str]:
        """
        テキストから企業固有プレースホルダーを抽出
        条件付きプレースホルダー [{}] と通常のプレースホルダー {} の両方に対応
        
        Args:
            text: 解析対象テキスト
            
        Returns:
            企業カラム名のセット（{client.*}や{targeting.*}を除く）
        """
        if not text:
            return set()
        
        company_fields = set()
        
        # パターン1: 条件付きプレースホルダー [{}] から抽出
        conditional_pattern = r'\[([^\[\]]*\{([^}]+)\}[^\[\]]*)\]'
        conditional_matches = re.findall(conditional_pattern, text)
        
        for full_content, placeholder_name in conditional_matches:
            placeholder_name = placeholder_name.strip()
            # client.*やtargeting.*以外を企業フィールドとして扱う
            if not placeholder_name.startswith(('client.', 'targeting.')):
                company_fields.add(placeholder_name)
        
        # パターン2: 通常のプレースホルダー {} から抽出
        placeholder_pattern = r'\{([^}]+)\}'
        matches = re.findall(placeholder_pattern, text)
        
        for match in matches:
            match = match.strip()
            # client.*やtargeting.*以外を企業フィールドとして扱う
            if '.' not in match:
                # 単純なフィールド名は企業フィールド
                company_fields.add(match)
            elif not match.startswith(('client.', 'targeting.')):
                # ドット記法だがclient/targeting以外も企業フィールドとして扱う
                company_fields.add(match)
        
        logger.debug(f"Extracted company placeholders: {company_fields}")
        return company_fields
    
    @staticmethod
    def get_required_company_columns(client_config: Dict[str, Any]) -> Set[str]:
        """
        クライアント設定から必要な企業カラムを取得
        
        Args:
            client_config: クライアント設定データ
            
        Returns:
            必要な企業カラム名のセット
        """
        targeting_message = client_config.get('message', '')
        targeting_subject = client_config.get('subject', '')
        
        # subjectとmessageの両方から企業固有プレースホルダーを抽出
        required_columns = set()
        required_columns.update(CompanyPlaceholderAnalyzer.extract_company_placeholders(targeting_message))
        required_columns.update(CompanyPlaceholderAnalyzer.extract_company_placeholders(targeting_subject))
        
        logger.info(f"Required company columns: {required_columns}")
        return required_columns
    
    @staticmethod
    def expand_company_placeholders(instruction_data: Dict[str, Any], 
                                  company_data: Dict[str, Any], 
                                  client_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        プレースホルダ展開処理
        
        Args:
            instruction_data: 指示書データ
            company_data: 企業データ
            client_data: クライアントデータ
            
        Returns:
            展開済み指示書データ
        """
        # InstructionTemplateProcessor を使用してプレースホルダ展開
        from .processor import InstructionTemplateProcessor
        
        # クライアントデータとターゲティングデータを統合
        combined_data = {}
        if isinstance(client_data, dict):
            # 2シート構造またはフラット構造に対応
            if 'client' in client_data and 'targeting' in client_data:
                combined_data = {
                    'client': client_data['client'],
                    'targeting': client_data['targeting']
                }
            else:
                # フラット構造の場合、client と targeting に分けて使用
                combined_data = {
                    'client': client_data,
                    'targeting': client_data
                }
        
        # 企業固有データも追加
        combined_data['company'] = company_data
        
        processor = InstructionTemplateProcessor(combined_data)
        expanded_instruction = processor.expand_placeholders(instruction_data)
        
        return expanded_instruction


class CompanyPlaceholderProcessor:
    """企業固有プレースホルダー変換処理クラス"""
    
    def __init__(self, company_data: Dict[str, Any]):
        """
        初期化
        
        Args:
            company_data: 企業データ（companiesテーブルの1行分）
        """
        self.company_data = company_data
    
    def process_company_placeholders(self, text: str) -> str:
        """
        テキスト内の企業固有プレースホルダーを企業データで置換
        条件付きプレースホルダー（[{field}テキスト]）も処理
        
        Args:
            text: 処理対象テキスト
            
        Returns:
            プレースホルダー置換後のテキスト
        """
        if not text:
            return text
        
        # ステップ1: 条件付きプレースホルダー処理 [{}] → 値があれば展開、なければ削除
        result = self._process_conditional_placeholders(text)
        
        # ステップ2: 通常のプレースホルダー処理 {}
        result = self._process_regular_placeholders(result)
        
        if result != text:
            logger.debug(f"Company placeholder processing: '{text}' -> '{result}'")
        
        return result
    
    def _process_conditional_placeholders(self, text: str) -> str:
        """
        条件付きプレースホルダー [{}] を処理
        値がある場合は[]を削除して内容を展開、なければ[]内全体を削除
        """
        # パターン: [任意の文字{プレースホルダー}任意の文字]
        # []内には1つの{}のみ含まれる想定
        conditional_pattern = r'\[([^\[\]]*\{([^}]+)\}[^\[\]]*)\]'
        
        def replace_conditional_placeholder(match):
            full_content = match.group(1)  # []内の全体（{placeholder}テキスト）
            placeholder_name = match.group(2).strip()  # {}内のフィールド名
            
            # client.*やtargeting.*は企業プレースホルダーではないのでそのまま返す
            if placeholder_name.startswith(('client.', 'targeting.')):
                return match.group(0)  # 元の[...]を返す
            
            # 企業データから値を取得
            value = self._get_company_field_value(placeholder_name)
            
            if value:  # 値がある場合
                # {}内だけを値で置換して[]を削除
                expanded_content = re.sub(
                    r'\{' + re.escape(placeholder_name) + r'\}', 
                    value, 
                    full_content
                )
                return expanded_content
            else:  # null/空文字の場合
                return ''  # []内全体を削除
        
        return re.sub(conditional_pattern, replace_conditional_placeholder, text)
    
    def _process_regular_placeholders(self, text: str) -> str:
        """
        通常のプレースホルダー {} を処理
        """
        placeholder_pattern = r'\{([^}]+)\}'
        
        def replace_company_placeholder(match):
            placeholder = match.group(1).strip()
            
            # client.*やtargeting.*は企業プレースホルダーではないのでそのまま返す
            if placeholder.startswith(('client.', 'targeting.')):
                return match.group(0)  # 元のプレースホルダーを返す
            
            # 企業データから値を取得
            return self._get_company_field_value(placeholder)
        
        return re.sub(placeholder_pattern, replace_company_placeholder, text)
    
    def _get_company_field_value(self, field_name: str) -> str:
        """
        企業データから指定フィールドの値を取得
        
        Args:
            field_name: フィールド名
            
        Returns:
            フィールドの値（存在しない場合は空文字列）
        """
        try:
            # ドット記法をサポート（例: contact.name）
            if '.' in field_name:
                parts = field_name.split('.')
                value = self.company_data
                for part in parts:
                    if isinstance(value, dict) and part in value:
                        value = value[part]
                    else:
                        logger.warning(f"Company field not found: {field_name}")
                        return ''
                
                return str(value) if value is not None else ''
            else:
                # 単純なフィールド名
                if field_name in self.company_data:
                    value = self.company_data[field_name]
                    return str(value) if value is not None else ''
                else:
                    logger.warning(f"Company field not found: {field_name}")
                    return ''
        
        except Exception as e:
            logger.error(f"Error processing company field {field_name}: {e}")
            return ''