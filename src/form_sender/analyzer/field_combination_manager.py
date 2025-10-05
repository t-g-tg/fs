"""
フィールド組み合わせマネージャー

分割フィールドの組み合わせルールと動的組み合わせ生成機能
ユーザー指定の厳密なルールに準拠した値生成システム
テーブル構造統合対応版
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class FieldCombination:
    """フィールド組み合わせ定義"""
    name: str                    # 組み合わせ名
    component_fields: List[str]  # 構成フィールド名
    separator: str               # 区切り文字
    description: str             # 説明


@dataclass
class TableFieldContext:
    """テーブル内フィールドのコンテキスト"""
    table_type: str              # form_table, data_table, layout_table
    row_index: int               # 行番号
    cell_index: int              # セル番号
    table_headers: List[str]     # テーブルヘッダー
    row_context: List[str]       # 同一行の他のセルテキスト
    column_context: str          # 対応する列ヘッダー


@dataclass  
class IntegratedFieldMapping:
    """統合されたフィールドマッピング"""
    field_name: str              # フィールド名
    element_info: Dict[str, Any] # 要素情報
    context: List[Dict[str, Any]] # コンテキスト情報
    is_in_table: bool            # テーブル内かどうか
    table_context: Optional[TableFieldContext] = None  # テーブル内の場合のコンテキスト
    combination_priority: int = 0  # 組み合わせ優先度（高いほど優先）


class FieldCombinationManager:
    """分割フィールドの組み合わせ管理システム"""
    
    def __init__(self):
        """初期化"""
        # 標準的な組み合わせルールの定義
        self.combination_rules = {
            # 姓名組み合わせ（全角スペース区切り）
            'full_name': FieldCombination(
                name='full_name',
                component_fields=['last_name', 'first_name'],
                separator='　',  # 全角スペース
                description='姓名の組み合わせ（姓　名）'
            ),
            
            # カナ姓名組み合わせ（全角スペース区切り）
            'full_name_kana': FieldCombination(
                name='full_name_kana',
                component_fields=['last_name_kana', 'first_name_kana'],
                separator='　',  # 全角スペース
                description='カナ姓名の組み合わせ（セイ　メイ）'
            ),
            
            # ひらがな姓名組み合わせ（全角スペース区切り）
            'full_name_hiragana': FieldCombination(
                name='full_name_hiragana',
                component_fields=['last_name_hiragana', 'first_name_hiragana'],
                separator='　',  # 全角スペース
                description='ひらがな姓名の組み合わせ（せい　めい）'
            ),
            
            # メールアドレス組み合わせ
            'email': FieldCombination(
                name='email',
                component_fields=['email_1', 'email_2'],
                separator='@',
                description='メールアドレスの組み合わせ（user@domain）'
            ),
            
            # 電話番号組み合わせ（直接連結）
            'phone': FieldCombination(
                name='phone',
                component_fields=['phone_1', 'phone_2', 'phone_3'],
                separator='',  # 区切り文字なし
                description='電話番号の組み合わせ（0368250324）'
            ),
            
            # 郵便番号組み合わせ（直接連結）
            'postal_code': FieldCombination(
                name='postal_code',
                component_fields=['postal_code_1', 'postal_code_2'],
                separator='',
                description='郵便番号の組み合わせ（1600023）'
            ),
            
            # 住所組み合わせ（address_4とaddress_5の間のみ全角スペース）
            'address': FieldCombination(
                name='address',
                component_fields=['address_1', 'address_2', 'address_3', 'address_4', 'address_5'],
                separator='',  # 基本的に区切り文字なし（特別処理）
                description='住所の組み合わせ（東京都新宿区西新宿３ー３ー１３　西新宿水間ビル６階）'
            )
        }
        
        # form_sender_name廃止ルール
        self.deprecated_fields = {
            'form_sender_name': {
                'replacement': 'full_name',
                'reason': 'form_sender_nameは使用禁止。姓名組み合わせを使用。'
            }
        }
        
        logger.info("FieldCombinationManager initialized with combination rules")
    
    def generate_combined_value(self, combination_name: str, client_data: Dict[str, Any]) -> str:
        """
        指定された組み合わせルールで値を生成
        
        Args:
            combination_name: 組み合わせ名
            client_data: クライアントデータ
            
        Returns:
            str: 組み合わせされた値
        """
        if combination_name not in self.combination_rules:
            logger.error(f"Unknown combination rule: {combination_name}")
            return ''
        
        combination = self.combination_rules[combination_name]
        # 入力データ形状に柔軟対応（フラット辞書 or {client:{...}}）
        if isinstance(client_data, dict):
            client_info = client_data.get('client') if 'client' in client_data else client_data
        else:
            client_info = {}
        
        # 特別処理：住所の場合
        if combination_name == 'address':
            return self._generate_address_value(client_info)
        
        # 標準的な組み合わせ処理
        values = []
        missing_fields = []
        
        for field_name in combination.component_fields:
            value = client_info.get(field_name, '').strip()
            if value:
                values.append(value)
            else:
                missing_fields.append(field_name)
        
        if not values:
            logger.debug(f"No values found for combination '{combination_name}': missing {missing_fields}")
            return ''
        
        # 不完全な組み合わせの警告
        if missing_fields:
            logger.warning(f"Incomplete combination '{combination_name}': missing {missing_fields}, "
                         f"using available: {[f for f in combination.component_fields if f not in missing_fields]}")
        
        combined_value = combination.separator.join(values)
        logger.debug(f"Generated combined value '{combination_name}': '{combined_value}'")
        
        return combined_value
    
    def _generate_address_value(self, client_info: Dict[str, Any]) -> str:
        """
        住所の特別な組み合わせ処理
        address_4とaddress_5の間のみ全角スペースを挿入
        
        Args:
            client_info: クライアント情報
            
        Returns:
            str: 組み合わせされた住所
        """
        address_parts = []
        
        # address_1～address_4まで（直接連結）
        for i in range(1, 5):
            value = client_info.get(f'address_{i}', '').strip()
            if value:
                address_parts.append(value)
        
        # address_5（全角スペース付きで追加）
        address_5 = client_info.get('address_5', '').strip()
        
        # 基本部分の結合
        base_address = ''.join(address_parts)
        
        # address_5がある場合は全角スペースを挟んで追加
        if address_5:
            combined_address = f"{base_address}　{address_5}"  # 全角スペース
        else:
            combined_address = base_address
        
        logger.debug(f"Generated address: '{combined_address}' from parts: {address_parts + ([address_5] if address_5 else [])}")
        
        return combined_address
    
    def get_field_value_for_type(self, field_name: str, field_type: str, client_data: Dict[str, Any]) -> str:
        """
        フィールドタイプに応じた適切な値を取得
        
        Args:
            field_name: フィールド名
            field_type: フィールドタイプ（'single' or combination名）
            client_data: クライアントデータ
            
        Returns:
            str: フィールド値
        """
        # form_sender_name廃止チェック
        if field_name == 'form_sender_name' or field_type == 'form_sender_name':
            logger.warning("form_sender_name is deprecated. Using full_name combination instead.")
            return self.generate_combined_value('full_name', client_data)
        
        # 組み合わせフィールドの場合
        if field_type in self.combination_rules:
            return self.generate_combined_value(field_type, client_data)
        
        # 単一フィールドの場合
        if isinstance(client_data, dict):
            client_info = client_data.get('client') if 'client' in client_data else client_data
            targeting_info = client_data.get('targeting', {})
        else:
            client_info, targeting_info = {}, {}
        
        # targetingフィールドのチェック
        # - 英語キー: subject/message
        # - 日本語キー: 件名（=subject）
        if field_name in ['subject', 'message']:
            return targeting_info.get(field_name, client_info.get(field_name, ''))
        if field_name == '件名':
            return targeting_info.get('subject', client_info.get('subject', ''))
        
        # 日本語→クライアントキーの標準マッピング
        jp_to_client = {
            '会社名': 'company_name',
            '会社名カナ': 'company_name_kana',
            '姓': 'last_name',
            '名': 'first_name',
            '姓カナ': 'last_name_kana',
            '名カナ': 'first_name_kana',
            '姓ひらがな': 'last_name_hiragana',
            '名ひらがな': 'first_name_hiragana',
            '企業URL': 'website_url',
            '部署名': 'department',
            '役職': 'position',
            '性別': 'gender',
        }
        if field_name in jp_to_client:
            return client_info.get(jp_to_client[field_name], '')

        # 組み合わせを要する日本語フィールド
        if field_name in ['メールアドレス', 'email']:
            return self.generate_combined_value('email', client_data)
        if field_name in ['電話番号', 'tel']:
            return self.generate_combined_value('phone', client_data)
        if field_name in ['住所', 'address']:
            return self.generate_combined_value('address', client_data)
        if field_name in ['都道府県']:
            # address_1 を都道府県として扱う（値が無ければ空を返し、アルゴリズム選択に委譲）
            return client_info.get('address_1', '')
        if field_name in ['郵便番号', 'postal_code']:
            return self.generate_combined_value('postal_code', client_data)
        if field_name in ['統合氏名カナ']:
            # デフォルトはカタカナの姓名連結
            return self.generate_combined_value('full_name_kana', client_data)

        # フォールバック: そのままキー参照
        return client_info.get(field_name, '')
    
    # リファクタリング: 未使用の detect_field_combination_pattern を削除
    
    def detect_unified_kana_field(self, form_elements: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        統合カナフィールドを検出し、カタカナ/ひらがなを判断
        
        Args:
            form_elements: フォーム要素のリスト
            
        Returns:
            dict: 統合カナフィールドの情報
        """
        unified_kana_info = {
            'detected': False,
            'element_name': None,
            'kana_type': 'katakana',  # デフォルトはカタカナ
            'field_mapping': None
        }
        
        for element in form_elements:
            element_name = element.get('name', '').lower()
            element_id = element.get('id', '').lower()
            element_class = element.get('class', '').lower()
            placeholder = element.get('placeholder', '').lower()
            
            # 統合カナフィールドの候補をチェック（拡張）
            if (
                element_name in ['kana','furigana'] or
                'kana' in element_name or 'furigana' in element_name or
                'kana' in element_id or 'furigana' in element_id or
                'フリガナ' in placeholder or 'カナ' in placeholder or
                element_name.endswith('-kana')
            ):
                unified_kana_info['detected'] = True
                unified_kana_info['element_name'] = element.get('name', '')
                
                # カタカナ・ひらがなの判断
                kana_type = self._determine_kana_type(element_name, element_id, element_class, placeholder, element)
                unified_kana_info['kana_type'] = kana_type
                
                # 適切なフィールドマッピングを決定
                if kana_type == 'hiragana':
                    unified_kana_info['field_mapping'] = 'full_name_hiragana'
                else:
                    unified_kana_info['field_mapping'] = 'full_name_kana'
                
                logger.info(f"Detected unified kana field: {element.get('name', '')} -> {kana_type}")
                break
        
        return unified_kana_info
    
    def _determine_kana_type(self, element_name: str, element_id: str, element_class: str, 
                            placeholder: str, element: Dict[str, Any]) -> str:
        """
        カタカナ/ひらがなを判断
        
        Args:
            element_name: 要素名
            element_id: 要素ID
            element_class: 要素クラス
            placeholder: プレースホルダー
            element: 要素情報
            
        Returns:
            str: 'katakana' or 'hiragana'
        """
        # コンテキスト情報を取得
        context_info = element.get('context', [])
        
        # 明確なひらがな指定がある場合
        hiragana_indicators = [
            'hiragana', 'ひらがな', 'せい', 'めい', 'やまだ', 'たろう'
        ]
        
        # 明確なカタカナ指定がある場合
        katakana_indicators = [
            'katakana', 'カタカナ', 'カナ', 'セイ', 'メイ', 'ヤマダ', 'タロウ', 'フリガナ'
        ]
        
        # 各要素からの判断
        all_text = f"{element_name} {element_id} {element_class} {placeholder}".lower()
        
        # コンテキストテキストも含める
        for ctx in context_info:
            context_text = ctx.get('text', '').lower()
            all_text += f" {context_text}"
        
        # ひらがなチェック
        for indicator in hiragana_indicators:
            if indicator in all_text:
                logger.debug(f"Detected hiragana indicator: {indicator}")
                return 'hiragana'
        
        # カタカナチェック
        for indicator in katakana_indicators:
            if indicator in all_text:
                logger.debug(f"Detected katakana indicator: {indicator}")
                return 'katakana'
        
        # デフォルトはカタカナ（ユーザー指示に従う）
        logger.debug("No specific kana type detected, defaulting to katakana")
        return 'katakana'
    
    def generate_unified_kana_value(self, kana_type: str, client_data: Dict[str, Any]) -> str:
        """
        統合カナフィールド用の値を生成
        
        Args:
            kana_type: 'katakana' or 'hiragana'
            client_data: クライアントデータ
            
        Returns:
            str: 統合カナ値
        """
        if kana_type == 'hiragana':
            return self.generate_combined_value('full_name_hiragana', client_data)
        else:
            return self.generate_combined_value('full_name_kana', client_data)
    
    def get_combination_info(self, combination_name: str) -> Optional[FieldCombination]:
        """組み合わせ情報を取得"""
        return self.combination_rules.get(combination_name)
    
    def is_deprecated_field(self, field_name: str) -> bool:
        """廃止フィールドかどうかチェック"""
        return field_name in self.deprecated_fields
    
    def get_replacement_for_deprecated(self, field_name: str) -> Optional[str]:
        """廃止フィールドの代替を取得"""
        if field_name in self.deprecated_fields:
            return self.deprecated_fields[field_name]['replacement']
        return None
    
    def validate_field_combinations(self, field_mappings: Dict[str, Any], client_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        フィールド組み合わせの妥当性検証
        
        Args:
            field_mappings: フィールドマッピング
            client_data: クライアントデータ
            
        Returns:
            tuple: (妥当性, エラーメッセージリスト)
        """
        issues = []
        
        # 姓名の順序チェック
        if 'last_name' in field_mappings and 'first_name' in field_mappings:
            # 実際の実装では要素の位置関係もチェックする必要がある
            # ここでは基本的な存在チェックのみ
            pass
        
        # form_sender_name使用チェック
        for field_name in field_mappings.keys():
            if self.is_deprecated_field(field_name):
                replacement = self.get_replacement_for_deprecated(field_name)
                issues.append(f"Deprecated field '{field_name}' detected. Use '{replacement}' instead.")
        
        # 分割フィールドの完全性チェック
        for combination_name, combination in self.combination_rules.items():
            mapped_components = [f for f in combination.component_fields if f in field_mappings]
            if mapped_components and len(mapped_components) < len(combination.component_fields):
                missing = [f for f in combination.component_fields if f not in mapped_components]
                issues.append(f"Incomplete {combination_name} combination: missing {missing}")
        
        return len(issues) == 0, issues
    
    def get_all_combination_names(self) -> List[str]:
        """すべての組み合わせ名を取得"""
        return list(self.combination_rules.keys())
    
    def get_summary(self) -> Dict[str, Any]:
        """マネージャーの状態サマリーを取得"""
        return {
            'combination_rules_count': len(self.combination_rules),
            'deprecated_fields_count': len(self.deprecated_fields),
            'available_combinations': list(self.combination_rules.keys()),
            'deprecated_fields': list(self.deprecated_fields.keys())
        }
    
    def create_integrated_field_mappings(self, field_mappings: List[Dict[str, Any]], 
                                       table_structures: List[Any]) -> List[IntegratedFieldMapping]:
        """
        フィールドマッピングとテーブル構造を統合
        
        Args:
            field_mappings: 基本フィールドマッピング
            table_structures: テーブル構造のリスト
            
        Returns:
            List[IntegratedFieldMapping]: 統合されたフィールドマッピング
        """
        integrated_mappings = []
        
        # テーブル内フィールドの処理
        table_field_map = self._map_table_fields(table_structures)
        
        for mapping in field_mappings:
            field_name = mapping.get('field_name', '')
            element_info = mapping.get('element_info', {})
            
            # テーブル内にあるかチェック
            table_context = self._find_table_context(element_info, table_field_map)
            
            # 組み合わせ優先度の計算
            priority = self._calculate_combination_priority(mapping, table_context)
            
            integrated_mapping = IntegratedFieldMapping(
                field_name=field_name,
                element_info=element_info,
                context=mapping.get('context', []),
                is_in_table=table_context is not None,
                table_context=table_context,
                combination_priority=priority
            )
            
            integrated_mappings.append(integrated_mapping)
        
        # 優先度でソート
        integrated_mappings.sort(key=lambda x: x.combination_priority, reverse=True)
        
        logger.info(f"Created {len(integrated_mappings)} integrated field mappings")
        return integrated_mappings
    
    def _map_table_fields(self, table_structures: List[Any]) -> Dict[str, Dict[str, Any]]:
        """
        テーブル構造からフィールドマップを作成
        
        Args:
            table_structures: テーブル構造のリスト
            
        Returns:
            Dict: {要素識別子: テーブル情報}
        """
        table_field_map = {}
        
        for table_structure in table_structures:
            try:
                headers = getattr(table_structure, 'headers', [])
                rows = getattr(table_structure, 'rows', [])
                table_type = getattr(table_structure, 'table_type', 'unknown')
                
                for row_info in rows:
                    row_index = row_info.get('row_index', 0)
                    cells = row_info.get('cells', [])
                    row_texts = [cell.get('text', '') for cell in cells]
                    
                    for cell in cells:
                        form_elements = cell.get('form_elements', [])
                        cell_index = cell.get('cell_index', 0)
                        
                        for form_element in form_elements:
                            element_id = form_element.get('id', '')
                            element_name = form_element.get('name', '')
                            
                            # 要素の識別子を作成
                            element_key = f"{element_id}_{element_name}_{cell_index}_{row_index}"
                            
                            column_context = headers[cell_index] if cell_index < len(headers) else ''
                            
                            table_field_map[element_key] = {
                                'table_type': table_type,
                                'row_index': row_index,
                                'cell_index': cell_index,
                                'table_headers': headers,
                                'row_context': row_texts,
                                'column_context': column_context
                            }
                            
            except Exception as e:
                logger.debug(f"Error mapping table fields: {e}")
                continue
        
        return table_field_map
    
    def _find_table_context(self, element_info: Dict[str, Any], 
                           table_field_map: Dict[str, Dict[str, Any]]) -> Optional[TableFieldContext]:
        """
        要素のテーブルコンテキストを検索
        
        Args:
            element_info: 要素情報
            table_field_map: テーブルフィールドマップ
            
        Returns:
            Optional[TableFieldContext]: テーブルコンテキスト
        """
        try:
            # 要素識別子の候補を生成
            element_id = element_info.get('id', '')
            element_name = element_info.get('name', '')
            bounding_box = element_info.get('bounding_box', {})
            
            # 位置情報から推測される行・列
            if bounding_box:
                estimated_row = int(bounding_box.get('y', 0) / 50)  # 概算
                estimated_cell = int(bounding_box.get('x', 0) / 100)  # 概算
            else:
                estimated_row = estimated_cell = 0
            
            # 複数の候補でマッチング試行
            candidates = [
                f"{element_id}_{element_name}_{estimated_cell}_{estimated_row}",
                f"{element_id}_{element_name}",
                f"_{element_name}",
                f"{element_id}_"
            ]
            
            for candidate in candidates:
                for key, table_info in table_field_map.items():
                    if candidate in key:
                        return TableFieldContext(
                            table_type=table_info['table_type'],
                            row_index=table_info['row_index'],
                            cell_index=table_info['cell_index'],
                            table_headers=table_info['table_headers'],
                            row_context=table_info['row_context'],
                            column_context=table_info['column_context']
                        )
            
        except Exception as e:
            logger.debug(f"Error finding table context: {e}")
        
        return None
    
    def _calculate_combination_priority(self, mapping: Dict[str, Any], 
                                      table_context: Optional[TableFieldContext]) -> int:
        """
        組み合わせ優先度を計算（バランス調整版）
        
        Args:
            mapping: フィールドマッピング
            table_context: テーブルコンテキスト
            
        Returns:
            int: 優先度スコア（高いほど優先）
        """
        priority = 0
        
        # 基本優先度
        field_name = mapping.get('field_name', '').lower()
        
        # 必須フィールドは高優先度（変更なし）
        if any(keyword in field_name for keyword in ['name', '氏名', 'email', 'メール']):
            priority += 100
        
        # 重要フィールドの追加（新規追加）
        if any(keyword in field_name for keyword in ['会社', 'company', '電話', 'phone', 'tel']):
            priority += 80
        
        # テーブル内フィールドの優先度調整（バランス版）
        if table_context:
            # フォームテーブル内の優先度調整
            if table_context.table_type == 'form_table':
                priority += 30  # 50→30に調整（テーブル外との差を縮小）
            elif table_context.table_type == 'data_table':
                priority += 20  # 新規追加
            
            # ヘッダーがある場合の優先度調整
            if table_context.column_context:
                priority += 15  # 30→15に調整
            
            # 行コンテキストが豊富な場合の優先度調整  
            if len(table_context.row_context) > 2:
                priority += 10  # 20→10に調整
        else:
            # テーブル外フィールドの優先度向上
            priority += 25  # 10→25に向上（テーブル内との差を縮小）
        
        # コンテキストの品質による調整（変更なし）
        contexts = mapping.get('context', [])
        if contexts:
            best_confidence = max(ctx.get('confidence', 0) for ctx in contexts)
            priority += int(best_confidence * 50)
        
        # 要素スコアによる優先度調整（新規追加）
        element_info = mapping.get('element_info', {})
        element_score = element_info.get('score', 0)
        if element_score > 0:
            priority += min(int(element_score / 10), 50)  # スコアを10で割って最大50ポイント
        
        return priority
    
    def optimize_field_combinations(self, integrated_mappings: List[IntegratedFieldMapping]) -> Dict[str, Any]:
        """
        統合されたフィールドマッピングを最適化
        
        Args:
            integrated_mappings: 統合フィールドマッピング
            
        Returns:
            Dict[str, Any]: 最適化された組み合わせ結果
        """
        optimization_results = {
            'optimized_mappings': [],
            'table_field_groups': {},
            'combination_recommendations': [],
            'priority_adjustments': []
        }
        
        # テーブルタイプ別のグルーピング
        table_groups = {}
        non_table_fields = []
        
        for mapping in integrated_mappings:
            if mapping.is_in_table and mapping.table_context:
                table_type = mapping.table_context.table_type
                if table_type not in table_groups:
                    table_groups[table_type] = []
                table_groups[table_type].append(mapping)
            else:
                non_table_fields.append(mapping)
        
        # テーブル内フィールドの最適化
        for table_type, mappings in table_groups.items():
            optimized_group = self._optimize_table_field_group(table_type, mappings)
            optimization_results['table_field_groups'][table_type] = optimized_group
        
        # 全体最適化
        optimization_results['optimized_mappings'] = self._merge_optimized_groups(
            table_groups, non_table_fields
        )
        
        # 組み合わせ推奨の生成
        optimization_results['combination_recommendations'] = self._generate_combination_recommendations(
            optimization_results['optimized_mappings']
        )
        
        logger.info(f"Optimized field combinations: {len(optimization_results['optimized_mappings'])} total mappings")
        
        return optimization_results
    
    def _optimize_table_field_group(self, table_type: str, 
                                   mappings: List[IntegratedFieldMapping]) -> Dict[str, Any]:
        """
        テーブル内フィールドグループの最適化
        
        Args:
            table_type: テーブルタイプ
            mappings: フィールドマッピングリスト
            
        Returns:
            Dict[str, Any]: 最適化結果
        """
        result = {
            'table_type': table_type,
            'field_count': len(mappings),
            'optimizations': []
        }
        
        if table_type == 'form_table':
            # フォームテーブル特有の最適化
            # 行ベースのグルーピング
            row_groups = {}
            for mapping in mappings:
                if mapping.table_context:
                    row_idx = mapping.table_context.row_index
                    if row_idx not in row_groups:
                        row_groups[row_idx] = []
                    row_groups[row_idx].append(mapping)
            
            # 同一行内の関連フィールド検出
            for row_idx, row_mappings in row_groups.items():
                if len(row_mappings) > 1:
                    combination_potential = self._analyze_row_combination_potential(row_mappings)
                    result['optimizations'].append({
                        'type': 'row_based_combination',
                        'row_index': row_idx,
                        'potential': combination_potential
                    })
        
        return result
    
    def _analyze_row_combination_potential(self, row_mappings: List[IntegratedFieldMapping]) -> Dict[str, Any]:
        """
        行内組み合わせ可能性を分析
        
        Args:
            row_mappings: 行内のフィールドマッピング
            
        Returns:
            Dict[str, Any]: 組み合わせ可能性分析結果
        """
        field_names = [mapping.field_name.lower() for mapping in row_mappings]
        
        potential = {
            'confidence': 0.0,
            'suggested_combination': None,
            'field_names': field_names
        }
        
        # 姓名の組み合わせチェック
        if any('姓' in name or 'last' in name for name in field_names) and \
           any('名' in name or 'first' in name for name in field_names):
            potential['confidence'] = 0.9
            potential['suggested_combination'] = 'full_name'
        
        # 電話番号の組み合わせチェック
        elif len([name for name in field_names if '電話' in name or 'tel' in name or 'phone' in name]) >= 2:
            potential['confidence'] = 0.8
            potential['suggested_combination'] = 'phone'
        
        # 住所の組み合わせチェック
        elif len([name for name in field_names if '住所' in name or 'address' in name]) >= 2:
            potential['confidence'] = 0.7
            potential['suggested_combination'] = 'address'
        
        return potential
    
    def _merge_optimized_groups(self, table_groups: Dict[str, List[IntegratedFieldMapping]], 
                               non_table_fields: List[IntegratedFieldMapping]) -> List[IntegratedFieldMapping]:
        """
        最適化されたグループをマージ
        
        Args:
            table_groups: テーブルグループ
            non_table_fields: テーブル外フィールド
            
        Returns:
            List[IntegratedFieldMapping]: マージされたフィールドマッピング
        """
        merged = []
        
        # テーブルフィールドを優先度順に追加
        for table_type in ['form_table', 'data_table', 'layout_table']:
            if table_type in table_groups:
                merged.extend(table_groups[table_type])
        
        # テーブル外フィールドを追加
        merged.extend(non_table_fields)
        
        # 最終的な優先度でソート
        merged.sort(key=lambda x: x.combination_priority, reverse=True)
        
        return merged
    
    def _generate_combination_recommendations(self, optimized_mappings: List[IntegratedFieldMapping]) -> List[Dict[str, Any]]:
        """
        組み合わせ推奨を生成
        
        Args:
            optimized_mappings: 最適化されたマッピング
            
        Returns:
            List[Dict[str, Any]]: 推奨リスト
        """
        recommendations = []
        
        field_names = [mapping.field_name.lower() for mapping in optimized_mappings]
        
        # 姓名組み合わせ推奨
        if 'last_name' in field_names and 'first_name' in field_names:
            recommendations.append({
                'type': 'name_combination',
                'combination_rule': 'full_name',
                'confidence': 0.95,
                'description': '姓名フィールドが検出されました。full_name組み合わせを推奨します。'
            })
        
        # 電話番号組み合わせ推奨
        phone_fields = [name for name in field_names if 'phone' in name or '電話' in name]
        if len(phone_fields) >= 2:
            recommendations.append({
                'type': 'phone_combination',
                'combination_rule': 'phone',
                'confidence': 0.85,
                'description': f'{len(phone_fields)}つの電話番号フィールドが検出されました。phone組み合わせを推奨します。'
            })
        
        return recommendations
