"""
分割パターン判定システム

住所・電話番号等の分割フィールドパターン自動判定機能
フィールド順序検証と連続性チェックシステム
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class SplitPattern(Enum):
    """分割パターンの種類"""
    SINGLE = "single"                    # 単一フィールド
    ADDRESS_2_SPLIT = "address_2_split"  # 住所2分割: [1,2,3,4][5]
    ADDRESS_3_SPLIT = "address_3_split"  # 住所3分割: [1][2,3,4][5]
    ADDRESS_4_SPLIT = "address_4_split"  # 住所4分割: [1][2][3,4][5]
    PHONE_3_SPLIT = "phone_3_split"      # 電話3分割: [1][2][3]
    PHONE_2_SPLIT = "phone_2_split"      # 電話2分割: [1,2][3]
    NAME_2_SPLIT = "name_2_split"        # 姓名2分割: [姓][名]
    # 既存の NAME_2_SPLIT を、ひらがな/カナの2分割にも共用する
    EMAIL_2_SPLIT = "email_2_split"      # メール2分割: [user][domain]
    POSTAL_2_SPLIT = "postal_2_split"    # 郵便番号2分割: [前3桁][後4桁]


@dataclass
class SplitFieldGroup:
    """分割フィールドグループ"""
    pattern: SplitPattern
    field_type: str              # フィールドタイプ（address, phone等）
    fields: List[Dict[str, Any]] # フィールド情報のリスト
    confidence: float            # パターン信頼度
    sequence_valid: bool         # 順序妥当性
    description: str             # パターン説明
    input_strategy: str          # 入力戦略（'combine' or 'split'）
    strategy_confidence: float   # 入力戦略の信頼度
    strategy_reason: str         # 判定理由


class SplitFieldDetector:
    """分割フィールド判定メインクラス"""
    
    def __init__(self):
        """初期化"""
        # パターン検出設定
        self.detection_rules = {
            'address': {
                'keywords': ['住所', 'address', '所在地', '番地', '都道府県', '市区町村', '町名', '建物'],
                'split_patterns': {
                    2: SplitPattern.ADDRESS_2_SPLIT,
                    3: SplitPattern.ADDRESS_3_SPLIT,
                    4: SplitPattern.ADDRESS_4_SPLIT
                },
                'max_fields': 5
            },
            'phone': {
                # 『番号』は郵便番号/会員番号等と衝突しやすいため除外
                'keywords': ['電話', 'tel', 'phone', '市外局番', '局番'],
                'split_patterns': {
                    2: SplitPattern.PHONE_2_SPLIT,
                    3: SplitPattern.PHONE_3_SPLIT
                },
                'max_fields': 3
            },
            'name': {
                # 注意: 誤検出防止のため『名』の単独キーワードは使用しない
                # 会社名（「会社名」「企業名」等）に含まれる『名』にヒットしてしまうのを避ける
                'keywords': ['姓', '苗字', 'last', 'first', 'family', 'given', 'lastname', 'firstname', 'family_name', 'given_name'],
                'split_patterns': {  # 厳格化: 2分割のみ対象
                    2: SplitPattern.NAME_2_SPLIT
                },
                'max_fields': 2
            },
            'name_hiragana': {
                # ひらがな/ふりがな（ひらがな指定）
                'keywords': ['姓ひらがな', '名ひらがな', 'ひらがな', 'ふりがな', 'hiragana'],
                'split_patterns': {
                    2: SplitPattern.NAME_2_SPLIT
                },
                'max_fields': 2
            },
            'name_kana': {
                # カタカナ/フリガナ（カタカナ指定）
                'keywords': ['姓カナ', '名カナ', 'カナ', 'カタカナ', 'フリガナ', 'katakana', 'kana'],
                'split_patterns': {
                    2: SplitPattern.NAME_2_SPLIT
                },
                'max_fields': 2
            },
            'email': {
                'keywords': ['email', 'mail', 'メール', '@'],
                'split_patterns': {
                    2: SplitPattern.EMAIL_2_SPLIT
                },
                'max_fields': 2
            },
            'postal_code': {
                'keywords': ['郵便番号', 'postal', 'zip', '〒'],
                'split_patterns': {
                    2: SplitPattern.POSTAL_2_SPLIT
                },
                'max_fields': 2
            }
        }
        
        # 分割フィールドの組み合わせルール
        self.combination_rules = {
            SplitPattern.ADDRESS_2_SPLIT: {
                'groups': [[1, 2, 3, 4], [5]],
                'separator': '　',  # address_4とaddress_5間のみ全角スペース
                'fields': ['address_1', 'address_2', 'address_3', 'address_4', 'address_5']
            },
            SplitPattern.ADDRESS_3_SPLIT: {
                'groups': [[1], [2, 3, 4], [5]],
                'separator': '　',
                'fields': ['address_1', 'address_2', 'address_3', 'address_4', 'address_5']
            },
            SplitPattern.ADDRESS_4_SPLIT: {
                'groups': [[1], [2], [3, 4], [5]],
                'separator': '　',
                'fields': ['address_1', 'address_2', 'address_3', 'address_4', 'address_5']
            },
            SplitPattern.PHONE_3_SPLIT: {
                'groups': [[1], [2], [3]],
                'separator': '',  # 直接連結
                'fields': ['phone_1', 'phone_2', 'phone_3']
            },
            SplitPattern.PHONE_2_SPLIT: {
                'groups': [[1, 2], [3]],
                'separator': '',
                'fields': ['phone_1', 'phone_2', 'phone_3']
            },
            SplitPattern.NAME_2_SPLIT: {
                'groups': [[1], [2]],
                'separator': '　',  # 全角スペース
                'fields': ['last_name', 'first_name']
            },
            SplitPattern.EMAIL_2_SPLIT: {
                'groups': [[1], [2]],
                'separator': '@',
                'fields': ['email_1', 'email_2']
            },
            SplitPattern.POSTAL_2_SPLIT: {
                'groups': [[1], [2]],
                'separator': '',
                'fields': ['postal_code_1', 'postal_code_2']
            }
        }
        
        # 設計者意図判定パターン（Critical修正1: 分割vs組み合わせ判定）
        self.designer_intent_patterns = {
            # 分割入力を示すパターン（「それぞれ入力してください」等）
            'split_indicators': [
                'それぞれ', 'それぞれ入力', 'それぞれご記入', 'それぞれご入力',
                'それぞれの項目', '各項目', '各フィールド', '各入力欄',
                'individually', 'separately', 'each field',
                '分けて入力', '分けてご記入', '個別に', '別々に',
                '3つに分けて', '2つに分けて', '4つに分けて'
            ],
            
            # 組み合わせ入力を示すパターン（「一度に入力してください」等）
            'combine_indicators': [
                '一度に', '一括で', 'まとめて', '続けて入力', '連続して',
                'combined', 'together', 'as one',
                'ハイフンなし', 'ハイフンを除く', 'スペースなし',
                '連結して', '結合して'
            ],
            
            # 統合フィールドの存在を示すパターン
            'unified_field_patterns': [
                'フルネーム', 'full name', '氏名', 'お名前',
                '完全な住所', 'full address', '住所全体',
                '電話番号（ハイフンなし）', 'phone number', '電話番号全体'
            ]
        }
        
        logger.info("SplitFieldDetector initialized")
    
    def detect_split_patterns(self, field_mappings: List[Dict[str, Any]], input_order: Optional[List[str]] = None) -> List[SplitFieldGroup]:
        """
        フィールドマッピングから分割パターンを検出（厳格化バージョン）
        
        Args:
            field_mappings: フィールドマッピング情報のリスト
                            例: [{'field_name': '住所', 'element_info': {...}, 'context': [...]}]
        
        Returns:
            List[SplitFieldGroup]: 検出された分割フィールドグループ（厳格な検証通過のみ）
        """
        # 入力欄のみの論理順（フォーム内順）を保持
        self._input_order = input_order or []
        split_groups = []
        
        # フィールドタイプ別にグループ化
        field_groups = self._group_fields_by_type(field_mappings)
        
        for field_type, fields in field_groups.items():
            # 適度な最小フィールド数チェック
            if len(fields) < 2:
                logger.debug(f"Skip {field_type}: insufficient fields ({len(fields)})")
                continue
            
            # 最大フィールド数チェック（緩和版）
            max_allowed = self.detection_rules[field_type]['max_fields']
            if len(fields) > max_allowed + 2:  # 余裕を持たせる（+2フィールド許可）
                logger.info(f"Skip {field_type}: too many fields ({len(fields)} > {max_allowed + 2})")
                continue
            
            # 分割パターンを検出
            detected_pattern = self._detect_pattern_for_group(field_type, fields)
            
            # バランス調整された信頼度チェック（0.45以上で許可）
            if detected_pattern and detected_pattern.confidence >= 0.45:
                split_groups.append(detected_pattern)
                logger.info(f"Detected BALANCED split pattern: {detected_pattern.pattern} for {field_type} (confidence: {detected_pattern.confidence:.2f})")
            elif detected_pattern:
                logger.info(f"Rejected split pattern: {detected_pattern.pattern} for {field_type} (confidence: {detected_pattern.confidence:.2f} < 0.45)")
            else:
                logger.debug(f"No split pattern found for {field_type}")
        
        logger.info(f"Total balanced split patterns detected: {len(split_groups)}")
        return split_groups
    
    def _group_fields_by_type(self, field_mappings: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """フィールドをタイプ別にグループ化"""
        groups = {}
        
        for field_mapping in field_mappings:
            field_name = field_mapping.get('field_name', '')
            field_type = self._identify_field_type(field_name, field_mapping)
            
            if field_type:
                if field_type not in groups:
                    groups[field_type] = []
                groups[field_type].append(field_mapping)
        
        return groups
    
    def _identify_field_type(self, field_name: str, field_mapping: Dict[str, Any]) -> Optional[str]:
        """フィールドタイプを識別"""
        # field_name_lower は未使用のため削除（ロジックには影響なし）

        # 1) まずは明確に分かっている正規化済みのフィールド名で判定（最優先）
        #    - 姓/名（日本語）または last_name/first_name（英語）だけを『name』タイプとして扱う
        canonical_name_keys = {'姓', '名', 'last_name', 'first_name'}
        if field_name in canonical_name_keys:
            return 'name'

        # ひらがな/カナの正規名に対応
        if field_name in {'姓ひらがな', '名ひらがな'}:
            return 'name_hiragana'
        if field_name in {'姓カナ', '名カナ'}:
            return 'name_kana'

        # 2) 誤検出ガード: 会社名/組織名/施設名など『〇〇名』は姓名ではない
        company_tokens = [
            '社名', '会社名', '企業名', '法人名', '団体名', '組織名',
            '部署名', '学校名', '店舗名', '病院名', '施設名',
            'company name', 'organization'
        ]
        if any(tok in field_name for tok in company_tokens):
            return None

        # コンテキストテキストも考慮（ただし会社名系語が含まれていれば name としない）
        context_texts = []
        if 'context' in field_mapping:
            context_texts = [ctx.text for ctx in field_mapping['context'] if hasattr(ctx, 'text')]
        combined_text = (field_name + ' ' + ' '.join(context_texts)).lower()
        if any(tok.lower() in combined_text for tok in company_tokens):
            return None

        # 3) 汎用キーワードでの判定（英語圏UIなど）
        for field_type, rules in self.detection_rules.items():
            # name タイプは厳格化（『名』単独は含めない。姓/last/family/given/firstname/lastname のみ）
            if field_type == 'name':
                for keyword in rules['keywords']:
                    if keyword.lower() in combined_text:
                        return 'name'
            elif field_type in {'name_hiragana', 'name_kana'}:
                # ふりがな/カタカナ/ひらがな手がかりを含む場合に限定
                for keyword in rules['keywords']:
                    if keyword.lower() in combined_text:
                        return field_type
            else:
                for keyword in rules['keywords']:
                    if keyword.lower() in combined_text:
                        return field_type

        return None
    
    def _detect_pattern_for_group(self, field_type: str, fields: List[Dict[str, Any]]) -> Optional[SplitFieldGroup]:
        """グループの分割パターンを検出"""
        if field_type not in self.detection_rules:
            return None
        
        rules = self.detection_rules[field_type]
        field_count = len(fields)
        
        # 分割パターンの特定
        pattern = None
        if field_count in rules['split_patterns']:
            pattern = rules['split_patterns'][field_count]
        elif field_count <= rules['max_fields']:
            # デフォルトパターン（最も一般的な分割）
            if field_type == 'address' and field_count <= 5:
                if field_count == 2:
                    pattern = SplitPattern.ADDRESS_2_SPLIT
                elif field_count == 3:
                    pattern = SplitPattern.ADDRESS_3_SPLIT
                elif field_count >= 4:
                    pattern = SplitPattern.ADDRESS_4_SPLIT
        
        if not pattern:
            return None
        
        # フィールドの順序検証
        sequence_valid = self._validate_field_sequence(field_type, fields)

        # 厳格条件: 姓名分割は『連続インデックス + ラベル検出』が満たされない場合は不採用
        if field_type == 'name' and not sequence_valid:
            logger.info("Rejecting name split pattern due to invalid sequence/labels")
            return None
        
        # 信頼度計算
        confidence = self._calculate_pattern_confidence(field_type, pattern, fields)
        
        # パターン説明生成
        description = self._generate_pattern_description(pattern, field_count)
        
        # Critical修正1: 入力戦略判定
        input_strategy, strategy_confidence, strategy_reason = self.determine_input_strategy(
            field_type, fields, pattern
        )
        
        return SplitFieldGroup(
            pattern=pattern,
            field_type=field_type,
            fields=fields,
            confidence=confidence,
            sequence_valid=sequence_valid,
            description=description,
            input_strategy=input_strategy,
            strategy_confidence=strategy_confidence,
            strategy_reason=strategy_reason
        )
    
    def _validate_field_sequence(self, field_type: str, fields: List[Dict[str, Any]]) -> bool:
        """
        フィールド順序の妥当性を検証。

        仕様変更（連続配置の定義の明確化）:
        - 「連続」は、入力欄（input/textarea/select）のみを抽出して並べたときに
          インデックスが連続していることを指す（物理的な距離や DOM 上の親子関係ではない）。
        - 住所/郵便番号/電話番号/姓名/ふりがな等の分割入力に一貫して適用する。
        """
        if len(fields) < 2:
            return True
        
        try:
            # 0. 入力欄だけの論理順による連続性チェック（最優先）
            if getattr(self, '_input_order', None):
                if not self._validate_input_order_contiguity(fields):
                    logger.info(f"Input-order contiguity failed for {field_type}")
                    return False
                # 論理順の連続が確認できた場合は合格とする（物理/DOM情報は参考に留める）
                return True

            # 1. 位置情報による順序チェック（参考用途、上記が無い場合のフォールバック）
            positions = []
            elements_with_position = []
            
            for field in fields:
                element_info = field.get('element_info', {})
                bounding_box = element_info.get('bounding_box')
                
                if bounding_box:
                    # 左上から右下への順序を基準とする
                    score = bounding_box['y'] * 1000 + bounding_box['x']
                    positions.append((score, field, bounding_box))
                    elements_with_position.append(field)
            
            if len(positions) != len(fields):
                logger.warning(f"位置情報不完全: {len(positions)}/{len(fields)} フィールド")
                # 入力順の連続性が未評価（_input_order無し）の場合のみ不合格にする
                # ここに到達するのは _input_order が無いケース
                return False
            
            # 位置順にソート
            positions.sort(key=lambda x: x[0])
            sorted_fields = [pos[1] for pos in positions]
            # sorted_boxes は現行ロジックでは未使用（隣接検証は参考のため無効化済み）
            
            # 2.（任意）物理／DOM 隣接は参考評価に留める（本要件では必須条件ではない）
            # adjacency_valid = self._validate_field_adjacency(sorted_fields, sorted_boxes, field_type)
            # dom_adjacency_valid = self._validate_dom_adjacency(sorted_fields, field_type)

            # 3. フィールドタイプ別の順序検証（従来の処理）
            type_sequence_valid = self._validate_type_specific_sequence(field_type, sorted_fields)
            if not type_sequence_valid:
                logger.info(f"Type-specific sequence validation failed for {field_type}")
                return False
            
            logger.info(f"All sequence validations passed for {field_type}")
            return True
            
        except Exception as e:
            logger.error(f"Error in field sequence validation: {e}")
            return False

    def _validate_input_order_contiguity(self, fields: List[Dict[str, Any]]) -> bool:
        """入力欄のみを取り出した論理順でインデックスが連続していることを検証"""
        try:
            if not self._input_order:
                return True

            # フィールドの selector を取得
            selectors = []
            for f in fields:
                # element_info 配下が無いケースもあるためトップレベルを優先
                sel = f.get('selector') or f.get('element_info', {}).get('selector', '')
                if not sel:
                    return False
                selectors.append(sel)

            # 入力欄順（_input_order）におけるインデックス列を作成
            indices = []
            for sel in selectors:
                try:
                    idx = self._input_order.index(sel)
                except ValueError:
                    return False
                indices.append(idx)

            indices.sort()
            # 連続性: 差分がすべて1
            return all((indices[i+1] - indices[i] == 1) for i in range(len(indices)-1))
        except Exception as e:
            logger.debug(f"Input-order contiguity check failed: {e}")
            return False
    
    def _validate_field_adjacency(self, sorted_fields: List[Dict[str, Any]], 
                                sorted_boxes: List[Dict[str, float]], 
                                field_type: str) -> bool:
        """
        Critical修正2: フィールドの物理的隣接性を検証
        分割フィールドが実際に連続して配置されているかをチェック
        """
        if len(sorted_fields) < 2:
            return True
        
        try:
            max_allowed_gap = self._get_max_allowed_gap(field_type)
            
            for i in range(len(sorted_boxes) - 1):
                current_box = sorted_boxes[i]
                next_box = sorted_boxes[i + 1]
                
                # 隣接フィールド間の距離を計算
                gap = self._calculate_field_gap(current_box, next_box)
                
                if gap > max_allowed_gap:
                    field_name1 = sorted_fields[i].get('field_name', 'unknown')
                    field_name2 = sorted_fields[i + 1].get('field_name', 'unknown')
                    logger.debug(f"Fields '{field_name1}' and '{field_name2}' are too far apart: {gap:.1f}px > {max_allowed_gap}px")
                    return False
                
                logger.debug(f"Adjacent fields gap: {gap:.1f}px (threshold: {max_allowed_gap}px)")
            
            return True
            
        except Exception as e:
            logger.debug(f"Error validating field adjacency: {e}")
            return False
    
    def _calculate_field_gap(self, box1: Dict[str, float], box2: Dict[str, float]) -> float:
        """2つのフィールド間のギャップを計算"""
        try:
            # 水平方向のギャップを優先的に計算
            if abs(box1['y'] - box2['y']) < 30:  # ほぼ同じ高さの場合
                # 水平配置：右端から左端までの距離
                if box1['x'] < box2['x']:
                    return box2['x'] - (box1['x'] + box1['width'])
                else:
                    return box1['x'] - (box2['x'] + box2['width'])
            else:
                # 垂直配置：下端から上端までの距離
                if box1['y'] < box2['y']:
                    return box2['y'] - (box1['y'] + box1['height'])
                else:
                    return box1['y'] - (box2['y'] + box2['height'])
                    
        except Exception as e:
            logger.debug(f"Error calculating field gap: {e}")
            return float('inf')  # エラー時は大きな値を返して隣接していないと判定
    
    def _get_max_allowed_gap(self, field_type: str) -> float:
        """フィールドタイプ別の最大許容ギャップを取得"""
        # フィールドタイプ別の許容ギャップ（px）
        gap_thresholds = {
            'phone': 50,        # 電話番号：比較的密接
            'postal_code': 30,  # 郵便番号：非常に密接
            'name': 80,         # 姓名：やや離れていても可
            'address': 100,     # 住所：住所項目は離れることがある
            'email': 20         # メール：@ で分割なので密接
        }
        
        return gap_thresholds.get(field_type, 60)  # デフォルト60px
    
    def _validate_dom_adjacency(self, sorted_fields: List[Dict[str, Any]], field_type: str) -> bool:
        """
        Critical修正2: DOM構造での隣接性を検証
        実際のHTML要素が隣接しているかをチェック
        """
        if len(sorted_fields) < 2:
            return True
        
        try:
            # DOM構造の隣接性は複雑なのでシンプルな検証を実装
            # セレクタ情報から隣接性を推測
            
            for i in range(len(sorted_fields) - 1):
                current_field = sorted_fields[i]
                next_field = sorted_fields[i + 1]
                
                # 要素情報を取得
                current_selector = current_field.get('element_info', {}).get('selector', '')
                next_selector = next_field.get('element_info', {}).get('selector', '')
                
                # セレクタが類似している場合は隣接していると判定
                if self._are_selectors_adjacent(current_selector, next_selector):
                    continue
                    
                # 同一親要素内での隣接性をチェック
                if self._are_in_same_parent_context(current_field, next_field):
                    continue
                
                # どちらの条件も満たさない場合は隣接していない
                field_name1 = current_field.get('field_name', 'unknown')
                field_name2 = next_field.get('field_name', 'unknown')
                logger.debug(f"Fields '{field_name1}' and '{field_name2}' are not DOM-adjacent")
                return False
            
            return True
            
        except Exception as e:
            logger.debug(f"Error validating DOM adjacency: {e}")
            return True  # エラー時は隣接していると仮定（厳しすぎないように）
    
    def _are_selectors_adjacent(self, selector1: str, selector2: str) -> bool:
        """セレクタの隣接性をチェック"""
        try:
            # セレクタが類似している（番号が連続している等）かチェック
            import re
            
            # 末尾の数字を抽出
            pattern = r'(\d+)(?!.*\d)'
            match1 = re.search(pattern, selector1)
            match2 = re.search(pattern, selector2)
            
            if match1 and match2:
                num1 = int(match1.group(1))
                num2 = int(match2.group(1))
                
                # 連続する番号の場合は隣接していると判定
                if abs(num2 - num1) == 1:
                    return True
            
            # セレクタのベース部分が同じかチェック
            base1 = re.sub(r'\d+', '', selector1)
            base2 = re.sub(r'\d+', '', selector2)
            
            return base1 == base2 and base1 != ''
            
        except Exception:
            return False
    
    def _are_in_same_parent_context(self, field1: Dict[str, Any], field2: Dict[str, Any]) -> bool:
        """同一親要素コンテキストにあるかチェック"""
        try:
            # コンテキスト情報から同一親要素かどうかを推測
            contexts1 = field1.get('context', [])
            contexts2 = field2.get('context', [])
            
            # 同じコンテキストソースを持つかチェック
            sources1 = set()
            sources2 = set()
            
            for ctx in contexts1:
                if hasattr(ctx, 'source_type'):
                    sources1.add(ctx.source_type)
            
            for ctx in contexts2:
                if hasattr(ctx, 'source_type'):
                    sources2.add(ctx.source_type)
            
            # dt_labelやth_labelなど親要素由来のコンテキストが共通している場合
            common_parent_sources = sources1.intersection(sources2)
            parent_context_types = {'dt_label', 'th_label', 'parent'}
            
            if common_parent_sources.intersection(parent_context_types):
                return True
            
            return False
            
        except Exception:
            return False
    
    def _validate_type_specific_sequence(self, field_type: str, sorted_fields: List[Dict[str, Any]]) -> bool:
        """フィールドタイプ固有の順序検証"""
        if field_type in {'name', 'name_hiragana', 'name_kana'}:
            # 姓名の順序：姓 → 名（厳格化）
            # 連続性チェックは別途 _validate_input_order_contiguity() で実施済み
            # ここではラベル/フィールド名の語彙検証を双方に適用する
            if len(sorted_fields) < 2:
                return False

            # field_name + コンテキストを連結してテキスト化
            def to_text(field: Dict[str, Any]) -> str:
                name_txt = str(field.get('field_name', '')).lower()
                ctx_txts = []
                if field.get('context'):
                    ctx_txts = [getattr(ctx, 'text', '').lower() for ctx in field['context'] if hasattr(ctx, 'text')]
                return (name_txt + ' ' + ' '.join(ctx_txts)).strip()

            first_text = to_text(sorted_fields[0])
            second_text = to_text(sorted_fields[1])

            # 会社名系の語が混じる場合は姓名分割とはみなさない
            company_tokens = ['会社名', '企業名', '法人名', '団体名', '組織名', 'company name', 'organization']
            if any(tok.lower() in first_text for tok in [t.lower() for t in company_tokens]):
                return False
            if any(tok.lower() in second_text for tok in [t.lower() for t in company_tokens]):
                return False

            surname_keywords = ['姓', '苗字', 'せい', 'last', 'lastname', 'family', 'family_name', 'surname']
            given_keywords   = ['名', 'めい', 'first', 'firstname', 'given', 'given_name', 'forename']

            # ひらがな/カナの追加手がかり（セイ/メイ、かな種別）
            if field_type in {'name_hiragana', 'name_kana'}:
                surname_keywords += ['セイ', 'sei']
                given_keywords += ['メイ', 'mei']
                if field_type == 'name_hiragana':
                    surname_keywords += ['ひらがな', 'ふりがな', 'hiragana']
                    given_keywords += ['ひらがな', 'ふりがな', 'hiragana']
                if field_type == 'name_kana':
                    surname_keywords += ['カナ', 'カタカナ', 'フリガナ', 'katakana']
                    given_keywords += ['カナ', 'カタカナ', 'フリガナ', 'katakana']

            # 最初が姓系、次が名系のいずれかに一致していること
            first_ok = any(k in first_text for k in surname_keywords)
            second_ok = any(k in second_text for k in given_keywords)
            return first_ok and second_ok
        
        elif field_type == 'address':
            # 住所の順序：都道府県 → 市区町村 → 番地 → 建物
            # 簡略版：最初のフィールドが都道府県関連かチェック
            if sorted_fields:
                first_field = sorted_fields[0]
                context_text = ''
                if first_field.get('context'):
                    context_text = ' '.join(ctx.text for ctx in first_field['context'] if hasattr(ctx, 'text')).lower()
                
                prefecture_keywords = ['都道府県', 'prefecture', '県', '都', '府']
                return any(keyword in context_text for keyword in prefecture_keywords)
        
        elif field_type == 'phone':
            # 電話番号の順序：市外局番 → 市内局番 → 加入者番号
            # 簡略版：最初のフィールドが市外局番関連かチェック
            if sorted_fields:
                first_field = sorted_fields[0]
                context_text = ''
                if first_field.get('context'):
                    context_text = ' '.join(ctx.text for ctx in first_field['context'] if hasattr(ctx, 'text')).lower()
                
                area_code_keywords = ['市外局番', 'area', '局番1', '03', '06']
                return any(keyword in context_text for keyword in area_code_keywords)
        
        # その他のタイプは基本的にOK
        return True
    
    def _calculate_pattern_confidence(self, field_type: str, pattern: SplitPattern, 
                                    fields: List[Dict[str, Any]]) -> float:
        """パターンの信頼度を計算（バランス調整版）"""
        base_confidence = 0.35  # 基本信頼度を適度に設定（0.2→0.35）
        
        # フィールド数による信頼度計算（緩和版）
        field_count = len(fields)
        
        # 適度な厳密さでフィールド数チェック
        if field_type == 'address':
            if field_count == 2:
                base_confidence += 0.25  # 2分割は最も確実（0.15→0.25）
            elif field_count == 3:
                base_confidence += 0.20  # 3分割は中程度（0.12→0.20）
            elif field_count == 4:
                base_confidence += 0.15  # 4分割は複雑（0.10→0.15）
            elif field_count == 5:
                base_confidence += 0.10  # 5分割も許可（新規追加）
            else:
                base_confidence -= 0.1   # 想定外は軽減点（-0.3→-0.1）
        elif field_type == 'phone':
            if field_count == 3:
                base_confidence += 0.30  # 電話3分割は標準的（0.25→0.30）
            elif field_count == 2:
                base_confidence += 0.20  # 2分割（0.15→0.20）
            else:
                base_confidence -= 0.1   # 想定外は軽減点（-0.3→-0.1）
        elif field_type == 'name':
            if field_count == 2:
                base_confidence += 0.35  # 姓名2分割は標準（0.3→0.35）
            elif field_count == 3:  # 姓名3分割を許可（新規追加）
                base_confidence += 0.20
            elif field_count == 4:  # 姓名4分割を許可（新規追加）
                base_confidence += 0.10
            else:
                base_confidence -= 0.15  # 減点緩和（-0.4→-0.15）
        
        # コンテキストテキストの品質評価（緩和版）
        context_quality = self._assess_context_quality_balanced(fields, field_type)
        base_confidence += context_quality * 0.25  # 重み調整（0.3→0.25）
        
        # 順序妥当性の検証（緩和版）
        sequence_valid = self._validate_field_sequence(field_type, fields)
        if sequence_valid:
            base_confidence += 0.15  # ボーナス適度（0.2→0.15）
        else:
            base_confidence -= 0.25  # 減点緩和（-0.5→-0.25）
        
        # キーワード一致度の評価（緩和版）
        keyword_score = self._calculate_keyword_balanced(fields, field_type)
        base_confidence += keyword_score * 0.15  # 重み調整（0.2→0.15）
        
        # 最終的な信頼度判定（閾値緩和）
        final_confidence = max(0.0, min(1.0, base_confidence))
        
        # バランス調整された閾値：0.45以上で分割と認める
        return final_confidence if final_confidence >= 0.45 else 0.0
    
    # リファクタリング: 未使用の品質評価関数を削除（_assess_context_quality / _assess_context_quality_strict）
    
    def _get_required_patterns_for_type(self, field_type: str) -> List[str]:
        """フィールドタイプ別の必須パターンを取得"""
        patterns = {
            'address': ['住所', '都道府県', '市区町村', '番地', '建物', 'address'],
            'phone': ['電話', '電話番号', 'tel', 'phone', '市外局番', '局番'],
            'name': ['名前', '姓', '名', 'name', 'last', 'first', '苗字'],
            'name_hiragana': ['ひらがな', 'ふりがな', 'せい', 'めい', 'hiragana'],
            'name_kana': ['カナ', 'カタカナ', 'フリガナ', 'セイ', 'メイ', 'katakana', 'kana'],
            'email': ['メール', 'email', 'mail', 'e-mail'],
            'postal_code': ['郵便番号', 'postal', 'zip', '〒']
        }
        return patterns.get(field_type, [])
    
    def _calculate_keyword_strictness(self, fields: List[Dict[str, Any]], field_type: str) -> float:
        """キーワード一致度の厳格な評価"""
        if not fields:
            return 0.0
        
        required_keywords = self._get_required_patterns_for_type(field_type)
        total_score = 0.0
        
        for field in fields:
            field_name = field.get('field_name', '').lower()
            contexts = field.get('context', [])
            
            # フィールド名と全コンテキストテキストを結合
            all_text = field_name + ' ' + ' '.join(
                ctx.text.lower() for ctx in contexts if hasattr(ctx, 'text')
            )
            
            # 厳密なキーワードマッチング
            exact_matches = sum(1 for keyword in required_keywords if keyword.lower() in all_text)
            partial_matches = sum(1 for keyword in required_keywords 
                                if any(part in all_text for part in keyword.lower().split()))
            
            # 完全マッチを重視、部分マッチは軽視
            field_score = (exact_matches * 1.0 + partial_matches * 0.3) / len(required_keywords)
            total_score += min(field_score, 1.0)  # 最大1.0に制限
        
        return total_score / len(fields)
    
    def _assess_context_quality_balanced(self, fields: List[Dict[str, Any]], field_type: str) -> float:
        """コンテキストテキストのバランス調整版品質評価"""
        if not fields:
            return 0.0
        
        total_quality = 0.0
        required_patterns = self._get_required_patterns_for_type(field_type)
        
        for field in fields:
            contexts = field.get('context', [])
            field_quality = 0.0
            
            if not contexts:
                field_quality = 0.2  # コンテキストなしでも最低限の品質を与える（厳格版は0）
            
            # 各コンテキストを適度に評価
            for context in contexts:
                context_text = context.text.lower() if hasattr(context, 'text') else ''
                confidence = context.confidence if hasattr(context, 'confidence') else 0
                source_type = context.source_type if hasattr(context, 'source_type') else ''
                
                # ソースタイプによる重み付け（緩和版）
                source_weight = {
                    'dt_label': 1.0,
                    'label': 0.9,      # 0.8→0.9
                    'placeholder': 0.7, # 0.6→0.7  
                    'adjacent_text': 0.5 # 0.4→0.5
                }.get(source_type, 0.3)  # 0.2→0.3
                
                # 必須パターンとの適度なマッチング
                pattern_match = 0.0
                for pattern in required_patterns:
                    if pattern.lower() in context_text:
                        pattern_match = 1.0
                        break
                    # 部分マッチのスコア向上
                    elif any(p in context_text for p in pattern.lower().split()):
                        pattern_match = max(pattern_match, 0.5)  # 0.3→0.5
                
                # 一致パターンがなくても基本品質を与える
                if pattern_match == 0.0:
                    pattern_match = 0.3  # 新規追加
                
                context_score = confidence * source_weight * pattern_match
                field_quality = max(field_quality, context_score)
            
            total_quality += field_quality
        
        avg_quality = total_quality / len(fields)
        return avg_quality
    
    def _calculate_keyword_balanced(self, fields: List[Dict[str, Any]], field_type: str) -> float:
        """キーワード一致度のバランス調整版評価"""
        if not fields:
            return 0.0
        
        required_keywords = self._get_required_patterns_for_type(field_type)
        total_score = 0.0
        
        for field in fields:
            field_name = field.get('field_name', '').lower()
            contexts = field.get('context', [])
            
            # フィールド名と全コンテキストテキストを結合
            all_text = field_name + ' ' + ' '.join(
                ctx.text.lower() for ctx in contexts if hasattr(ctx, 'text')
            )
            
            # 適度なキーワードマッチング
            exact_matches = sum(1 for keyword in required_keywords if keyword.lower() in all_text)
            partial_matches = sum(1 for keyword in required_keywords 
                                if any(part in all_text for part in keyword.lower().split()))
            
            # マッチング評価の緩和
            field_score = (exact_matches * 1.0 + partial_matches * 0.5) / len(required_keywords)  # 0.3→0.5
            
            # マッチがなくても基本品質を与える
            if exact_matches == 0 and partial_matches == 0:
                field_score = 0.25  # 新規追加：最低限の品質保証
            
            total_score += min(field_score, 1.0)  # 最大1.0に制限
        
        return total_score / len(fields)
    
    def determine_input_strategy(self, field_type: str, fields: List[Dict[str, Any]], 
                                pattern: SplitPattern) -> Tuple[str, float, str]:
        """
        Critical修正1: 分割vs組み合わせ入力戦略の判定
        
        Args:
            field_type: フィールドタイプ（phone, address等）
            fields: フィールドリスト
            pattern: 検出された分割パターン
            
        Returns:
            Tuple[str, float, str]: (戦略, 信頼度, 理由)
                戦略: 'combine' (組み合わせ) or 'split' (分割入力)
                信頼度: 0.0-1.0
                理由: 判定理由の説明
        """
        try:
            # 基本戦略スコア
            combine_score = 0.0
            split_score = 0.0
            reasons = []
            
            # 1. コンテキストテキスト分析
            context_analysis = self._analyze_designer_intent_from_context(fields)
            if context_analysis['has_split_indicators']:
                split_score += 0.4
                reasons.append(f"Split indicators found: {context_analysis['split_indicators']}")
            if context_analysis['has_combine_indicators']:
                combine_score += 0.4
                reasons.append(f"Combine indicators found: {context_analysis['combine_indicators']}")
            
            # 2. 統合フィールド存在チェック
            unified_analysis = self._check_unified_field_existence(fields, field_type)
            if unified_analysis['has_unified_field']:
                combine_score += 0.5
                reasons.append(f"Unified field detected: {unified_analysis['unified_pattern']}")
            else:
                split_score += 0.3
                reasons.append("No unified field detected")
            
            # 3. 並列要素構造分析
            structure_analysis = self._analyze_parallel_structure(fields)
            if structure_analysis['is_clearly_separate']:
                split_score += 0.3
                reasons.append(f"Clearly separate fields: {structure_analysis['separation_reason']}")
            
            # 4. フィールド数による判定
            field_count = len(fields)
            if field_count >= 3:
                split_score += 0.2
                reasons.append(f"Many fields ({field_count}) suggest split input")
            elif field_count == 2:
                # 2分割は両方の可能性あり
                split_score += 0.1
                combine_score += 0.1
            
            # 5. フィールドタイプ別の傾向
            type_tendency = self._get_field_type_tendency(field_type, field_count)
            if type_tendency == 'split':
                split_score += 0.2
                reasons.append(f"Field type '{field_type}' tends toward split input")
            elif type_tendency == 'combine':
                combine_score += 0.2
                reasons.append(f"Field type '{field_type}' tends toward combine input")
            
            # 6. 最終判定
            if split_score > combine_score:
                strategy = 'split'
                confidence = min(split_score, 0.95)
                reason = f"Split input (score: {split_score:.2f} vs {combine_score:.2f}). " + " | ".join(reasons)
            else:
                strategy = 'combine' 
                confidence = min(combine_score, 0.95)
                reason = f"Combine input (score: {combine_score:.2f} vs {split_score:.2f}). " + " | ".join(reasons)
            
            logger.info(f"Input strategy determined: {strategy} (confidence: {confidence:.2f}) for {field_type}")
            logger.debug(f"Strategy reason: {reason}")
            
            return strategy, confidence, reason
            
        except Exception as e:
            logger.error(f"Error determining input strategy: {e}")
            # エラー時はデフォルト（組み合わせ）
            return 'combine', 0.5, f"Default strategy due to error: {e}"
    
    def _analyze_designer_intent_from_context(self, fields: List[Dict[str, Any]]) -> Dict[str, Any]:
        """コンテキストテキストから設計者意図を分析"""
        result = {
            'has_split_indicators': False,
            'has_combine_indicators': False,
            'split_indicators': [],
            'combine_indicators': []
        }
        
        try:
            # 全フィールドのコンテキストテキストを収集
            all_context_texts = []
            for field in fields:
                contexts = field.get('context', [])
                for context in contexts:
                    if hasattr(context, 'text') and context.text:
                        all_context_texts.append(context.text.lower())
            
            combined_text = ' '.join(all_context_texts)
            
            # 分割指標をチェック
            for indicator in self.designer_intent_patterns['split_indicators']:
                if indicator.lower() in combined_text:
                    result['has_split_indicators'] = True
                    result['split_indicators'].append(indicator)
            
            # 組み合わせ指標をチェック
            for indicator in self.designer_intent_patterns['combine_indicators']:
                if indicator.lower() in combined_text:
                    result['has_combine_indicators'] = True
                    result['combine_indicators'].append(indicator)
            
        except Exception as e:
            logger.debug(f"Error analyzing designer intent: {e}")
        
        return result
    
    def _check_unified_field_existence(self, fields: List[Dict[str, Any]], field_type: str) -> Dict[str, Any]:
        """統合フィールドの存在をチェック"""
        result = {
            'has_unified_field': False,
            'unified_pattern': None
        }
        
        try:
            # 各フィールドのコンテキストテキストで統合フィールドパターンをチェック
            for field in fields:
                contexts = field.get('context', [])
                for context in contexts:
                    if hasattr(context, 'text') and context.text:
                        context_text = context.text.lower()
                        
                        for pattern in self.designer_intent_patterns['unified_field_patterns']:
                            if pattern.lower() in context_text:
                                result['has_unified_field'] = True
                                result['unified_pattern'] = pattern
                                return result
            
        except Exception as e:
            logger.debug(f"Error checking unified field: {e}")
        
        return result
    
    def _analyze_parallel_structure(self, fields: List[Dict[str, Any]]) -> Dict[str, Any]:
        """並列要素構造を分析"""
        result = {
            'is_clearly_separate': False,
            'separation_reason': ''
        }
        
        try:
            if len(fields) < 2:
                return result
            
            # フィールド間の距離をチェック
            positions = []
            for field in fields:
                element_info = field.get('element_info', {})
                bounding_box = element_info.get('bounding_box')
                if bounding_box:
                    positions.append((bounding_box['x'], bounding_box['y']))
            
            if len(positions) >= 2:
                # 最小・最大距離を計算
                min_distance = float('inf')
                max_distance = 0
                
                for i in range(len(positions)):
                    for j in range(i + 1, len(positions)):
                        x1, y1 = positions[i]
                        x2, y2 = positions[j]
                        distance = ((x2 - x1) ** 2 + (y2 - y1) ** 2) ** 0.5
                        min_distance = min(min_distance, distance)
                        max_distance = max(max_distance, distance)
                
                # 距離が大きい場合は明確に分離されている
                if min_distance > 80:  # 80px以上離れている
                    result['is_clearly_separate'] = True
                    result['separation_reason'] = f"Fields are well separated (min: {min_distance:.0f}px)"
                elif len(fields) >= 3:
                    # 3つ以上のフィールドが存在する場合は分割入力の可能性が高い
                    result['is_clearly_separate'] = True
                    result['separation_reason'] = f"Multiple fields ({len(fields)}) suggest individual input"
            
        except Exception as e:
            logger.debug(f"Error analyzing parallel structure: {e}")
        
        return result
    
    def _get_field_type_tendency(self, field_type: str, field_count: int) -> str:
        """フィールドタイプ別の入力傾向を取得"""
        try:
            # フィールドタイプごとのデフォルト傾向
            type_tendencies = {
                'phone': 'split' if field_count >= 3 else 'combine',  # 3分割なら分割、2分割なら組み合わせ
                'address': 'split' if field_count >= 3 else 'combine',  # 住所も同様
                'name': 'combine' if field_count == 2 else 'split',     # 姓名2分割は組み合わせが一般的
                'email': 'split',    # メールは@で分割が一般的
                'postal_code': 'combine'  # 郵便番号は組み合わせが一般的
            }
            
            return type_tendencies.get(field_type, 'split')  # デフォルトは分割
            
        except Exception:
            return 'split'
    
    def _generate_pattern_description(self, pattern: SplitPattern, field_count: int) -> str:
        """パターンの説明を生成"""
        descriptions = {
            SplitPattern.ADDRESS_2_SPLIT: f"住所2分割パターン（{field_count}フィールド）: [都道府県〜番地][建物名]",
            SplitPattern.ADDRESS_3_SPLIT: f"住所3分割パターン（{field_count}フィールド）: [都道府県][市区町村〜番地][建物名]",
            SplitPattern.ADDRESS_4_SPLIT: f"住所4分割パターン（{field_count}フィールド）: [都道府県][市区町村][番地][建物名]",
            SplitPattern.PHONE_3_SPLIT: f"電話3分割パターン（{field_count}フィールド）: [市外局番][市内局番][加入者番号]",
            SplitPattern.PHONE_2_SPLIT: f"電話2分割パターン（{field_count}フィールド）: [市外・市内局番][加入者番号]",
            SplitPattern.NAME_2_SPLIT: f"姓名2分割パターン（{field_count}フィールド）: [姓][名]",
            SplitPattern.EMAIL_2_SPLIT: f"メール2分割パターン（{field_count}フィールド）: [ユーザー名][ドメイン]",
            SplitPattern.POSTAL_2_SPLIT: f"郵便番号2分割パターン（{field_count}フィールド）: [前3桁][後4桁]"
        }
        
        return descriptions.get(pattern, f"{pattern}パターン（{field_count}フィールド）")
    
    def generate_field_assignments(self, split_groups: List[SplitFieldGroup], 
                                 client_data: Dict[str, Any]) -> Dict[str, str]:
        """
        分割パターンに基づくフィールド割り当てを生成（Critical修正1: 入力戦略対応）
        
        Args:
            split_groups: 分割フィールドグループ
            client_data: クライアントデータ
            
        Returns:
            dict: フィールド名 -> 値のマッピング
        """
        assignments = {}
        
        for group in split_groups:
            if group.pattern not in self.combination_rules:
                continue
            
            # Critical修正1: 入力戦略に基づく処理分岐
            # 特例: phone/postal/address で検出フィールドが1つしかない場合は常に統合値を割り当て
            if group.field_type in {'phone', 'postal_code', 'address'} and len(group.fields) == 1:
                value = self._generate_single_field_value(group, client_data)
                assignments[group.fields[0].get('field_name','')] = value
                continue

            if group.input_strategy == 'split':
                # 分割入力：個別フィールドに分割された値を割り当て
                split_assignments = self._generate_split_field_assignments(group, client_data)
                assignments.update(split_assignments)
                logger.info(f"Generated split assignments for {group.field_type}: {len(split_assignments)} fields")
            else:
                # 組み合わせ入力：従来の組み合わせ処理
                combine_assignments = self._generate_combine_field_assignments(group, client_data)
                assignments.update(combine_assignments)
                logger.info(f"Generated combine assignments for {group.field_type}: {len(combine_assignments)} fields")
        
        return assignments

    def _generate_single_field_value(self, group: SplitFieldGroup, client_data: Dict[str, Any]) -> str:
        """単一フィールド時に割り当てる統合値を生成（共通化）。"""
        try:
            ci = (client_data.get('client', {}) if isinstance(client_data, dict) else client_data)
            if group.field_type == 'phone':
                return ''.join([
                    ci.get('phone_1','').strip(),
                    ci.get('phone_2','').strip(),
                    ci.get('phone_3','').strip()
                ])
            if group.field_type == 'postal_code':
                return ''.join([
                    ci.get('postal_code_1','').strip(),
                    ci.get('postal_code_2','').strip()
                ])
            if group.field_type == 'address':
                addr = ''.join([ci.get(f'address_{i}','').strip() for i in range(1,5)])
                bld  = ci.get('address_5','').strip()
                return f"{addr}　{bld}" if bld else addr
        except Exception as e:
            logger.debug(f"single field value generation failed: {e}")
        return ''
    
    def _generate_split_field_assignments(self, group: SplitFieldGroup, 
                                        client_data: Dict[str, Any]) -> Dict[str, str]:
        """
        分割入力用のフィールド割り当てを生成
        各フィールドに個別の値を割り当てる
        """
        assignments = {}
        client_info = client_data.get('client', {}) if isinstance(client_data, dict) else {}
        
        # フィールドタイプ別の分割入力処理
        if group.field_type == 'phone':
            assignments.update(self._generate_split_phone_assignments(group, client_info))
        elif group.field_type == 'address':
            assignments.update(self._generate_split_address_assignments(group, client_info))
        elif group.field_type == 'name':
            assignments.update(self._generate_split_name_assignments(group, client_info))
        elif group.field_type == 'email':
            assignments.update(self._generate_split_email_assignments(group, client_info))
        elif group.field_type == 'postal_code':
            assignments.update(self._generate_split_postal_assignments(group, client_info))
        
        return assignments
    
    def _generate_combine_field_assignments(self, group: SplitFieldGroup, 
                                          client_data: Dict[str, Any]) -> Dict[str, str]:
        """
        組み合わせ入力用のフィールド割り当てを生成（従来の処理）
        """
        assignments = {}
        rule = self.combination_rules[group.pattern]
        groups_def = rule['groups']
        client_info = client_data.get('client', {}) if isinstance(client_data, dict) else {}
        
        # パターンに応じた値の組み合わせ
        if group.pattern in [SplitPattern.ADDRESS_2_SPLIT, SplitPattern.ADDRESS_3_SPLIT, SplitPattern.ADDRESS_4_SPLIT]:
            # 単一フィールドしか検出されていない場合は住所全体を割り当て
            if len(group.fields) == 1:
                addr = ''.join([client_info.get(f'address_{i}', '').strip() for i in range(1,5)])
                bld  = client_info.get('address_5','').strip()
                value = f"{addr}　{bld}" if bld else addr
                assignments[group.fields[0].get('field_name','')] = value
            else:
                assignments.update(self._generate_address_assignments(group, groups_def, client_info))
        
        elif group.pattern in [SplitPattern.PHONE_2_SPLIT, SplitPattern.PHONE_3_SPLIT]:
            # 単一フィールドのみの場合は統合電話番号を割り当て
            if len(group.fields) == 1:
                phone = ''.join([
                    client_info.get('phone_1','').strip(),
                    client_info.get('phone_2','').strip(),
                    client_info.get('phone_3','').strip()
                ])
                assignments[group.fields[0].get('field_name','')] = phone
            else:
                assignments.update(self._generate_phone_assignments(group, groups_def, client_info))
        
        elif group.pattern == SplitPattern.NAME_2_SPLIT:
            assignments.update(self._generate_name_assignments(group, client_info))
        
        elif group.pattern == SplitPattern.EMAIL_2_SPLIT:
            assignments.update(self._generate_email_assignments(group, client_info))
        
        elif group.pattern == SplitPattern.POSTAL_2_SPLIT:
            if len(group.fields) == 1:
                postal = ''.join([
                    client_info.get('postal_code_1','').strip(),
                    client_info.get('postal_code_2','').strip()
                ])
                assignments[group.fields[0].get('field_name','')] = postal
            else:
                assignments.update(self._generate_postal_assignments(group, client_info))
        
        # 姓名かな/ひらがな（同じ NAME_2_SPLIT を使用）
        if group.pattern == SplitPattern.NAME_2_SPLIT and group.field_type == 'name_hiragana':
            assignments.update(self._generate_name_hiragana_assignments(group, client_info))
        if group.pattern == SplitPattern.NAME_2_SPLIT and group.field_type == 'name_kana':
            assignments.update(self._generate_name_kana_assignments(group, client_info))
        
        return assignments
    
    def _generate_split_phone_assignments(self, group: SplitFieldGroup, 
                                        client_info: Dict[str, Any]) -> Dict[str, str]:
        """電話番号の分割入力用割り当て"""
        assignments = {}
        
        # 電話番号の基本データ
        phone_parts = [
            client_info.get('phone_1', '03').strip(),     # 市外局番
            client_info.get('phone_2', '6825').strip(),   # 市内局番  
            client_info.get('phone_3', '0324').strip()    # 加入者番号
        ]
        
        # 各フィールドに個別の値を割り当て
        for i, field in enumerate(group.fields):
            if i < len(phone_parts):
                field_name = field.get('field_name', '')
                assignments[field_name] = phone_parts[i]
        
        logger.debug(f"Split phone assignments: {assignments}")
        return assignments
    
    def _generate_split_address_assignments(self, group: SplitFieldGroup, 
                                          client_info: Dict[str, Any]) -> Dict[str, str]:
        """住所の分割入力用割り当て"""
        assignments = {}
        
        # 住所の基本データ
        address_parts = [
            client_info.get('address_1', '東京都').strip(),      # 都道府県
            client_info.get('address_2', '新宿区').strip(),      # 市区町村
            client_info.get('address_3', '西新宿３ー３ー１３').strip(),  # 町名番地
            client_info.get('address_4', '').strip(),            # 番地詳細
            client_info.get('address_5', '西新宿水間ビル６階').strip() # 建物名
        ]
        
        # 各フィールドに個別の値を割り当て
        for i, field in enumerate(group.fields):
            if i < len(address_parts) and address_parts[i]:
                field_name = field.get('field_name', '')
                assignments[field_name] = address_parts[i]
        
        logger.debug(f"Split address assignments: {assignments}")
        return assignments
    
    def _generate_split_name_assignments(self, group: SplitFieldGroup, 
                                       client_info: Dict[str, Any]) -> Dict[str, str]:
        """姓名の分割入力用割り当て"""
        assignments = {}
        
        name_parts = [
            client_info.get('last_name', '五十嵐').strip(),   # 姓
            client_info.get('first_name', '駿').strip()       # 名
        ]
        
        # 各フィールドに個別の値を割り当て
        for i, field in enumerate(group.fields):
            if i < len(name_parts):
                field_name = field.get('field_name', '')
                assignments[field_name] = name_parts[i]
        
        logger.debug(f"Split name assignments: {assignments}")
        return assignments

    def _generate_name_hiragana_assignments(self, group: SplitFieldGroup, client_info: Dict[str, Any]) -> Dict[str, str]:
        """姓名ひらがなの分割入力用割り当て"""
        assignments = {}
        last = client_info.get('last_name_hiragana', '').strip()
        first = client_info.get('first_name_hiragana', '').strip()
        if len(group.fields) >= 2:
            assignments[group.fields[0].get('field_name', '')] = last
            assignments[group.fields[1].get('field_name', '')] = first
        return assignments

    def _generate_name_kana_assignments(self, group: SplitFieldGroup, client_info: Dict[str, Any]) -> Dict[str, str]:
        """姓名カナ（カタカナ）の分割入力用割り当て"""
        assignments = {}
        last = client_info.get('last_name_kana', '').strip()
        first = client_info.get('first_name_kana', '').strip()
        if len(group.fields) >= 2:
            assignments[group.fields[0].get('field_name', '')] = last
            assignments[group.fields[1].get('field_name', '')] = first
        return assignments
    
    def _generate_split_email_assignments(self, group: SplitFieldGroup, 
                                        client_info: Dict[str, Any]) -> Dict[str, str]:
        """メールの分割入力用割り当て"""
        assignments = {}
        
        email_parts = [
            client_info.get('email_1', 's.igarashi').strip(),  # ユーザー名
            client_info.get('email_2', 'neurify.jp').strip()   # ドメイン
        ]
        
        # 各フィールドに個別の値を割り当て
        for i, field in enumerate(group.fields):
            if i < len(email_parts):
                field_name = field.get('field_name', '')
                assignments[field_name] = email_parts[i]
        
        logger.debug(f"Split email assignments: {assignments}")
        return assignments
    
    def _generate_split_postal_assignments(self, group: SplitFieldGroup, 
                                         client_info: Dict[str, Any]) -> Dict[str, str]:
        """郵便番号の分割入力用割り当て"""
        assignments = {}
        
        postal_parts = [
            client_info.get('postal_code_1', '160').strip(),   # 前3桁
            client_info.get('postal_code_2', '0023').strip()   # 後4桁
        ]
        
        # 各フィールドに個別の値を割り当て
        for i, field in enumerate(group.fields):
            if i < len(postal_parts):
                field_name = field.get('field_name', '')
                assignments[field_name] = postal_parts[i]
        
        logger.debug(f"Split postal assignments: {assignments}")
        return assignments
    
    def _generate_address_assignments(self, group: SplitFieldGroup, groups_def: List[List[int]], 
                                    client_info: Dict[str, Any]) -> Dict[str, str]:
        """住所分割パターンの割り当て生成"""
        assignments = {}
        
        # 基本住所データ
        address_parts = []
        for i in range(1, 6):
            value = client_info.get(f'address_{i}', '').strip()
            address_parts.append(value)
        
        # グループ定義に従って組み合わせ
        for i, field in enumerate(group.fields):
            if i >= len(groups_def):
                break
                
            group_indices = groups_def[i]
            combined_parts = []
            
            for idx in group_indices:
                if idx <= len(address_parts) and address_parts[idx-1]:
                    combined_parts.append(address_parts[idx-1])
            
            if combined_parts:
                if i == len(groups_def) - 1 and len(groups_def) > 1:
                    # 最後のグループ（建物名）は全角スペースで区切り
                    value = '　'.join(combined_parts)
                else:
                    # その他は直接連結
                    value = ''.join(combined_parts)
                
                field_name = field.get('field_name', '')
                assignments[field_name] = value
        
        return assignments
    
    def _generate_phone_assignments(self, group: SplitFieldGroup, groups_def: List[List[int]], 
                                  client_info: Dict[str, Any]) -> Dict[str, str]:
        """電話番号分割パターンの割り当て生成"""
        assignments = {}
        
        phone_parts = [
            client_info.get('phone_1', '').strip(),
            client_info.get('phone_2', '').strip(),
            client_info.get('phone_3', '').strip()
        ]
        
        for i, field in enumerate(group.fields):
            if i >= len(groups_def):
                break
                
            group_indices = groups_def[i]
            combined_parts = []
            
            for idx in group_indices:
                if idx <= len(phone_parts) and phone_parts[idx-1]:
                    combined_parts.append(phone_parts[idx-1])
            
            if combined_parts:
                # 電話番号は直接連結
                value = ''.join(combined_parts)
                field_name = field.get('field_name', '')
                assignments[field_name] = value
        
        return assignments
    
    def _generate_name_assignments(self, group: SplitFieldGroup, client_info: Dict[str, Any]) -> Dict[str, str]:
        """姓名分割パターンの割り当て生成"""
        assignments = {}
        
        last_name = client_info.get('last_name', '').strip()
        first_name = client_info.get('first_name', '').strip()
        
        if len(group.fields) >= 2:
            # 順序検証済みなので、最初が姓、次が名
            assignments[group.fields[0].get('field_name', '')] = last_name
            assignments[group.fields[1].get('field_name', '')] = first_name
        
        return assignments
    
    def _generate_email_assignments(self, group: SplitFieldGroup, client_info: Dict[str, Any]) -> Dict[str, str]:
        """メール分割パターンの割り当て生成"""
        assignments = {}
        
        email_1 = client_info.get('email_1', '').strip()
        email_2 = client_info.get('email_2', '').strip()
        
        if len(group.fields) >= 2 and email_1 and email_2:
            assignments[group.fields[0].get('field_name', '')] = email_1
            assignments[group.fields[1].get('field_name', '')] = email_2
        
        return assignments
    
    def _generate_postal_assignments(self, group: SplitFieldGroup, client_info: Dict[str, Any]) -> Dict[str, str]:
        """郵便番号分割パターンの割り当て生成"""
        assignments = {}
        
        postal_1 = client_info.get('postal_code_1', '').strip()
        postal_2 = client_info.get('postal_code_2', '').strip()
        
        if len(group.fields) >= 2 and postal_1 and postal_2:
            assignments[group.fields[0].get('field_name', '')] = postal_1
            assignments[group.fields[1].get('field_name', '')] = postal_2
        
        return assignments
    
    # リファクタリング: 未使用の分割割り当て妥当性検証関数を削除
    
    def get_detector_summary(self, split_groups: List[SplitFieldGroup]) -> Dict[str, Any]:
        """検出器のサマリーを取得"""
        pattern_counts = {}
        total_confidence = 0.0
        
        for group in split_groups:
            pattern_name = group.pattern.value
            pattern_counts[pattern_name] = pattern_counts.get(pattern_name, 0) + 1
            total_confidence += group.confidence
        
        return {
            'total_groups': len(split_groups),
            'patterns': pattern_counts,
            'avg_confidence': total_confidence / len(split_groups) if split_groups else 0.0,
            'valid_sequences': sum(1 for g in split_groups if g.sequence_valid)
        }
    
    # リファクタリング: 未使用の住所関連ユーティリティ群を削除
