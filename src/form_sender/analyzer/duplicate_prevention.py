"""
重複値入力防止システム

同一値が複数フィールドに入力されることを防ぐ制御機能
例外: メールアドレス確認入力のみ許可
"""

import logging
from typing import Dict, List, Any, Set, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class FieldAssignment:
    """フィールド割り当て情報"""
    field_name: str
    value: str
    score: int
    element_info: Dict[str, Any]
    is_primary: bool = True  # 主要フィールド（重複解決時に優先）


class DuplicatePreventionManager:
    """重複値入力防止マネージャー"""
    
    def __init__(self):
        """初期化"""
        self.assignments: Dict[str, FieldAssignment] = {}
        self.value_registry: Dict[str, List[str]] = {}  # {値: [フィールド名のリスト]}
        self.email_confirmation_patterns = [
            # 代表的な英語/日本語表現
            'email_confirm', 'mail_confirm', 'email_confirmation',
            'confirm_email', 'confirm_mail', 'メール確認', '確認用メール',
            'email_check', 'mail_check', 're_email', 're_mail',
            # 本システムの自動生成ラベル（救済/自動処理）
            'auto_email_confirm',
            # 一般的な2番目シグナル（mail2/email2 など）は element 側で拾うためここでは最小限
        ]
        
        # フィールド優先順位（重複解決用）- 電話番号系重複防止強化
        self.field_priority = {
            'メールアドレス': 100,     # 最重要
            'お問い合わせ本文': 95,    # 最重要
            '会社名': 90,
            '統合氏名': 85,           # 統合氏名は分割名より優先
            # '送信者氏名': 85, <- 廃止：姓名組み合わせを使用
            '姓': 80,
            '名': 80,
            '電話番号': 85,              # 統合電話番号フィールド（最優先）
            '件名': 75,
            '会社名カナ': 70,
            '統合氏名カナ': 70,       # 統合カナは分割カナより優先
            '姓カナ': 65,
            '名カナ': 65,
            '役職': 60,
            '部署名': 55,
            '企業URL': 50,
            # 電話分割フィールドは大幅に優先度下げ（電話番号との重複時に必ず負ける）
            '電話1': 15,  # 45→15: 電話番号フィールドとの重複防止
            '電話2': 10,  # 45→10: 電話番号フィールドとの重複防止 
            '電話3': 5,   # 45→5:  電話番号フィールドとの重複防止
            '郵便番号1': 40,
            '郵便番号2': 40,
            '住所': 35,
            '姓ひらがな': 30,
            '名ひらがな': 30,
            '性別': 25
        }
        
        # 電話番号系フィールドグループ（相互排他制御用）
        self.phone_field_group = {'電話番号', '電話1', '電話2', '電話3'}
        
        # 郵便番号系フィールドグループ（相互排他制御用）
        self.postal_field_group = {'郵便番号1', '郵便番号2'}
        
        logger.info("DuplicatePreventionManager initialized")
    
    def register_field_assignment(self, field_name: str, value: str, 
                                score: int, element_info: Dict[str, Any]) -> bool:
        """
        フィールド割り当てを登録（重複チェック付き）
        
        Args:
            field_name: フィールド名
            value: 入力値
            score: 要素スコア
            element_info: 要素情報
            
        Returns:
            bool: 登録成功かどうか
        """
        if not value or not field_name:
            logger.debug(f"Empty value or field name: field='{field_name}', value='{value}'")
            return False
        
        # プレースホルダ値（全角空白など空白類のみ）は重複管理対象外として登録
        if self._is_placeholder_value(value):
            assignment = FieldAssignment(
                field_name=field_name,
                value=value,
                score=score,
                element_info=element_info
            )
            self.assignments[field_name] = assignment
            logger.debug(f"Registered placeholder assignment for '{field_name}' (excluded from duplicate registry)")
            return True
        
        # メールアドレス確認フィールドの特別処理
        if self._is_email_confirmation_field(field_name, element_info):
            return self._register_email_confirmation(field_name, value, score, element_info)
        
        # フィールドグループ内での相互排他チェック（同一データ型の重複防止強化）
        if self._has_conflicting_field_assignment(field_name, value):
            logger.warning(f"Field group conflict detected: '{field_name}' conflicts with existing assignments (value redacted)")
            return self._resolve_field_group_conflict(field_name, value, score, element_info)
        
        # 既存の重複チェック
        if value in self.value_registry:
            existing_fields = self.value_registry[value]
            logger.warning(
                f"Duplicate value detected: value already assigned to {existing_fields}, attempting assignment to '{field_name}'"
            )
            
            return self._resolve_duplicate(field_name, value, score, element_info, existing_fields)
        
        # 新規登録
        assignment = FieldAssignment(
            field_name=field_name,
            value=value,
            score=score,
            element_info=element_info
        )
        
        self.assignments[field_name] = assignment
        if value not in self.value_registry:
            self.value_registry[value] = []
        self.value_registry[value].append(field_name)
        
        logger.info(f"Field assignment registered: '{field_name}' (score: {score})")
        return True

    def _is_placeholder_value(self, value: str) -> bool:
        """プレースホルダ（空白類のみ）の値を判定。全角空白を含む。"""
        try:
            # 半角/全角スペースのみ → プレースホルダ扱い
            if value is None:
                return True
            stripped = str(value).replace('　', '').strip()
            return stripped == ''
        except Exception:
            return False
    
    def _has_conflicting_field_assignment(self, field_name: str, value: str) -> bool:
        """フィールドグループ内での競合がある場合にTrueを返す"""
        target_group = None
        
        # 新しいフィールドがどのグループに属するかを判定
        if field_name in self.phone_field_group:
            target_group = self.phone_field_group
        elif field_name in self.postal_field_group:
            target_group = self.postal_field_group
        
        if not target_group:
            return False
        
        # 既存の同グループ割当の一覧
        existing_group_fields = [fname for fname in self.assignments.keys() if fname in target_group]
        if not existing_group_fields:
            return False
        
        # 電話番号系の相互排他（汎用化）
        # - 統合フィールド（電話番号）と分割フィールド（電話1/2/3）は同時に割り当てない
        # - 分割フィールド同士は共存可能（1/2/3は同一番号の構成要素のため）
        if target_group is self.phone_field_group:
            is_unified = (field_name == '電話番号')
            has_unified_assigned = any(f == '電話番号' for f in existing_group_fields)
            has_split_assigned = any(f in {'電話1', '電話2', '電話3'} for f in existing_group_fields)
            
            # 新規が統合で既に分割がある、または新規が分割で既に統合がある場合は競合
            if (is_unified and has_split_assigned) or (not is_unified and has_unified_assigned):
                logger.debug(
                    f"Phone group conflict: new '{field_name}' conflicts with existing {existing_group_fields}"
                )
                return True
            
            # 分割同士は競合扱いしない
            return False
        
        # それ以外のグループ（郵便番号など）は「同一値」のみ競合として扱う（従来の挙動）
        for assigned_field, assignment in self.assignments.items():
            if (
                assigned_field != field_name
                and assigned_field in target_group
                and assignment.value == value
            ):
                logger.debug(
                    f"Field group conflict: '{field_name}' conflicts with '{assigned_field}' in group {target_group}"
                )
                return True
        
        return False
    
    def _resolve_field_group_conflict(self, new_field: str, value: str, new_score: int, new_element_info: Dict[str, Any]) -> bool:
        """フィールドグループ内の競合を解決"""
        target_group = None
        if new_field in self.phone_field_group:
            target_group = self.phone_field_group
        elif new_field in self.postal_field_group:
            target_group = self.postal_field_group
        
        if not target_group:
            return False
        
        # 競合対象の特定ロジック
        conflicting_fields = []
        if target_group is self.phone_field_group:
            # 統合 vs 分割のみ相互排他
            if new_field == '電話番号':
                conflicting_fields = [f for f in self.assignments.keys() if f in {'電話1', '電話2', '電話3'}]
            elif new_field in {'電話1', '電話2', '電話3'}:
                if '電話番号' in self.assignments:
                    conflicting_fields = ['電話番号']
        else:
            # 従来の同一値競合
            for assigned_field, assignment in self.assignments.items():
                if (
                    assigned_field != new_field
                    and assigned_field in target_group
                    and assignment.value == value
                ):
                    conflicting_fields.append(assigned_field)
        
        if not conflicting_fields:
            return False
        
        # 最適なフィールドを決定（既存フィールド vs 新しいフィールド）
        best_field = self._determine_best_field(new_field, new_score, conflicting_fields)
        
        if best_field == new_field:
            # 新しいフィールドが最適 - 競合フィールドを削除
            for conflicting_field in conflicting_fields:
                logger.info(f"Removing conflicting field assignment: '{conflicting_field}' (field group conflict)")
                del self.assignments[conflicting_field]
                if value in self.value_registry:
                    self.value_registry[value] = [f for f in self.value_registry[value] if f != conflicting_field]
            
            # 新しい割り当てを登録
            assignment = FieldAssignment(
                field_name=new_field,
                value=value,
                score=new_score,
                element_info=new_element_info
            )
            self.assignments[new_field] = assignment
            if value not in self.value_registry:
                self.value_registry[value] = []
            if new_field not in self.value_registry[value]:
                self.value_registry[value].append(new_field)
            
            logger.info(f"Field group conflict resolved: '{new_field}' wins (score: {new_score})")
            return True
        else:
            # 既存のフィールドが最適 - 新しい割り当てを拒否
            logger.info(f"Field group conflict resolved: existing field '{best_field}' keeps current value")
            return False
    
    def _is_email_confirmation_field(self, field_name: str, element_info: Dict[str, Any]) -> bool:
        """メールアドレス確認フィールドかどうか判定"""
        field_name_lower = field_name.lower()
        
        # フィールド名による判定
        for pattern in self.email_confirmation_patterns:
            if pattern in field_name_lower:
                logger.debug(f"Email confirmation field detected by name: '{field_name}'")
                return True
        
        # 要素属性による判定
        element_name = element_info.get('name', '').lower()
        element_id = element_info.get('id', '').lower()
        element_placeholder = element_info.get('placeholder', '').lower()
        
        for attr_value in [element_name, element_id, element_placeholder]:
            for pattern in self.email_confirmation_patterns:
                if pattern in attr_value:
                    logger.debug(f"Email confirmation field detected by attribute: '{field_name}' ({attr_value})")
                    return True
        
        return False
    
    def _register_email_confirmation(self, field_name: str, value: str, 
                                   score: int, element_info: Dict[str, Any]) -> bool:
        """メールアドレス確認フィールドの登録"""
        # メールアドレス確認は既存のメールアドレスと同じ値のみ許可
        if value not in self.value_registry:
            logger.warning(
                f"Email confirmation field '{field_name}' has value (redacted) but no matching primary email field found"
            )
            return False
        
        # 既存のメールアドレスフィールドがあるかチェック
        existing_fields = self.value_registry[value]
        has_primary_email = any(
            self.assignments[field].field_name == 'メールアドレス' 
            for field in existing_fields
        )
        
        if not has_primary_email:
            logger.warning(
                f"Email confirmation field '{field_name}' cannot be assigned: no primary email field with same value (redacted)"
            )
            return False
        
        # 確認フィールドとして登録
        assignment = FieldAssignment(
            field_name=field_name,
            value=value,
            score=score,
            element_info=element_info,
            is_primary=False  # 確認フィールドは非主要
        )
        
        self.assignments[field_name] = assignment
        self.value_registry[value].append(field_name)
        
        logger.info(f"Email confirmation field registered: '{field_name}' (value redacted)")
        return True
    
    def _resolve_duplicate(self, new_field: str, value: str, new_score: int,
                         new_element_info: Dict[str, Any], existing_fields: List[str]) -> bool:
        """重複解決処理"""
        # より適切なフィールドを決定
        best_field = self._determine_best_field(new_field, new_score, existing_fields)
        
        if best_field == new_field:
            # 新しいフィールドが最適 - 既存の割り当てを削除
            for existing_field in existing_fields:
                if existing_field in self.assignments:
                    logger.info(f"Removing conflicting assignment: '{existing_field}' = '{value}'")
                    del self.assignments[existing_field]
            
            # 新しい割り当てを登録
            assignment = FieldAssignment(
                field_name=new_field,
                value=value,
                score=new_score,
                element_info=new_element_info
            )
            self.assignments[new_field] = assignment
            self.value_registry[value] = [new_field]
            
            logger.info(f"Duplicate resolved: '{new_field}' wins with '{value}' (score: {new_score})")
            return True
        else:
            # 既存のフィールドが最適 - 新しい割り当てを拒否
            logger.info(f"Duplicate resolved: existing field '{best_field}' keeps '{value}'")
            return False
    
    def _determine_best_field(self, new_field: str, new_score: int, existing_fields: List[str]) -> str:
        """最適なフィールドを決定"""
        # フィールド優先順位による判定
        new_priority = self.field_priority.get(new_field, 0)
        
        best_field = new_field
        best_priority = new_priority
        best_score = new_score
        
        for existing_field in existing_fields:
            if existing_field not in self.assignments:
                continue
                
            existing_assignment = self.assignments[existing_field]
            existing_priority = self.field_priority.get(existing_field, 0)
            existing_score = existing_assignment.score
            
            # 優先順位が高い方を選択
            if existing_priority > best_priority:
                best_field = existing_field
                best_priority = existing_priority
                best_score = existing_score
            elif existing_priority == best_priority:
                # 優先順位が同じ場合はスコアで判定
                if existing_score > best_score:
                    best_field = existing_field
                    best_priority = existing_priority
                    best_score = existing_score
        
        logger.debug(f"Best field determination: '{best_field}' "
                    f"(priority: {best_priority}, score: {best_score})")
        return best_field
    
    def get_final_assignments(self) -> Dict[str, str]:
        """最終的なフィールド割り当てを取得"""
        final_assignments = {}
        for field_name, assignment in self.assignments.items():
            final_assignments[field_name] = assignment.value
        
        logger.info(f"Final assignments: {len(final_assignments)} fields")
        return final_assignments
    
    def get_assignment_summary(self) -> Dict[str, Any]:
        """割り当てサマリーを取得"""
        primary_count = sum(1 for a in self.assignments.values() if a.is_primary)
        confirmation_count = sum(1 for a in self.assignments.values() if not a.is_primary)
        
        # 値の使用状況
        value_usage = {}
        for value, fields in self.value_registry.items():
            value_usage[value] = {
                'fields': fields,
                'count': len(fields),
                'is_duplicate': len(fields) > 1
            }
        
        duplicates = {v: info for v, info in value_usage.items() if info['is_duplicate']}
        
        return {
            'total_assignments': len(self.assignments),
            'primary_fields': primary_count,
            'confirmation_fields': confirmation_count,
            'unique_values': len(self.value_registry),
            'duplicate_values': len(duplicates),
            'duplicates_detail': duplicates,
            'field_priority_applied': bool(duplicates)
        }
    
    def validate_assignments(self) -> Tuple[bool, List[str]]:
        """割り当ての妥当性を検証（フィールドグループ重複チェック強化）"""
        issues = []
        
        # 重複値チェック（メール確認以外）
        for value, fields in self.value_registry.items():
            if len(fields) > 1:
                # メール確認の組み合わせかチェック
                field_types = [self.assignments[f].field_name for f in fields if f in self.assignments]
                has_email_primary = 'メールアドレス' in field_types
                confirmation_fields = [f for f in fields if not self.assignments.get(f, FieldAssignment('', '', 0, {})).is_primary]
                
                if not (has_email_primary and len(confirmation_fields) == len(fields) - 1):
                    issues.append(f"Invalid duplicate value '{value}' in fields: {fields}")
        
        # フィールドグループ内の重複チェック（電話番号・郵便番号系）
        self._validate_field_group_duplicates(self.phone_field_group, "phone", issues)
        self._validate_field_group_duplicates(self.postal_field_group, "postal", issues)
        
        # 必須フィールドの存在チェック（改良版：空文字も考慮）
        # 注意: この検証は_validate_final_assignmentsが空文字をスキップして再登録する問題のため無効化
        # 代わりに、rule_based_analyzer側で適切な検証を実装
        logger.debug("Skipping required field validation in DuplicatePreventionManager - handled by main analyzer")
        
        return len(issues) == 0, issues
    
    def _validate_field_group_duplicates(self, field_group: Set[str], group_name: str, issues: List[str]):
        """フィールドグループ内の重複値チェック"""
        group_values = {}  # {値: [フィールド名のリスト]}
        
        for field_name, assignment in self.assignments.items():
            if field_name in field_group:
                value = assignment.value
                if value not in group_values:
                    group_values[value] = []
                group_values[value].append(field_name)
        
        # 同一値が複数フィールドに割り当てられている場合はエラー
        for value, fields in group_values.items():
            if len(fields) > 1:
                issues.append(f"Invalid {group_name} field group duplicate: value duplicated in {fields}")
                logger.error(f"Field group validation failed: {group_name} value duplicated in {fields}")
    
    def clear_assignments(self):
        """すべての割り当てをクリア"""
        self.assignments.clear()
        self.value_registry.clear()
        logger.info("All assignments cleared")
