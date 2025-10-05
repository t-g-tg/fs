"""
クライアントデータとフォームフィールドのマッピングロジック
"""
import logging
from typing import Dict, Any, Optional, List

logger = logging.getLogger(__name__)

class ClientDataMapper:
    """
    フォームフィールドとクライアントデータを対応付け、入力値を生成する責務を持つ
    """

    @staticmethod
    def get_value_for_rule_based_field(
        field_name: str, client_data: Dict[str, Any]
    ) -> Optional[str]:
        """
        ルールベースで発見されたフィールド名に基づき、クライアントデータから適切な値を取得する。
        field_patterns.pyで定義されたフィールド名との完全一致で判定する。

        Args:
            field_name: RuleBasedAnalyzerによって特定されたフィールド名
            client_data: クライアントデータ（'client'と'targeting'のキーを持つDict）

        Returns:
            フィールドに入力すべき文字列値。見つからない場合はNoneや空文字。
        """
        client_info = client_data.get('client', {}) if isinstance(client_data, dict) else {}
        targeting_info = client_data.get('targeting', {}) if isinstance(client_data, dict) else {}

        # 1. 会社名
        if field_name == '会社名':
            return client_info.get('company_name', '')
        # 2. メールアドレス
        elif field_name == 'メールアドレス':
            email_1 = client_info.get('email_1', '')
            email_2 = client_info.get('email_2', '')
            if email_1 and email_2:
                return f"{email_1}@{email_2}"
            return email_1 or ''
        # 3. 送信者氏名
        elif field_name == '送信者氏名':
            last_name = client_info.get('last_name', '')
            first_name = client_info.get('first_name', '')
            if last_name and first_name:
                return f"{last_name}　{first_name}"  # 全角スペース
            return last_name or first_name or ''
        # 4. 姓
        elif field_name == '姓':
            return client_info.get('last_name', '')
        # 5. 名
        elif field_name == '名':
            return client_info.get('first_name', '')
        # 6. 電話番号 (分割)
        elif field_name == '電話1':
            return client_info.get('phone_1', '')
        elif field_name == '電話2':
            return client_info.get('phone_2', '')
        elif field_name == '電話3':
            return client_info.get('phone_3', '')
        # 7. お問い合わせ本文
        elif field_name == 'お問い合わせ本文':
            return targeting_info.get('message', '')
        # 8. 部署名
        elif field_name == '部署名':
            return client_info.get('department', '')
        # 9. 姓カナ
        elif field_name == '姓カナ':
            return client_info.get('last_name_kana', '')
        # 10. 名カナ
        elif field_name == '名カナ':
            return client_info.get('first_name_kana', '')
        # 11. 役職
        elif field_name == '役職':
            return client_info.get('position', '')
        # 12. 企業URL
        elif field_name == '企業URL':
            return client_info.get('website_url', '')
        # 13. 件名
        elif field_name == '件名':
            return targeting_info.get('subject', '')
        # 14. 会社名カナ
        elif field_name == '会社名カナ':
            return client_info.get('company_name_kana', '')
        # 15. 姓ひらがな
        elif field_name == '姓ひらがな':
            return client_info.get('last_name_hiragana', '')
        # 16. 名ひらがな
        elif field_name == '名ひらがな':
            return client_info.get('first_name_hiragana', '')
        # 17. 性別
        elif field_name == '性別':
            return client_info.get('gender', '')
        # 18. 郵便番号1
        elif field_name == '郵便番号1':
            return client_info.get('postal_code_1', '')
        # 19. 郵便番号2
        elif field_name == '郵便番号2':
            return client_info.get('postal_code_2', '')
        # 20. 住所
        elif field_name == '住所':
            address_parts = [
                client_info.get('address_1', ''),
                client_info.get('address_2', ''),
                client_info.get('address_3', ''),
                client_info.get('address_4', ''),
            ]
            base_address = ''.join(filter(None, address_parts))
            address_5 = client_info.get('address_5', '')
            if address_5:
                return f"{base_address}　{address_5}"
            return base_address

        # フォールバック処理
        return ClientDataMapper._get_fallback_value(field_name, client_info, targeting_info)

    @staticmethod
    def _get_fallback_value(field_name: str, client_info: Dict[str, Any], targeting_info: Dict[str, Any]) -> str:
        """
        フィールド名が完全一致しない場合のフォールバックロジック
        """
        field_name_lower = field_name.lower()
        # メッセージ系
        if any(keyword in field_name_lower for keyword in ['内容', 'メッセージ', 'コメント', 'message', 'content', 'inquiry']):
            return targeting_info.get('message', '')
        # 名前系
        if any(keyword in field_name_lower for keyword in ['名前', '氏名', 'name', 'お名前']):
            last_name = client_info.get('last_name', '')
            first_name = client_info.get('first_name', '')
            if last_name and first_name:
                return f"{last_name}　{first_name}"
            return last_name or first_name or ''
        # 電話系
        if any(keyword in field_name_lower for keyword in ['電話', 'phone', 'tel']):
            parts = [client_info.get(f'phone_{i}', '') for i in range(1, 4)]
            return ''.join(filter(None, parts))
        # 郵便番号系
        if any(keyword in field_name_lower for keyword in ['郵便', 'postal', 'zip', '〒']):
            parts = [client_info.get(f'postal_code_{i}', '') for i in range(1, 3)]
            return ''.join(filter(None, parts))
        # 住所系
        if any(keyword in field_name_lower for keyword in ['住所', 'address', '所在地']):
            address_parts = [client_info.get(f'address_{i}', '') for i in range(1, 5)]
            base_address = ''.join(filter(None, address_parts))
            address_5 = client_info.get('address_5', '')
            if address_5:
                return f"{base_address}　{address_5}"
            return base_address

        # 最終フォールバック
        logger.error(f"UNKNOWN FIELD '{field_name}' -> FALLBACK to TARGETING message")
        logger.error(f"このフィールド名は field_patterns.py で定義されていません: '{field_name}'")
        return targeting_info.get('message', '')
