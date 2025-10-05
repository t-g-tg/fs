"""
フィールドパターン定義システム

日本企業フォーム特化の26種類フィールドパターンを定義
参考: ListersForm復元システムのフィールドパターン辞書
SYSTEM.md 1.2節の重み付けスコアリングシステムに準拠
"""

from typing import Dict, List, Any
import logging

logger = logging.getLogger(__name__)


class FieldPatterns:
    """フィールドパターン定義・管理クラス"""
    
    def __init__(self):
        """フィールドパターンの初期化"""
        self.patterns = self._init_field_patterns()
        logger.info(f"Initialized {len(self.patterns)} field patterns")
    
    def get_patterns(self) -> Dict[str, Dict[str, Any]]:
        """全フィールドパターンを取得"""
        return self.patterns
    
    def get_pattern(self, field_name: str) -> Dict[str, Any]:
        """指定フィールドのパターンを取得"""
        return self.patterns.get(field_name, {})
    
    def get_sorted_patterns_by_weight(self) -> List[tuple]:
        """重要度順にソートされたフィールドパターンを取得"""
        return sorted(
            self.patterns.items(),
            key=lambda x: x[1].get('weight', 0),
            reverse=True
        )
    
    def _init_field_patterns(self) -> Dict[str, Dict[str, Any]]:
        """
        実システム完全対応版20パターンフィールド定義
        GAS/SpreadsheetClient.gsの必須フィールドに完全整合
        """
        return {
            # 基本情報フィールド (1-4)
            
            # 4. 統合氏名カナ (unified_fullname_kana) - 統合フリガナ（カタカナ主体）
            "統合氏名カナ": {
                "names": [
                    "your-name-kana", "name_kana", "fullname_kana", "full_name_kana",
                    "furigana", "kana", "katakana", "フリガナ", "カナ", "カタカナ"
                ],
                "ids": [
                    "your-name-kana", "name_kana", "fullname_kana", "furigana", "kana", "katakana"
                ],
                "classes": ["kana", "katakana", "furigana"],
                "placeholders": ["フリガナ", "ふりがな", "カタカナ", "セイ メイ", "セイ　メイ"],
                "types": ["text"],
                "tags": ["input"],
                "weight": 18,
                "strict_patterns": ["フリガナ", "カタカナ", "kana", "furigana"],
                "kana_indicator": ["kana", "カナ", "カタカナ", "furigana", "ruby", "ルビ", "るび"],
                "exclude_patterns": [
                    "company", "会社", "企業", "法人", "団体", "organization",
                    "zip", "postal", "郵便", "住所", "address", "tel", "phone", "電話",
                    "email", "mail", "メール", "captcha", "認証", "image_auth", "spam-block",
                    # 分割名のシグナルが含まれる場合は統合カナから除外（split優先）
                    "last", "first", "lastname", "firstname", "last_name", "first_name", "last-name", "first-name", "sei", "mei"
                ]
            },

            # 1. 会社名 (company_name) - GAS/スプレッドシート必須
            "会社名": {
                "names": [
                    "company", "corp", "corporation", "会社", "会社名", "団体", "団体名", "company_name",
                    "firm", "organization", "org", "enterprise", "kaisha", "kaisya", "business",
                    "corporate", "company-name", "corporate_name", "business_name",
                    # 汎用的な表記ゆれ・英語圏CMSの命名
                    "companyname", "organization_name", "org_name", "corp_name", "corporation_name",
                    "customer-company-name", "customer_company_name", "your-company", "your_company",
                    # 追加: 所属/所属先・Affiliation 系（ラベル/属性双方で広く使われる）
                    "affiliation", "affiliations", "affiliation_name", "affiliation-name",
                    "shozoku", "syosoku", "shozokusaki", "shozoku_saki"
                ],
                "ids": [
                    "company", "corp", "company-name", "company_name", "firm", "org", "business",
                    "corporate", "corporate_name", "business_name", "companyname", "organization_name",
                    "org_name", "corp_name", "corporation_name"
                ],
                "classes": [
                    "company", "corp", "company-name", "firm", "organization", "business",
                    "corporate", "company_name", "companyname", "org_name"
                ],
                "placeholders": [
                    "会社名", "Company Name", "会社名を入力してください", "Company",
                    "企業名", "組織名", "法人名", "会社・団体名", "Corporate Name",
                    "Business Name", "Organization", "社名", "御社名", "貴社名",
                    # 追加: 所属/所属先・Affiliation の表記ゆれ
                    "所属", "ご所属", "所属先", "ご所属先", "Affiliation",
                    # 追加: 複合ラベル（会社名または氏名 等）
                    "会社名または氏名", "会社名・氏名", "会社名またはお名前", "企業名または氏名"
                ],
                "types": ["text"],
                "tags": ["input"],
                "weight": 25,  # 重要度最高レベル（メール22より高い）
                "strict_patterns": ["会社", "会社名", "団体名", "company", "corporation", "firm", "enterprise", "business"],
                "exclude_patterns": [
                    "your-subject", "your_subject", "subject", "件名", "タイトル",
                    "your-furigana", "your_furigana", "furigana",
                    "LOGIN_ID", "PASSWORD", "OTP", "TOTP", "MFAOTP", "captcha", "login_id", "password", "signin", "auth", "verification", "mfa",
                    "kana", "カナ", "katakana", "hiragana", "フリガナ", "ふりがな", "furi", "yomi", "読み",
                    "tkna515", "tkna001",
                    # 個人名系の除外（会社名と誤マッチしないよう強化）
                    "sei", "mei", "姓", "lastname", "firstname", "family_name", "given_name",
                    "氏名", "お名前", "your-name", "your_name", "fullname", "full_name",
                    # 確認系（メール確認など）
                    "confirm", "mailcheck", "mail_check", "mail-check", "email_check", "email-check",
                    "confirm_mail", "confirm-email", "mail_confirm", "email_confirm",
                    "mailaddressconfirm", "mail_address_confirm", "email_address_confirm",
                    # 管理会社や年月に関する語（誤検出抑止・汎用）
                    "管理会社", "kanri", "syunkou", "竣工", "年月日"
                ]  # 氏名系の除外を強化し、汎用nameは除外しない
            },
            
            # 2. 部署名 (department) - スプレッドシート必須
            "部署名": {
                "names": ["department", "dept", "division", "部署", "部署名", "busho", "busyo", 
                         "section", "team", "group"],
                "ids": ["department", "dept", "busho", "division", "section"],
                "classes": ["department", "dept", "division", "section"],
                "placeholders": ["部署名", "Department", "部署", "Division", "所属部署", 
                               "部門", "課", "係"],
                "types": [],
                "tags": ["input"],
                "weight": 8
            },
            
            # 3. 統合氏名 (unified_fullname) - 統合氏名フィールド専用
        "統合氏名": {
                "names": [
                    "fullname", "full_name", "name", "氏名", "お名前", "ご氏名", "姓名",
                    "your-name", "your_name", "namae", "personal_name", "user_name", "member_name",
                    # 追加: 日本の実フォームで頻出のラベル表現
                    "ご担当者名", "担当者名", "担当者"
                ],
                "ids": ["fullname", "full-name", "full_name", "name", "氏名", "your-name", 
                       "your_name", "namae", "personal_name", "user_name", "member_name"],
                "classes": ["fullname", "full-name", "name", "your-name", "personal", 
                           "user", "member", "contact", "personal_name"],
                "placeholders": [
                    "お名前", "氏名", "Name", "Personal Name", "Full Name", "姓名",
                    "フルネーム", "名前を入力してください", "お名前を入力してください",
                    # 追加: 担当者系のプレースホルダ
                    "ご担当者名", "担当者名"
                ],
                "types": ["text"],
                "tags": ["input"],
                "weight": 26,  # 会社名(25)より高い優先度で最優先
                "strict_patterns": [
                    "fullname", "full_name", "氏名", "お名前", "name", "your-name", "your_name", "姓名",
                    # 追加: 担当者系（強い一致）
                    "ご担当者名", "担当者名"
                ],
                "exclude_patterns": ["company", "会社", "社名", "corp", "corporation", "firm", "organization", "business", 
                                   "LOGIN_ID", "PASSWORD", "OTP", "TOTP", "MFAOTP", "captcha", "login_id", "password", 
                                   "signin", "auth", "verification", "mfa", "phone", "tel", "電話", "zip", "postal", "郵便", 
                                   "address", "住所", "email", "mail", "メール", "subject", "件名", "message", "本文",
                                   "last", "first", "lastname", "firstname", "姓", "kana", "カナ", "フリガナ", "ふりがな",
                                   # 〇〇名系（個人名ではない）
                                   "法人名", "団体名", "組織名", "部署名", "学校名", "店舗名", "病院名", "施設名", "会社名・団体名", "団体・組織名"]
            },
            
            # 4. 姓 (last_name) - GAS/スプレッドシート必須
        "姓": {
                "names": ["lastname", "last_name", "last-name", "family_name", "family-name", "姓", "苗字", "sei", "myoji",
                         "surname", "user_name", "member_name", "client_name", "contact_name", "person_name", "last-name-kanji", "last_name_kanji", "family-name-kanji", "family_name_kanji",
                         "lname", "l_name"],
                "ids": ["lastname", "family-name", "last_name", "last-name", "sei", "surname", 
                       "personal_name", "user_name", "member_name", "last-name-kanji", "last_name_kanji", "family-name-kanji", "family_name_kanji"],
                # class名の実態に合わせて 'last-name' / 'last_name' も許容
                "classes": ["lastname", "last-name", "last_name", "input-last-name", "family-name", "surname", "sei",
                           "personal", "user", "member", "contact"],
                # 一般的なダミー例（山田）を追加して分割姓名の検出力を向上
                "placeholders": ["姓", "Last Name", "苗字", "Family Name", "お名前（姓）", 
                               "ファミリーネーム", "姓名の姓", "Personal Name", "山田"],
                "types": [],
                "tags": ["input"],
                "weight": 24,  # 会社名(25)に次ぐ高い優先度
                "strict_patterns": ["姓", "苗字", "lastname", "family_name", "surname"],
                # 統合氏名専用のワードは除外するが、'name1'/'name2' 等の分割属性名への過剰除外を避けるため
                # 汎用的な 'name' 系の除外は外す（誤検出はスコアしきい値で制御）
                "exclude_patterns": ["company", "会社", "社名", "corp", "corporation", "firm", "organization", "business", "LOGIN_ID", "PASSWORD", "OTP", "TOTP", "MFAOTP", "captcha", "login_id", "password", "signin", "auth", "verification", "mfa", "phone", "tel", "電話", "zip", "postal", "郵便", "address", "住所", "email", "mail", "メール", "subject", "件名", "message", "本文", "unified_name_field",
                                   # 〇〇名系（個人名ではない）
                                   "法人名", "団体名", "組織名", "部署名", "学校名", "店舗名", "病院名", "施設名", "会社名・団体名", "団体・組織名"]  # 統合氏名パターンを除外に追加
            },
            
            # 5. 名 (first_name) - GAS/スプレッドシート必須
        "名": {
                "names": ["firstname", "first_name", "first-name", "given_name", "given-name", "名", "mei", 
                         "forename", "user_name", "member_name", "client_name", "contact_name", "person_name", "first-name-kanji", "first_name_kanji", "given-name-kanji", "given_name_kanji",
                         "fname", "f_name"],
                "ids": ["firstname", "first-name", "first_name", "given_name", "given-name", "mei", 
                       "personal_name", "user_name", "member_name", "first-name-kanji", "first_name_kanji", "given-name-kanji", "given_name_kanji"],
                "classes": ["firstname", "first-name", "input-first-name", "given", "mei",
                           "personal", "user", "member", "contact"],
                # 一般的なダミー例（太郎）を追加して分割姓名の検出力を向上
                "placeholders": ["名", "First Name", "Given Name", "お名前（名）", 
                               "ファーストネーム", "姓名の名", "Personal Name", "太郎"],
                "types": [],
                "tags": ["input"],
                "weight": 23,  # 会社名(25)、姓(24)に次ぐ高い優先度
                "strict_patterns": ["名", "firstname", "first_name", "given_name"],
                # 'name' 系の過剰除外は外す（分割フィールド name1/name2 を許容）
                "exclude_patterns": ["company", "会社", "社名", "corp", "corporation", "firm", "organization", "business", "LOGIN_ID", "PASSWORD", "OTP", "TOTP", "MFAOTP", "captcha", "login_id", "password", "signin", "auth", "verification", "mfa", "phone", "tel", "電話", "zip", "postal", "郵便", "address", "住所", "email", "mail", "メール", "subject", "件名", "message", "本文", "unified_name_field",
                                   # 〇〇名系（個人名ではない）
                                   "法人名", "団体名", "組織名", "部署名", "学校名", "店舗名", "病院名", "施設名", "会社名・団体名", "団体・組織名"]  # 統合氏名パターンを除外に追加
            },
            
            # カナ名フィールド (5-8) - 実システム必須
            
            # 5. 姓カナ (last_name_kana) - GAS/スプレッドシート必須 
        "姓カナ": {
                "names": ["last_name_kana", "lastname_kana", "last-name-kana", "kana_last", "katakana_sei", 
                         "セイ", "カナ姓", "sei_kana", "family_kana", "lastname_katakana", "lastkananame", "furigana"],
                "ids": ["lastname_kana", "last_name_kana", "last-name-kana", "kana-last", "katakana-sei", "sei", "lastname_katakana", "lastkananame", "furigana"],
                "classes": ["kana", "katakana", "sei", "kana-lastname", "lastname_kana", "lastname-katakana", "furigana"],
                "placeholders": ["セイ", "ヤマダ", "カタカナ", "Kana Last", "姓（カタカナ）", 
                               "フリガナ（姓）", "セイ（全角カタカナ）", "Lastname Kana"],
                "types": ["text"],
                "tags": ["input"],
                "weight": 18,
                "strict_patterns": ["セイ", "姓カナ", "sei_kana", "lastname_kana", "katakana_sei", "kana"],
                # かな判定: フィールド名に"kana"が含まれているかで判断
                "kana_indicator": ["kana", "カナ", "katakana", "furigana", "ruby"],
                # 除外パターンから漢字フィールドを強く除外
                "exclude_patterns": [
                    "company", "会社", "corp", "corporation", "firm", "organization", "business",
                    "phone", "tel", "電話", "zip", "postal", "郵便", "address", "住所", 
                    "email", "mail", "メール", "subject", "件名", "message", "本文", "漢字", 
                    "氏名", "市町村", "都道府県", "prefecture", "city", "unified_name_field",
                    # 認証/ログイン系の強い除外（一般化）
                    "LOGIN_ID", "login_id", "login", "signin", "sign_in", "auth", "authentication",
                    "PASSWORD", "password", "pass", "pswd",
                    "OTP", "TOTP", "MFAOTP", "mfa", "otp", "totp",
                    "captcha", "image_auth", "image-auth", "spam-block", "verify", "verification"
                ]  # カナフィールド名は除外しない
            },
            
            # 6. 名カナ (first_name_kana) - GAS/スプレッドシート必須
        "名カナ": {
                "names": ["first_name_kana", "firstname_kana", "first-name-kana", "kana_first", "katakana_mei", 
                         "メイ", "カナ名", "mei_kana", "given_kana", "firstname_katakana", "kana", "furigana"],
                "ids": ["firstname_kana", "first_name_kana", "first-name-kana", "kana-first", "katakana-mei", "mei", "firstname_katakana", "furigana"],
                "classes": ["kana", "katakana", "mei", "kana-firstname", "firstname_kana", "firstname-katakana", "furigana"],
                "placeholders": ["メイ", "タロウ", "カタカナ", "Kana First", "名（カタカナ）", 
                               "フリガナ（名）", "メイ（全角カタカナ）", "Firstname Kana"],
                "types": ["text"],
                "tags": ["input"],
                "weight": 12,
                "strict_patterns": ["メイ", "名カナ", "mei_kana", "firstname_kana", "katakana_mei", "kana"],
                "kana_indicator": ["kana", "カナ", "katakana", "furigana", "ruby"],
                # ふりがな系の属性名（furigana）は分割カナで一般的に使われるため除外しない
                "exclude_patterns": [
                    "company", "会社", "corp", "corporation", "firm", "organization", "business",
                    "phone", "tel", "電話", "zip", "postal", "郵便", "address", "住所", 
                    "email", "mail", "メール", "subject", "件名", "message", "本文", "漢字", 
                    "氏名", "市町村", "都道府県", "prefecture", "city", "town", "unified_name_field",
                    # 認証/ログイン系の強い除外（一般化）
                    "LOGIN_ID", "login_id", "login", "signin", "sign_in", "auth", "authentication",
                    "PASSWORD", "password", "pass", "pswd",
                    "OTP", "TOTP", "MFAOTP", "mfa", "otp", "totp",
                    "captcha", "image_auth", "image-auth", "spam-block", "verify", "verification"
                ]
            },
            
            # 7. 姓ひらがな (last_name_hiragana) - GAS必須
            "姓ひらがな": {
                "names": ["last_name_hiragana", "lastname_hiragana", "hiragana_last", 
                         "hiragana_sei", "ひらがな姓", "sei_hiragana", "姓ふりがな"],
                "ids": ["lastname_hiragana", "last_name_hiragana", "hiragana-last", "hiragana-sei"],
                "classes": ["hiragana", "sei", "hiragana-lastname", "lastname_hiragana"],
                "placeholders": ["せい", "やまだ", "ひらがな", "ふりがな", "姓ふりがな", "姓（ひらがな）", 
                               "ふりがな（姓）", "せい（ひらがな）"],
                "types": [],
                "tags": ["input"],
                "weight": 10,
                "exclude_patterns": ["zip", "postal", "郵便", "郵便番号", "postcode", "zipcode", "address", "住所", "addr", "phone", "tel", "電話", "fax", "ファックス", "your-fax", "your_fax", "company", "会社", "email", "mail", "メール", "katakana", "カタカナ", "kana"]  # 郵便番号・住所・電話・FAX・会社名・メールアドレス・カタカナフィールド除外（"your-name", "your_name", "name", "お名前", "fullname", "personal_name"を除外から削除、ひらがな専用として"katakana", "カタカナ", "kana"を除外追加）
            },
            
            # 8. 名ひらがな (first_name_hiragana) - GAS必須
            "名ひらがな": {
                "names": ["first_name_hiragana", "firstname_hiragana", "hiragana_first", 
                         "hiragana_mei", "ひらがな名", "mei_hiragana", "名ふりがな"],
                "ids": ["firstname_hiragana", "first_name_hiragana", "hiragana-first", "hiragana-mei"],
                "classes": ["hiragana", "mei", "hiragana-firstname", "firstname_hiragana"],
                "placeholders": ["めい", "たろう", "ひらがな", "ふりがな", "名ふりがな", "名（ひらがな）", 
                               "ふりがな（名）", "めい（ひらがな）"],
                "types": [],
                "tags": ["input"],
                "weight": 10,
                "exclude_patterns": ["company", "会社", "corp", "corporation", "firm", "organization", "business", "zip", "postal", "郵便", "address", "住所", "email", "mail", "メール", "phone", "tel", "電話", "fax", "ファックス", "unified_name_field", "来場", "人数", "大人", "子供", "年齢", "age", "visitor", "adult", "child", "建築", "エリア", "希望", "時間", "日時", "予約", "area", "time", "date", "reservation", "件名", "subject", "本文", "message", "content", "katakana", "カタカナ", "kana"]  # ひらがな専用として"katakana", "カタカナ", "kana"を除外追加、その他は第6サイクル誤マッピング防止: 来場人数・年齢・業務フィールド除外強化（"your-name", "your_name", "name", "お名前", "氏名", "fullname", "personal_name"を除外から削除）
            },
            
            # 送信者・役職情報 (9-11) - 実システム必須
            # 注意: 送信者氏名(form_sender_name)は廃止。姓名組み合わせを使用。
            
            # 10. 役職 (position) - GAS必須
            "役職": {
                "names": ["position", "job_position", "role", "役職", "職位", "yakushoku", 
                         "job_title", "post"],  # "title", "office" 除去（競合回避）
                "ids": ["position", "job_position", "role", "yakushoku", "job_title"],  # "title" 除去
                "classes": ["position", "job-position", "role", "yakushoku"],  # "title" 除去
                "placeholders": ["役職", "Position", "職位", "お役職", 
                               "Job Title", "部長・課長等", "役職名"],  # "Title" 除去
                "types": [],
                "tags": ["input", "select"],
                "weight": 11,
                "strict_patterns": ["役職", "職位", "position", "job_title", "yakushoku"],  # 厳密一致用
                "exclude_patterns": [
                    "your-name", "your_name", "name", "お名前", "fullname", "personal_name", "姓", "名", "lastname", "firstname",
                    "company", "会社", "email", "mail", "メール", "zip", "postal", "郵便", "address", "住所", "tel", "phone", "電話", "fax", "ファックス",
                    # 汎用除外: 『お問い合わせジャンル/種別/カテゴリ』系は役職ではない
                    "お問い合わせ", "お問合せ", "ジャンル", "種別", "カテゴリー", "カテゴリ", "category"
                ]  # 個人名・会社名・連絡先・住所・電話・FAX・郵便番号・問い合わせ分類系を除外
            },
            
            # 11. 性別 (gender) - GAS必須
            "性別": {
                "names": ["gender", "sex", "性別", "seibetsu", "male_female"],
                "ids": ["gender", "sex", "seibetsu", "male_female"],
                "classes": ["gender", "sex", "seibetsu"],
                "placeholders": ["性別", "Gender", "男性・女性", "選択してください", 
                               "Sex", "男女"],
                # 一般化改善: 性別はラジオボタンが主流のため優先（selectはtagsで担保）
                "types": ["radio"],
                "tags": ["select", "input"],
                "weight": 9,
                "exclude_patterns": [
                    "name", "your-name", "your_name", "お名前", "氏名", "fullname", "personal_name", "姓", "名", "lastname", "firstname", 
                    "company", "会社", "email", "mail", "メール", "tel", "phone", "電話", "address", "住所", "postal", "郵便", "zip",
                    # 明確に異なるフィールド
                    "年齢", "age", "歳",
                    # 連絡時間・時間帯に関するフィールド（性別ではない）
                    "希望連絡時間帯", "連絡時間", "時間帯", "contact_time", "time_of_connection", "person_time_of_connection"
                ]  # 個人名・会社名・連絡先・時間帯関連フィールド除外
            },
            
            # 連絡先情報 (12-13) - 最重要
            
            # 12. メールアドレス (email_1@email_2) - GAS/スプレッドシート必須・最重要
            "メールアドレス": {
                "names": [
                    "email", "mail", "e-mail", "メール", "メールアドレス", "e_mail",
                    "email_address", "mail_address", "contact_email", "email_1", "email_2",
                    "your-email", "your_email",
                    # 汎用追加: 英語圏CMS/国産CMSの表記ゆれ
                    "emailaddress", "mailaddress", "mailaddr", "mail_address", "emailaddress1", "emailaddress2",
                    "field_570741", "mcon", "tkem"
                ],  # tkem追加: formzuシステムの標準パターン
                "ids": [
                    "email", "mail", "e-mail", "email-address", "mail-address",
                    "contact-email", "email_1", "email_2", "tkem", "emailaddress", "mailaddress"
                ],
                "classes": ["email", "mail", "e-mail", "email-input", "wpcf7-email", "fldemail"],
                "placeholders": ["メール", "Email", "メールアドレス", "email@example.com", 
                               "連絡先メールアドレス", "your@email.com", "E-mail"],
                "types": ["email", "mail", "text"],  # 一部サイトの独自型 type="mail" を正式サポート
                "tags": ["input"],
                "weight": 22,  # 最重要
                # 文脈評価用の厳密パターン（DT/THラベル一致時のボーナス強化）
                "strict_patterns": ["メールアドレス", "メール", "Email", "E-mail", "email", "mail"],
                "exclude_patterns": [
                    "LOGIN_ID", "PASSWORD", "OTP", "TOTP", "MFAOTP", "captcha", "login_id", "password", "signin", "auth", "verification", "mfa",
                    "tkph", "phone", "tel", "電話", "check", "confirm", "確認",
                    # 検索・サイト内検索などの一般的な検索欄の除外（汎用）
                    "q", "search", "検索", "サイト内検索", "site-search", "keyword", "キーワード", "describe your issue",
                    # 連絡手段や連絡時間の自由記述欄（メールではない）
                    "その他の連絡方法", "連絡方法", "other_contact", "other_connection", "person_other_connection",
                    "希望連絡時間帯", "連絡時間", "時間帯", "contact_time", "time_of_connection", "person_time_of_connection"
                ]  # 認証/電話/確認/検索欄の除外
            },
            
            # 13. 企業URL (website_url) - スプレッドシート必須
            "企業URL": {
                "names": ["url", "website", "homepage", "企業URL", "サイト", "web", "site", 
                         "company_url", "website_url", "homepage_url"],
                "ids": ["url", "website", "homepage", "company-url", "site", "web", "website_url"],
                "classes": ["url", "website", "homepage", "site"],
                "placeholders": ["https://", "URL", "企業URL", "Website", "ホームページ", 
                               "会社サイト", "webサイト"],
                "types": ["url"],
                "tags": ["input"],
                "weight": 7
            },
            
            # 電話番号フィールド (14-16) - GAS/スプレッドシート必須
            
            # 14. 電話1 (phone_1) - 市外局番
            "電話1": {
                "names": ["phone_1", "phone1", "tel1", "tel_1", "電話1", "市外局番", "area_code",
                         "phone_area", "tel_area", "area"],
                "ids": ["phone_1", "phone1", "tel1", "tel-1", "area-code", "phone-area"],
                "classes": ["tel", "phone", "tel1", "area", "area-code", "phone_1"],
                "placeholders": ["03", "市外局番", "Area", "Phone1", "電話番号（市外局番）", 
                               "0X", "局番1"],
                "types": ["tel", "text"],
                "tags": ["input"],
                "weight": 12,
                "exclude_patterns": ["fax", "ファックス", "ファクス", "FAX", "Fax", "お名前", "氏名", "name", "fullname", "your_name", "personal_name", "姓", "名", "lastname", "firstname", "kana", "カナ", "フリガナ", "ふりがな", "katakana", "hiragana", "yomi", "読み", "郵便", "郵便番号", "postal", "zip", "postcode", "zipcode", "住所", "address", "addr", "市区町村", "都道府県", "丁目", "番地", "building", "年齢", "age", "来場", "人数", "大人", "子供", "adult", "child", "visitor", "建築", "エリア", "希望", "時間", "日時", "予約", "area", "time", "date", "reservation", "email", "mail", "メール", "件名", "subject", "本文", "message", "content", "captcha", "CAPTCHA", "送信確認", "確認", "verification", "verify", "security", "confirm", "validation", "code"]  # 誤マッピング防止: カナ/ふりがな/CAPTCHA/確認系除外
            },
            
            # 15. 電話2 (phone_2) - 市内局番
            "電話2": {
                "names": ["phone_2", "phone2", "tel2", "tel_2", "電話2", "局番", "exchange",
                         "phone_exchange", "tel_exchange", "local"],
                "ids": ["phone_2", "phone2", "tel2", "tel-2", "exchange", "phone-exchange"],
                "classes": ["tel", "phone", "tel2", "exchange", "local", "phone_2"],
                "placeholders": ["1234", "局番", "Exchange", "Phone2", "電話番号（市内局番）", 
                               "XXXX", "局番2"],
                "types": ["tel", "text"],
                "tags": ["input"],
                "weight": 12,
                "exclude_patterns": ["fax", "ファックス", "ファクス", "FAX", "Fax", "お名前", "氏名", "name", "fullname", "your_name", "personal_name", "姓", "名", "lastname", "firstname", "kana", "カナ", "フリガナ", "ふりがな", "katakana", "hiragana", "yomi", "読み", "郵便", "郵便番号", "postal", "zip", "postcode", "zipcode", "住所", "address", "addr", "市区町村", "都道府県", "丁目", "番地", "building", "年齢", "age", "来場", "人数", "大人", "子供", "adult", "child", "visitor", "建築", "エリア", "希望", "時間", "日時", "予約", "area", "time", "date", "reservation", "email", "mail", "メール", "件名", "subject", "本文", "message", "content", "captcha", "CAPTCHA", "送信確認", "確認", "verification", "verify", "security", "confirm", "validation", "code"]  # 誤マッピング防止: カナ/ふりがな/CAPTCHA/確認系除外
            },
            
            # 16. 電話3 (phone_3) - 番号
            "電話3": {
                "names": ["phone_3", "phone3", "tel3", "tel_3", "電話3", "tel_subscriber",
                         "phone_number", "tel_number", "subscriber"],  # "番号", "number" 除去（汎用すぎ）
                "ids": ["phone_3", "phone3", "tel3", "tel-3", "phone-number", "tel-number"],
                "classes": ["tel", "phone", "tel3", "subscriber", "phone_3"],  # "number" 除去
                "placeholders": ["5678", "Phone3", "電話番号（番号）", 
                               "XXXX", "番号3", "下4桁"],  # "番号", "Number" 除去
                "types": ["tel", "text"],
                "tags": ["input"],
                "weight": 12,
                "strict_patterns": ["phone_3", "tel_3", "tel3", "subscriber"],  # 厳密一致用
                "exclude_patterns": ["fax", "ファックス", "ファクス", "FAX", "Fax", "お名前", "氏名", "name", "fullname", "your_name", "personal_name", "姓", "名", "lastname", "firstname", "kana", "カナ", "フリガナ", "ふりがな", "katakana", "hiragana", "yomi", "読み", "郵便", "郵便番号", "postal", "zip", "postcode", "zipcode", "住所", "address", "addr", "市区町村", "都道府県", "丁目", "番地", "building", "年齢", "age", "来場", "人数", "大人", "子供", "adult", "child", "visitor", "建築", "エリア", "希望", "時間", "日時", "予約", "area", "time", "date", "reservation", "email", "mail", "メール", "件名", "subject", "本文", "message", "content", "captcha", "CAPTCHA", "送信確認", "確認", "verification", "verify", "security", "confirm", "validation", "code"]  # 誤マッピング防止: カナ/ふりがな/CAPTCHA/確認系除外
            },
            
            # 統合電話番号フィールド（単一フィールド）
            "電話番号": {
                "names": ["tel", "phone", "電話", "電話番号", "telephone", "mobile", "phone_number", 
                         "tel_number", "contact", "連絡先", "携帯", "phone_no", "tel_no", 
                         "連絡先電話番号", "連絡先電話", "連絡先tel", "contact_phone", "contact_tel"],
                "ids": ["tel", "phone", "電話", "telephone", "mobile", "phone-number", 
                       "tel-number", "contact", "phone_no", "tel_no"],
                "classes": ["tel", "phone", "telephone", "mobile", "contact"],
                "placeholders": ["電話番号", "Phone", "Tel", "Telephone", "連絡先", "携帯番号", 
                               "03-1234-5678", "090-1234-5678", "固定電話", "携帯電話", 
                               "連絡先電話番号", "連絡先電話", "Contact Phone"],
                "types": ["tel", "text"],
                "tags": ["input"],
                "weight": 15,
                "strict_patterns": ["tel", "phone", "電話", "電話番号", "telephone", "連絡先電話番号"],  # 厳密一致用
                "exclude_patterns": ["fax", "ファックス", "ファクス", "FAX", "Fax", "your-name", "your_name", "name", "お名前", "氏名", "captcha", "CAPTCHA", "送信確認", "verification", "verify", "security", "confirm", "validation", "code", "fullname", "personal_name", "姓", "名", "lastname", "firstname", "subject", "件名", "タイトル", "topic", "title", "mail_subject", "email_subject", "inquiry_subject", "contact_subject", "mail", "email", "メール", "mailcheck", "mail_check", "mail-check", "email_check", "email-check", "confirm_mail", "confirm_email", "確認用メール", "メールアドレス確認", "postal", "zip", "郵便", "郵便番号", "post", "postcode", "zipcode", "住所", "address", "addr", "市区町村", "都道府県", "丁目", "番地", "building", "年齢", "age", "来場", "人数", "大人", "子供", "adult", "child", "visitor", "建築", "エリア", "希望", "時間", "日時", "予約", "area", "time", "date", "reservation", "本文", "message", "content"]  # 第6サイクル誤マッピング防止: 住所・年齢・人数・業務フィールド除外追加
            },
            
            # 住所フィールド (17-19) - GAS/スプレッドシート必須

            # 16.5. 郵便番号（統合） - 単一入力の郵便番号
            # 分割(郵便番号1/2) が存在する場合はそちらが優先されるが、
            # 単一フィールドのみのサイトも多いため統合ラベルを定義
            "郵便番号": {
                "names": [
                    "zip", "postal", "postcode", "zipcode", "郵便番号", "postal_code", "post_code"
                ],
                "ids": [
                    "zip", "postal", "postcode", "zipcode", "postal_code", "post_code"
                ],
                "classes": ["zip", "postal", "postcode", "zipcode"],
                "placeholders": ["郵便番号", "〒", "Zip", "Post"],
                "types": ["tel", "text"],
                "tags": ["input"],
                "weight": 12,
                # 明確に別種のフィールドは除外
                "exclude_patterns": [
                    "address", "住所", "addr", "street", "building", "市区町村", "都道府県", "prefecture",
                    "phone", "tel", "電話", "fax", "メール", "email", "mail",
                    "your-name", "your_name", "name", "お名前", "氏名", "fullname",
                    "captcha", "verification", "confirm", "確認", "code"
                ]
            },
            
            # 17. 郵便番号1 (postal_code_1) - 前3桁
            "郵便番号1": {
                "names": [
                    "postal_code_1", "zip1", "postal1", "post1", "郵便1", "郵便番号1",
                    "zipcode1", "postcode1", "zip_code1",
                    # 左右分割系の一般的命名
                    "zip_left", "postal_left", "post_left", "zipcode_left", "postcode_left"
                ],
                "ids": [
                    "postal_code_1", "zip1", "postal1", "post-1", "zipcode1", "postcode1",
                    # 左右分割系の一般的命名（id）
                    "zip_left", "postal_left", "post_left", "zipcode_left", "postcode_left"
                ],
                "classes": ["zip", "postal", "zip1", "postcode", "zipcode", "postal_code_1", "zip_left"],
                "placeholders": ["123", "郵便番号", "Zip1", "Post1", "〒前3桁", "郵便番号（前3桁）", "XXX"],
                "types": ["text"],
                "tags": ["input"],
                # 分割を優先するため、統合より高い重みを設定
                "weight": 12,
                "exclude_patterns": [
                    "address", "住所", "addr", "street", "building", "番地", "市区町村", "都道府県", "prefecture", "city", "town",
                    "fax", "ファックス", "your-fax", "your_fax", "phone", "tel", "電話", "company", "会社",
                    "kana", "カナ", "フリガナ", "ふりがな", "katakana", "hiragana", "yomi", "読み",
                    "your-name", "your_name", "name", "お名前", "store", "支店", "部署", "department", "your-store", "your_store", "shop",
                    "来場", "人数", "大人", "子供", "年齢", "age", "visitor", "adult", "child", "建築", "エリア", "希望", "時間", "日時", "予約", "area", "time", "date", "reservation",
                    "件名", "subject", "本文", "message", "content", "email", "mail", "メール", "captcha", "CAPTCHA", "送信確認", "確認", "verification", "verify", "security", "confirm", "validation", "code"
                ]  # 誤マッピング防止: カナ/ふりがな/CAPTCHA/確認系除外
            },
            
            # 18. 郵便番号2 (postal_code_2) - 後4桁
            "郵便番号2": {
                "names": [
                    "postal_code_2", "zip2", "postal2", "post2", "郵便2", "郵便番号2",
                    "zipcode2", "postcode2", "zip_code2",
                    # 左右分割系の一般的命名
                    "zip_right", "postal_right", "post_right", "zipcode_right", "postcode_right"
                ],
                "ids": [
                    "postal_code_2", "zip2", "postal2", "post-2", "zipcode2", "postcode2",
                    # 左右分割系の一般的命名（id）
                    "zip_right", "postal_right", "post_right", "zipcode_right", "postcode_right"
                ],
                "classes": ["zip", "postal", "zip2", "postcode", "zipcode", "postal_code_2", "zip_right"],
                "placeholders": ["4567", "郵便番号", "Zip2", "Post2", "〒後4桁", "郵便番号（後4桁）", "XXXX"],
                "types": ["text"],
                "tags": ["input"],
                # 分割を優先するため、統合より高い重みを設定
                "weight": 12,
                "exclude_patterns": [
                    "address", "住所", "addr", "street", "building", "番地", "市区町村", "都道府県", "prefecture", "city", "town",
                    "fax", "ファックス", "your-fax", "your_fax", "phone", "tel", "電話", "company", "会社",
                    "kana", "カナ", "フリガナ", "ふりがな", "katakana", "hiragana", "yomi", "読み",
                    "your-name", "your_name", "name", "お名前", "store", "支店", "部署", "department", "your-store", "your_store", "shop",
                    "captcha", "CAPTCHA", "送信確認", "確認", "verification", "verify", "security", "confirm", "validation", "code"
                ]  # 住所・FAX・電話・会社名・カナ/ふりがな・個人名・部署/支店・CAPTCHA/確認系除外
            },
            
            # 19. 住所 (address_1-5統合) - GAS/スプレッドシート必須 (address_1-4必須、address_5任意)
            "住所": {
                "names": ["address", "住所", "所在地", "address_1", "address_2", "address_3", "address_4", 
                         "address_5", "都道府県", "市区町村", "番地", "建物", "street", 
                         "prefecture", "city", "building", "street_address", "区", "市区",
                         "ビル", "部屋番号", "room", "apt", "apartment", "マンション", "town",
                         "addr", "pref", "city_name", "town_name", "building_name"],
                "ids": ["address", "address_1", "address_2", "address_3", "address_4", "address_5",
                       "prefecture", "city", "street", "building", "pref", "addr", "room", 
                       "apt", "apartment", "building_name"],
                "classes": ["address", "prefecture", "city", "street", "building", "addr", "pref"],
                "placeholders": ["住所", "Address", "都道府県", "市区町村", "番地・建物名", 
                               "東京都新宿区", "1-1-1", "Street Address", "区・町名", "番地",
                               "建物名・部屋番号", "ビル・マンション名", "部屋番号"],
                "types": ["text"],
                "tags": ["input", "select"],  # 都道府県はselectが多い
                "weight": 13,
                "exclude_patterns": ["your-name", "your_name", "name", "お名前", "氏名", "fullname", 
                                   "first_name", "last_name", "姓", "名", "company", "会社", 
                                   "corp", "corporation", "phone", "tel", "電話", "email", "mail", 
                                   "メール", "subject", "件名", "message", "本文", "kana", "カナ", 
                                   "フリガナ", "katakana", "hiragana", "ふりがな", "zip", "postal", 
                                   "郵便番号"]  # セマンティック除外強化: 郵便番号を除外追加
            },
            
        # 20. お問い合わせ本文 (message) - 最重要
        "お問い合わせ本文": {
                "names": [
                    "message", "inquiry_body", "inquiry_content", "本文", "メッセージ",
                    "comment", "inquiry_message", "contact_message", "message_body",
                    "ご質問・ご要望", "ご質問", "ご要望", "question", "request", "お問い合わせ内容",
                    "inquiry", "content", "details", "詳細", "備考", "remarks", "remark",
                    # 英語圏CMSで多い命名
                    "note", "notes",
                    # 汎用追加: よくある言い換え
                    "ご意見", "ご感想", "ご相談内容", "ご連絡内容", "お問い合わせの内容",
                    # 表記ゆれ対策（お問い/お問/問合せ）
                    "お問合せ", "お問合わせ", "お問合せ内容", "お問合わせ内容", "お問い合わせ", "otoiawase",
                    # 汎用追加: 単純な 'body' キー（日本語フォームでも広く使用）
                    "body"
                ],  # ご質問・ご要望パターン追加（汎用語 'text' は除外）
                "ids": [
                    "message", "inquiry-body", "contact-message", "comment", "inquiry_content",
                    "question", "request", "inquiry", "content", "details", "body"
                ],  # 汎用語 'text' を除外し誤検出抑制
                "classes": [
                    "message", "inquiry", "comment", "inquiry-content", "contact-message",
                    "question", "request", "content", "details"
                ],  # 対応するclassパターン追加
                "placeholders": [
                    "本文", "Message", "お問い合わせ内容", "メッセージ",
                    "詳細内容", "ご質問・ご相談内容", "お問い合わせの詳細", "お問い合わせ本文",
                    "ご質問・ご要望", "ご質問", "ご要望", "Content", "Details", "備考",
                    # 汎用追加: よくあるフォームの文言
                    "お問い合わせ内容をご記入ください", "ご相談内容", "ご意見・ご要望", "お問い合わせの内容",
                    # 表記ゆれ対策
                    "お問合せ内容", "お問合わせ内容"
                ],  # 対応するplaceholder追加
                # 備考/メッセージ欄が input[type="text"] の実装も一定数存在
                # 誤検出抑制は name/id/placeholder/コンテキストのスコアで担保する
                "types": ["text"],
                "tags": ["textarea", "input"],
                "weight": 20,  # 最重要
                "strict_patterns": ["本文", "メッセージ", "inquiry", "message", "comment", "ご質問・ご要望", "ご質問", "ご要望", "お問合せ", "お問合わせ", "お問い合わせ", "ご相談内容"],  # 厳密一致用に追加
                # 認証/検索/ログイン関連のフィールドに誤割当てしないための除外
                # 汎用改善: 問い合わせ本文はログイン/認証/検索には絶対にマッピングしない
                "exclude_patterns": [
                    "LOGIN_ID", "login_id", "login", "signin", "sign_in", "auth", "authentication",
                    "PASSWORD", "password", "pass", "pswd",
                    "OTP", "TOTP", "MFAOTP", "mfa", "otp", "totp",
                    "captcha", "image_auth", "image-auth", "spam-block", "verify", "verification",
                    "q", "search", "検索", "site-search", "keyword"
                ]
            },
            
        # 21. 件名 (subject) - targeting情報・重要度高
        "件名": {
            "names": ["subject", "inquiry_subject", "件名", "タイトル", "表題", "topic", "heading", 
                     "inquiry_title", "contact_subject", "subject_line", "your-subject", "your_subject",
                     "mail_subject", "email_subject", "inquiry_topic", "contact_topic"],
                "ids": ["subject", "topic", "heading", "inquiry-title", "contact-subject", "inquiry_subject",
                       "mail_subject", "email_subject", "inquiry_topic"],
                "classes": ["subject", "topic", "heading", "inquiry-title", "contact-subject", "subject-line",
                           "mail-subject", "email-subject"],
                "placeholders": ["件名", "Subject", "タイトル", "お問い合わせ件名", 
                               "Topic", "表題", "Subject Line", "問い合わせ件名", "Mail Subject"],
                "types": [],
                "tags": ["input"],
            "weight": 19,  # 重要度向上
            "strict_patterns": ["件名", "subject", "inquiry_subject", "contact_subject", "タイトル", "topic"],
            # 誤マッピング防止: 採用系やプロフィール系の『Job Title/役職』は除外
            "exclude_patterns": [
                "your-name", "your_name", "name", "お名前", "氏名", "fullname",
                "company", "会社", "corp", "corporation",
                "zip", "postal", "郵便", "address", "住所",
                "email", "mail", "メール",
                "市町村", "都道府県", "prefecture", "city", "town",
                "kana", "カナ", "フリガナ", "katakana", "hiragana",
                "番地", "建物", "street", "building",
                "tkph", "tkna", "tkad",
                "phone", "tel", "電話", "telephone", "mobile", "連絡先",
                # ここから役職関連の明確な除外ワード
                "job title", "job_title", "job-title", "position", "role", "yakushoku", "役職", "職位", "post"
            ]  # 件名と役職（Job Title）の混同を防止
        },
            
            # 郵便番号統合フィールド（単一フィールド）
            "郵便番号": {
                "names": ["郵便番号", "zip", "postal", "postcode", "zipcode", "郵便", 
                         "postal_code", "zip_code", "post_code", "〒"],
                "ids": ["zip", "postal", "postcode", "zipcode", "郵便番号", "postal_code", 
                       "zip_code", "post_code"],
                "classes": ["zip", "postal", "postcode", "zipcode"],
                "placeholders": ["郵便番号", "0000000", "123-4567", "〒", "Postal Code", 
                               "Zip Code", "Post Code", "1234567"],
                "types": ["tel", "text"],  # type="tel"も対象に含める
                "tags": ["input"],
                # 分割（郵便番号1/2）を優先するため統合は相対的に低くする
                "weight": 8,
                "strict_patterns": ["郵便番号", "zip", "postal", "postcode", "zipcode"],
                "exclude_patterns": [
                    "address", "住所", "addr", "street", "building",
                    "phone", "tel", "電話", "fax", "ファックス",
                    "your-name", "your_name", "name", "お名前", "fullname",
                    "email", "mail", "メール", "company", "会社",
                    # 認証/確認系（誤検出抑止：CAPTCHA/OTP/トークン等）
                    "captcha", "image_auth", "image-auth", "spam-block",
                    "token", "otp", "totp", "mfa", "verification", "verify", "confirm", "確認"
                ]
                # 「郵便番号」自体は除外パターンから削除
            },
            
            # 21b. 都道府県 (prefecture) - 住所内の選択式（select優先）
            "都道府県": {
                "names": ["pref", "prefecture", "todouhuken", "todofuken", "都道府県", "region"],
                "ids": ["pref", "prefecture", "region", "p-region"],
                "classes": ["pref", "prefecture", "p-region", "region"],
                "placeholders": ["都道府県", "Prefecture"],
                "types": ["text"],  # 一部フォームでは text input を使用
                "tags": ["select", "input"],  # select 以外に input も許容
                "weight": 14,
                "strict_patterns": ["都道府県", "prefecture", "pref"],
                "exclude_patterns": [
                    # 人名/連絡先/件名・本文など明確に別物
                    "name", "お名前", "氏名", "fullname", "kana", "カナ", "フリガナ", "ふりがな",
                    "email", "mail", "メール", "phone", "tel", "電話", "zip", "postal", "郵便",
                    "件名", "subject", "message", "本文",
                    # 住所系でも都道府県以外のパーツを強く除外
                    "address", "addr", "street", "building", "apartment", "room", "号室",
                    "address1", "address_1", "address2", "address_2", "address3", "address_3",
                    "address4", "address_4", "address5", "address_5",
                    "city", "ward", "区", "市", "町", "村", "丁目", "番地"
                ]
            },

            # 22. 会社名カナ (company_name_kana) - GAS必須
            "会社名カナ": {
                "names": [
                    "company_kana", "corp_kana", "company_name_kana", "corporation_kana",
                    "会社名カナ", "会社カナ", "kaisha_kana", "kaisya_kana", "firm_kana",
                    # 一般的な別名
                    "company_ruby", "corp_ruby", "organization_ruby", "org_ruby",
                    "company_furigana", "corporation_furigana", "org_furigana"
                ],
                "ids": [
                    "company_kana", "corp_kana", "company-name-kana", "corporation-kana",
                    "company_name_kana", "company_ruby", "corp_ruby", "organization_ruby", "org_ruby",
                    "company_furigana"
                ],
                "classes": ["company-kana", "corp-kana", "kana", "ruby", "furigana", "company_name_kana"],
                "kana_indicator": ["kana", "カナ", "katakana", "furigana", "ruby", "ルビ", "るび"],
                "placeholders": [
                    "会社名カナ", "カブシキガイシャ", "Company Kana", "コーポレーション",
                    "法人名（カタカナ）", "会社名（カタカナ）", "組織名カナ",
                    # 追加: 会社名または氏名(ふりがな)
                    "会社名または氏名(ふりがな)", "会社名または氏名（ふりがな）", "会社名または氏名 カナ",
                ],
                "types": [],
                "tags": ["input"],
                "weight": 12,
                "strict_patterns": ["会社名カナ", "会社カナ", "company_kana", "company_name_kana"],  # 厳密一致用
                "exclude_patterns": [
                    "zip", "postal", "郵便", "郵便番号", "postcode", "zipcode", "address", "住所", "addr",
                    "phone", "tel", "電話", "fax", "ファックス",
                    "your-name", "your_name", "name", "お名前", "fullname", "personal_name",
                    "email", "mail", "メール",
                    # 汎用改善: カナ欄の誤検出抑止（文脈）
                    "団体", "団体名", "会社名・団体名"
                ]  # 郵便/住所/電話/個人名/メール + 団体関連の文脈を除外
            }
        }
    
    def get_all_field_names(self) -> List[str]:
        """全フィールド名のリストを取得"""
        return list(self.patterns.keys())
    
    def is_high_priority_field(self, field_name: str) -> bool:
        """高優先度フィールド判定（weight >= 15）"""
        pattern = self.get_pattern(field_name)
        return pattern.get('weight', 0) >= 15
    
    def get_email_fields(self) -> List[str]:
        """メール関連フィールドを取得"""
        return [name for name, pattern in self.patterns.items() 
                if 'email' in pattern.get('types', [])]
    
    def get_tel_fields(self) -> List[str]:
        """電話関連フィールドを取得"""
        return [name for name, pattern in self.patterns.items() 
                if 'tel' in pattern.get('types', [])]
    
    def get_textarea_fields(self) -> List[str]:
        """textarea関連フィールドを取得"""
        return [name for name, pattern in self.patterns.items() 
                if 'textarea' in pattern.get('tags', [])]
    
    def get_select_fields(self) -> List[str]:
        """select関連フィールドを取得"""
        return [name for name, pattern in self.patterns.items() 
                if 'select' in pattern.get('tags', [])]
    
    def get_unified_name_patterns(self) -> List[str]:
        """統一名前フィールドのパターンを取得"""
        return [
            "your-name", "your_name", "yourname",
            "full-name", "full_name", "fullname",
            "name", "名前", "お名前", "氏名", "姓名",
            "user-name", "username", "member-name"
        ]

    def get_unified_field_patterns(self) -> Dict[str, List[str]]:
        """統合フィールドのパターンを辞書で取得"""
        return {
            'fullname': ['fullname', 'full_name', '氏名', 'name', 'お名前', 'ご氏名', 'namae', 'your-name', 'your_name'],
            'kana_unified': ['kana', 'katakana', 'カナ', 'カタカナ', 'フリガナ'],
            'hiragana_unified': ['furigana', 'hiragana', 'ひらがな', 'ふりがな'],
            'phone_unified': ['phone', 'tel', '電話番号', '電話', 'telephone'],
            'address_unified': ['address', '住所', 'addr', '所在地', 'full_address'],
            'zipcode_unified': ['zipcode', 'postal_code', '郵便番号', 'zip', 'postcode']
        }
