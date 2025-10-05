"""
要素スコアリングシステム

HTMLフォーム要素の6属性による重み付けスコアリング機能
参考: ListersForm復元システムのSYSTEM.md 1.2節
"""

import re
import logging
from typing import Dict, List, Any, Optional, Tuple
import unicodedata
from playwright.async_api import Locator

logger = logging.getLogger(__name__)


class ElementScorer:
    """HTML要素のスコア計算クラス"""

    # 単純化された配点表（基本属性マッチング重視）
    SCORE_WEIGHTS = {
        "type": 100,  # type属性マッチ（最高優先度）
        "dt_context": 80,  # DTラベル・コンテキストマッチ（第2優先）
        "name": 60,  # name属性マッチ（第3優先）
        "id": 60,  # id属性マッチ（第3優先）
        "tag": 50,  # tagName マッチ
        "placeholder": 40,  # placeholder属性マッチ
        "class": 30,  # class属性マッチ
        "japanese_morphology": 25,  # 日本語形態素（placeholder等の語彙一致の加点）
        "visibility_penalty": -200,  # 非表示要素ペナルティ（強化）
    }

    # 位置ベース（position_* / nearby / parent_element 等）のコンテキストのみで
    # 過剰に昇格させないための得点上限（将来拡張可能）
    POSITION_BASED_SCORE_LIMITS = {
        "郵便番号": 40,
        # '電話番号': 35,  # 拡張時の例
    }

    # 個人名ではない「〇〇名」を一括で検出するための正規表現
    NON_PERSONAL_NAME_PATTERN = re.compile(
        "("
        "会社名|法人名|団体名|組織名|部署名|学校名|店舗名|病院名|施設名|"
        "建物名|マンション名|ビル名|邸名|棟名|館名|校名|園名|"
        "商品名|品名|製品名|サービス名|プロジェクト名|"
        "件名|題名|書名|名称"
        ")",
        re.IGNORECASE,
    )

    # 誤検出を招きやすい曖昧トークン（語境界必須）
    AMBIGUOUS_TOKENS = {"firm", "corp", "org"}

    # セキュリティ観点で class 除外を強く効かせたい短語（ハイフン/アンダースコア境界を対象）
    CRITICAL_CLASS_EXCLUDE_TOKENS = {
        "auth",
        "login",
        "signin",
        "otp",
        "mfa",
        "totp",
        "password",
        "verify",
        "verification",
        "token",
        "captcha",
        "confirm",
        "confirmation",
        "confirm_email",
        "email_confirmation",
        # 追加: 5-7 文字の重要短語や広く使われる用語
        "csrf",
        "session",
        # 追加: 罠/スパム対策系の一般的クラス名（誤入力抑止）
        "honeypot",
        "trap",
        "botfield",
        "no-print",
        "noprint",
        "hidden",
    }

    # 罠フィールドの代表トークン（name/id/classに対して語単位で高速判定）
    TRAP_TOKENS_SET = frozenset({
        "honeypot", "honey", "trap", "botfield", "no-print", "noprint"
    })

    # 長語のしきい値（class 部分一致を許可する長さ）
    # 根拠: 実フォーム観察で security-critical な長語（verification/password/authentication 等）が
    # 8文字以上に分布しており、false positive を最小化しつつ検出力を確保できる経験値。
    # なお、より厳密にしたい場合は設定化の候補（現状はコード定数で運用）。
    LONG_EXCLUDE_LENGTH = 8

    # 汎用減点のバイパスを許可するフィールド（class 主導が妥当な代表）
    CLASS_BYPASS_WHITELIST_FIELDS = {
        "統合氏名",
        "姓",
        "名",
        "統合氏名カナ",
        "姓カナ",
        "名カナ",
        "会社名",
        "メールアドレス",
        "電話番号",
        "お問い合わせ本文",
    }

    # 正規化キャッシュの最大件数（性能/メモリのバランス）
    NORM_CACHE_MAX_SIZE = 4096

    def __init__(
        self,
        context_extractor=None,
        shared_cache: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        """
        スコア計算器の初期化

        Args:
            context_extractor: 共有ContextTextExtractorインスタンス（パフォーマンス最適化）
        """
        self._context_extractor = context_extractor
        # RuleBasedAnalyzer 由来の共有要素キャッシュ（str(locator) -> 属性辞書）
        self._shared_cache = shared_cache
        # 日本語形態素解析用の基本パターン（簡略版）
        # フィールド名ベースの語彙集合（placeholder等の曖昧表現を拾うための軽量辞書）
        # 汎用性重視: 日本の代表的な表記ゆれ・言い換えを幅広くカバー
        self.japanese_patterns = {
            "会社名": ["会社", "企業", "法人", "団体", "組織", "社名", "会社名", "所属", "ご所属", "所属先", "ご所属先"],
            "メールアドレス": [
                "メール",
                "メールアドレス",
                "mail",
                "email",
                "e-mail",
                "アドレス",
            ],
            "姓": ["姓", "苗字", "名字", "せい", "みょうじ"],
            "名": ["名", "名前", "めい"],
            # 統合氏名（担当者系の言い換えを広く受ける）
            "統合氏名": ["氏名", "お名前", "姓名", "フルネーム", "担当者", "担当者名", "ご担当者名"],
            "姓ひらがな": ["ひらがな", "せい", "姓"],
            "名ひらがな": ["ひらがな", "めい", "名"],
            # 『携帯』はモバイル番号専用ラベルに引っ張られやすいため除外
            "電話番号": ["電話", "電話番号", "tel", "phone", "連絡先"],
            "住所": ["住所", "所在地", "じゅうしょ", "都道府県", "市区町村"],
            "件名": ["件名", "タイトル", "表題", "用件"],
            "お問い合わせ本文": [
                "お問い合わせ",
                "問い合わせ",
                "本文",
                "メッセージ",
                "内容",
                "ご相談",
                "ご要望",
                "ご質問",
            ],
            # 役職系（Job Title）
            "役職": ["役職", "職位", "job title", "job", "position", "role"],
        }

        # 重要短語の境界一致正規表現を事前コンパイル（ホットパス最適化）
        try:
            self._critical_boundary_regex = {
                ep: re.compile(rf"(^|[-_]){re.escape(ep)}($|[-_])")
                for ep in self.CRITICAL_CLASS_EXCLUDE_TOKENS
            }
        except Exception:
            self._critical_boundary_regex = {}

        # CJK検出のための正規表現を事前コンパイル（ホットパス最適化）
        # ひらがな/カタカナ/CJK統合漢字/半角カナ
        try:
            self._cjk_re = re.compile(r"[\u3040-\u30ff\u3400-\u9fff\uff66-\uff9f]")
        except Exception:
            # フォールバック（コンパイル失敗時は None）
            self._cjk_re = None

        # 軽量正規化キャッシュ（ホットパス最適化用）。
        # キー: 元文字列、値: NFKC+lower 文字列。サイズ上限を超えたらクリアする簡易方式。
        self._norm_cache: Dict[str, str] = {}

        # email/phone 等の構造的プレースホルダー用パターン（高速化のため事前コンパイル）
        try:
            # RFC準拠までは厳密にせず、実務上の判定（*@*.* を包含）
            self._email_like_re = re.compile(
                r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
            )
        except Exception:
            self._email_like_re = None
        try:
            # 日本の電話番号例を広く許容（記号・国番号・桁区切りを許容）
            self._phone_like_re = re.compile(
                r"^(?:\+?\d{1,3}[-.\s]?)?(?:\d{2,4}[-.\s]?){2,4}\d{2,4}$"
            )
        except Exception:
            self._phone_like_re = None

    def _has_cjk(self, s: str) -> bool:
        """日本語(CJK)文字を含むかの軽量判定（ユーティリティへ委譲）。"""
        try:
            from .text_utils import has_cjk as _has_cjk_util

            return _has_cjk_util(s)
        except Exception:
            # 従来のフォールバック（互換維持）
            try:
                if not s:
                    return False
                if self._cjk_re is None:
                    return bool(re.search(r"[\u3040-\u30ff\u3400-\u9fff\uff66-\uff9f]", s))
                return self._cjk_re.search(s) is not None
            except Exception:
                return False

    def _should_bypass_generic_text_penalty(
        self, field_name: str, score_details: Dict[str, Any]
    ) -> bool:
        """汎用減点をバイパスすべきか（可読性向上のため分離）。

        - class一致が十分（class満点）かつ、ホワイトリスト化されたコア項目のみ許可。
        """
        try:
            class_score = int(score_details.get("score_breakdown", {}).get("class", 0))
            has_sufficient_class = class_score >= int(
                self.SCORE_WEIGHTS.get("class", 30)
            )
            is_whitelisted = field_name in self.CLASS_BYPASS_WHITELIST_FIELDS
            return has_sufficient_class and is_whitelisted
        except Exception:
            return False

    def _contains_token_with_boundary(self, text: str, token: str) -> bool:
        """語境界を考慮した包含判定（ユーティリティへ委譲）。"""
        try:
            from .text_utils import contains_token_with_boundary as _ctwb

            return _ctwb(text, token)
        except Exception as e:
            logger.debug(f"boundary match failed: {e}")
            # 互換フォールバック（case-insensitive の部分一致）
            return (token or "").lower() in (text or "").lower()

    def _normalize(self, s: str) -> str:
        """比較用の正規化: NFKC + lower（全角/半角差異を吸収）。簡易キャッシュ付き。"""
        try:
            key = s or ""
            v = self._norm_cache.get(key)
            if v is not None:
                return v
            v = unicodedata.normalize("NFKC", key).lower()
            # 簡易サイズ制御（過剰成長抑制）
            if len(self._norm_cache) > self.NORM_CACHE_MAX_SIZE:
                self._norm_cache.clear()
            self._norm_cache[key] = v
            return v
        except Exception:
            try:
                key = s or ""
                v = (key).lower()
                if len(self._norm_cache) > self.NORM_CACHE_MAX_SIZE:
                    self._norm_cache.clear()
                self._norm_cache[key] = v
                return v
            except Exception:
                return ""

    async def calculate_element_score(
        self, element: Locator, field_patterns: Dict[str, Any], field_name: str
    ) -> Tuple[int, Dict[str, Any]]:
        """
        要素のスコア計算（詳細情報付き）

        Args:
            element: Playwright要素
            field_patterns: フィールドパターン辞書
            field_name: フィールド名

        Returns:
            tuple: (合計スコア, スコア詳細情報)
        """
        score_details = {
            "total_score": 0,
            "score_breakdown": {},
            "matched_patterns": [],
            "element_info": {},
            "penalties": [],
        }

        try:
            # 要素の基本情報を取得（並列グループ情報は今回未使用）
            element_info = await self._get_element_info(element)
            score_details["element_info"] = element_info

            # カナ/ふりがな系の特別処理（誤マッピング抑止を強化）
            element_name = (element_info.get("name", "") or "").lower()
            element_id = (element_info.get("id", "") or "").lower()
            element_class = (element_info.get("class", "") or "").lower()
            element_placeholder = (element_info.get("placeholder", "") or "").lower()

            def _is_kana_like_text(t: str) -> bool:
                if not t:
                    return False
                t = t.lower()
                kana_tokens = [
                    "kana",
                    "katakana",
                    "hiragana",
                    "furigana",
                    # 追加: 日本語フォームで『ふりがな』を意味する一般的な別名
                    # 多くの実サイトで id/name/class に 'ruby' を用いるため、
                    # 誤検出抑止のための他ロジックと併用して包含
                    "ruby",
                    "ルビ",
                    "るび",
                    "ｶﾅ",
                    "ｶﾀｶﾅ",
                    "ﾌﾘｶﾞﾅ",
                    "カナ",
                    "カタカナ",
                    "フリガナ",
                    "ふりがな",
                    "ひらがな",
                    "読み",
                    "よみ",
                ]
                return any(tok in t for tok in kana_tokens)

            def _is_hiragana_like_text(t: str) -> bool:
                if not t:
                    return False
                t = t.lower()
                hiragana_tokens = [
                    "hiragana",
                    "ひらがな",
                ]
                return any(tok in t for tok in hiragana_tokens)

            def _is_katakana_like_text(t: str) -> bool:
                if not t:
                    return False
                t = t.lower()
                katakana_tokens = [
                    "katakana",
                    # 追加: ルビ指定もカタカナ欄として扱うケースが多い
                    "ruby",
                    "カタカナ",
                    "ｶﾀｶﾅ",
                    "カナ",
                    "ｶﾅ",
                    "ルビ",
                    "るび",
                    "フリガナ",
                    "ふりがな",
                ]  # 『フリガナ/ふりがな』は多くがカタカナ指定として扱う
                return any(tok.lower() in t for tok in katakana_tokens)

            def _field_is_kana_like(name: str, patterns: Dict[str, Any]) -> bool:
                try:
                    if any(k in name for k in ["カナ", "ひらがな", "ふりがな"]):
                        return True
                    for lst_key in ("names", "placeholders", "ids", "classes"):
                        for v in patterns.get(lst_key, []) or []:
                            if _is_kana_like_text(str(v)):
                                return True
                except Exception:
                    pass
                return False

            has_kana_in_element = (
                _is_kana_like_text(element_name)
                or _is_kana_like_text(element_id)
                or _is_kana_like_text(element_class)
                or _is_kana_like_text(element_placeholder)
            )

            # ひらがな/カタカナを区別した属性
            is_hira_in_element = (
                _is_hiragana_like_text(element_name)
                or _is_hiragana_like_text(element_id)
                or _is_hiragana_like_text(element_class)
                or _is_hiragana_like_text(element_placeholder)
            )
            is_kata_in_element = (
                _is_katakana_like_text(element_name)
                or _is_katakana_like_text(element_id)
                or _is_katakana_like_text(element_class)
                or _is_katakana_like_text(element_placeholder)
            )

            kana_indicators = field_patterns.get("kana_indicator", [])
            is_kana_field = bool(kana_indicators) or _field_is_kana_like(
                field_name, field_patterns
            )

            # ケース1: 要素がカナ/ふりがな系で、フィールドが非カナ系 → 強制除外
            if has_kana_in_element and not is_kana_field:
                logger.debug(
                    f"Element excluded for non-kana field '{field_name}': name='{element_name}', ph='{element_placeholder}'"
                )
                score_details["total_score"] = -999
                score_details["excluded"] = True
                score_details["exclusion_reason"] = (
                    "kana_like_element_for_non_kana_field"
                )
                return -999, score_details

            # ケース2: フィールドが『カナ』指定なのに要素がひらがな寄り → 除外（誤マッチ防止）
            if "カナ" in field_name and is_hira_in_element and not is_kata_in_element:
                score_details["total_score"] = -999
                score_details["excluded"] = True
                score_details["exclusion_reason"] = (
                    "hiragana_like_element_for_kana_field"
                )
                return -999, score_details

            # ケース3: フィールドが『ひらがな』指定なのに要素がカタカナ寄り → 除外
            if "ひらがな" in field_name and is_kata_in_element:
                score_details["total_score"] = -999
                score_details["excluded"] = True
                score_details["exclusion_reason"] = (
                    "katakana_like_element_for_hiragana_field"
                )
                return -999, score_details

            # ケース2: フィールドがカナで、要素がカナを含まない → 除外
            if is_kana_field and not has_kana_in_element:
                # ただし、コンテキストにカナ関連の言葉があるかは後で判定するため、ここでは強制除外しない
                pass

            # 追加の分割判定: 『姓カナ/名カナ』『姓ひらがな/名ひらがな』の取り違え抑止
            try:
                blob_attr = " ".join([
                    element_info.get("name", "") or "",
                    element_info.get("id", "") or "",
                    element_info.get("class", "") or "",
                    element_info.get("placeholder", "") or "",
                ])
                # セイ/メイの強い手掛かり（カタカナ表記）
                has_sei_hint = any(tok in blob_attr for tok in ["セイ", "せい", "姓", "sei", "lastname"])
                has_mei_hint = any(tok in blob_attr for tok in ["メイ", "めい", "名", "mei", "firstname"])

                if field_name in {"姓カナ", "姓ひらがな"} and has_mei_hint and not has_sei_hint:
                    score_details["total_score"] = -999
                    score_details["excluded"] = True
                    score_details["exclusion_reason"] = "mei_hint_for_last_name_field"
                    return -999, score_details
                if field_name in {"名カナ", "名ひらがな"} and has_sei_hint and not has_mei_hint:
                    score_details["total_score"] = -999
                    score_details["excluded"] = True
                    score_details["exclusion_reason"] = "sei_hint_for_first_name_field"
                    return -999, score_details
                # 統合カナが split(セイ/メイ)入力に割り当てられないように保護
                if field_name == "統合氏名カナ" and (has_sei_hint or has_mei_hint):
                    score_details["total_score"] = -999
                    score_details["excluded"] = True
                    score_details["exclusion_reason"] = "unified_kana_on_split_field"
                    return -999, score_details
            except Exception:
                pass

            # 除外パターンのチェック（最初に実行して早期除外）
            if self._is_excluded_element(element_info, field_patterns):
                logger.info(
                    f"Element excluded due to exclude_patterns for field {field_name}: "
                    f"name='{element_info.get('name', '')}', id='{element_info.get('id', '')}', "
                    f"class='{element_info.get('class', '')}', placeholder='{element_info.get('placeholder', '')}'"
                )
                # 除外された要素は完全無効化（-999でマーク）
                score_details["total_score"] = -999
                score_details["excluded"] = True
                score_details["exclusion_reason"] = "exclude_patterns_match"
                return -999, score_details

            # 各属性のスコア計算
            total_score = 0

            # 1. type属性マッチ（最高優先度）
            type_score, type_matches = self._calculate_type_score(
                element_info.get("type", ""), field_patterns
            )
            total_score += type_score
            score_details["score_breakdown"]["type"] = type_score
            if type_matches:
                score_details["matched_patterns"].extend(type_matches)

            # 2. tag名マッチ
            tag_score, tag_matches = self._calculate_tag_score(
                element_info.get("tag_name", ""), field_patterns
            )
            total_score += tag_score
            score_details["score_breakdown"]["tag"] = tag_score
            if tag_matches:
                score_details["matched_patterns"].extend(tag_matches)

            # 3. name属性マッチ
            name_score, name_matches = self._calculate_name_score(
                element_info.get("name", ""), field_patterns
            )
            total_score += name_score
            score_details["score_breakdown"]["name"] = name_score
            if name_matches:
                score_details["matched_patterns"].extend(name_matches)

            # 4. id属性マッチ
            id_score, id_matches = self._calculate_id_score(
                element_info.get("id", ""), field_patterns
            )
            total_score += id_score
            score_details["score_breakdown"]["id"] = id_score
            if id_matches:
                score_details["matched_patterns"].extend(id_matches)

            # 5. placeholder属性マッチ
            placeholder_score, placeholder_matches = self._calculate_placeholder_score(
                element_info.get("placeholder", ""), field_patterns, field_name
            )
            total_score += placeholder_score
            score_details["score_breakdown"]["placeholder"] = placeholder_score
            if placeholder_matches:
                score_details["matched_patterns"].extend(placeholder_matches)

            # 6. class属性マッチ
            class_score, class_matches = self._calculate_class_score(
                element_info.get("class", ""), field_patterns
            )
            total_score += class_score
            score_details["score_breakdown"]["class"] = class_score
            if class_matches:
                score_details["matched_patterns"].extend(class_matches)

            # 6.5. コンテキストマッチ（DTラベル対応）
            context_score, context_matches = await self._calculate_context_score(
                element, field_patterns, field_name
            )

            # Important修正3: コンテキストテキスト優先のメタデータ調整
            # 表示テキストが存在する場合、メタデータ属性の重要度を相対的に下げる
            if context_score >= 40:  # 有効なコンテキストテキストが存在する場合
                metadata_reduction_factor = 0.7  # メタデータ属性を30%減点

                # type, name, id属性のスコアを調整
                if type_score > 0:
                    adjusted_type = int(type_score * metadata_reduction_factor)
                    adjustment = adjusted_type - type_score
                    total_score += adjustment
                    score_details["score_breakdown"]["type_adjustment"] = adjustment
                    logger.debug(
                        f"Type score adjusted for context priority: {type_score} -> {adjusted_type}"
                    )

                if name_score > 0:
                    adjusted_name = int(name_score * metadata_reduction_factor)
                    adjustment = adjusted_name - name_score
                    total_score += adjustment
                    score_details["score_breakdown"]["name_adjustment"] = adjustment

                if id_score > 0:
                    adjusted_id = int(id_score * metadata_reduction_factor)
                    adjustment = adjusted_id - id_score
                    total_score += adjustment
                    score_details["score_breakdown"]["id_adjustment"] = adjustment

                logger.debug(
                    f"Metadata scores adjusted for context priority in {field_name}"
                )

            total_score += context_score
            score_details["score_breakdown"]["context"] = context_score
            if context_matches:
                score_details["matched_patterns"].extend(context_matches)

            # コンテキスト含む除外チェック（コンテキスト計算後に実行）
            if await self._is_excluded_element_with_context(
                element_info, element, field_patterns
            ):
                logger.info(
                    f"Element excluded due to context exclude_patterns for field {field_name}: "
                    f"name='{element_info.get('name', '')}', id='{element_info.get('id', '')}', "
                    f"class='{element_info.get('class', '')}', placeholder='{element_info.get('placeholder', '')}'"
                )
                # 除外された要素は完全無効化（-999でマーク）
                score_details["total_score"] = -999
                score_details["excluded"] = True
                score_details["exclusion_reason"] = "context_exclude_patterns_match"
                return -999, score_details

            # 7. 必須判定はスコアに含めない（分離済み）
            score_details["score_breakdown"]["bonus"] = 0

            # 7.5 フィールド固有の軽微な優遇: お問い合わせ本文は textarea を優先
            try:
                if field_name == "お問い合わせ本文" and (
                    element_info.get("tag_name", "").lower() == "textarea"
                ):
                    total_score += 20
                    score_details["score_breakdown"]["textarea_bonus"] = 20
            except Exception:
                pass

            # 8. ペナルティ計算（単純化）
            penalty_score, penalties = await self._calculate_penalties(
                element, element_info
            )
            total_score += penalty_score  # penalty_scoreは負数
            score_details["score_breakdown"]["penalty"] = penalty_score
            score_details["penalties"] = penalties

            # 8.5 汎用text単独一致の減点（name/id/placeholder/コンテキストが全て0の場合）
            try:
                if (
                    element_info.get("type", "").lower() == "text"
                    and score_details["score_breakdown"].get("name", 0) == 0
                    and score_details["score_breakdown"].get("id", 0) == 0
                    and score_details["score_breakdown"].get("placeholder", 0) == 0
                    and score_details["score_breakdown"].get("context", 0) == 0
                    and not self._should_bypass_generic_text_penalty(
                        field_name, score_details
                    )
                ):
                    # type(text) + tag(input) 程度の弱い一致は強く抑制
                    total_score -= 40
                    score_details["penalties"].append("generic_text_without_signals")
                    score_details["score_breakdown"]["penalty_generic_text"] = -40
            except Exception:
                pass

            # フィールド重要度は処理順序にのみ使用（スコアには不干渉）
            score_details["score_breakdown"]["field_weight"] = field_patterns.get(
                "weight", 0
            )

            score_details["total_score"] = max(0, total_score)  # 最低0点

            logger.debug(
                f"Element score calculated for {field_name}: {score_details['total_score']} "
                f"(type:{type_score}, tag:{tag_score}, name:{name_score}, "
                f"id:{id_score}, placeholder:{placeholder_score}, class:{class_score})"
            )

            return score_details["total_score"], score_details

        except Exception as e:
            logger.error(f"Error calculating element score for {field_name}: {e}")
            return 0, score_details

    async def calculate_element_score_quick(
        self, element: Locator, field_patterns: Dict[str, Any], field_name: str
    ) -> int:
        """軽量スコア計算（キャッシュ優先・軽量ペナルティ・コンテキスト非依存）"""
        try:
            # キャッシュ優先の軽量属性取得
            element_info = await self._get_element_info_quick(element)

            # 属性ベースの除外を先に適用
            if self._is_excluded_element(element_info, field_patterns):
                return -999

            total = 0
            # 基本属性スコア（コンテキスト抜き）
            s, _ = self._calculate_type_score(
                element_info.get("type", ""), field_patterns
            )
            total += s
            s, _ = self._calculate_tag_score(
                element_info.get("tag_name", ""), field_patterns
            )
            total += s
            s, _ = self._calculate_name_score(
                element_info.get("name", ""), field_patterns
            )
            total += s
            s, _ = self._calculate_id_score(element_info.get("id", ""), field_patterns)
            total += s
            s, _ = self._calculate_placeholder_score(
                element_info.get("placeholder", ""), field_patterns, field_name
            )
            total += s
            s, _ = self._calculate_class_score(
                element_info.get("class", ""), field_patterns
            )
            total += s

            # 必須判定はスコアに含めない（分離済み）

            # 軽量ペナルティ（可視/有効のみ）。ハニーポット等は本採点で実施。
            if not element_info.get("visible", True):
                total += self.SCORE_WEIGHTS["visibility_penalty"]
            if not element_info.get("enabled", True):
                total += self.SCORE_WEIGHTS["visibility_penalty"] // 2
            if element_info.get("type", "").lower() == "hidden":
                total += self.SCORE_WEIGHTS["visibility_penalty"]

            return max(-999, total)

        except Exception as e:
            logger.debug(f"Quick score failed for {field_name}: {e}")
            return 0

    async def _get_element_info_quick(self, element: Locator) -> Dict[str, Any]:
        """quick用の軽量属性取得。共有キャッシュを最優先で利用。"""
        key = str(element)
        cached = None
        try:
            cached = (
                self._shared_cache.get(key)
                if isinstance(self._shared_cache, dict)
                else None
            )
        except Exception:
            cached = None

        if cached:
            return {
                "tag_name": (cached.get("tagName") or "").lower(),
                "type": cached.get("type") or "",
                "name": cached.get("name") or "",
                "id": cached.get("id") or "",
                "class": cached.get("className") or "",
                "placeholder": cached.get("placeholder") or "",
                "value": cached.get("value") or "",
                "required": bool(
                    cached.get("requiredAttr")
                    or (str(cached.get("ariaRequired", "")).lower() == "true")
                ),
                "visible": bool(cached.get("visible", True)),
                "enabled": bool(cached.get("enabled", True)),
                # penalty用追加属性（キャッシュから取得）
                "style": cached.get("style") or "",
                "aria_hidden": cached.get("ariaHidden") or "",
                "tabindex": cached.get("tabindex") or "",
            }

        # フォールバック: 単発evaluateで必要最小限を取得（penalty用属性も含む）
        try:
            bulk = await element.evaluate(
                """
                el => ({
                    tagName: (el.tagName || '').toLowerCase(),
                    type: (el.getAttribute('type') || ''),
                    name: (el.getAttribute('name') || ''),
                    id: (el.getAttribute('id') || ''),
                    className: (el.getAttribute('class') || ''),
                    placeholder: (el.getAttribute('placeholder') || ''),
                    value: (el.getAttribute('value') || ''),
                    visibleLite: !!(el.offsetParent !== null &&
                                    el.style.display !== 'none' && el.style.visibility !== 'hidden'),
                    enabledLite: !el.disabled,
                    requiredAttr: el.hasAttribute('required'),
                    ariaRequired: el.getAttribute('aria-required') || '',
                    // penalty用追加属性
                    style: el.getAttribute('style') || '',
                    ariaHidden: el.getAttribute('aria-hidden') || '',
                    tabindex: el.getAttribute('tabindex') || ''
                })
                """
            )
        except Exception:
            bulk = {}

        return {
            "tag_name": (bulk.get("tagName") or "").lower(),
            "type": (bulk.get("type") or ""),
            "name": bulk.get("name") or "",
            "id": bulk.get("id") or "",
            "class": bulk.get("className") or "",
            "placeholder": bulk.get("placeholder") or "",
            "value": bulk.get("value") or "",
            "required": bool(
                bulk.get("requiredAttr")
                or (str(bulk.get("ariaRequired", "")).lower() == "true")
            ),
            "visible": bool(bulk.get("visibleLite", True)),
            "enabled": bool(bulk.get("enabledLite", True)),
            # penalty用追加属性
            "style": bulk.get("style") or "",
            "aria_hidden": bulk.get("ariaHidden") or "",
            "tabindex": bulk.get("tabindex") or "",
        }

    async def _get_element_info(
        self, element: Locator, parallel_groups: List[List] = None
    ) -> Dict[str, Any]:
        """要素の基本情報を取得（共有キャッシュ優先・必要時のみDOM確認）"""
        try:
            try:
                await element.wait_for(state="attached", timeout=100)
            except Exception:
                pass

            # 共有キャッシュを最優先
            key = str(element)
            bulk = None
            try:
                bulk = (
                    self._shared_cache.get(key)
                    if isinstance(self._shared_cache, dict)
                    else None
                )
            except Exception:
                bulk = None
            if bulk is None:
                # 可能な限り1回のevaluateで主要属性を取得
                try:
                    bulk = await element.evaluate(
                        """
                        el => ({
                            tagName: (el.tagName || '').toLowerCase(),
                            type: (el.getAttribute('type') || ''),
                            name: (el.getAttribute('name') || ''),
                            id: (el.getAttribute('id') || ''),
                            className: (el.getAttribute('class') || ''),
                            placeholder: (el.getAttribute('placeholder') || ''),
                            value: (el.getAttribute('value') || ''),
                            // 簡易可視・有効判定（Playwrightのis_visible/is_enabledは後段で補完）
                            visibleLite: !!(el.offsetParent !== null &&
                                            el.style.display !== 'none' && el.style.visibility !== 'hidden'),
                            enabledLite: !el.disabled,
                            // 追加: ペナルティ/必須補助
                            style: (el.getAttribute('style') || ''),
                            ariaHidden: (el.getAttribute('aria-hidden') || ''),
                            tabindex: (el.getAttribute('tabindex') || ''),
                            requiredAttr: el.hasAttribute('required'),
                            ariaRequired: el.getAttribute('aria-required') || ''
                        })
                        """
                    )
                except Exception:
                    bulk = None

            element_info = {
                "tag_name": (
                    (bulk.get("tagName") if bulk else "").lower() if bulk else ""
                ),
                "type": (bulk.get("type") if bulk else "") or "",
                "name": (bulk.get("name") if bulk else "") or "",
                "id": (bulk.get("id") if bulk else "") or "",
                "class": (bulk.get("className") if bulk else "") or "",
                "placeholder": (bulk.get("placeholder") if bulk else "") or "",
                "value": (bulk.get("value") if bulk else "") or "",
            }

            # 必須判定は既存の統合メソッドに委譲（並列グループ情報は今回未使用）
            element_info["required"] = await self._detect_required_status(element)

            # 可視性・有効状態はPlaywright APIで最終確認
            try:
                element_info["visible"] = await element.is_visible()
                element_info["enabled"] = await element.is_enabled()
            except Exception:
                # evaluate結果をフォールバックとして採用
                element_info["visible"] = (
                    bool(bulk.get("visibleLite")) if bulk else False
                )
                element_info["enabled"] = (
                    bool(bulk.get("enabledLite")) if bulk else False
                )

            # ペナルティ補助属性（キャッシュがあれば流用）
            try:
                element_info["style"] = bulk.get("style", "") if bulk else ""
                element_info["aria_hidden"] = bulk.get("ariaHidden", "") if bulk else ""
                element_info["tabindex"] = bulk.get("tabindex", "") if bulk else ""
            except Exception:
                element_info["style"] = ""
                element_info["aria_hidden"] = ""
                element_info["tabindex"] = ""

            return element_info

        except Exception as e:
            logger.warning(f"Failed to get element info: {e}")
            return {
                "tag_name": "",
                "type": "",
                "name": "",
                "id": "",
                "class": "",
                "placeholder": "",
                "required": False,
                "value": "",
                "visible": False,
                "enabled": False,
            }

    def _calculate_type_score(
        self, element_type: str, field_patterns: Dict[str, Any]
    ) -> Tuple[int, List[str]]:
        """type属性によるスコア計算

        - email/tel/url/number 等のセマンティック型は高配点
        - text は汎用のため低配点（単独一致による誤検出を抑制）
        """
        if not element_type:
            return 0, []

        pattern_types = field_patterns.get("types", [])
        matches = []

        for pattern_type in pattern_types:
            if pattern_type.lower() == element_type.lower():
                matches.append(f"type:{pattern_type}")
                logger.debug(f"Type match found: {pattern_type}")
                # 汎用textは配点を抑制（type一致だけでの誤検出防止）
                if element_type.lower() == "text":
                    return int(self.SCORE_WEIGHTS["type"] * 0.2), matches  # 20点相当
                return self.SCORE_WEIGHTS["type"], matches

        return 0, matches

    def _calculate_tag_score(
        self, tag_name: str, field_patterns: Dict[str, Any]
    ) -> Tuple[int, List[str]]:
        """tag名によるスコア計算（80点）"""
        if not tag_name:
            return 0, []

        pattern_tags = field_patterns.get("tags", [])
        matches = []

        for pattern_tag in pattern_tags:
            if pattern_tag.lower() == tag_name.lower():
                matches.append(f"tag:{pattern_tag}")
                logger.debug(f"Tag match found: {pattern_tag}")
                return self.SCORE_WEIGHTS["tag"], matches

        return 0, matches

    def _calculate_name_score(
        self, element_name: str, field_patterns: Dict[str, Any]
    ) -> Tuple[int, List[str]]:
        """name属性によるスコア計算（60点）

        パターンが要素nameに含まれる場合のみ加点（逆包含は誤検出の温床のため排除）
        """
        if not element_name:
            return 0, []

        pattern_names = field_patterns.get("names", [])
        matches = []
        element_name_lower = self._normalize(element_name)

        for pattern_name in pattern_names:
            pattern_lower = self._normalize(pattern_name)
            # 短い/曖昧トークンは語境界を要求（<=4 もしくは曖昧トークン）
            if len(pattern_lower) <= 4 or pattern_lower in self.AMBIGUOUS_TOKENS:
                if self._contains_token_with_boundary(
                    element_name_lower, pattern_lower
                ):
                    matches.append(f"name:{pattern_name}")
                    logger.debug(
                        f"Name token-boundary match: {pattern_name} in {element_name}"
                    )
                    return self.SCORE_WEIGHTS["name"], matches
            else:
                if pattern_lower in element_name_lower:
                    matches.append(f"name:{pattern_name}")
                    logger.debug(f"Name match found: {pattern_name} in {element_name}")
                    return self.SCORE_WEIGHTS["name"], matches

        return 0, matches

    def _calculate_id_score(
        self, element_id: str, field_patterns: Dict[str, Any]
    ) -> Tuple[int, List[str]]:
        """id属性によるスコア計算（60点）

        パターンが要素idに含まれる場合のみ加点（逆包含は排除）
        """
        if not element_id:
            return 0, []

        pattern_ids = field_patterns.get("ids", [])
        matches = []
        element_id_lower = self._normalize(element_id)

        for pattern_id in pattern_ids:
            pattern_lower = self._normalize(pattern_id)
            # 短い/曖昧トークンは語境界を要求
            if len(pattern_lower) <= 4 or pattern_lower in self.AMBIGUOUS_TOKENS:
                if self._contains_token_with_boundary(element_id_lower, pattern_lower):
                    matches.append(f"id:{pattern_id}")
                    logger.debug(
                        f"ID token-boundary match: {pattern_id} in {element_id}"
                    )
                    return self.SCORE_WEIGHTS["id"], matches
            else:
                if pattern_lower in element_id_lower:
                    matches.append(f"id:{pattern_id}")
                    logger.debug(f"ID match found: {pattern_id} in {element_id}")
                    return self.SCORE_WEIGHTS["id"], matches

        return 0, matches

    def _calculate_placeholder_score(
        self, placeholder: str, field_patterns: Dict[str, Any], field_name: str
    ) -> Tuple[int, List[str]]:
        """placeholder属性によるスコア計算（40点相当）

        汎用精度改善:
        - 逆包含（placeholder ⊂ pattern）の禁止
        - 日本語の語境界（CJK）を考慮した短トークンの厳格一致
        - 非カナ氏名フィールドに対する『ふりがな/カナ/ひらがな』含有の無効化
        """
        if not placeholder:
            return 0, []

        pattern_placeholders = field_patterns.get("placeholders", [])
        matches = []
        total_score = 0
        placeholder_lower = self._normalize(placeholder)
        matched_placeholder_lower = ""

        # 安全ガード: 『会社名』系の曖昧一致抑止
        # 背景: プレースホルダーが『名』である氏名フィールドが、
        #       パターン『会社名』に対して逆包含的に誤加点される事例があった。
        # 対策: 『会社名』フィールドでは、プレースホルダーに企業を示唆する語が
        #       一切含まれない場合は placeholder マッチを無効化する。
        if field_name == "会社名":
            # 多言語対応の企業性ヒント
            # - 日本語: 会社/企業/法人/団体/組織/社名/御社/貴社 など
            # - 英語: company/corporation/corporate/corp/organization/organisation/business/enterprise/firm/employer
            #   及び "company name"/"corporate name"/"business name" 等のフレーズ
            jp_hints = [
                "会社",
                "企業",
                "法人",
                "団体",
                "組織",
                "社名",
                "御社",
                "貴社",
                "会社・団体",
                "店舗",
                "病院",
                "施設",
                "学校",
                "大学",
                "園",
                "館",
                "事業者",
                "屋号",
            ]
            import re

            en_word_boundary = re.compile(
                r"\b(company|companies|corp|corporation|corporate|organization|organisation|business|enterprise|firm|employer)\b"
            )
            en_phrase = re.compile(
                r"\b(company\s+name|corporate\s+name|organization\s+name|organisation\s+name|business\s+name|enterprise\s+name|employer\s+name)\b"
            )

            has_jp = any(h in placeholder_lower for h in jp_hints)
            has_en = bool(
                en_word_boundary.search(placeholder_lower)
                or en_phrase.search(placeholder_lower)
            )

            if not (has_jp or has_en):
                # 企業性を示すヒントがなければ placeholder による加点はしない
                pattern_placeholders = []

        # 非カナ氏名フィールドは『ふりがな/カナ/ひらがな』を含むplaceholderを無効化
        kana_like_tokens = [
            "kana",
            "katakana",
            "hiragana",
            "furigana",
            "カナ",
            "カタカナ",
            "フリガナ",
            "ふりがな",
            "ひらがな",
        ]
        if field_name in {"姓", "名", "統合氏名"} and any(
            tok in placeholder_lower for tok in kana_like_tokens
        ):
            # スコア付与なし（強い誤マッチ抑止）
            return 0, []

        for pattern_placeholder in pattern_placeholders:
            pattern_lower = pattern_placeholder.lower()
            # 逆包含は誤検出の温床となるため排除
            # 短い/曖昧トークンは語境界必須で判定
            if len(pattern_lower) <= 2 or pattern_lower in self.AMBIGUOUS_TOKENS:
                if self._contains_token_with_boundary(placeholder_lower, pattern_lower):
                    matches.append(f"placeholder:{pattern_placeholder}")
                    total_score += self.SCORE_WEIGHTS["placeholder"]
                    matched_placeholder_lower = pattern_lower
                    logger.debug(
                        f"Placeholder token-boundary match: {pattern_placeholder} in {placeholder}"
                    )
                    break
            else:
                if pattern_lower in placeholder_lower:
                    matches.append(f"placeholder:{pattern_placeholder}")
                    total_score += self.SCORE_WEIGHTS["placeholder"]
                    matched_placeholder_lower = pattern_lower
                    logger.debug(
                        f"Placeholder match found: {pattern_placeholder} in {placeholder}"
                    )
                    break

        # 追加: 『姓/名（およびカナ/ひらがな）』のプレースホルダ強化ブースト（汎用・安全）
        # 目的: name="...first/last" の英語圏命名に引っ張られた逆転誤りを、
        #       日本語placeholderの明示（『姓』『名』『セイ』『メイ』『せい』『めい』等）で正す。
        try:
            if matched_placeholder_lower:
                boost = 0
                if field_name == "姓":
                    if any(k in matched_placeholder_lower for k in [
                        "姓", "last name", "family name", "苗字", "ファミリーネーム"
                    ]):
                        boost = 50
                elif field_name == "名":
                    # 『名』は曖昧だが、上のマッチは語境界済みか長語のみ
                    if any(k in matched_placeholder_lower for k in [
                        "名", "first name", "given name", "ファーストネーム", "下の名前", "お名前（名）"
                    ]):
                        boost = 50
                elif field_name == "姓カナ":
                    if any(k in matched_placeholder_lower for k in [
                        "セイ", "lastname kana", "kana last", "姓（カタカナ）"
                    ]):
                        boost = 40
                elif field_name == "名カナ":
                    if any(k in matched_placeholder_lower for k in [
                        "メイ", "firstname kana", "kana first", "名（カタカナ）"
                    ]):
                        boost = 40
                elif field_name == "姓ひらがな":
                    if any(k in matched_placeholder_lower for k in [
                        "せい", "ひらがな", "ふりがな", "姓（ひらがな）"
                    ]):
                        boost = 40
                elif field_name == "名ひらがな":
                    if any(k in matched_placeholder_lower for k in [
                        "めい", "ひらがな", "ふりがな", "名（ひらがな）"
                    ]):
                        boost = 40

                if boost:
                    total_score += boost
                    matches.append(f"placeholder_boost:+{boost}")
        except Exception:
            pass

        # 追加: 『逆語』抑止（汎用・安全）
        # 例: 姓フィールド候補の placeholder に『名』が含まれている場合は強く減点。
        #     名フィールド候補の placeholder に『姓』が含まれている場合も同様。
        try:
            # 語境界を考慮（CJK対応）
            def _has_tok(tok: str) -> bool:
                try:
                    return self._contains_token_with_boundary(placeholder_lower, tok)
                except Exception:
                    return tok in placeholder_lower

            neg = 0
            if field_name == "姓" and _has_tok("名"):
                neg = 80
            elif field_name == "名" and _has_tok("姓"):
                neg = 80
            elif field_name == "姓カナ" and _has_tok("メイ"):
                neg = 70
            elif field_name == "名カナ" and _has_tok("セイ"):
                neg = 70
            elif field_name == "姓ひらがな" and _has_tok("めい"):
                neg = 60
            elif field_name == "名ひらがな" and _has_tok("せい"):
                neg = 60

            if neg:
                total_score -= neg
                matches.append(f"placeholder_conflict:-{neg}")
        except Exception:
            pass

        # 追加: プレースホルダーの構造からの汎用高信頼判定
        # - メール: *@*.* の形（例: "xxxx@example.com"）
        # - 住所: 日本の住所に頻出するトークンを複合的に含む
        try:
            # すでにplaceholder一致で加点済みなら重複加点を避ける
            already_placeholder_matched = any(
                str(m).startswith("placeholder:") for m in matches
            )
            if field_name == "メールアドレス" and not already_placeholder_matched:
                pl = placeholder.strip()
                looks_like_email = False
                if self._email_like_re is not None:
                    looks_like_email = bool(self._email_like_re.match(pl))
                else:
                    # フォールバック: 『@』とその後に『.』を含む
                    at = pl.find("@")
                    dot = pl.rfind(".")
                    looks_like_email = at > 0 and dot > at + 1 and dot < len(pl) - 1
                if looks_like_email:
                    matches.append("placeholder:email_like")
                    total_score += self.SCORE_WEIGHTS["placeholder"]

            if field_name == "住所" and not already_placeholder_matched:
                pl = placeholder_lower
                # 住所の否定ヒント（建物名/部屋番号等）は本文住所として不適切
                negative = [
                    "建物名",
                    "建物",
                    "マンション",
                    "アパート",
                    "部屋番号",
                    "号室",
                    "階",
                ]
                if any(t in pl for t in negative):
                    # 明示的にプレースホルダ加点を抑止
                    pass
                else:
                    # 住所を示す強いシグナル（複合判定）
                    tokens = [
                        "都道府県",
                        "住所",
                        "丁目",
                        "番地",
                        "号",
                        "県",
                        "市",
                        "区",
                        "町",
                        "村",
                    ]
                    if any(t in placeholder for t in tokens):
                        matches.append("placeholder:address_like")
                        total_score += self.SCORE_WEIGHTS["placeholder"]
        except Exception:
            pass

        # 日本語形態素解析ボーナス（ただし非カナ氏名×ふりがな/カナ/ひらがなは除外）
        jp_score_allowed = True
        if field_name in {"姓", "名", "統合氏名"} and any(
            tok in placeholder_lower for tok in kana_like_tokens
        ):
            jp_score_allowed = False

        if jp_score_allowed:
            japanese_score = self._calculate_japanese_morphology_score(
                placeholder, field_name
            )
            if japanese_score > 0:
                total_score += japanese_score
                matches.append(f"japanese_morphology:{field_name}")

        return total_score, matches

    def _calculate_class_score(
        self, element_class: str, field_patterns: Dict[str, Any]
    ) -> Tuple[int, List[str]]:
        """class属性によるスコア計算（20点）"""
        if not element_class:
            return 0, []

        pattern_classes = field_patterns.get("classes", [])
        matches = []
        element_class_lower = self._normalize(element_class)

        for pattern_class in pattern_classes:
            pattern_lower = pattern_class.lower()
            # classは単語境界を考慮
            if re.search(r"\b" + re.escape(pattern_lower) + r"\b", element_class_lower):
                matches.append(f"class:{pattern_class}")
                logger.debug(f"Class match found: {pattern_class} in {element_class}")
                return self.SCORE_WEIGHTS["class"], matches

        return 0, matches

    async def _calculate_context_score(
        self, element: Locator, field_patterns: Dict[str, Any], field_name: str
    ) -> Tuple[int, List[str]]:
        """コンテキスト（DTラベル等）によるスコア計算（100点）"""
        try:
            # 共有ContextTextExtractorを使用（パフォーマンス最適化）
            if self._context_extractor:
                contexts = await self._context_extractor.extract_context_for_element(
                    element
                )
            else:
                # フォールバック: 新しいインスタンスを作成
                from .context_text_extractor import ContextTextExtractor

                context_extractor = ContextTextExtractor(element.page)
                contexts = await context_extractor.extract_context_for_element(element)

            if not contexts:
                return 0, []

            matches = []
            max_score = 0
            best_source = ""
            min_penalty = 0  # コンテキスト由来の負スコア（最小＝最大の減点）

            # 各コンテキストテキストをフィールドパターンと照合
            for context in contexts:
                score = self._match_context_with_patterns(
                    context.text, field_patterns, field_name
                )
                # 負スコアは減点として記録（後段でまとめて適用）
                if score < 0:
                    if score < min_penalty:
                        min_penalty = score
                    continue
                if score > 0:
                    # ラベル/テーブル見出しの優先度強化（general 改善）
                    st = context.source_type or ""
                    if st in ("dt_label", "dt_label_index"):
                        score = int(score * 3.0)  # 最優先
                    elif st in ("th_label", "th_label_index"):
                        score = int(score * 2.0)
                    elif st in ("label_for", "aria_labelledby", "ul_li_label"):
                        score = int(score * 2.5)  # ラベル明示関連付けを強く優先
                    elif st in ("label_parent",):
                        score = int(score * 1.8)

                    if score > max_score:
                        max_score = score
                        matches = [f"context:{context.source_type}:{context.text[:20]}"]
                        try:
                            best_source = context.source_type or ""
                        except Exception:
                            best_source = ""

            # Important修正3: コンテキストテキスト完全優先化（汎用改善拡張）
            # 表示テキストの信頼性を全面的にメタデータより優先
            strong_sources = {
                "dt_label",
                "dt_label_index",
                "th_label",
                "th_label_index",
                "label_for",
                "aria_labelledby",
                "ul_li_label",
            }
            if any(
                getattr(context, "source_type", "") in strong_sources
                for context in contexts
            ):
                # 追加ガード: 強いラベルが『ふりがな/カナ/ひらがな』系を示すのに
                # 非カナ氏名フィールド（姓/名/統合氏名）を評価している場合は強制的に負スコア
                try:
                    strong_texts = " ".join([
                        (getattr(c, "text", "") or "")
                        for c in (contexts or [])
                        if getattr(c, "source_type", "") in strong_sources
                    ]).lower()
                    kana_like = [
                        "kana",
                        "katakana",
                        "hiragana",
                        "furigana",
                        "カナ",
                        "カタカナ",
                        "フリガナ",
                        "ふりがな",
                        "ひらがな",
                    ]
                    if (field_name in {"姓", "名", "統合氏名"}) and any(
                        k.lower() in strong_texts for k in kana_like
                    ):
                        # 非カナ氏名 × カナ系ラベルは不整合
                        return -80, []
                except Exception:
                    pass

                # テーブルラベル（th_label, dt_label）は設計者の明確な意図
                final_score = max_score  # 上限制限を撤廃、完全なマッチスコアを使用

                # 完全一致時は圧倒的スコアを付与（type=100 + tag=50を上回る）
                if max_score >= 60:  # 良好なマッチの場合
                    final_score = min(200, max_score + 50)  # 最大200点まで加算
            elif max_score >= 40:  # Important修正3: 一般コンテキストも強化
                # 一般的なコンテキストテキスト（label, parent等）もメタデータより優先
                # 「表示テキストの方が信頼性が高い」要件に対応
                final_score = min(120, max_score + 20)  # type属性(100)を上回る最大120点
                logger.debug(
                    f"General context prioritized over metadata for {field_name}: {final_score}"
                )
            else:
                # 弱いコンテキストマッチは従来通り制限
                final_score = min(
                    max_score, self.SCORE_WEIGHTS["dt_context"] // 2
                )  # 40点上限

            # フィールド固有の安全ガード: 郵便番号は近傍/位置ベースの文脈だけでは高得点にしない
            try:
                pos_like = best_source.startswith("position_") or best_source in {
                    "nearby",
                    "parent_element",
                }
            except Exception:
                pos_like = False
            limit = self.POSITION_BASED_SCORE_LIMITS.get(field_name)
            if limit and pos_like:
                final_score = min(final_score, limit)

            if final_score > 0:
                logger.debug(
                    f"Context match found for {field_name}: {final_score} points"
                )

            # セマンティック不整合等による減点を適用（負スコアはそのまま減点として反映）
            if min_penalty < 0:
                final_score = max(-100, final_score + min_penalty)
                logger.debug(
                    f"Applied context penalty {min_penalty} for {field_name}, final={final_score}"
                )
                # 強否定（例: ふりがな/カナ/ひらがな）の場合は汎用ブーストの影響を打ち消し、
                # 個人名系フィールドの誤検出を抑止するため0にクリップ
                if min_penalty <= -80 and field_name in {
                    "姓",
                    "名",
                    "姓カナ",
                    "名カナ",
                    "姓ひらがな",
                    "名ひらがな",
                }:
                    final_score = 0

            return final_score, matches

        except Exception as e:
            logger.debug(f"Error calculating context score for {field_name}: {e}")
            return 0, []

    def _match_context_with_patterns(
        self, context_text: str, field_patterns: Dict[str, Any], field_name: str
    ) -> int:
        """コンテキストテキストをフィールドパターンと照合（セマンティック検証付き）"""
        if not context_text:
            return 0

        context_lower = context_text.lower()
        max_score = 0

        # 0. セマンティック検証（最優先）
        semantic_validation_score = self._validate_semantic_consistency(
            context_text, field_name
        )
        # 明確な不整合は即時に負スコアを返す（減点を上位へ伝播）
        if semantic_validation_score < 0:
            return semantic_validation_score
        if semantic_validation_score > 0:
            max_score = max(max_score, semantic_validation_score)

        # 1. フィールド名との直接一致
        # 単一文字の『名』などは汎用語への誤反応が極めて多いため除外し、
        # それ以外のみカナ/ひらがな除去のバリエーションを許可する。
        base_name = field_name.replace("カナ", "").replace("ひらがな", "")
        field_variations = []
        if len(base_name) > 1 and base_name not in {"名"}:
            field_variations = [field_name.lower(), base_name.lower()]

        for variation in field_variations:
            # 語境界を考慮してマッチ精度を上げる
            if self._contains_token_with_boundary(context_lower, variation):
                max_score = max(max_score, 80)

        # 2. パターンとの一致チェック
        all_patterns = []
        for pattern_type in ["names", "placeholders"]:
            patterns = field_patterns.get(pattern_type, [])
            all_patterns.extend(patterns)

        for pattern in all_patterns:
            pattern_lower = pattern.lower()
            if pattern_lower in context_lower or context_lower in pattern_lower:
                # マッチの強さに応じてスコア調整
                if len(pattern) > 3:  # 長いパターンほど信頼度高
                    max_score = max(max_score, 60)
                else:
                    max_score = max(max_score, 40)

        # 3. 日本語意味解析
        semantic_score = self._calculate_japanese_semantic_match(
            context_text, field_name
        )
        max_score = max(max_score, semantic_score)

        return max_score

    def _validate_semantic_consistency(self, context_text: str, field_name: str) -> int:
        """セマンティック一貫性検証（DTラベルの明確な指示を最優先）"""
        context_lower = context_text.lower()

        # DTラベルが明確にフィールド種別を示している場合の高精度マッチング
        definitive_mappings = {
            "メールアドレス": ["mail", "メール", "email", "e-mail", "アドレス"],
            # 電話番号: 汎用語『番号』は過検出の主要因となるため除外
            "電話番号": ["tel", "電話", "phone", "telephone", "tel."],
            # 会社/団体/組織/施設など「〇〇名」系は個人名ではない
            "会社名": [
                "会社",
                "企業",
                "法人",
                "団体",
                "組織",
                "社名",
                "法人名",
                "団体名",
                "組織名",
                "部署名",
                "学校名",
                "店舗名",
                "病院名",
                "施設名",
                # 追加: 英語圏ラベルで頻出
                "affiliation",
            ],
            "姓": ["姓", "苗字", "せい", "みょうじ", "名字", "姓名"],
            # 『名』単独は「マンション名」等の複合語に反応しやすいので除外
            "名": [
                "名前",
                "お名前",
                "ファーストネーム",
                "下の名前",
                "given name",
                "first name",
            ],
            "お問い合わせ本文": [
                "内容",
                "本文",
                "メッセージ",
                "問い合わせ",
                "お問合せ",
                "ご要望",
                "ご質問",
                "備考",
                "ご相談",
                "ご意見",
                "note",
            ],
            "件名": ["件名", "タイトル", "表題", "用件"],
            "住所": ["住所", "所在地", "じゅうしょ", "address"],
            "郵便番号": ["郵便番号", "〒", "ゆうびん", "zip"],
            # 役職（Job Title）を明確化
            "役職": ["役職", "職位", "job title", "position", "role", "job"],
        }

        # 特別ガード: 電話番号
        if field_name == "電話番号":
            # FAXは明確に不適合
            if any(k in context_lower for k in ["fax", "ファックス", "ファクス"]):
                return -80
        # 追加: 電話系（分割含む）が『氏名/お名前/フリガナ/カナ/ひらがな』文脈に反応しない
        if field_name in {"電話番号", "電話1", "電話2", "電話3"}:
            name_like_ctx = ["氏名", "お名前", "名前", "フリガナ", "ふりがな", "カナ", "ひらがな", "セイ", "メイ"]
            if any(t.lower() in context_lower for t in name_like_ctx):
                return -80

        # 特別ガード: メールアドレスの文脈に『電話』『tel』『お電話』『phone』が含まれる場合は負スコア
        # 特別ガード: 氏名(カナ以外)に『カナ/ふりがな/ひらがな』が含まれる場合は除外（誤割当抑止）
        if field_name in {"姓", "名", "統合氏名"}:
            kana_like = ["カナ", "ｶﾅ", "カタカナ", "ﾌﾘｶﾞﾅ", "フリガナ", "ふりがな", "ひらがな", "furigana", "kana", "hiragana", "katakana"]
            if any(k.lower() in context_lower for k in kana_like):
                return -80
        if field_name == "メールアドレス":
            if any(
                k in context_lower
                for k in ["電話", "お電話", "tel", "phone", "telephone"]
            ):
                return -60
        # 追加ガード: 名前系フィールドがメールアドレス文脈に反応しないよう強く否定
        if field_name in {"姓", "名", "統合氏名"}:
            if any(k in context_lower for k in ["メール", "mail", "email", "e-mail", "アドレス"]):
                return -80

        # 特別ガード: 会社名の文脈で『管理会社』『竣工』『年月日』などは不適切
        if field_name == "会社名":
            if any(k in context_lower for k in ["管理会社"]):
                return -70
            if any(k in context_lower for k in ["竣工", "年月日"]):
                return -50

        # 特別ガード: 郵便番号 vs 従業員番号の取り違え抑止
        if field_name == "郵便番号":
            if any(
                k in context_lower
                for k in ["従業員番号", "社員番号", "employee id", "employee number"]
            ):
                return -90

        # 汎用ガード: 『名』『カナ』等の個人名系フィールドは、
        # 会社名・建物名・商品名などの「〇〇名」複合語と衝突する。
        # 文脈にこれらが含まれる場合は強い負スコアを返してマッピング対象から外す。
        name_like_fields = {
            "統合氏名",
            "統合氏名カナ",
            "姓",
            "名",
            "姓カナ",
            "名カナ",
            "姓ひらがな",
            "名ひらがな",
        }
        if field_name in name_like_fields and self.NON_PERSONAL_NAME_PATTERN.search(
            context_text
        ):
            return -80  # 明確な不整合
        # ふりがな/カナ/ひらがなを含む文脈では、漢字の『姓』『名』を強く否定
        if field_name in {"姓", "名"}:
            if any(
                k in context_lower
                for k in [
                    "ふりがな",
                    "フリガナ",
                    "ｶﾅ",
                    "かな",
                    "カナ",
                    "kana",
                    "ひらがな",
                    "平仮名",
                ]
            ):
                return -90

        # 会社名: 個人名コンテキストが混在していれば最優先で減点（先に評価）
        if field_name == "会社名":
            personal_ctx = [
                "お名前", "氏名", "姓名", "full name", "first name", "given name", "last name", "family name"
            ]
            if any(t in context_lower for t in [s.lower() for s in personal_ctx]):
                return -75
        # ポジティブマッチング（従来通り）
        keywords = definitive_mappings.get(field_name, [])
        positive_hit = any(keyword in context_lower for keyword in keywords)
        if positive_hit:
            # 現フィールド種別の明確なラベルが含まれる場合は、
            # 後続のネガティブ検証をスキップして安定した高スコアを返す。
            return 90  # 高スコア（ネガティブと競合させない）

        # ネガティブセマンティック検証（汎用改善）
        # 明らかに異なるフィールドタイプの場合はマイナススコア
        negative_score = self._check_semantic_conflicts(
            context_text, field_name, definitive_mappings
        )
        if negative_score < 0:
            return negative_score

        return 0

    def _check_semantic_conflicts(
        self,
        context_text: str,
        field_name: str,
        definitive_mappings: Dict[str, List[str]],
    ) -> int:
        """セマンティック不整合検出（汎用改善: 明らかな不整合にマイナススコア）"""
        context_lower = context_text.lower()

        # フィールドタイプ別の互換性マッピング
        field_type_groups = {
            "phone": ["電話番号", "電話1", "電話2", "電話3"],
            "postal": ["郵便番号", "郵便番号1", "郵便番号2"],
            "address": ["住所"],
            "name": ["姓", "名", "姓カナ", "名カナ", "姓ひらがな", "名ひらがな"],
            "email": ["メールアドレス"],
            "company": ["会社名", "会社名カナ"],
            "message": ["お問い合わせ本文", "件名"],
            "personal_info": ["年齢", "性別", "役職"],
            "business": ["来場", "人数", "予約", "希望", "建築", "エリア"],
        }

        # 現在のフィールドのタイプを特定
        current_field_type = None
        for ftype, fields in field_type_groups.items():
            if field_name in fields:
                current_field_type = ftype
                break

        if not current_field_type:
            return 0  # 不明なフィールドは判定しない

        # 現在のフィールド種別の決定的キーワードが既に含まれる場合、
        # 他種別キーワード（例: 「会社名」中の「名」）による誤減点を抑止する。
        current_keywords = definitive_mappings.get(field_name, [])
        if any(kw in context_lower for kw in current_keywords):
            return 0

        # 他のフィールドタイプのキーワードが含まれている場合はマイナススコア
        for other_field_name, keywords in definitive_mappings.items():
            if other_field_name == field_name:
                continue  # 自分自身はスキップ

            # 他のフィールドタイプを特定
            other_field_type = None
            for ftype, fields in field_type_groups.items():
                if other_field_name in fields:
                    other_field_type = ftype
                    break

            if other_field_type == current_field_type:
                continue  # 同じタイプはスキップ

            # 明確に異なるタイプのキーワードが含まれている場合
            for keyword in keywords:
                if keyword in context_lower:
                    # 会社名系の複合語（会社名/法人名/団体名/組織名/部署名/学校名/店舗名/病院名/施設名）に含まれる
                    # 一文字の「名」を個人名と誤認しないよう保護する。
                    if field_name == "会社名" and keyword in ["名", "名前"]:
                        # 会社名の決定語が含まれていれば衝突扱いしない
                        company_tokens = [
                            "会社名",
                            "法人名",
                            "団体名",
                            "組織名",
                            "部署名",
                            "学校名",
                            "店舗名",
                            "病院名",
                            "施設名",
                            "社名",
                        ]
                        if any(tok in context_lower for tok in company_tokens):
                            continue
                    logger.debug(
                        f"Semantic conflict detected: '{context_text}' contains '{keyword}' but field is {field_name}"
                    )
                    return -50  # マイナススコア付与

        # 特別なケース: 業務・個人情報関連のキーワード
        business_keywords = [
            "来場",
            "人数",
            "大人",
            "子供",
            "年齢",
            "予約",
            "希望",
            "建築",
            "エリア",
            "時間",
        ]
        if current_field_type in ["phone", "name", "email", "postal"]:
            for keyword in business_keywords:
                if keyword in context_lower:
                    logger.debug(
                        f"Business context conflict: '{context_text}' contains business keyword but field is {field_name}"
                    )
                    return -75  # より強いマイナススコア

        return 0

    def _calculate_japanese_semantic_match(
        self, context_text: str, field_name: str
    ) -> int:
        """日本語の意味的マッチング"""
        context_lower = context_text.lower()

        # フィールドタイプ別の意味的キーワード
        semantic_mappings = {
            # 一般化改善: '連絡先' はグループ見出しで曖昧なためメールの同義語から除外
            "メールアドレス": ["メール", "mail", "email", "e-mail", "アドレス"],
            "電話番号": [
                "電話",
                "tel",
                "phone",
                "番号",
                "連絡先",
                "携帯",
                "mobile",
                "cell",
            ],
            # 会社/団体/組織/施設の各種ラベル
            "会社名": [
                "会社",
                "企業",
                "法人",
                "団体",
                "組織",
                "社名",
                "法人名",
                "団体名",
                "組織名",
                "部署名",
                "学校名",
                "店舗名",
                "病院名",
                "施設名",
            ],
            "姓": ["姓", "苗字", "名字", "せい", "みょうじ"],
            # 『名』単独は除外し、より明確な表現のみ
            "名": [
                "名前",
                "お名前",
                "ファーストネーム",
                "下の名前",
                "given name",
                "first name",
            ],
            "お問い合わせ本文": [
                "問い合わせ",
                "お問い合わせ",
                "内容",
                "本文",
                "メッセージ",
                "ご要望",
                "ご質問",
                "備考",
                "ご相談",
                "ご意見",
                "note",
            ],
            "件名": ["件名", "タイトル", "表題", "用件"],
            "役職": ["役職", "職位", "job title", "position", "role"],
            "住所": ["住所", "所在地", "じゅうしょ"],
            "郵便番号": ["郵便番号", "郵便", "ゆうびん", "〒"],
        }

        keywords = semantic_mappings.get(field_name, [])
        for keyword in keywords:
            if keyword in context_lower:
                return 50

        return 0

    def _calculate_japanese_morphology_score(self, text: str, field_name: str) -> int:
        """日本語形態素解析による追加スコア（25点）

        ガード:
        - 非カナ氏名（姓/名/統合氏名）に対し、『ふりがな/カナ/ひらがな』を含むテキストでは加点しない
        """
        if not text:
            return 0

        text_lower = str(text).lower()
        if field_name in {"姓", "名", "統合氏名"}:
            if any(
                tok in text_lower
                for tok in [
                    "kana",
                    "katakana",
                    "hiragana",
                    "furigana",
                    "カナ",
                    "カタカナ",
                    "フリガナ",
                    "ふりがな",
                    "ひらがな",
                ]
            ):
                return 0

        # 簡略版形態素解析（完全な形態素解析は重いため）
        field_keywords = self.japanese_patterns.get(field_name, [])

        for keyword in field_keywords:
            if keyword in text:
                logger.debug(f"Japanese morphology match: {keyword} in {text}")
                return self.SCORE_WEIGHTS["japanese_morphology"]

        return 0

    # 削除: 複雑なボーナス計算は不要（単純化のため）

    async def _calculate_penalties(
        self, element: Locator, element_info: Dict[str, Any]
    ) -> Tuple[int, List[str]]:
        """ペナルティスコア計算（外部モジュールへ委譲）。"""
        from .penalties import calculate_penalties as _calc

        return await _calc(element, element_info, self.SCORE_WEIGHTS)

    def compare_elements(
        self, element1_score: Dict[str, Any], element2_score: Dict[str, Any]
    ) -> int:
        """
        2つの要素スコアを比較

        Returns:
            1 if element1 > element2
            -1 if element1 < element2
            0 if equal
        """
        score1 = element1_score["total_score"]
        score2 = element2_score["total_score"]

        if score1 > score2:
            return 1
        elif score1 < score2:
            return -1
        else:
            # 同点の場合は詳細要素で判定
            # type属性マッチがある方を優先
            type1 = element1_score["score_breakdown"].get("type", 0)
            type2 = element2_score["score_breakdown"].get("type", 0)
            if type1 != type2:
                return 1 if type1 > type2 else -1

            # 必須要素を優先
            bonus1 = element1_score["score_breakdown"].get("bonus", 0)
            bonus2 = element2_score["score_breakdown"].get("bonus", 0)
            if bonus1 != bonus2:
                return 1 if bonus1 > bonus2 else -1

            return 0

    def get_score_summary(self, score_details: Dict[str, Any]) -> str:
        """スコア詳細のサマリー文字列を生成"""
        total = score_details["total_score"]
        breakdown = score_details["score_breakdown"]
        matches = score_details["matched_patterns"]

        summary_parts = [f"Total: {total}"]

        for attr, score in breakdown.items():
            if score > 0:
                summary_parts.append(f"{attr}:{score}")

        if matches:
            summary_parts.append(f"Matches: {', '.join(matches[:3])}")  # 最初の3つのみ

        return " | ".join(summary_parts)

    # 削除: 複雑な完全一致ボーナス計算は基本属性マッチングに統合

    # 削除: 部分一致ペナルティは複雑すぎるため削除（基本マッチングに集中）

    # 削除: 不要になったヘルパーメソッド（単純化のため）

    def _is_excluded_element(
        self, element_info: Dict[str, Any], field_patterns: Dict[str, Any]
    ) -> bool:
        from .exclusion_rules import is_excluded_element as _impl

        return _impl(element_info, field_patterns)

    async def _is_excluded_element_with_context(
        self,
        element_info: Dict[str, Any],
        element: Locator,
        field_patterns: Dict[str, Any],
    ) -> bool:
        from .exclusion_rules import (
            is_excluded_element_with_context as _impl_ctx,
        )

        return await _impl_ctx(
            element_info, element, field_patterns, context_extractor=self._context_extractor
        )

    async def _detect_required_status(
        self, element: Locator, parallel_groups: List[List] = None
    ) -> bool:
        """
        要素の必須状態を総合的に検出

        Args:
            element: 検査対象のLocator
            parallel_groups: 並列要素グループ（FormElement のリスト）

        Returns:
            必須の場合True
        """
        try:
            # 1) 標準HTML属性（最優先）
            required_attr = await element.get_attribute("required")
            if required_attr is not None:
                return True

            aria_required = await element.get_attribute("aria-required")
            if aria_required and aria_required.lower() == "true":
                return True

            # 2) 要素自身/先祖のclassヒント（CF7等）
            try:
                class_attr = (await element.get_attribute("class")) or ""
            except Exception:
                class_attr = ""
            class_lower = class_attr.lower()
            required_class_tokens = [
                "required",
                "require",
                "mandatory",
                "must",
                "necessary",
                "必須",
                # Contact Form 7系
                "wpcf7-validates-as-required",
            ]
            # 自要素の class に強い必須トークンが含まれる場合のみ即時 True
            # 祖先側の class は誤検出が多いため、後段の文脈判定と組み合わせて判断する
            if any(tok in class_lower for tok in required_class_tokens):
                return True

            # 先祖方向にスキャンして必須系クラスを検出（例: wrap/span/divに付与されるケース）
            try:
                ancestor_has_required = await element.evaluate(
                    """
                    el => {
                      const TOKENS = ['required','require','mandatory','must','necessary','必須','wpcf7-validates-as-required'];
                      let p = el.parentElement;
                      let depth = 0;
                      while (p && depth < 6) {
                        const cls = (p.getAttribute('class') || '').toLowerCase();
                        if (TOKENS.some(t => cls.includes(t))) return true;
                        p = p.parentElement; depth++;
                      }
                      return false;
                    }
                """
                )
            except Exception:
                ancestor_has_required = False
            # 祖先の class による必須ヒントは即採用しない（コンテキストと併用して厳格化）
            ancestor_required_hint = bool(ancestor_has_required)

            # 3) name属性に明示マーカー
            try:
                name_attr = (await element.get_attribute("name")) or ""
            except Exception:
                name_attr = ""
            if any(marker in name_attr for marker in ["必須", "required", "mandatory"]):
                return True

            # 4) 近傍テキストのインジケータ（ContextTextExtractor活用）
            #    祖先クラスのヒントがある場合でも、明示的な『必須』コンテキストが無ければ必須とみなさない
            if await self._detect_required_markers_with_context(element):
                return True

            # 5) DL構造のdt側クラス（例: <dt class="need">）を検出
            try:
                dt_class = await element.evaluate(
                    """
                    el => {
                      // dd祖先→直前のdtを探索
                      let p = el;
                      while (p && p.tagName && p.tagName.toLowerCase() !== 'dd') { p = p.parentElement; }
                      if (!p) return '';
                      let dt = p.previousElementSibling;
                      while (dt && dt.tagName && dt.tagName.toLowerCase() !== 'dt') { dt = dt.previousElementSibling; }
                      if (!dt) return '';
                      return (dt.getAttribute('class') || '').toLowerCase();
                    }
                """
                )
            except Exception:
                dt_class = ""
            if isinstance(dt_class, str) and any(
                k in dt_class for k in ["need", "required", "必須", "must", "mandatory"]
            ):
                return True

            # 5.1) テーブル構造の th 側クラス（例: <th class="required">）を検出
            try:
                th_class = await element.evaluate(
                    """
                    el => {
                      // td祖先→直前のthを探索
                      let p = el;
                      while (p && p.tagName && p.tagName.toLowerCase() !== 'td') { p = p.parentElement; }
                      if (!p) return '';
                      let th = p.previousElementSibling;
                      while (th && th.tagName && th.tagName.toLowerCase() !== 'th') { th = th.previousElementSibling; }
                      if (!th) return '';
                      return (th.getAttribute('class') || '').toLowerCase();
                    }
                """
                )
            except Exception:
                th_class = ""
            if isinstance(th_class, str) and any(
                k in th_class for k in ["need", "required", "必須", "must", "mandatory"]
            ):
                return True

            # 5.5) ラベル近傍の必須マーク（span.require 等）の明示検出
            try:
                near_mark = await element.evaluate(
                    """
                    el => {
                      const hasMark = (node) => {
                        if (!node) return false;
                        const txt = (node.innerText || node.textContent || '').trim();
                        const cls = (node.getAttribute && (node.getAttribute('class') || '').toLowerCase()) || '';
                        // よくある表記ゆれを網羅（must/need/mandatory/is-required/required-mark等）
                        if (cls.includes('require') || cls.includes('required') || cls.includes('must') || cls.includes('need') || cls.includes('mandatory') || cls.includes('is-required') || cls.includes('required-mark')) return true;
                        if (txt === '*' || txt === '＊' || txt.includes('必須')) return true;
                        // 『※』は注記と紛れるため短文(<=10文字)に限定
                        // 『※』単独は注記の可能性が高いため無効。『※必須』等の組合せのみ許可
                        if ((/※\s*必須/.test(txt)) || ['*','＊'].includes(txt.trim())) return true;
                        return false;
                      };
                      let p = el.parentElement; let depth = 0;
                      while (p && depth < 2) { // 直近の親までに限定（セクション跨ぎの誤検出防止）
                        // 親内の強調要素のみを走査（兄弟ブロックは走査しない：誤検出抑止）
                        const spans = p.querySelectorAll('span.require, span.required, span.must, span.mandatory, span.required-mark, i, em, b, strong');
                        for (const sp of spans) { if (hasMark(sp)) return true; }
                        p = p.parentElement; depth++;
                      }
                      const id = el.getAttribute('id');
                      if (id) {
                        const labels = document.querySelectorAll(`label[for="${id}"] span, label[for="${id}"] i, label[for="${id}"] b, label[for="${id}"] strong`);
                        for (const sp of labels) { if (hasMark(sp)) return true; }
                      }
                      return false;
                    }
                """
                )
            except Exception:
                near_mark = False
            if near_mark:
                return True

            # 5.6) 画像のaltテキストによる必須表示（例: <img alt="必須">）
            try:
                alt_mark = await element.evaluate(
                    """
                    el => {
                      const hasAltRequired = (root) => {
                        if (!root) return false;
                        const imgs = root.querySelectorAll('img[alt]');
                        for (const im of imgs) {
                          const alt = (im.getAttribute('alt') || '').toLowerCase();
                          if (!alt) continue;
                          if (alt.includes('必須') || alt.includes('required')) return true;
                        }
                        return false;
                      };
                      // 直近の親にある <img alt="必須"> / 直前兄弟セル(td/th)内の画像も確認
                      const p = el.parentElement;
                      if (hasAltRequired(p)) return true;
                      const prev = p && p.previousElementSibling;
                      if (hasAltRequired(prev)) return true;
                      // さらに1階層上まで限定的に確認
                      const gp = p && p.parentElement;
                      if (hasAltRequired(gp)) return true;
                      return false;
                    }
                """
                )
            except Exception:
                alt_mark = False
            if alt_mark:
                return True

            # 6) 並列グループ内の必須マーカー検出（新規追加）
            if parallel_groups and await self._detect_required_in_parallel_group(
                element, parallel_groups
            ):
                return True

            # 6.5) 追加フォールバック: 親→前方兄弟ブロックの明示『必須』検出
            # 事例: <div class="contact__title"><p>ふりがな</p><p>必須</p></div>
            #       <input name="姓ふりがな"> <input name="名ふりがな">
            # 入力要素の直近親から最大3階層まで遡り、それぞれの直前の兄弟ブロック（最大3件）に
            # 『必須/Required/Mandatory/Must』が含まれていれば必須とみなす。
            try:
                extended_mark = await element.evaluate(
                    """
                    el => {
                      // フォールバックは誤検出が多いため、非常に限定的に適用：
                      // 直近の親の直前の兄弟 最大3件 を確認し、『必須』等の明示があればTrue。
                      const MARKS = ['必須','required','Required','MANDATORY','Mandatory','Must'];
                      const hasReq = (node) => {
                        if (!node) return false;
                        try {
                          const txt = (node.innerText || node.textContent || '').trim();
                          if (!txt) return false;
                          if (MARKS.some(m => txt.includes(m))) return true;
                          // 『※』は短文（<=10文字）のときのみ必須と判断
                          // 『※必須』のみ必須扱い（※単独は無効）、スターは許容
                          if ((/※\s*必須/.test(txt)) || ['*','＊'].includes(txt.trim())) return true;
                          return false;
                        } catch { return false; }
                      };
                      const parent = el.parentElement;
                      if (!parent) return false;
                      let sib = parent.previousElementSibling;
                      let checked = 0;
                      while (sib && checked < 3) {
                        if (hasReq(sib)) return true;
                        sib = sib.previousElementSibling;
                        checked++;
                      }
                      return false;
                    }
                """
                )
            except Exception:
                extended_mark = False
            if extended_mark:
                return True

            # 6.6) aria-labelledby の参照先に必須表示があるかの確認（汎用）
            # 例: <input aria-labelledby="label_ruby"> / <label id="label_ruby">ふりがな <span>*</span></label>
            try:
                aria_ids = await element.get_attribute("aria-labelledby")
            except Exception:
                aria_ids = None
            # 安全側: 未設定時に未初期化参照を避ける
            aria_found = False
            if aria_ids:
                try:
                    ids = [s.strip() for s in str(aria_ids).split() if s.strip()]
                    aria_found = await element.evaluate(
                        """
                        (el, ids) => {
                          const MARKS = ['必須','required','Required','MANDATORY','Mandatory','Must'];
                          const hasReq = (node) => {
                            if (!node) return false;
                            const txt = (node.innerText || node.textContent || '').trim();
                            if (!txt) return false;
                            if (MARKS.some(m => txt.includes(m))) return true;
                            // 『※必須』のみ必須扱い（※単独は無効）、スターは許容
                            if ((/※\s*必須/.test(txt)) || ['*','＊'].includes(txt.trim())) return true;
                            return false;
                          };
                          for (const id of ids) {
                            const n = document.getElementById(id);
                            if (n && hasReq(n)) return true;
                          }
                          return false;
                        }
                    """,
                        ids,
                    )
                except Exception:
                    aria_found = False
            if aria_found:
                return True

            # 6.7) aria-describedby の参照先に必須表示/マークがあるか確認
            try:
                aria_desc = await element.get_attribute("aria-describedby")
            except Exception:
                aria_desc = None
            if aria_desc:
                try:
                    ids = [s.strip() for s in str(aria_desc).split() if s.strip()]
                    described_found = await element.evaluate(
                        """
                        (el, ids) => {
                          const MARKS = ['必須','required','Required','MANDATORY','Mandatory','Must'];
                          const hasReq = (node) => {
                            if (!node) return false;
                            const txt = (node.innerText || node.textContent || '').trim();
                            const cls = (node.getAttribute && (node.getAttribute('class') || '').toLowerCase()) || '';
                            if (MARKS.some(m => txt.includes(m))) return true;
                            if (cls.includes('require') || cls.includes('required') || cls.includes('must') || cls.includes('mandatory') || cls.includes('is-required') || cls.includes('required-mark')) return true;
                            if ((/※\s*必須/.test(txt)) || ['*','＊'].includes(txt.trim())) return true;
                            return false;
                          };
                          for (const id of ids) {
                            const n = document.getElementById(id);
                            if (n && hasReq(n)) return true;
                          }
                          return false;
                        }
                        """,
                        ids,
                    )
                except Exception:
                    described_found = False
                if described_found:
                    return True

            # 祖先クラスのヒントのみがあるケースを限定的に許可
            # 例: <div class="required"> ... <input> ... </div>
            # 多くの日本語サイトでは CSS の疑似要素で『必須』が描画され、テキストはDOMに現れない。
            # そのため、近傍テキストに『必須』が無くても、祖先クラスのみで必須表示されるケースを拾う。
            # ただし、誤検出を避けるため以下の条件で採用する：
            # - 深さ制限内（<=3）の最も近い祖先に required 系クラスがある
            # - その祖先のテキストに『任意/optional』等の否定マーカーが含まれない
            # - 祖先の class に captcha/login/token 等の認証系シグナルが含まれない
            if ancestor_required_hint:
                try:
                    ancestor_required_confirmed = await element.evaluate(
                        """
                        el => {
                          const REQ = ['required','require','mandatory','must','necessary','必須','wpcf7-validates-as-required'];
                          const NEG = ['任意','optional'];
                          const EXCL = ['captcha','image_auth','token','otp','verification','login','signin','auth','password'];
                          let p = el.parentElement; let depth = 0;
                          while (p && depth < 3) {
                            const cls = (p.getAttribute('class') || '').toLowerCase();
                            if (REQ.some(t => cls.includes(t))) {
                              if (EXCL.some(t => cls.includes(t))) return false;
                              const txt = ((p.innerText || p.textContent || '') + '').toLowerCase();
                              if (NEG.some(t => txt.includes(t))) return false;
                              return true;
                            }
                            p = p.parentElement; depth++;
                          }
                          return false;
                        }
                    """
                    )
                except Exception:
                    ancestor_required_confirmed = False
                if ancestor_required_confirmed:
                    return True
                # 条件に合致しなければ非必須扱い
                return False

            return False

        except Exception as e:
            logger.debug(f"Failed to detect required status: {e}")
            return False

    async def _detect_required_in_parallel_group(
        self, element: Locator, parallel_groups: List[List]
    ) -> bool:
        """
        並列グループ内の必須マーカーを検出

        Args:
            element: 対象要素
            parallel_groups: 並列要素グループ（FormElement のリスト）

        Returns:
            並列グループ内で必須マーカーが見つかった場合 True
        """
        try:
            # 対象要素が含まれる並列グループを特定
            target_group = None
            element_selector = await self._get_element_selector(element)

            for group in parallel_groups:
                for form_element in group:
                    if (
                        hasattr(form_element, "selector")
                        and form_element.selector == element_selector
                    ):
                        target_group = group
                        break
                if target_group:
                    break

            if not target_group:
                return False

            # 同じグループ内の非入力要素から必須マーカーを探す
            # 『※』単独は注記用途が多く誤検出の原因になるため除外（『※必須』等は Context 判定側で許容）
            required_markers = [
                "*",
                "必須",
                "Required",
                "Mandatory",
                "Must",
                "(必須)",
                "（必須）",
                "[必須]",
                "［必須］",
            ]

            # グループ内の各要素の親コンテナ内で必須マーカーを探索
            for form_element in target_group:
                if not hasattr(form_element, "locator"):
                    continue

                try:
                    # 要素の親コンテナ内のテキスト要素を調査
                    container_text = await form_element.locator.locator(
                        ".."
                    ).inner_text()
                    if any(marker in container_text for marker in required_markers):
                        return True

                    # 兄弟要素も調査（並列構造の場合、必須マーカーは兄弟要素にある可能性）
                    siblings = await form_element.locator.locator("../*").all()
                    for sibling in siblings:
                        if await sibling.count() > 0:
                            sibling_text = await sibling.inner_text()
                            if any(
                                marker in sibling_text for marker in required_markers
                            ):
                                return True
                except:
                    continue

            return False

        except Exception as e:
            logger.debug(f"Failed to detect required in parallel group: {e}")
            return False

    async def _get_element_selector(self, element: Locator) -> str:
        """要素のセレクターを生成（軽量版）"""
        try:
            info = await element.evaluate(
                "el => ({ id: el.getAttribute('id')||'', name: el.getAttribute('name')||'', tag: el.tagName.toLowerCase(), type: el.getAttribute('type')||'' })"
            )

            if info.get("id"):
                esc_id = str(info["id"]).replace("\\", r"\\").replace('"', r"\"")
                return f'[id="{esc_id}"]'

            name = info.get("name")
            tag = info.get("tag") or "input"
            typ = info.get("type")

            if name:
                esc_name = str(name).replace("\\", r"\\").replace('"', r"\"")
                if typ:
                    esc_type = str(typ).replace("\\", r"\\").replace('"', r"\"")
                    return f'{tag}[name="{esc_name}"][type="{esc_type}"]'
                return f'{tag}[name="{esc_name}"]'

            if typ:
                esc_type2 = str(typ).replace("\\", r"\\").replace('"', r"\"")
                return f'{tag}[type="{esc_type2}"]'
            return tag

        except Exception:
            return "input"

    async def _detect_required_markers_with_context(self, element: Locator) -> bool:
        """
        ContextTextExtractorを活用した必須マーカー検出

        Args:
            element: 対象要素

        Returns:
            必須マーカーが見つかった場合 True
        """
        try:
            # NOTE: ContextTextExtractor は __init__ で _context_extractor として渡される。
            # 以前は self.context_extractor を参照しており、常にフォールバック側に落ちて精度が低下していた。
            # 正しくは self._context_extractor を使用し、extract_context_for_element を呼び出す。
            if not self._context_extractor:
                # フォールバック: 基本的な周辺テキスト取得
                surrounding_text = await self._get_surrounding_text(element)
                required_markers = [
                    "*",
                    "＊",
                    "必須",
                    "Required",
                    "Mandatory",
                    "Must",
                    "(必須)",
                    "（必須）",
                    "[必須]",
                    "［必須］",
                ]
                optional_markers = [
                    "任意",
                    "optional",
                    "お分かりの場合",
                    "分かる場合",
                    "お持ちの場合",
                    "あれば",
                    "可能な範囲",
                ]
                if any(tok in surrounding_text for tok in optional_markers):
                    return False
                return any(marker in surrounding_text for marker in required_markers)

            # ContextTextExtractorを使った高度な必須マーカー検出
            contexts = await self._context_extractor.extract_context_for_element(
                element
            )
            required_markers = [
                "*",
                "＊",
                "必須",
                "Required",
                "Mandatory",
                "Must",
                "(必須)",
                "（必須）",
                "[必須]",
                "［必須］",
            ]
            optional_markers = [
                "任意",
                "optional",
                "お分かりの場合",
                "分かる場合",
                "お持ちの場合",
                "あれば",
                "可能な範囲",
            ]

            def _is_strict_required_text(s: str) -> bool:
                if not s:
                    return False
                t = s.strip()
                # 純粋な必須表示のみ許可（誤検出抑止）
                return t in {
                    "必須",
                    "*",
                    "＊",
                    "※必須",
                    "※ 必須",
                    "(必須)",
                    "（必須）",
                    "[必須]",
                    "［必須］",
                }

            strong_sources = {
                "dt_label",
                "dt_label_index",
                "th_label",
                "th_label_index",
                "label_for",
                "aria_labelledby",
                "ul_li_label",
            }
            near_sources_prefix = ("prev_sibling_", "label_parent")

            # 1) 強いコンテキストのみでの判定（推奨）
            for context in contexts:
                text = context.text or ""
                if any(tok in text for tok in optional_markers):
                    return False
                if context.source_type in strong_sources:
                    # 明示マーカー（必須、*, ＊ など）
                    if any(marker in text for marker in required_markers):
                        return True
                    # 『※』単独は許容しない（※必須のみ許容）。スターは許容。
                    if text.strip() in {"*", "＊"}:
                        return True

            # 2) 次善: 近傍の兄弟/親ラベル系
            for context in contexts:
                text = context.text or ""
                st = getattr(context, "source_type", "") or ""
                if any(tok in text for tok in optional_markers):
                    return False
                if st.startswith(near_sources_prefix) or st in {"parent_element"}:
                    if any(marker in text for marker in required_markers):
                        return True

            # 3) 位置ベース: 厳格条件（テキストが『必須』系のみ、近距離、上/左位置）
            for context in contexts:
                text = context.text or ""
                st = getattr(context, "source_type", "") or ""
                pos = getattr(context, "position_relative", "") or ""
                dist = float(getattr(context, "distance", 9999) or 9999)
                if st in {"nearby", "position", ""} or st not in strong_sources:
                    if (
                        _is_strict_required_text(text)
                        and pos in {"above", "left"}
                        and dist <= 50
                    ):
                        return True

            # 4) 旧式テーブル: 左隣のセル（TD/TH）に必須表示（※/必須）がある場合
            try:
                left_td_text = await element.evaluate(
                    """
                    el => {
                      const cell = el.closest('td,th');
                      if (!cell) return '';
                      const tr = cell.closest('tr');
                      if (!tr) return '';
                      const cells = Array.from(tr.children);
                      const idx = cells.indexOf(cell);
                      const pick = (node) => (node && node.tagName && ['td','th'].includes(node.tagName.toLowerCase())) ? (node.textContent||'').trim() : '';
                      let t='';
                      if (idx>0) t = pick(cells[idx-1]);
                      if (!t && cells.length>=2) t = pick(cells[0]);
                      return t;
                    }
                """
                )
            except Exception:
                left_td_text = ""
            if left_td_text:
                lt = left_td_text.strip()
                # 任意/optional が含まれる場合は除外
                if not any(x in lt for x in ["任意", "optional"]):
                    if (
                        "必須" in lt
                        or lt in {"*", "＊"}
                        or "※必須" in lt or "※ 必須" in lt
                        or "(必須)" in lt or "（必須）" in lt or "[必須]" in lt or "［必須］" in lt
                    ):
                        return True

            return False

        except Exception as e:
            logger.debug(f"Failed to detect required markers with context: {e}")
            # フォールバック
            surrounding_text = await self._get_surrounding_text(element)
            required_markers = [
                "*",
                "＊",
                "必須",
                "Required",
                "Mandatory",
                "Must",
                "(必須)",
                "（必須）",
                "[必須]",
                "［必須］",
            ]
            optional_markers = [
                "任意",
                "optional",
                "お分かりの場合",
                "分かる場合",
                "お持ちの場合",
                "あれば",
                "可能な範囲",
            ]
            if any(tok in surrounding_text for tok in optional_markers):
                return False
            return any(marker in surrounding_text for marker in required_markers)

    async def _get_surrounding_text(self, element: Locator) -> str:
        """
        要素の周辺テキストを取得（ラベル、前後のテキストなど）

        Args:
            element: 対象要素

        Returns:
            周辺テキスト
        """
        try:
            surrounding_text = ""

            # ラベル要素の検索（for属性による関連付け）
            element_id = await element.get_attribute("id")
            if element_id:
                try:
                    labels = element.page.locator(f"label[for='{element_id}']")
                    label_count = await labels.count()
                    for i in range(label_count):
                        label_text = await labels.nth(i).inner_text()
                        surrounding_text += " " + label_text
                except:
                    pass

            # 親要素内のテキスト検索
            try:
                parent = element.locator("..")
                parent_text = await parent.inner_text()
                surrounding_text += " " + parent_text
            except:
                pass

            # 前後の兄弟要素のテキスト
            try:
                # 前の兄弟要素
                prev_sibling = element.locator("xpath=preceding-sibling::*[1]")
                if await prev_sibling.count() > 0:
                    prev_text = await prev_sibling.inner_text()
                    surrounding_text += " " + prev_text

                # 次の兄弟要素
                next_sibling = element.locator("xpath=following-sibling::*[1]")
                if await next_sibling.count() > 0:
                    next_text = await next_sibling.inner_text()
                    surrounding_text += " " + next_text
            except:
                pass

            return surrounding_text.strip()

        except Exception as e:
            logger.debug(f"Failed to get surrounding text: {e}")
            return ""
