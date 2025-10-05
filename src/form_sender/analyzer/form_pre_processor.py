import asyncio
import logging
import re
from typing import Dict, List, Any

from playwright.async_api import Page

from .form_structure_analyzer import FormStructure, FormElement
from .split_field_detector import SplitFieldDetector
from .element_scorer import ElementScorer

logger = logging.getLogger(__name__)

class FormPreProcessor:
    """フォーム解析前の準備処理を担当するクラス"""

    def __init__(self, page: Page, element_scorer: ElementScorer, split_field_detector: SplitFieldDetector, field_patterns):
        self.page = page
        self.element_scorer = element_scorer
        self.split_field_detector = split_field_detector
        self.field_patterns = field_patterns
        # 分割姓名検出用の定数（漢字系のみを対象）
        self.LAST_NAME_TOKENS = [
            'lastname', 'last_name', 'last-name', 'last', 'family-name', 'family_name', 'surname', 'sei', '姓',
            'lname', 'l_name'
        ]
        self.FIRST_NAME_TOKENS = [
            'firstname', 'first_name', 'first-name', 'first', 'given-name', 'given_name', 'forename', 'mei', '名',
            'fname', 'f_name'
        ]
        # カナ/ふりがな/ひらがな等の指標（含む要素は分割姓名検出から除外）
        self.KANA_HIRA_INDICATORS = ['kana', 'katakana', 'furigana', 'フリガナ', 'カタカナ', 'ひらがな', 'hiragana']
        # 事前コンパイル（ホットパス最適化）
        self._kana_hira_boundary_re = re.compile(r'(^|[_\-])(kana|furigana|hiragana)($|[_\-])', re.IGNORECASE)
        self._katakana_boundary_re = re.compile(r'(^|[_\-])katakana($|[_\-])', re.IGNORECASE)
        
        # 推定グリッドの係数（マジックナンバー排除）
        self.ESTIMATED_ROW_HEIGHT = 50
        self.ESTIMATED_CELL_WIDTH = 100
        
    def _contains_kana_hira_indicator(self, blob: str) -> bool:
        """カナ/ふりがな指標の厳密判定（語境界/区切り対応・事前コンパイル済み）。"""
        s = (blob or '')
        sl = s.lower()
        if self._kana_hira_boundary_re.search(sl):
            return True
        if self._katakana_boundary_re.search(sl):
            return True
        for jp in ['フリガナ', 'カタカナ', 'ひらがな']:
            if jp in s:
                return True
        return False

    async def check_if_scroll_needed(self) -> bool:
        try:
            visible_form_elements = await self.page.locator('input, textarea, select').count()
            page_height = await self.page.evaluate("document.body.scrollHeight")
            viewport_height = await self.page.evaluate("window.innerHeight")
            if page_height <= viewport_height * 1.5 and visible_form_elements <= 10:
                return False
            bottom_elements_query = self.page.locator('input, textarea, select').nth(-1)
            if await bottom_elements_query.count() > 0:
                bottom_elements = await bottom_elements_query.bounding_box()
                if bottom_elements and bottom_elements['y'] > viewport_height:
                    return True
            return page_height > viewport_height * 2
        except Exception as e:
            logger.debug(f"Error checking scroll necessity, defaulting to scroll: {e}")
            return True

    async def perform_progressive_scroll(self):
        try:
            logger.info("Starting progressive scroll")
            page_height = await self.page.evaluate("document.body.scrollHeight")
            viewport_height = await self.page.evaluate("window.innerHeight")
            scroll_step = int(viewport_height * 0.75)
            scroll_positions = list(range(0, page_height + scroll_step, scroll_step))
            for i, position in enumerate(scroll_positions):
                await self.page.evaluate(f"window.scrollTo(0, {position})")
                await asyncio.sleep(0.3)
            await self.page.evaluate("window.scrollTo(0, 0)")
            await asyncio.sleep(0.2)
            logger.info("Progressive scroll completed")
        except Exception as e:
            logger.warning(f"Error during progressive scroll: {e}")

    def detect_unified_fields(self, structured_elements: List[FormElement]) -> Dict[str, Any]:
        patterns = self.field_patterns.get_unified_field_patterns()
        unified_info = {f"has_{key}": False for key in patterns.keys()}
        unified_info['detected_patterns'] = []
        # 分割姓名の存在可否（統合氏名の誤占有を防ぐために公開する）
        unified_info['has_name_split_fields'] = False
        
        # 厳密な分割姓名（漢字）検出: カナ/ひらがなを除外し、姓/名が別要素で存在する場合のみ True
        unified_info['has_name_split_fields'] = self._detect_split_name_fields_kanji_only(structured_elements)
        # カナ/ひらがな分割の存在可否（統合カナ/ひらがなの誤占有を防ぐ）
        unified_info['has_name_kana_split_fields'] = self._detect_split_name_kana_fields(structured_elements)
        has_name_split_fields = unified_info['has_name_split_fields']
        
        for el in structured_elements:
            text = f"{(el.name or '').lower()} {(el.id or '').lower()} {(el.label_text or '').lower()}"
            for key, pats in patterns.items():
                info_key = f"has_{key}"
                if not unified_info[info_key]:
                    # Skip unified fullname/kana/hiragana detection if split name fields are present
                    if key in ('fullname', 'kana_unified', 'hiragana_unified') and (
                        has_name_split_fields or unified_info.get('has_name_kana_split_fields')
                    ):
                        continue
                    
                    if any(p in text for p in pats):
                        unified_info[info_key] = True
                        unified_info['detected_patterns'].append(f"{key}:{el.name or el.id or el.label_text[:20]}")
                        logger.info(f"Unified {key} field detected: {el.name or el.id}")
                        break 
        return unified_info

    def _detect_split_name_kana_fields(self, structured_elements: List[FormElement]) -> bool:
        """カナ/ひらがなの分割姓名（セイ/メイ）が存在するかを判定。

        - name/id/placeholder/label にカナ/ふりがな指標が含まれること
        - かつ『セイ/姓』系と『メイ/名』系のシグナルが別要素に存在すること
        """
        last_like = []
        first_like = []
        for el in structured_elements:
            blob = ' '.join([
                (el.name or ''), (el.id or ''), (el.class_name or ''), (el.placeholder or ''),
                (el.label_text or ''), (el.associated_text or '')
            ])
            if not blob:
                continue
            if not self._contains_kana_hira_indicator(blob):
                continue
            s = blob
            # カタカナ/日本語の代表的な表記に対応
            if any(tok in s for tok in ['セイ', '姓']):
                last_like.append(el)
            if any(tok in s for tok in ['メイ', '名']):
                first_like.append(el)
        if not last_like or not first_like:
            return False
        return len({id(x) for x in last_like}.union({id(y) for y in first_like})) >= 2

    def _detect_split_name_fields_kanji_only(self, structured_elements: List[FormElement]) -> bool:
        """漢字の分割姓名が存在するか厳密に判定する。

        - カナ/ふりがな/ひらがな等の指標を含む要素は除外（sei_kana, メイ（カタカナ）等）。
        - 姓トークンと名トークンを満たす "異なる" 入力要素が少なくとも1つずつ存在する場合のみ True。
        - 1パス走査で収集して効率化。
        """
        last_elements: list[FormElement] = []
        first_elements: list[FormElement] = []

        for el in structured_elements:
            # placeholder も含めて分割姓名の手がかりにする（(姓)/(名) プレースホルダ対応）
            blob = ' '.join([
                (el.name or ''), (el.id or ''), (el.class_name or ''), (el.placeholder or ''),
                (el.label_text or ''), (el.associated_text or '')
            ]).lower()

            if not blob:
                continue

            # カナ/ひらがな要素は分割姓名として扱わない
            if self._contains_kana_hira_indicator(blob):
                continue

            # 入力系のみに限定
            tag = (el.tag_name or '').lower()
            if tag not in ['input', 'textarea']:
                continue

            if any(tok in blob for tok in self.LAST_NAME_TOKENS):
                last_elements.append(el)
            if any(tok in blob for tok in self.FIRST_NAME_TOKENS):
                first_elements.append(el)

        if not last_elements or not first_elements:
            return False
        last_ids = {id(el) for el in last_elements}
        first_ids = {id(el) for el in first_elements}
        # 少なくとも2つの異なる要素が姓/名に関与しているか（片側のみ・同一要素のみは除外）
        return len(last_ids.union(first_ids)) >= 2

    async def analyze_required_fields(self, structured_elements: List[FormElement]) -> Dict[str, Any]:
        analysis = {'total_form_elements': 0, 'required_elements_count': 0, 'required_elements': [], 'non_required_elements_count': 0, 'has_required_detection': False, 'treat_all_as_required': False}
        input_elements = [el for el in structured_elements if el.tag_name in ['input', 'textarea', 'select'] and el.element_type not in ['submit', 'button', 'hidden', 'image']]
        analysis['total_form_elements'] = len(input_elements)
        for el in input_elements:
            try:
                if await self.element_scorer._detect_required_status(el.locator):
                    analysis['required_elements_count'] += 1
                    analysis['required_elements'].append(el.name or el.id)
                else:
                    analysis['non_required_elements_count'] += 1
            except Exception as e:
                logger.debug(f"Failed to analyze required status for element: {e}")
                analysis['non_required_elements_count'] += 1
        if analysis['required_elements_count'] > 0:
            analysis['has_required_detection'] = True
        else:
            analysis['treat_all_as_required'] = True
        analysis['analysis_summary'] = f"Found {analysis['required_elements_count']} required of {analysis['total_form_elements']} fields"
        return analysis

    async def detect_form_type(self, structured_elements: List[FormElement], form_structure: FormStructure) -> Dict[str, Any]:
        """フォーム種別の簡易判定（汎用ヒューリスティクス）

        目的:
        - ニュースレター登録/サイト内検索など、問い合わせ以外のフォームを判別し、
          不要な必須項目チェック（例: お問い合わせ本文）を回避する。

        判定方針（複合スコアリング）:
        - contact_form: textarea もしくは message/本文/お問い合わせ などの語が要素に出現
        - search_form: input[name~="q"], placeholderに「検索」, actionに"search"
        - newsletter_form: email入力があり、textareaが無く、subscribe/登録/解除 等の語が出現
        - order_form/feedback_form: actionやテキストに該当語が出現
        - いずれにも強く当てはまらない場合は other_form
        """
        try:
            # 要素統計
            cnt = {
                'email': 0, 'text': 0, 'textarea': 0, 'select': 0, 'search': 0,
                'checkbox': 0, 'radio': 0, 'total_inputs': 0
            }
            tokens = []  # 各要素の name/id/placeholder/label/周辺テキストを集約

            for el in structured_elements or []:
                t = (el.element_type or '').lower()
                cnt['total_inputs'] += int(el.tag_name in ['input', 'textarea', 'select'])
                if el.tag_name == 'textarea':
                    cnt['textarea'] += 1
                elif el.tag_name == 'select':
                    cnt['select'] += 1
                elif el.tag_name == 'input':
                    if t == 'email':
                        cnt['email'] += 1
                    elif t == 'search':
                        cnt['search'] += 1
                    elif t == 'checkbox':
                        cnt['checkbox'] += 1
                    elif t == 'radio':
                        cnt['radio'] += 1
                    else:
                        cnt['text'] += 1

                blob = ' '.join([
                    (el.name or ''), (el.id or ''), (el.placeholder or ''), (el.label_text or ''),
                    (el.associated_text or ''), ' '.join(el.nearby_text or [])
                ]).lower()
                if blob:
                    tokens.append(blob)

            # form属性テキスト
            form_attr_text = ''
            if form_structure and form_structure.form_locator:
                try:
                    form_attr_text = (await form_structure.form_locator.evaluate(
                        "f => (f.getAttribute('action') || '') + ' ' + (f.id || '') + ' ' + (f.className || '')"
                    ) or '').lower()
                except Exception:
                    form_attr_text = ''

            def has_any(hay: str, keys: list[str]) -> bool:
                s = hay or ''
                return any(k in s for k in keys)

            def any_token(keys: list[str]) -> bool:
                return any(has_any(tok, keys) for tok in tokens)

            # キーワード集合
            contact_kw = ['contact', 'inquiry', 'お問い合わせ', '問い合わせ', 'お問合せ', '問合せ', 'toiawase', 'メッセージ', '本文', '内容']
            message_kw = ['message', '本文', 'ご用件', 'ご質問', 'ご相談', 'お問い合わせ内容', '内容']
            search_kw = ['search', '検索', 'site-search', 'cse']
            newsletter_kw = ['subscribe', 'subscription', 'newsletter', 'mailchimp', 'regist', 'unreg', 'mag2', 'rdemail', 'メルマガ', '購読', '登録', '解除']
            order_kw = ['order', 'checkout', 'cart', '購入', '決済']
            feedback_kw = ['feedback', 'アンケート', 'survey', 'ご意見', '評価']

            # スコアリング
            scores = {
                'contact_form': 0.0,
                'auth_form': 0.0,
                'search_form': 0.0,
                'newsletter_form': 0.0,
                'order_form': 0.0,
                'feedback_form': 0.0,
                'other_form': 0.0,
            }

            # contact: textarea or message-like tokens
            if cnt['textarea'] > 0:
                scores['contact_form'] += 3.0
            if any_token(contact_kw) or any_token(message_kw) or has_any(form_attr_text, contact_kw):
                scores['contact_form'] += 2.0
            if cnt['email'] > 0:
                scores['contact_form'] += 0.5  # 問い合わせでもよくある

            # auth: password/otp/captcha/login tokens
            # 強い指標: input[type=password] が存在
            # 'captcha' は問い合わせフォームでも一般的なため認証判定から除外
            if any(' password ' in f' {tok} ' for tok in tokens) or any('otp' in tok for tok in tokens):
                scores['auth_form'] += 1.0
            try:
                pwd_count = sum(1 for el in structured_elements if el.tag_name == 'input' and (el.element_type or '').lower() == 'password')
            except Exception:
                pwd_count = 0
            if pwd_count > 0:
                scores['auth_form'] += 3.0
            # 誤検出抑止: 'confirm/確認' は問い合わせフォームでも頻出のため除外
            auth_kw = ['login', 'signin', 'sign-in', 'sign_in', 'auth', 'authentication', 'ログイン', 'サインイン', 'パスワード', '認証', '二段階', 'ワンタイム', '確認コード', '認証コード', 'otp', 'mfa']
            if any_token(auth_kw) or has_any(form_attr_text, auth_kw):
                scores['auth_form'] += 2.0

            # search: search input or q/検索
            if cnt['search'] > 0:
                scores['search_form'] += 2.5
            # name=q/placeholder=検索 等
            if any(' q ' in f' {tok} ' or '検索' in tok for tok in tokens):
                scores['search_form'] += 1.5
            if has_any(form_attr_text, search_kw):
                scores['search_form'] += 1.0

            # newsletter: email only + subscribe-like tokens + no textarea
            if cnt['email'] >= 1 and cnt['textarea'] == 0:
                if any_token(newsletter_kw) or has_any(form_attr_text, newsletter_kw):
                    scores['newsletter_form'] += 3.0
                # 入力が少なく email 中心なら加点
                if cnt['total_inputs'] <= 3:
                    scores['newsletter_form'] += 1.0

            # order / feedback
            if any_token(order_kw) or has_any(form_attr_text, order_kw):
                scores['order_form'] += 2.0
            if any_token(feedback_kw) or has_any(form_attr_text, feedback_kw):
                scores['feedback_form'] += 2.0

            # 決定
            primary_type = max(scores.items(), key=lambda x: x[1])[0]
            top_score = scores[primary_type]

            # contact_formの誤検出回避：textareaもmessage語も無い場合は他タイプを優先
            if primary_type == 'contact_form' and cnt['textarea'] == 0 and not (any_token(message_kw) or any_token(contact_kw)):
                # newsletter/search がそれなりにスコアを持つならそちらに切替
                if scores['newsletter_form'] >= 2.0:
                    primary_type = 'newsletter_form'
                    top_score = scores['newsletter_form']
                elif scores['search_form'] >= 2.0:
                    primary_type = 'search_form'
                    top_score = scores['search_form']
                else:
                    primary_type = 'other_form'
                    top_score = scores['other_form']

            # auth_form は「正の証拠」がある場合のみ切り替える
            # 0点同点（全カテゴリ0）のケースでは auth にしない
            auth_score = scores['auth_form']
            others_max = max(
                scores['contact_form'], scores['newsletter_form'], scores['search_form'],
                scores['order_form'], scores['feedback_form'], scores['other_form']
            )
            if auth_score > 0 and auth_score >= others_max:
                primary_type = 'auth_form'
                top_score = auth_score

            # 信頼度（0-1に正規化の簡易版）
            confidence = min(1.0, max(0.0, top_score / 5.0))

            # 参考: 関連/非関連フィールド
            if primary_type == 'auth_form':
                relevant_fields = []
                irrelevant_fields = list(self.field_patterns.get_patterns().keys())
            elif primary_type == 'newsletter_form':
                relevant_fields = ['メールアドレス']
                irrelevant_fields = []
            elif primary_type == 'search_form':
                relevant_fields = []
                irrelevant_fields = []
            else:
                relevant_fields = list(self.field_patterns.get_patterns().keys())
                irrelevant_fields = []

            return {
                'primary_type': primary_type,
                'confidence': confidence,
                'relevant_fields': relevant_fields,
                'irrelevant_fields': irrelevant_fields
            }
        except Exception as e:
            logger.debug(f"detect_form_type failed, fallback to contact_form: {e}")
            return {
                'primary_type': 'contact_form',
                'confidence': 0.5,
                'relevant_fields': list(self.field_patterns.get_patterns().keys()),
                'irrelevant_fields': []
            }
