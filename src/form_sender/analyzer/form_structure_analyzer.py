"""
フォーム構造解析エンジン

<form>要素内の構造解析と並列要素検出機能
フィールドグループ化と説明テキストペアリングシステム
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from playwright.async_api import Locator
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class FormElement:
    """フォーム要素の詳細情報"""
    locator: Locator
    tag_name: str
    element_type: str
    selector: str
    
    # 属性情報
    name: str = ''
    id: str = ''
    class_name: str = ''
    placeholder: str = ''
    
    # 位置・表示情報
    bounding_box: Optional[Dict[str, float]] = None
    is_visible: bool = True
    is_enabled: bool = True
    
    # 構造情報
    parent_info: Optional[Dict[str, Any]] = None
    children_info: List[Dict[str, Any]] = field(default_factory=list)
    sibling_info: List[Dict[str, Any]] = field(default_factory=list)
    
    # テキスト情報
    associated_text: str = ''
    nearby_text: List[str] = field(default_factory=list)
    label_text: str = ''
    
    def __hash__(self):
        """ハッシュ値を計算（セット操作でのユニーク性確保用）"""
        return hash((self.selector, self.tag_name, self.element_type, self.name, self.id))
    
    def __eq__(self, other):
        """等価比較（ハッシュと組み合わせて使用）"""
        if not isinstance(other, FormElement):
            return False
        return (self.selector == other.selector and 
                self.tag_name == other.tag_name and
                self.element_type == other.element_type and
                self.name == other.name and
                self.id == other.id)


@dataclass 
class TableStructure:
    """テーブル構造の情報"""
    table_locator: Locator
    rows: List[Dict[str, Any]] = field(default_factory=list)
    headers: List[str] = field(default_factory=list)
    form_fields_in_table: List[FormElement] = field(default_factory=list)
    table_type: str = 'form_table'  # form_table, data_table, layout_table


@dataclass
class FormStructure:
    """フォーム全体の構造情報"""
    form_locator: Optional[Locator]
    form_bounds: Optional[Dict[str, float]]
    elements: List[FormElement] = field(default_factory=list)
    groups: List[List[FormElement]] = field(default_factory=list)
    parallel_groups: List[List[FormElement]] = field(default_factory=list)
    table_structures: List[TableStructure] = field(default_factory=list)  # テーブル構造情報


class FormStructureAnalyzer:
    """フォーム構造解析メインクラス"""
    
    def __init__(self, page_or_frame):
        """
        Args:
            page_or_frame: PlaywrightのPageまたはFrameオブジェクト
        """
        self.page = page_or_frame
        
        # 解析設定
        self.settings = {
            'form_boundary_strict': True,      # <form>要素境界の厳密チェック
            'parallel_threshold': 0.8,         # 並列要素判定の閾値
            'text_search_radius': 100,         # 周辺テキスト検索半径（px）
            'group_detection_enabled': True,   # グループ検出を有効にする
            'sibling_analysis_depth': 3,       # 兄弟要素解析の深度
            # Important修正4: フォーム境界チェック強化
            'require_form_element': True,      # <form>要素の存在を必須とする
            'allow_sales_prohibition_check': True,  # 営業禁止文言判定のための例外（ページ全体検索）
            'max_elements_without_form': 20    # <form>なし時の最大要素数制限
        }
        
        logger.info("FormStructureAnalyzer initialized")
    
    async def analyze_form_structure(self) -> FormStructure:
        """
        フォーム全体の構造解析を実行
        
        Returns:
            FormStructure: 解析されたフォーム構造
        """
        logger.info("Starting form structure analysis")
        
        # Step 1: <form>要素の特定
        form_locator = await self._find_primary_form()
        
        # Important修正4: フォーム境界チェック強化
        if not form_locator:
            if self.settings['require_form_element']:
                logger.warning("No <form> element found - applying strict boundary rules")
                # フォーム要素なしでの処理制限
                return await self._handle_no_form_case()
            else:
                logger.warning("No <form> element found, analyzing entire page")
        
        # Step 2: フォーム境界内の要素収集（厳格化）
        form_elements = await self._collect_form_elements(form_locator)
        logger.info(f"Collected {len(form_elements)} form elements from {'form boundary' if form_locator else 'entire page'}")
        
        # Step 3: 要素の詳細情報取得
        detailed_elements = []
        for element in form_elements:
            try:
                detailed_element = await self._create_detailed_element(element)
                if detailed_element:
                    detailed_elements.append(detailed_element)
            except Exception as e:
                logger.debug(f"Error creating detailed element: {e}")
                continue
        
        logger.info(f"Created {len(detailed_elements)} detailed elements")
        
        # Step 4: 並列要素群の検出
        parallel_groups = await self._detect_parallel_groups(detailed_elements)
        logger.info(f"Detected {len(parallel_groups)} parallel groups")
        
        # Step 5: テーブル構造解析（新機能）
        table_structures = await self._analyze_table_structures(form_locator, detailed_elements)
        logger.info(f"Detected {len(table_structures)} table structures")
        
        # Step 6: フィールドグループの作成
        field_groups = await self._create_field_groups(detailed_elements, parallel_groups)
        logger.info(f"Created {len(field_groups)} field groups")
        
        # Step 7: フォーム境界の情報取得
        form_bounds = None
        if form_locator:
            try:
                form_bounds = await form_locator.bounding_box()
            except Exception as e:
                logger.debug(f"Could not get form bounds: {e}")
        
        # 構造オブジェクトの作成
        structure = FormStructure(
            form_locator=form_locator,
            form_bounds=form_bounds,
            elements=detailed_elements,
            groups=field_groups,
            parallel_groups=parallel_groups,
            table_structures=table_structures
        )
        
        logger.info("Form structure analysis completed successfully")
        return structure
    
    async def _find_primary_form(self) -> Optional[Locator]:
        """
        主要な<form>要素を特定
        
        Returns:
            Optional[Locator]: 見つかった<form>要素
        """
        try:
            # 全ての<form>要素を取得
            forms = await self.page.locator('form').all()
            
            if not forms:
                return None
            
            if len(forms) == 1:
                logger.info("Single form element found")
                return forms[0]
            
            # 複数のフォームがある場合、問い合わせフォームらしさをスコアリングして選択
            best_form = None
            best_score = -1.0
            contact_keywords = ['contact', 'inquiry', 'お問い合わせ', '問い合わせ', 'toiawase', 'お問合せ', '問合せ']
            negative_action_keywords = ['search', 'order', 'checkout', 'cart']

            for form in forms:
                try:
                    is_visible = await form.is_visible()

                    # 単発evaluateでカウントと主要属性を一括取得（拡張）
                    try:
                        data = await form.evaluate(
                            """
                            f => ({
                                email: f.querySelectorAll('input[type="email"], input[type="mail"]').length,
                                text: f.querySelectorAll('input[type="text"], input[type="tel"], input[type="url"], input[type="number"], input:not([type])').length,
                                textarea: f.querySelectorAll('textarea').length,
                                select: f.querySelectorAll('select').length,
                                search: f.querySelectorAll('input[type="search"]').length,
                                hidden: f.querySelectorAll('input[type="hidden"]').length,
                                submit: f.querySelectorAll('input[type="submit"], button[type="submit"], button').length,
                                action: f.getAttribute('action') || '',
                                klass: f.getAttribute('class') || '',
                                fid: f.getAttribute('id') || '',
                                role: f.getAttribute('role') || '',
                                reqCount: f.querySelectorAll('[required], [aria-required="true"], .wpcf7-validates-as-required').length,
                                btnText: (f.querySelector('button, input[type="submit"]')?.innerText || f.querySelector('input[type="submit"]')?.value || '')
                            })
                            """
                        )
                    except Exception:
                        data = {
                            'email': 0, 'text': 0, 'textarea': 0, 'select': 0,
                            'search': 0, 'hidden': 0, 'submit': 0,
                            'action': '', 'klass': '', 'fid': '', 'role': '',
                            'reqCount': 0, 'btnText': ''
                        }

                    cnt_email = data['email']
                    cnt_text = data['text']
                    cnt_textarea = data['textarea']
                    cnt_select = data['select']
                    cnt_search = data['search']
                    cnt_hidden = data['hidden']
                    cnt_submit = data['submit']

                    action = data['action']
                    form_class = data['klass']
                    form_id = data['fid']
                    role = data['role']
                    attr_text = f"{action} {form_class} {form_id} {role}".lower()

                    # スコア計算（問い合わせフォームらしさ重視 + 誤選択抑止）
                    score = 0.0
                    score += cnt_email * 3.0
                    score += cnt_textarea * 3.5  # textarea の存在は問い合わせ性が高い
                    score += cnt_text * 1.5
                    score += cnt_select * 1.0
                    score += min(cnt_submit, 3) * 0.2  # 送信ボタンは軽めの加点
                    score -= cnt_search * 2.0          # 検索フォームは強く減点
                    score -= min(cnt_hidden, 10) * 0.05

                    if any(k in attr_text for k in contact_keywords):
                        score += 5.0
                    # subscribe/unsubscribe の扱いを明確化
                    btn_text = (data.get('btnText','') or '').lower()
                    meta = (attr_text + ' ' + btn_text)
                    if any(k in meta for k in ['subscribe','登録']):
                        score += 2.0
                    neg_keys = negative_action_keywords + ['unsubscribe','解除','配信停止','退会','削除']
                    if any(k in meta for k in neg_keys):
                        score -= 6.0
                    # 必須項目の多さ（問い合わせフォームらしさ）
                    try:
                        score += min(5.0, float(data.get('reqCount',0)) * 0.5)
                    except Exception:
                        pass

                    if not is_visible:
                        score *= 0.1

                    logger.debug(
                        f"Form score: email={cnt_email}, textarea={cnt_textarea}, text={cnt_text}, select={cnt_select}, "
                        f"search={cnt_search}, hidden={cnt_hidden}, submit={cnt_submit}, visible={is_visible} -> score={score:.2f}"
                    )

                    if score > best_score:
                        best_score = score
                        best_form = form
                except Exception as e:
                    logger.debug(f"Error evaluating form: {e}")
                    continue
            
            if best_form:
                logger.info(f"Selected best form with score {best_score:.2f}")
            
            return best_form
            
        except Exception as e:
            logger.error(f"Error finding primary form: {e}")
            return None
    
    async def _handle_no_form_case(self) -> FormStructure:
        """
        <form>要素がない場合の厳格処理
        フォーム要素が存在しない場合は、フォームマッピング処理を行わない
        
        Returns:
            FormStructure: 空の構造情報
        """
        logger.info("No form elements found - returning empty structure for form mapping")
        
        # form要素がない場合はフォームマッピングを実行しない
        # 営業禁止文言判定は別途SalesProhibitionDetectorで実行される
        return FormStructure(
            form_locator=None,
            form_bounds=None,
            elements=[],                # 空のリスト - form要素外の要素は処理しない
            groups=[],
            parallel_groups=[],
            table_structures=[]
        )
    
    async def _collect_form_elements(self, form_locator: Optional[Locator]) -> List[Locator]:
        """
        フォーム境界内の要素を収集（厳格なform要素境界チェック）
        
        Args:
            form_locator: フォーム要素（Noneの場合は要素収集を行わない）
            
        Returns:
            List[Locator]: 収集された要素
        """
        # form要素が存在しない場合は要素収集を行わない
        if not form_locator:
            if self.settings.get('require_form_element', True):
                logger.info("No form locator provided - skipping element collection to maintain form boundary")
                return []
            else:
                logger.warning("Form boundary enforcement is disabled - this may include elements outside forms")
        
        element_selectors = [
            'input[type="text"]',
            'input[type="email"]',
            'input[type="mail"]',  # 一部サイトの独自型（メール）を正式サポート
            'input[type="tel"]', 
            'input[type="url"]',
            'input[type="number"]',
            'input[type="password"]',
            'input[type="search"]',
            'input:not([type]), input[type=""]',
            'textarea',
            'select',
            'input[type="checkbox"]',
            'input[type="radio"]'
        ]
        
        elements = []
        
        for selector in element_selectors:
            try:
                # form_locator内の要素のみを収集（厳格な境界チェック）
                found_elements = await form_locator.locator(selector).all()
                elements.extend(found_elements)
                logger.debug(f"Found {len(found_elements)} elements within form boundary for selector: {selector}")
                
            except Exception as e:
                logger.debug(f"Error collecting elements for selector {selector}: {e}")
                continue
        
        # 重複除去（過剰除去の安全弁つき）
        unique_elements = await self._deduplicate_elements(elements)
        # 過剰に 1 件へ潰れてしまうケースを回避（要素が十分あるのに1件以下になったら元リストを採用）
        try:
            if len(elements) >= 5 and len(unique_elements) <= 1:
                logger.warning(f"Deduplication reduced {len(elements)} -> {len(unique_elements)}; reverting to original list for safety")
                return elements
        except Exception:
            pass
        return unique_elements
    
    async def _deduplicate_elements(self, elements: List[Locator]) -> List[Locator]:
        """要素の重複除去"""
        unique_elements = []
        seen_signatures = set()
        
        for element in elements:
            try:
                # 要素の一意識別子を作成
                tag_name = await element.evaluate("el => el.tagName.toLowerCase()")
                element_id = await element.get_attribute('id') or ''
                element_name = await element.get_attribute('name') or ''
                element_type = await element.get_attribute('type') or ''
                
                # 位置情報も含めてより厳密に判定
                try:
                    bounds = await element.bounding_box()
                    position = f"{bounds['x']},{bounds['y']}" if bounds else "0,0"
                except:
                    position = "0,0"
                
                # オブジェクトIDも含めてサインを生成（過剰な重複除去を防止）
                base = f"{tag_name}|{element_id}|{element_name}|{element_type}|{position}"
                signature = f"{base}|obj:{id(element)}"
                
                if signature not in seen_signatures:
                    seen_signatures.add(signature)
                    unique_elements.append(element)
                    
            except Exception as e:
                logger.debug(f"Error creating element signature: {e}")
                continue
        
        logger.debug(f"Deduplicated {len(elements)} elements to {len(unique_elements)}")
        return unique_elements
    
    async def _create_detailed_element(self, locator: Locator) -> Optional[FormElement]:
        """
        要素の詳細情報を作成
        
        Args:
            locator: 要素のロケーター
            
        Returns:
            Optional[FormElement]: 詳細情報付き要素
        """
        try:
            # 基本属性の取得
            tag_name = await locator.evaluate("el => el.tagName.toLowerCase()")
            element_type = (await locator.get_attribute('type') or '').lower()
            
            # 属性情報の取得
            name = await locator.get_attribute('name') or ''
            element_id = await locator.get_attribute('id') or ''
            class_name = await locator.get_attribute('class') or ''
            placeholder = await locator.get_attribute('placeholder') or ''
            
            # セレクター生成
            selector = await self._generate_element_selector(locator)
            
            # 位置・表示情報
            bounding_box = None
            is_visible = True
            is_enabled = True
            
            try:
                bounding_box = await locator.bounding_box()
                is_visible = await locator.is_visible()
                is_enabled = await locator.is_enabled()
            except Exception as e:
                logger.debug(f"Error getting element properties: {e}")
            
            # 周辺テキストの取得
            associated_text, nearby_text = await self._extract_associated_text(locator, bounding_box)
            
            # ラベルテキストの取得
            label_text = await self._extract_label_text(locator)
            
            # 親子関係の情報取得
            parent_info = await self._get_parent_info(locator)
            sibling_info = await self._get_sibling_info(locator)
            
            element = FormElement(
                locator=locator,
                tag_name=tag_name,
                element_type=element_type,
                selector=selector,
                name=name,
                id=element_id,
                class_name=class_name,
                placeholder=placeholder,
                bounding_box=bounding_box,
                is_visible=is_visible,
                is_enabled=is_enabled,
                parent_info=parent_info,
                sibling_info=sibling_info,
                associated_text=associated_text,
                nearby_text=nearby_text,
                label_text=label_text
            )
            
            return element
            
        except Exception as e:
            logger.debug(f"Error creating detailed element: {e}")
            return None
    
    async def _extract_associated_text(self, locator: Locator, bounding_box: Optional[Dict[str, float]]) -> Tuple[str, List[str]]:
        """
        要素に関連するテキストを抽出
        
        Args:
            locator: 要素のロケーター
            bounding_box: 要素の位置情報
            
        Returns:
            tuple: (関連テキスト, 近隣テキストリスト)
        """
        associated_text = ''
        nearby_text = []
        
        try:
            # 親要素のテキストコンテンツを取得
            parent = locator.locator('..')
            parent_text = await parent.text_content() or ''
            
            # 入力要素のテキストを除外
            input_text = await locator.evaluate("""
                el => {
                    if (el.tagName.toLowerCase() === 'input') return el.value || '';
                    if (el.tagName.toLowerCase() === 'textarea') return el.textContent || '';
                    return '';
                }
            """) or ''
            
            # 親要素のテキストから入力値を除外
            if parent_text and input_text:
                associated_text = parent_text.replace(input_text, '').strip()
            else:
                associated_text = parent_text.strip()
            
            # 近隣要素のテキストも取得（簡略版）
            if bounding_box:
                # 実際の実装では周辺の要素を位置ベースで取得する
                # ここでは簡略化して兄弟要素のテキストを取得
                siblings = await locator.evaluate("""
                    el => {
                        const siblings = [];
                        let sibling = el.previousElementSibling;
                        while (sibling && siblings.length < 3) {
                            const text = sibling.textContent?.trim();
                            if (text && text.length < 100) siblings.push(text);
                            sibling = sibling.previousElementSibling;
                        }
                        sibling = el.nextElementSibling;
                        while (sibling && siblings.length < 5) {
                            const text = sibling.textContent?.trim();
                            if (text && text.length < 100) siblings.push(text);
                            sibling = sibling.nextElementSibling;
                        }
                        return siblings;
                    }
                """)
                
                nearby_text = siblings or []
            
        except Exception as e:
            logger.debug(f"Error extracting associated text: {e}")
        
        return associated_text, nearby_text
    
    async def _extract_label_text(self, locator: Locator) -> str:
        """
        ラベルテキストを抽出
        
        Args:
            locator: 要素のロケーター
            
        Returns:
            str: ラベルテキスト
        """
        try:
            # 1. label要素での関連付けをチェック
            element_id = await locator.get_attribute('id')
            if element_id:
                esc_for = str(element_id).replace('\\', r'\\').replace('"', r'\"')
                label_locator = self.page.locator(f'label[for="{esc_for}"]')
                label_count = await label_locator.count()
                if label_count > 0:
                    label_text = await label_locator.text_content()
                    if label_text:
                        return label_text.strip()
            
            # 2. 親要素がlabelかチェック
            parent_tag = await locator.evaluate("el => el.parentElement?.tagName.toLowerCase()")
            if parent_tag == 'label':
                parent_locator = locator.locator('..')
                label_text = await parent_locator.text_content()
                if label_text:
                    return label_text.strip()
            
            # 3. aria-labelledbyでの関連付けをチェック
            labelledby_id = await locator.get_attribute('aria-labelledby')
            if labelledby_id:
                # aria-labelledby は空白区切りで複数IDになることがあるため先頭のみ使用
                first_id = (labelledby_id or '').split()[0].strip()
                if first_id:
                    esc_first = first_id.replace('\\', r'\\').replace('"', r'\"')
                    label_locator = self.page.locator(f'[id="{esc_first}"]')
                    label_count = await label_locator.count()
                    if label_count > 0:
                        label_text = await label_locator.text_content()
                        if label_text:
                            return label_text.strip()
            
        except Exception as e:
            logger.debug(f"Error extracting label text: {e}")
        
        return ''
    
    async def _get_parent_info(self, locator: Locator) -> Optional[Dict[str, Any]]:
        """親要素の情報を取得"""
        try:
            parent_info = await locator.evaluate("""
                el => {
                    const parent = el.parentElement;
                    if (!parent) return null;
                    
                    return {
                        tag_name: parent.tagName.toLowerCase(),
                        class_name: parent.className || '',
                        id: parent.id || '',
                        text_content: parent.textContent?.substring(0, 200) || ''
                    };
                }
            """)
            return parent_info
        except:
            return None
    
    async def _get_sibling_info(self, locator: Locator) -> List[Dict[str, Any]]:
        """兄弟要素の情報を取得"""
        try:
            sibling_info = await locator.evaluate(f"""
                el => {{
                    const siblings = [];
                    const maxSiblings = {self.settings['sibling_analysis_depth']};
                    
                    // Previous siblings
                    let sibling = el.previousElementSibling;
                    let count = 0;
                    while (sibling && count < maxSiblings) {{
                        siblings.push({{
                            tag_name: sibling.tagName.toLowerCase(),
                            class_name: sibling.className || '',
                            id: sibling.id || '',
                            text_content: sibling.textContent?.substring(0, 100) || '',
                            position: 'before'
                        }});
                        sibling = sibling.previousElementSibling;
                        count++;
                    }}
                    
                    // Next siblings
                    sibling = el.nextElementSibling;
                    count = 0;
                    while (sibling && count < maxSiblings) {{
                        siblings.push({{
                            tag_name: sibling.tagName.toLowerCase(),
                            class_name: sibling.className || '',
                            id: sibling.id || '',
                            text_content: sibling.textContent?.substring(0, 100) || '',
                            position: 'after'
                        }});
                        sibling = sibling.nextElementSibling;
                        count++;
                    }}
                    
                    return siblings;
                }}
            """)
            return sibling_info or []
        except:
            return []
    
    async def _generate_element_selector(self, locator: Locator) -> str:
        """要素のセレクターを生成（軽量版）"""
        try:
            try:
                info = await locator.evaluate(
                    "el => ({ id: el.getAttribute('id')||'', name: el.getAttribute('name')||'', tag: el.tagName.toLowerCase(), type: el.getAttribute('type')||'' })"
                )
            except Exception:
                info = {'id': '', 'name': '', 'tag': 'input', 'type': ''}

            if info.get('id'):
                esc_id = str(info['id']).replace('\\', r'\\').replace('"', r'\"')
                return f"[id=\"{esc_id}\"]"

            name = info.get('name')
            tag = info.get('tag') or 'input'
            typ = info.get('type')
            if name:
                esc_name = str(name).replace('\\', r'\\').replace('"', r'\"')
                if typ:
                    esc_type = str(typ).replace('\\', r'\\').replace('"', r'\"')
                    return f"{tag}[name=\"{esc_name}\"][type=\"{esc_type}\"]"
                return f"{tag}[name=\"{esc_name}\"]"

            if typ:
                esc_type2 = str(typ).replace('\\', r'\\').replace('"', r'\"')
                return f"{tag}[type=\"{esc_type2}\"]"
            return tag
        except Exception as e:
            logger.warning(f"Error generating selector: {e}")
            return 'input'
    
    async def _detect_parallel_groups(self, elements: List[FormElement]) -> List[List[FormElement]]:
        """
        並列要素群を検出
        
        Args:
            elements: フォーム要素のリスト
            
        Returns:
            List[List[FormElement]]: 並列要素群のリスト
        """
        if not self.settings['group_detection_enabled']:
            return []
        
        parallel_groups = []
        processed_elements = set()
        
        for element in elements:
            if element in processed_elements:
                continue
            
            # この要素と類似した構造の要素を検索
            similar_elements = await self._find_similar_structure_elements(element, elements)
            
            if len(similar_elements) >= 2:  # 自分自身を含めて2つ以上
                parallel_groups.append(similar_elements)
                processed_elements.update(similar_elements)
                logger.debug(f"Detected parallel group with {len(similar_elements)} elements")
        
        return parallel_groups
    
    async def _find_similar_structure_elements(self, target_element: FormElement, all_elements: List[FormElement]) -> List[FormElement]:
        """
        類似構造の要素を検索
        
        Args:
            target_element: 基準要素
            all_elements: 全要素リスト
            
        Returns:
            List[FormElement]: 類似構造の要素リスト
        """
        similar_elements = [target_element]
        
        for element in all_elements:
            if element == target_element:
                continue
            
            # 構造類似性の評価
            similarity_score = await self._calculate_structure_similarity(target_element, element)
            
            if similarity_score >= self.settings['parallel_threshold']:
                similar_elements.append(element)
        
        return similar_elements
    
    async def _calculate_structure_similarity(self, element1: FormElement, element2: FormElement) -> float:
        """
        構造類似性スコアを計算
        
        Args:
            element1: 要素1
            element2: 要素2
            
        Returns:
            float: 類似性スコア（0-1）
        """
        score = 0.0
        total_weight = 0.0
        
        # タグ名の一致
        if element1.tag_name == element2.tag_name:
            score += 0.3
        total_weight += 0.3
        
        # タイプの一致
        if element1.element_type == element2.element_type:
            score += 0.2
        total_weight += 0.2
        
        # 親要素の一致
        if (element1.parent_info and element2.parent_info and 
            element1.parent_info.get('tag_name') == element2.parent_info.get('tag_name')):
            score += 0.3
        total_weight += 0.3
        
        # クラス名の類似性
        class1 = set(element1.class_name.split()) if element1.class_name else set()
        class2 = set(element2.class_name.split()) if element2.class_name else set()
        if class1 and class2:
            class_similarity = len(class1.intersection(class2)) / len(class1.union(class2))
            score += class_similarity * 0.2
        total_weight += 0.2
        
        return score / total_weight if total_weight > 0 else 0.0
    
    async def _create_field_groups(self, elements: List[FormElement], parallel_groups: List[List[FormElement]]) -> List[List[FormElement]]:
        """
        フィールドグループを作成
        
        Args:
            elements: 全要素
            parallel_groups: 並列要素群
            
        Returns:
            List[List[FormElement]]: フィールドグループ
        """
        field_groups = []
        grouped_elements = set()
        
        # 並列要素群をそのままグループとする
        for parallel_group in parallel_groups:
            field_groups.append(parallel_group)
            grouped_elements.update(parallel_group)
        
        # 残りの要素を個別グループとして追加
        for element in elements:
            if element not in grouped_elements:
                field_groups.append([element])
        
        return field_groups
    
    async def _analyze_table_structures(self, form_locator: Optional[Locator], 
                                      form_elements: List[FormElement]) -> List[TableStructure]:
        """
        テーブル構造を解析（強化版）
        
        Args:
            form_locator: フォーム要素
            form_elements: フォーム内の要素リスト
            
        Returns:
            List[TableStructure]: 検出されたテーブル構造
        """
        table_structures = []
        
        try:
            # フォーム内または全ページのテーブルを検索
            if form_locator and self.settings['form_boundary_strict']:
                tables = await form_locator.locator('table').all()
            else:
                tables = await self.page.locator('table').all()
            
            logger.debug(f"Found {len(tables)} table elements")
            
            for table in tables:
                try:
                    # テーブル内にフォーム要素があるかチェック
                    table_form_elements = await self._find_form_elements_in_table(table, form_elements)
                    
                    if len(table_form_elements) >= 1:  # 少なくとも1つのフォーム要素がある
                        table_structure = await self._analyze_single_table_structure(table, table_form_elements)
                        if table_structure:
                            table_structures.append(table_structure)
                            logger.info(f"Added table structure with {len(table_form_elements)} form elements")
                    
                except Exception as e:
                    logger.debug(f"Error analyzing table: {e}")
                    continue
            
        except Exception as e:
            logger.debug(f"Error in table structure analysis: {e}")
        
        return table_structures
    
    async def _find_form_elements_in_table(self, table_locator: Locator, 
                                         form_elements: List[FormElement]) -> List[FormElement]:
        """
        テーブル内のフォーム要素を特定
        
        Args:
            table_locator: テーブル要素
            form_elements: 全フォーム要素
            
        Returns:
            List[FormElement]: テーブル内のフォーム要素
        """
        table_form_elements = []
        
        try:
            # テーブルの境界を取得
            table_bounds = await table_locator.bounding_box()
            if not table_bounds:
                return table_form_elements
            
            for element in form_elements:
                if not element.bounding_box:
                    continue
                
                # 要素がテーブル内に含まれているかチェック
                element_bounds = element.bounding_box
                
                is_inside = (
                    element_bounds['x'] >= table_bounds['x'] and
                    element_bounds['y'] >= table_bounds['y'] and
                    element_bounds['x'] + element_bounds['width'] <= table_bounds['x'] + table_bounds['width'] and
                    element_bounds['y'] + element_bounds['height'] <= table_bounds['y'] + table_bounds['height']
                )
                
                if is_inside:
                    table_form_elements.append(element)
            
        except Exception as e:
            logger.debug(f"Error finding form elements in table: {e}")
        
        return table_form_elements
    
    async def _analyze_single_table_structure(self, table_locator: Locator, 
                                            table_form_elements: List[FormElement]) -> Optional[TableStructure]:
        """
        単一テーブルの構造解析
        
        Args:
            table_locator: テーブル要素
            table_form_elements: テーブル内のフォーム要素
            
        Returns:
            Optional[TableStructure]: テーブル構造情報
        """
        try:
            # テーブルヘッダーの抽出
            headers = await self._extract_table_headers(table_locator)
            
            # テーブル行の解析
            rows = await self._extract_table_rows(table_locator, table_form_elements)
            
            # テーブルタイプの判定
            table_type = await self._determine_table_type(table_locator, table_form_elements)
            
            return TableStructure(
                table_locator=table_locator,
                rows=rows,
                headers=headers,
                form_fields_in_table=table_form_elements,
                table_type=table_type
            )
            
        except Exception as e:
            logger.debug(f"Error analyzing single table structure: {e}")
            return None
    
    async def _extract_table_headers(self, table_locator: Locator) -> List[str]:
        """
        テーブルヘッダーを抽出
        
        Args:
            table_locator: テーブル要素
            
        Returns:
            List[str]: ヘッダーテキストのリスト
        """
        headers = []
        
        try:
            # th要素を優先的に検索
            th_elements = await table_locator.locator('th').all()
            
            if th_elements:
                for th in th_elements:
                    header_text = await th.text_content()
                    if header_text and header_text.strip():
                        headers.append(header_text.strip())
            
            # th要素がない場合、最初の行のtd要素をヘッダーとして扱う
            if not headers:
                first_row = table_locator.locator('tr').first
                if await first_row.count() > 0:
                    first_row_cells = await first_row.locator('td').all()
                    for cell in first_row_cells:
                        cell_text = await cell.text_content()
                        if cell_text and cell_text.strip():
                            headers.append(cell_text.strip())
            
            logger.debug(f"Extracted {len(headers)} table headers: {headers[:5]}")  # 最初の5つのみログ
            
        except Exception as e:
            logger.debug(f"Error extracting table headers: {e}")
        
        return headers
    
    async def _extract_table_rows(self, table_locator: Locator, 
                                table_form_elements: List[FormElement]) -> List[Dict[str, Any]]:
        """
        テーブル行の詳細情報を抽出
        
        Args:
            table_locator: テーブル要素
            table_form_elements: テーブル内のフォーム要素
            
        Returns:
            List[Dict[str, Any]]: 行情報のリスト
        """
        rows = []
        
        try:
            tr_elements = await table_locator.locator('tr').all()
            
            for i, tr in enumerate(tr_elements):
                try:
                    row_info = {
                        'row_index': i,
                        'cells': [],
                        'form_elements_in_row': [],
                        'row_type': 'data'  # header, data, form
                    }
                    
                    # セル情報の抽出
                    cells = await tr.locator('td, th').all()
                    
                    for j, cell in enumerate(cells):
                        cell_info = {
                            'cell_index': j,
                            'text': (await cell.text_content() or '').strip(),
                            'has_form_element': False,
                            'form_elements': []
                        }
                        
                        # このセル内のフォーム要素を特定
                        cell_bounds = await cell.bounding_box()
                        if cell_bounds:
                            for form_element in table_form_elements:
                                if self._is_element_in_cell(form_element, cell_bounds):
                                    cell_info['has_form_element'] = True
                                    cell_info['form_elements'].append({
                                        'element_type': form_element.element_type,
                                        'name': form_element.name,
                                        'id': form_element.id
                                    })
                                    row_info['form_elements_in_row'].append(form_element)
                        
                        row_info['cells'].append(cell_info)
                    
                    # 行タイプの判定
                    if i == 0 and any(cell.get('text') for cell in row_info['cells']):
                        row_info['row_type'] = 'header'
                    elif row_info['form_elements_in_row']:
                        row_info['row_type'] = 'form'
                    
                    rows.append(row_info)
                    
                except Exception as e:
                    logger.debug(f"Error processing table row {i}: {e}")
                    continue
            
            logger.debug(f"Extracted {len(rows)} table rows")
            
        except Exception as e:
            logger.debug(f"Error extracting table rows: {e}")
        
        return rows
    
    def _is_element_in_cell(self, form_element: FormElement, cell_bounds: Dict[str, float]) -> bool:
        """
        フォーム要素がセル内に含まれているかチェック
        
        Args:
            form_element: フォーム要素
            cell_bounds: セルの境界情報
            
        Returns:
            bool: セル内に含まれているかどうか
        """
        if not form_element.bounding_box:
            return False
        
        element_bounds = form_element.bounding_box
        
        # 要素の中心点がセル内にあるかチェック
        element_center_x = element_bounds['x'] + element_bounds['width'] / 2
        element_center_y = element_bounds['y'] + element_bounds['height'] / 2
        
        return (
            element_center_x >= cell_bounds['x'] and
            element_center_x <= cell_bounds['x'] + cell_bounds['width'] and
            element_center_y >= cell_bounds['y'] and
            element_center_y <= cell_bounds['y'] + cell_bounds['height']
        )
    
    async def _determine_table_type(self, table_locator: Locator, 
                                  table_form_elements: List[FormElement]) -> str:
        """
        テーブルタイプを判定
        
        Args:
            table_locator: テーブル要素
            table_form_elements: テーブル内のフォーム要素
            
        Returns:
            str: テーブルタイプ (form_table, data_table, layout_table)
        """
        try:
            # フォーム要素の比率でタイプを判定
            total_cells = await table_locator.locator('td, th').count()
            form_element_count = len(table_form_elements)
            
            if total_cells == 0:
                return 'layout_table'
            
            form_ratio = form_element_count / total_cells
            
            if form_ratio > 0.3:  # 30%以上のセルにフォーム要素
                return 'form_table'
            elif form_ratio > 0:  # フォーム要素が少しでもある
                return 'form_table'
            else:
                # クラス名やID属性でレイアウトテーブルかチェック
                table_class = await table_locator.get_attribute('class') or ''
                table_id = await table_locator.get_attribute('id') or ''
                
                layout_keywords = ['layout', 'design', 'style', 'container']
                if any(keyword in table_class.lower() or keyword in table_id.lower() 
                      for keyword in layout_keywords):
                    return 'layout_table'
                else:
                    return 'data_table'
            
        except Exception as e:
            logger.debug(f"Error determining table type: {e}")
            return 'form_table'  # デフォルト
    
    def get_structure_summary(self, structure: FormStructure) -> Dict[str, Any]:
        """構造解析のサマリーを取得（テーブル情報強化版）"""
        # テーブル関連の統計情報
        table_stats = {
            'table_count': len(structure.table_structures),
            'form_tables': sum(1 for t in structure.table_structures if t.table_type == 'form_table'),
            'data_tables': sum(1 for t in structure.table_structures if t.table_type == 'data_table'),
            'layout_tables': sum(1 for t in structure.table_structures if t.table_type == 'layout_table'),
            'total_table_form_elements': sum(len(t.form_fields_in_table) for t in structure.table_structures),
            'tables_with_headers': sum(1 for t in structure.table_structures if len(t.headers) > 0)
        }
        
        return {
            'total_elements': len(structure.elements),
            'has_form_boundary': structure.form_locator is not None,
            'groups_count': len(structure.groups),
            'parallel_groups_count': len(structure.parallel_groups),
            'visible_elements': sum(1 for e in structure.elements if e.is_visible),
            'elements_with_labels': sum(1 for e in structure.elements if e.label_text),
            'elements_with_associated_text': sum(1 for e in structure.elements if e.associated_text),
            **table_stats  # テーブル統計情報を展開
        }
