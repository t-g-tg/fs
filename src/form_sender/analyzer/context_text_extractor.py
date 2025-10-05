"""
周辺テキスト抽出機能

フォーム要素の周辺コンテキストテキスト抽出システム
説明表示テキストの解析と活用機能
"""

import re
import logging
from typing import Dict, List, Any, Optional
from playwright.async_api import Locator
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class TextContext:
    """テキストコンテキスト情報"""
    text: str                   # テキスト内容
    source_type: str            # テキストソース（label, sibling, parent等）
    confidence: float           # 信頼度（0-1）
    position_relative: str      # 相対位置（before, after, above, below等）
    distance: Optional[float]   # 距離（px）


class ContextTextExtractor:
    """周辺テキスト抽出メインクラス"""
    
    def __init__(self, page_or_frame):
        """
        Args:
            page_or_frame: PlaywrightのPageまたはFrameオブジェクト
        """
        self.page = page_or_frame
        self._context_cache = {}  # 要素別コンテキストキャッシュ
        self._form_bounds = None  # フォーム境界（位置ベース抽出の制限用）
        self._label_for_index = None  # {for_id: label_text}
        self._dtdd_index = None       # [{'x','y','width','height','text'}]
        self._thtd_index = None       # [{'x','y','width','height','text'}]
        
        # 抽出設定
        self.settings = {
            'search_radius': 150,           # 周辺テキスト検索半径（px）
            'max_text_length': 200,         # 最大テキスト長
            'min_text_length': 2,           # 最小テキスト長
            'confidence_threshold': 0.3,    # 信頼度閾値
            'enable_context_shortcircuit': True,  # 高速化: 強いコンテキストでの位置探索スキップ
            'position_weight': {            # 位置による重み
                'above': 0.9,
                'left': 0.8,
                'right': 0.7,
                'below': 0.6,
                'parent': 0.85,
                'label': 1.0
            }
        }
        
        # 日本語フィールド識別パターン（包括的）
        self.japanese_field_patterns = {
            '会社名': ['会社', '会社名', '企業名', '法人名', '団体名', '組織名', '会社・団体名', 
                     'company', 'corp', 'corporation', 'firm', 'organization', 'kaisha', 'kaisya'],
            '部署名': ['部署', '部署名', '部門', '課', '係', '所属部署', '所属', 
                     'department', 'dept', 'division', 'section', 'team', 'group', 'busho', 'busyo'],
            '姓': ['姓', '苗字', '名字', 'せい', 'みょうじ', 'ファミリーネーム', 'お名前（姓）', 
                  'lastname', 'last_name', 'family_name', 'surname', '姓名の姓'],
            '名': ['名', 'めい', 'ファーストネーム', '下の名前', 'お名前（名）', 
                  'firstname', 'first_name', 'given_name', 'forename', '姓名の名'],
            '姓カナ': ['セイ', 'せい', 'カナ姓', 'フリガナ（姓）', '姓（カタカナ）', '姓（カナ）',
                     'kana', 'katakana', 'lastname_kana', 'family_kana'],
            '名カナ': ['メイ', 'めい', 'カナ名', 'フリガナ（名）', '名（カタカナ）', '名（カナ）',
                     'firstname_kana', 'given_kana'],
            '姓ひらがな': ['せい', 'ひらがな姓', 'ふりがな（姓）', '姓（ひらがな）',
                        'hiragana', 'lastname_hiragana'],
            '名ひらがな': ['めい', 'ひらがな名', 'ふりがな（名）', '名（ひらがな）',
                        'firstname_hiragana'],
            'メールアドレス': ['メール', 'メールアドレス', 'メルアド', 'mail', 'email', 'e-mail', 
                           'お客様のメールアドレス', 'ご連絡先メール'],
            '電話番号': ['電話', '電話番号', 'でんわ', 'でんわばんごう', 'tel', 'phone', 'telephone',
                       'お客様の電話番号', 'ご連絡先電話番号', '連絡先'],
            '郵便番号': ['郵便番号', '郵便', 'ゆうびん', 'ゆうびんばんごう', 'zip', 'postal', '〒'],
            '住所': ['住所', '所在地', 'じゅうしょ', 'address', 'ご住所', 'お客様の住所'],
            '役職': ['役職', '職位', '肩書き', 'position', 'title', 'post'],
            '件名': ['件名', '題名', 'タイトル', 'subject', 'title', 'お問い合わせ件名'],
            'お問い合わせ本文': ['本文', 'お問い合わせ内容', 'メッセージ', 'ご質問', 'お問い合わせ本文',
                             'message', 'content', 'inquiry', 'details', '詳細', '備考', 'note'],
            '性別': ['性別', 'せいべつ', 'gender', 'sex'],
            '年齢': ['年齢', 'ねんれい', 'age', '歳'],
            '業種': ['業種', 'ぎょうしゅ', 'industry', '事業内容'],
            '従業員数': ['従業員数', '社員数', '人数', 'employees', 'staff'],
            '資本金': ['資本金', 'しほんきん', 'capital'],
            'FAX番号': ['fax', 'ファックス', 'ファクス', 'FAX', 'Fax', 'ふぁっくす'],
            'URL': ['url', 'ホームページ', 'website', 'サイト', 'webサイト', 'hp'],
            '利用規約': ['利用規約', '規約', '同意', 'terms', 'agreement']
        }

        # 日本語特有の文脈表現パターン（周辺テキストのノイズ除去・ヒント検出用）
        self.japanese_context_patterns = {
            'required_indicators': ['必須', '※必須', '必要', 'required', '*', '＊', '（必須）', '(必須)',
                                   'を入力してください', '入力必須', 'は必ずご入力ください'],
            'optional_indicators': ['任意', '（任意）', '(任意)', 'optional', 'お好みで', '必要に応じて'],
            'input_guides': ['を入力してください', 'をご記入ください', 'を記入してください',
                           'をお書きください', 'を選択してください', 'を入力', 'を記入'],
            'polite_forms': ['ご記入', 'ご入力', 'お書き', 'お選び', 'お客様の', 'ご自身の', 'ご連絡先'],
            'format_hints': ['半角英数', '全角ひらがな', '全角カタカナ', 'ハイフンあり', 'ハイフンなし',
                           '例：', '形式：', '※', '（例）', '(例)', '例）']
        }

        # ノイズテキストパターン（日本語対応）
        self.noise_patterns = [
            r'^[\s\*\-\=\|]+$',     # 記号のみ
            r'^\d+$',               # 数字のみ
            r'^[a-zA-Z]$',          # 単一アルファベット
            r'^[あ-ん]$',           # 単一ひらがな
            r'^[ア-ン]$',           # 単一カタカナ
            r'cookie',              # Cookie関連
            r'javascript',          # JavaScript関連
            r'css',                 # CSS関連
            r'debug',               # デバッグ関連
            r'error',               # エラー関連
            r'loading',             # ローディング関連
            r'click',               # クリック関連
            r'submit',              # サブミット関連（フィールド名としては除外）
            r'^送信$',              # 送信ボタン
            r'^確認$',              # 確認ボタン
            r'^戻る$',              # 戻るボタン
            r'^リセット$',          # リセットボタン
            r'^クリア$',            # クリアボタン
            r'^検索$',              # 検索ボタン
            r'プライバシー',        # プライバシーポリシー関連
            r'利用規約',            # 利用規約関連（フィールドとしては除外）
        ]

        logger.info("ContextTextExtractor initialized")

    def set_form_bounds(self, form_bounds: Optional[Dict[str, float]]):
        """フォームの境界ボックスを設定（位置ベース抽出をフォーム内に限定）"""
        self._form_bounds = form_bounds
    
    def extract_parallel_element_labels(self, parallel_elements: List[Any]) -> Dict[str, str]:
        """
        並列要素グループの説明テキストを特定
        
        Args:
            parallel_elements: 並列要素のリスト（FormElementまたは辞書形式）
        
        Returns:
            Dict[str, str]: {要素識別子: 説明テキスト}
        """
        element_labels = {}
        
        try:
            logger.debug(f"Extracting labels for {len(parallel_elements)} parallel elements")
            
            # 要素の親構造を分析
            parent_structure = self._analyze_parallel_parent_structure(parallel_elements)
            
            for i, element in enumerate(parallel_elements):
                element_id = self._get_element_identifier(element, i)
                
                # 優先度順にラベル候補を取得
                label_candidates = self._get_label_candidates_for_element(element, parent_structure)
                
                # 最適なラベルを選択
                best_label = self._select_best_label(label_candidates)
                
                if best_label:
                    element_labels[element_id] = best_label
                    logger.debug(f"Found label for element {element_id}: '{best_label[:50]}...'")
                else:
                    logger.debug(f"No label found for element {element_id}")
            
            return element_labels
            
        except Exception as e:
            logger.error(f"Error extracting parallel element labels: {e}")
            return {}
    
    def _analyze_parallel_parent_structure(self, parallel_elements: List[Any]) -> Dict[str, Any]:
        """並列要素の親構造を分析"""
        structure_info = {
            'is_table_structure': False,
            'is_definition_list': False,
            'is_fieldset_group': False,
            'common_parent_type': None,
            'has_header_row': False,
            'table_headers': [],
            'structure_type': 'unknown'
        }
        
        try:
            # 最初の要素から構造を推測（実装簡略化）
            if parallel_elements:
                first_element = parallel_elements[0]
                
                # 要素情報の取得方法を統一
                if hasattr(first_element, 'parent_info'):
                    parent_info = first_element.parent_info or {}
                elif isinstance(first_element, dict):
                    parent_info = first_element.get('parent_info', {})
                else:
                    parent_info = {}
                
                # 親要素のタグ名から構造タイプを判定
                parent_tag = parent_info.get('tag_name', '').lower()
                
                if parent_tag in ['td', 'th']:
                    structure_info['is_table_structure'] = True
                    structure_info['structure_type'] = 'table'
                elif parent_tag == 'dd':
                    structure_info['is_definition_list'] = True
                    structure_info['structure_type'] = 'definition_list'
                elif parent_tag == 'fieldset':
                    structure_info['is_fieldset_group'] = True
                    structure_info['structure_type'] = 'fieldset'
                else:
                    structure_info['structure_type'] = 'generic_container'
                
                logger.debug(f"Detected parallel structure type: {structure_info['structure_type']}")
        
        except Exception as e:
            logger.debug(f"Error analyzing parent structure: {e}")
        
        return structure_info
    
    def _get_element_identifier(self, element: Any, index: int) -> str:
        """要素の一意識別子を生成"""
        try:
            if hasattr(element, 'name') and element.name:
                return element.name
            elif hasattr(element, 'id') and element.id:
                return element.id
            elif isinstance(element, dict):
                return element.get('name') or element.get('id') or f"element_{index}"
            else:
                return f"element_{index}"
        except:
            return f"element_{index}"
    
    def _get_label_candidates_for_element(self, element: Any, parent_structure: Dict[str, Any]) -> List[Dict[str, Any]]:
        """要素のラベル候補を優先度順に取得"""
        candidates = []
        
        try:
            # 1. dt/dd構造のdtラベル（最高優先度）
            if parent_structure.get('is_definition_list'):
                dt_label = self._extract_dt_label_for_element(element)
                if dt_label:
                    candidates.append({
                        'text': dt_label,
                        'source': 'dt_label',
                        'confidence': 1.0
                    })
            
            # 2. テーブル構造のthラベル（高優先度）
            if parent_structure.get('is_table_structure'):
                th_label = self._extract_th_label_for_element(element)
                if th_label:
                    candidates.append({
                        'text': th_label,
                        'source': 'th_label', 
                        'confidence': 0.95
                    })
            
            # 3. 直接のlabel要素（高優先度）
            direct_label = self._extract_direct_label_for_element(element)
            if direct_label:
                candidates.append({
                    'text': direct_label,
                    'source': 'label_element',
                    'confidence': 0.9
                })
            
            # 4. 隣接テキスト（中優先度）
            adjacent_text = self._extract_adjacent_text_for_element(element)
            if adjacent_text:
                candidates.append({
                    'text': adjacent_text,
                    'source': 'adjacent_text',
                    'confidence': 0.7
                })
            
            # 5. placeholder（低優先度）
            placeholder_text = self._extract_placeholder_for_element(element)
            if placeholder_text:
                candidates.append({
                    'text': placeholder_text,
                    'source': 'placeholder',
                    'confidence': 0.5
                })
        
        except Exception as e:
            logger.debug(f"Error getting label candidates: {e}")
        
        # 信頼度順にソート
        candidates.sort(key=lambda x: x['confidence'], reverse=True)
        return candidates
    
    def _extract_dt_label_for_element(self, element: Any) -> Optional[str]:
        """要素に対応するdtラベルを抽出（簡易実装）"""
        # 実際の実装では、DOM構造を解析してdd要素に対応するdt要素を見つける
        # ここでは簡略化してcontextから取得
        try:
            if hasattr(element, 'associated_text') and element.associated_text:
                return element.associated_text
            elif isinstance(element, dict) and element.get('context'):
                for ctx in element['context']:
                    if isinstance(ctx, dict) and ctx.get('source_type') == 'dt_label':
                        return ctx.get('text')
        except:
            pass
        return None
    
    def _extract_th_label_for_element(self, element: Any) -> Optional[str]:
        """要素に対応するthラベルを抽出（簡易実装）"""
        try:
            if isinstance(element, dict) and element.get('context'):
                for ctx in element['context']:
                    if isinstance(ctx, dict) and ctx.get('source_type') == 'th_label':
                        return ctx.get('text')
        except:
            pass
        return None
    
    def _extract_direct_label_for_element(self, element: Any) -> Optional[str]:
        """直接のlabel要素からテキストを抽出"""
        try:
            if hasattr(element, 'label_text') and element.label_text:
                return element.label_text
            elif isinstance(element, dict) and element.get('context'):
                for ctx in element['context']:
                    if isinstance(ctx, dict) and ctx.get('source_type') == 'label':
                        return ctx.get('text')
        except:
            pass
        return None
    
    def _extract_adjacent_text_for_element(self, element: Any) -> Optional[str]:
        """隣接テキストを抽出"""
        try:
            if hasattr(element, 'nearby_text') and element.nearby_text:
                # 最も関連性の高いテキストを選択
                return element.nearby_text[0] if element.nearby_text else None
            elif isinstance(element, dict) and element.get('context'):
                for ctx in element['context']:
                    if isinstance(ctx, dict) and ctx.get('source_type') in ['adjacent_text', 'sibling_text']:
                        return ctx.get('text')
        except:
            pass
        return None
    
    def _extract_placeholder_for_element(self, element: Any) -> Optional[str]:
        """placeholder属性からテキストを抽出"""
        try:
            if hasattr(element, 'placeholder') and element.placeholder:
                return element.placeholder
            elif isinstance(element, dict) and element.get('placeholder'):
                return element['placeholder']
        except:
            pass
        return None
    
    def _select_best_label(self, candidates: List[Dict[str, Any]]) -> Optional[str]:
        """ラベル候補から最適なものを選択"""
        if not candidates:
            return None
        
        # 最も信頼度の高い候補を選択
        best_candidate = candidates[0]
        label_text = best_candidate['text']
        
        # テキストの品質チェック
        if not label_text or len(label_text.strip()) < 2:
            return None
        
        # 長すぎるテキストは切り詰め
        if len(label_text) > self.settings['max_text_length']:
            label_text = label_text[:self.settings['max_text_length']] + '...'
        
        return label_text.strip()

    async def build_form_context_index(self):
        """フォーム境界内の DL(dt/dd) ・TABLE(th/td) 見出しインデックスを事前構築"""
        form_area = None
        if isinstance(self._form_bounds, dict) and all(k in self._form_bounds for k in ('x','y','width','height')):
            form_area = {
                'x': float(self._form_bounds['x']),
                'y': float(self._form_bounds['y']),
                'width': float(self._form_bounds['width']),
                'height': float(self._form_bounds['height'])
            }

        # dt/dd インデックス
        try:
            dtdd = await self.page.evaluate(f"""
                () => {{
                    const formArea = {form_area if form_area else 'null'};
                    const arr = [];
                    const dls = Array.from(document.querySelectorAll('dl'));
                    for (const dl of dls) {{
                        const children = Array.from(dl.children);
                        for (let i=0;i<children.length;i++) {{
                            const el = children[i];
                            if (el.tagName && el.tagName.toLowerCase() === 'dd') {{
                                let j = i-1; let dtText = '';
                                while (j >= 0) {{
                                    const prev = children[j];
                                    if (prev.tagName && prev.tagName.toLowerCase() === 'dt') {{
                                        dtText = (prev.textContent||'').trim();
                                        break;
                                    }}
                                    j--;
                                }}
                                const r = el.getBoundingClientRect();
                                if (r.width === 0 || r.height === 0) continue;
                                if (formArea) {{
                                    const ih = Math.min(r.right, formArea.x + formArea.width) - Math.max(r.left, formArea.x);
                                    const iv = Math.min(r.bottom, formArea.y + formArea.height) - Math.max(r.top, formArea.y);
                                    if (ih <= 0 || iv <= 0) continue;
                                }}
                                arr.push({{ x:r.left, y:r.top, width:r.width, height:r.height, text:dtText }});
                            }}
                        }}
                    }}
                    return arr;
                }}
            """)
        except Exception:
            dtdd = []

        # th/td インデックス
        try:
            thtd = await self.page.evaluate(f"""
                () => {{
                    const formArea = {form_area if form_area else 'null'};
                    const records = [];
                    const tables = Array.from(document.querySelectorAll('table'));
                    for (const table of tables) {{
                        let colHeaders = [];
                        const thead = table.querySelector('thead');
                        if (thead) {{
                            const ths = thead.querySelectorAll('th');
                            colHeaders = Array.from(ths).map(th => (th.textContent||'').trim());
                        }}
                        const rows = Array.from(table.querySelectorAll('tr'));
                        for (const tr of rows) {{
                            const cells = Array.from(tr.children);
                            let rowHeader = '';
                            for (const c of cells) {{
                                if (c.tagName && c.tagName.toLowerCase() === 'th') {{
                                    rowHeader = (c.textContent||'').trim();
                                }}
                            }}
                            for (let ci=0; ci<cells.length; ci++) {{
                                const c = cells[ci];
                                if (!(c.tagName && c.tagName.toLowerCase() === 'td')) continue;
                                const r = c.getBoundingClientRect();
                                if (r.width === 0 || r.height === 0) continue;
                                if (formArea) {{
                                    const ih = Math.min(r.right, formArea.x + formArea.width) - Math.max(r.left, formArea.x);
                                    const iv = Math.min(r.bottom, formArea.y + formArea.height) - Math.max(r.top, formArea.y);
                                    if (ih <= 0 || iv <= 0) continue;
                                }}
                                let headerText = rowHeader;
                                if (!headerText && colHeaders.length > ci) headerText = colHeaders[ci] || '';
                                // Fallback: common layout with td(label)+td(input)
                                if (!headerText) {{
                                    try {{
                                        const prev = cells[ci-1];
                                        if (prev && prev.tagName && prev.tagName.toLowerCase() === 'td') {{
                                            const t = (prev.textContent||'').trim();
                                            if (t) headerText = t;
                                        }} else if (ci === 0 && cells.length >= 2) {{
                                            const maybeLabel = cells[0];
                                            if (maybeLabel && maybeLabel !== c && maybeLabel.tagName && maybeLabel.tagName.toLowerCase() === 'td') {{
                                                const t = (maybeLabel.textContent||'').trim();
                                                if (t) headerText = t;
                                            }}
                                        }}
                                    }} catch (e) {{}}
                                }}
                                records.push({{ x:r.left, y:r.top, width:r.width, height:r.height, text: headerText }});
                            }}
                        }}
                    }}
                    return records;
                }}
            """)
        except Exception:
            thtd = []

        self._dtdd_index = dtdd or []
        self._thtd_index = thtd or []
        return True
    
    async def extract_context_for_element(self, element_locator: Locator, 
                                        element_bounds: Optional[Dict[str, float]] = None) -> List[TextContext]:
        """
        要素の周辺コンテキストを抽出（キャッシュ対応）
        
        Args:
            element_locator: 要素のロケーター
            element_bounds: 要素の境界情報
            
        Returns:
            List[TextContext]: 抽出されたコンテキストリスト
        """
        # キャッシュキーを作成
        element_key = str(element_locator)
        if element_key in self._context_cache:
            logger.debug(f"Using cached context for element: {element_key[:50]}...")
            return self._context_cache[element_key]
        
        contexts = []
        
        try:
            # 要素の境界情報を取得（未提供の場合）
            if not element_bounds:
                element_bounds = await element_locator.bounding_box()
            
            if not element_bounds:
                logger.debug("Could not get element bounds for context extraction")
                # 境界情報がない場合も空の結果をキャッシュ
                self._context_cache[element_key] = contexts
                return contexts
            
            # 1. ラベル要素からの抽出
            label_contexts = await self._extract_from_labels(element_locator)
            contexts.extend(label_contexts)

            # 1.5. UL/LI レイアウトの見出し抽出（li_left → li_right 構造）
            list_label_contexts = await self._extract_from_list_labels(element_locator, element_bounds)
            contexts.extend(list_label_contexts)
            
            # 2. 親要素からの抽出
            parent_contexts = await self._extract_from_parent(element_locator)
            contexts.extend(parent_contexts)
            
            # 3. 兄弟要素からの抽出
            sibling_contexts = await self._extract_from_siblings(element_locator)
            contexts.extend(sibling_contexts)
            
            # 4. DTラベル要素からの抽出（最優先）
            dt_contexts = await self._extract_from_dt_labels(element_locator, element_bounds)
            contexts.extend(dt_contexts)
            
            # 5. THラベル要素からの抽出（テーブル構造対応）
            th_contexts = await self._extract_from_th_labels(element_locator, element_bounds)
            contexts.extend(th_contexts)
            
            # 高速化: 強いコンテキスト抽出でのショートサーキット判定
            skip_position_search = False
            if self.settings.get('enable_context_shortcircuit', True):
                skip_position_search = self._should_skip_position_search(contexts)
                if skip_position_search:
                    logger.debug("Skipping position search due to strong label/header contexts")
            
            # 6. 位置ベースでの周辺テキスト抽出（条件付きスキップ）
            if not skip_position_search:
                position_contexts = await self._extract_by_position(element_locator, element_bounds)
                contexts.extend(position_contexts)
            
            # 7. コンテキストのフィルタリングと信頼度計算
            filtered_contexts = self._filter_and_score_contexts(contexts)
            
            # キャッシュに結果を保存
            self._context_cache[element_key] = filtered_contexts
            
            logger.debug(f"Extracted {len(filtered_contexts)} context items for element")
            return filtered_contexts
            
        except Exception as e:
            logger.error(f"Error extracting context for element: {e}")
            # エラー時も空の結果をキャッシュ
            self._context_cache[element_key] = []
            return []

    async def _extract_from_list_labels(self, element_locator: Locator, element_bounds: Optional[Dict[str, float]] = None) -> List[TextContext]:
        """UL/LIベースのフォームで、入力LIと見出しLIの対応を抽出

        期待構造:
            <ul>
              <li class="li_left">ラベル</li>
              <li class="li_center">必須</li>
              <li class="li_right">…入力要素…</li>
            </ul>
        """
        contexts: List[TextContext] = []
        try:
            label_text = await element_locator.evaluate("""
                el => {
                    // 1) 自身の属する LI を特定
                    let li = el.closest('li');
                    if (!li) return null;
                    // 2) 同一UL内で前方のラベルLIを探索
                    let ul = li.closest('ul');
                    if (!ul) return null;
                    // 2-a) まず li_left を優先
                    let prev = li.previousElementSibling;
                    while (prev) {
                        if (prev.tagName && prev.tagName.toLowerCase() === 'li') {
                            const cls = (prev.getAttribute('class')||'').toLowerCase();
                            if (/li_left/.test(cls)) {
                                const t = (prev.textContent||'').trim();
                                if (t) return t;
                            }
                        }
                        prev = prev.previousElementSibling;
                    }
                    // 2-b) 次に、入力を含まないテキストLIを探索（必須/任意などの補助表示は除外）
                    prev = li.previousElementSibling;
                    const isIndicator = (tx) => {
                        const s = (tx||'').trim();
                        if (!s) return true;
                        const indicators = ['必須','※必須','任意','(必須)','（必須）','(任意)','（任意）'];
                        return indicators.some(ind => s.indexOf(ind) !== -1) || s.replace(/\\s+/g,'').length <= 2;
                    };
                    while (prev) {
                        if (prev.tagName && prev.tagName.toLowerCase() === 'li') {
                            const text = (prev.textContent||'').trim();
                            const hasInputs = prev.querySelector('input,textarea,select,button') !== null;
                            if (!hasInputs && text && !isIndicator(text)) {
                                return text;
                            }
                        }
                        prev = prev.previousElementSibling;
                    }
                    return null;
                }
            """)
            if label_text and self._is_valid_text(label_text):
                contexts.append(TextContext(
                    text=label_text.strip(),
                    source_type='ul_li_label',
                    confidence=0.95,
                    position_relative='associated',
                    distance=0
                ))
        except Exception as e:
            logger.debug(f"Error extracting from list labels: {e}")
        return contexts
    
    async def _extract_from_labels(self, element_locator: Locator) -> List[TextContext]:
        """ラベル要素からテキストを抽出"""
        contexts = []
        
        try:
            # 1. for属性によるラベル関連付け
            element_id = await element_locator.get_attribute('id')
            if element_id:
                # ラベルインデックスを初回のみ構築
                if self._label_for_index is None:
                    try:
                        pairs = await self.page.evaluate(
                            """
                            () => Array.from(document.querySelectorAll('label[for]')).map(l => [l.getAttribute('for'), (l.textContent||'').trim()])
                            """
                        )
                        idx = {}
                        for k, v in (pairs or []):
                            if k and v:
                                idx[k] = v
                        self._label_for_index = idx
                    except Exception:
                        self._label_for_index = {}

                label_text = None
                try:
                    label_text = self._label_for_index.get(element_id)
                except Exception:
                    label_text = None

                if not label_text:
                    # フォールバック: 個別検索（まれ）
                    label_locator = self.page.locator(f'label[for="{element_id}"]')
                    if await label_locator.count() > 0:
                        label_text = await label_locator.text_content()

                if label_text and self._is_valid_text(label_text):
                    contexts.append(TextContext(
                        text=label_text.strip(),
                        source_type='label_for',
                        confidence=1.0,
                        position_relative='associated',
                        distance=0
                    ))
            
            # 2. 親要素がlabelの場合
            parent_tag = await element_locator.evaluate("el => el.parentElement?.tagName.toLowerCase()")
            if parent_tag == 'label':
                parent_locator = element_locator.locator('..')
                label_text = await parent_locator.text_content()
                
                if label_text and self._is_valid_text(label_text):
                    # 入力要素のvalue/textを除外
                    input_text = await self._get_element_text_content(element_locator)
                    clean_text = label_text.replace(input_text, '').strip()
                    
                    if clean_text:
                        contexts.append(TextContext(
                            text=clean_text,
                            source_type='label_parent',
                            confidence=0.95,
                            position_relative='parent',
                            distance=0
                        ))
            
            # 3. aria-labelledbyによる関連付け（複数ID対応）
            labelledby_id = await element_locator.get_attribute('aria-labelledby')
            if labelledby_id:
                collected_texts: List[str] = []
                try:
                    for ref in str(labelledby_id).split():  # 空白区切りで複数IDに対応
                        if not ref:
                            continue
                        esc_ref = ref.replace('\\', r'\\').replace('"', r'\"')
                        loc = self.page.locator(f'[id=\"{esc_ref}\"]')
                        if await loc.count() > 0:
                            t = await loc.text_content()
                            if t and self._is_valid_text(t):
                                collected_texts.append(t.strip())
                except Exception:
                    collected_texts = []
                if collected_texts:
                    contexts.append(TextContext(
                        text=' '.join(collected_texts),
                        source_type='aria_labelledby',
                        confidence=0.9,
                        position_relative='associated',
                        distance=0
                    ))
        
        except Exception as e:
            logger.debug(f"Error extracting from labels: {e}")
        
        return contexts
    
    async def _extract_from_parent(self, element_locator: Locator) -> List[TextContext]:
        """親要素からテキストを抽出"""
        contexts = []
        
        try:
            # 直接の親要素
            parent_locator = element_locator.locator('..')
            parent_text = await parent_locator.text_content()
            
            if parent_text and self._is_valid_text(parent_text):
                # 子要素のテキストを除外して親要素固有のテキストを抽出
                child_texts = await parent_locator.evaluate("""
                    parent => {
                        const childTexts = [];
                        for (const child of parent.children) {
                            const text = child.textContent?.trim();
                            if (text) childTexts.push(text);
                        }
                        return childTexts;
                    }
                """)
                
                clean_text = parent_text
                for child_text in (child_texts or []):
                    clean_text = clean_text.replace(child_text, '').strip()
                
                if clean_text and len(clean_text) > self.settings['min_text_length']:
                    contexts.append(TextContext(
                        text=clean_text,
                        source_type='parent_element',
                        confidence=0.7,
                        position_relative='parent',
                        distance=0
                    ))
            
            # 祖父母要素（fieldsetなど）
            grandparent_locator = element_locator.locator('../..')
            try:
                grandparent_tag = await grandparent_locator.evaluate("el => el.tagName.toLowerCase()")
                if grandparent_tag in ['fieldset', 'div', 'section']:
                    # legendまたは見出し要素を探す
                    legend_locator = grandparent_locator.locator('legend, h1, h2, h3, h4, h5, h6').first
                    legend_count = await legend_locator.count()
                    
                    if legend_count > 0:
                        legend_text = await legend_locator.text_content()
                        if legend_text and self._is_valid_text(legend_text):
                            contexts.append(TextContext(
                                text=legend_text.strip(),
                                source_type='fieldset_legend',
                                confidence=0.6,
                                position_relative='ancestor',
                                distance=0
                            ))
            except:
                pass
        
        except Exception as e:
            logger.debug(f"Error extracting from parent: {e}")
        
        return contexts
    
    async def _extract_from_dt_labels(self, element_locator: Locator, element_bounds: Optional[Dict[str, float]] = None) -> List[TextContext]:
        """DTラベル要素からテキストを抽出（dl/dt/dd構造対応）"""
        contexts = []
        
        try:
            # インデックス利用（あれば優先）
            if element_bounds and self._dtdd_index:
                try:
                    cx = element_bounds['x'] + element_bounds['width']/2.0
                    cy = element_bounds['y'] + element_bounds['height']/2.0
                    for rec in self._dtdd_index:
                        x,y,w,h = rec.get('x',0), rec.get('y',0), rec.get('width',0), rec.get('height',0)
                        if x <= cx <= x+w and y <= cy <= y+h:
                            t = (rec.get('text') or '').strip()
                            if t and self._is_valid_text(t):
                                contexts.append(TextContext(
                                    text=self._clean_dt_text(t),
                                    source_type='dt_label_index',
                                    confidence=0.95,
                                    position_relative='associated',
                                    distance=0
                                ))
                                return contexts
                except Exception:
                    pass
            # 入力要素を含むdd要素を特定
            dd_parent = await element_locator.evaluate("""
                el => {
                    let parent = el.parentElement;
                    while (parent && parent.tagName.toLowerCase() !== 'dd') {
                        parent = parent.parentElement;
                        if (parent === document.body) return null;
                    }
                    return parent;
                }
            """)
            
            if dd_parent:
                # dd要素に対応するdt要素を検索
                dt_text = await element_locator.evaluate("""
                    el => {
                        let dd = el.parentElement;
                        while (dd && dd.tagName.toLowerCase() !== 'dd') {
                            dd = dd.parentElement;
                            if (dd === document.body) return null;
                        }
                        
                        if (!dd) return null;
                        
                        // dd要素の前の兄弟要素でdtタグを探す
                        let dt = dd.previousElementSibling;
                        while (dt && dt.tagName.toLowerCase() !== 'dt') {
                            dt = dt.previousElementSibling;
                        }
                        
                        if (dt) {
                            return dt.textContent?.trim() || null;
                        }
                        return null;
                    }
                """)
                
                if dt_text and self._is_valid_text(dt_text):
                    # 必須マーカーを除去してクリーンなテキストを取得
                    clean_text = self._clean_dt_text(dt_text)
                    
                    if clean_text:
                        contexts.append(TextContext(
                            text=clean_text,
                            source_type='dt_label',
                            confidence=1.0,  # DTラベルは最高信頼度
                            position_relative='associated',
                            distance=0
                        ))
                        logger.debug(f"Extracted DT label: '{clean_text}' for element")
        
        except Exception as e:
            logger.debug(f"Error extracting from DT labels: {e}")
        
        return contexts
    
    async def _extract_from_th_labels(self, element_locator: Locator, element_bounds: Optional[Dict[str, float]] = None) -> List[TextContext]:
        """THラベル要素からテキストを抽出（テーブル構造対応）"""
        contexts = []
        
        try:
            # インデックス利用（あれば優先）
            if element_bounds and self._thtd_index:
                try:
                    cx = element_bounds['x'] + element_bounds['width']/2.0
                    cy = element_bounds['y'] + element_bounds['height']/2.0
                    for rec in self._thtd_index:
                        x,y,w,h = rec.get('x',0), rec.get('y',0), rec.get('width',0), rec.get('height',0)
                        if x <= cx <= x+w and y <= cy <= y+h:
                            t = (rec.get('text') or '').strip()
                            if t and self._is_valid_text(t):
                                contexts.append(TextContext(
                                    text=self._clean_th_text(t),
                                    source_type='th_label_index',
                                    confidence=0.9,
                                    position_relative='table_header',
                                    distance=0
                                ))
                                return contexts
                except Exception:
                    pass
            # 要素が含まれるテーブル行内のth要素を検索
            th_text = await element_locator.evaluate("""
                el => {
                    // 要素の親要素をtr要素まで遡る
                    let td = el.parentElement;
                    while (td && td.tagName.toLowerCase() !== 'td') {
                        td = td.parentElement;
                        if (td === document.body) return null;
                    }
                    
                    if (!td) return null;
                    
                    // td要素が含まれる行（tr）を取得
                    let tr = td.parentElement;
                    while (tr && tr.tagName.toLowerCase() !== 'tr') {
                        tr = tr.parentElement;
                        if (tr === document.body) return null;
                    }
                    
                    if (!tr) return null;
                    
                    // 同じ行内のth要素を探す
                    const ths = tr.querySelectorAll('th');
                    if (ths.length > 0) {
                        // 最初のth要素のテキストを取得
                        return ths[0].textContent?.trim() || null;
                    }

                    // Fallback: 一部サイトでは th ではなく td が見出しセルとして使われる
                    // 例: <td class="form_index">ご住所</td><td class="list_value"><input ...></td>
                    const cells = Array.from(tr.children);
                    const tdIndex = cells.indexOf(td);
                    if (tdIndex > 0) {
                        // 直前セル、もしくは先頭セルをラベル候補として採用
                        const pickLabelTd = (node) => {
                          if (!node || !node.tagName) return '';
                          const tag = node.tagName.toLowerCase();
                          if (tag !== 'td') return '';
                          const txt = (node.textContent||'').trim();
                          // 短すぎる/必須インジケータのみのセルは除外
                          if (!txt || txt.replace(/\s+/g,'').length < 2) return '';
                          return txt;
                        };
                        let t = pickLabelTd(cells[tdIndex-1]);
                        if (!t) t = pickLabelTd(cells[0]);
                        if (t) return t;
                    }

                    // 同じ行にthがない場合、前の行のth要素を探す
                    let prevTr = tr.previousElementSibling;
                    while (prevTr) {
                        const prevThs = prevTr.querySelectorAll('th');
                        if (prevThs.length > 0) {
                            // セルのインデックスに基づいてth要素を選択
                            const tdIndex = Array.from(tr.children).indexOf(td);
                            if (tdIndex >= 0 && tdIndex < prevThs.length) {
                                return prevThs[tdIndex].textContent?.trim() || null;
                            }
                            return prevThs[0].textContent?.trim() || null;
                        }
                        prevTr = prevTr.previousElementSibling;
                    }
                    
                    return null;
                }
            """)
            
            if th_text and self._is_valid_text(th_text):
                clean_text = self._clean_th_text(th_text)
                
                if clean_text:
                    contexts.append(TextContext(
                        text=clean_text,
                        source_type='th_label',
                        confidence=1.0,  # THラベルは最高信頼度
                        position_relative='table_header',
                        distance=0
                    ))
        except Exception as e:
            logger.debug(f"Error extracting from TH labels: {e}")
        
        return contexts
    
    def _clean_th_text(self, text: str) -> str:
        """THラベルテキストのクリーニング"""
        if not text:
            return ''
        
        # 必須マーカーの除去
        cleaned = re.sub(r'[*＊※]|必須|required|Required', '', text)
        
        # 括弧内の説明除去
        cleaned = re.sub(r'[（(][^）)]*[）)]', '', cleaned)
        
        # 特殊文字の除去
        cleaned = re.sub(r'[：:｜|]', '', cleaned)
        
        # 空白の正規化
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        return cleaned
    
    def _clean_dt_text(self, dt_text: str) -> str:
        """DTラベルテキストから不要な記号・マーカーを除去"""
        if not dt_text:
            return ""
        
        # 必須マーカーのパターン
        required_markers = [
            r'\s*※\s*必須',
            r'\s*\*\s*必須',
            r'\s*＊\s*必須',
            r'\s*※',
            r'\s*\*',
            r'\s*＊',
            r'\s*必須\s*',
            r'\s*REQUIRED\s*',
            r'\s*required\s*'
        ]
        
        cleaned_text = dt_text.strip()
        
        # 各マーカーパターンを除去
        for pattern in required_markers:
            cleaned_text = re.sub(pattern, '', cleaned_text, flags=re.IGNORECASE)
        
        # 余分な空白を整理
        cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
        
        return cleaned_text
    
    async def _extract_from_siblings(self, element_locator: Locator) -> List[TextContext]:
        """兄弟要素からテキストを抽出"""
        contexts = []
        
        try:
            # 前の兄弟要素
            prev_siblings = await element_locator.evaluate("""
                el => {
                    const siblings = [];
                    let sibling = el.previousElementSibling;
                    let count = 0;
                    
                    while (sibling && count < 3) {
                        const text = sibling.textContent?.trim();
                        const tag = sibling.tagName.toLowerCase();
                        const isInput = ['input', 'textarea', 'select'].includes(tag);
                        
                        if (text && !isInput && text.length < 200) {
                            siblings.push({
                                text: text,
                                tag: tag,
                                position: count
                            });
                        }
                        sibling = sibling.previousElementSibling;
                        count++;
                    }
                    return siblings;
                }
            """)
            
            for sibling_info in (prev_siblings or []):
                if self._is_valid_text(sibling_info['text']):
                    confidence = 0.8 - (sibling_info['position'] * 0.1)  # 近いほど高い信頼度
                    contexts.append(TextContext(
                        text=sibling_info['text'],
                        source_type=f'prev_sibling_{sibling_info["tag"]}',
                        confidence=max(confidence, 0.3),
                        position_relative='before',
                        distance=sibling_info['position'] * 50  # 推定距離
                    ))
            
            # 次の兄弟要素
            next_siblings = await element_locator.evaluate("""
                el => {
                    const siblings = [];
                    let sibling = el.nextElementSibling;
                    let count = 0;
                    
                    while (sibling && count < 2) {
                        const text = sibling.textContent?.trim();
                        const tag = sibling.tagName.toLowerCase();
                        const isInput = ['input', 'textarea', 'select'].includes(tag);
                        
                        if (text && !isInput && text.length < 200) {
                            siblings.push({
                                text: text,
                                tag: tag,
                                position: count
                            });
                        }
                        sibling = sibling.nextElementSibling;
                        count++;
                    }
                    return siblings;
                }
            """)
            
            for sibling_info in (next_siblings or []):
                if self._is_valid_text(sibling_info['text']):
                    confidence = 0.6 - (sibling_info['position'] * 0.1)  # 前の兄弟より少し低い
                    contexts.append(TextContext(
                        text=sibling_info['text'],
                        source_type=f'next_sibling_{sibling_info["tag"]}',
                        confidence=max(confidence, 0.2),
                        position_relative='after',
                        distance=sibling_info['position'] * 50
                    ))
        
        except Exception as e:
            logger.debug(f"Error extracting from siblings: {e}")
        
        return contexts
    
    async def _extract_by_position(self, element_locator: Locator, 
                                 element_bounds: Dict[str, float]) -> List[TextContext]:
        """位置ベースで周辺テキストを抽出"""
        contexts = []

        try:
            # 検索範囲の計算
            search_radius = self.settings['search_radius']
            search_area = {
                'x': element_bounds['x'] - search_radius,
                'y': element_bounds['y'] - search_radius,
                'width': element_bounds['width'] + (search_radius * 2),
                'height': element_bounds['height'] + (search_radius * 2)
            }
            # フォーム境界（存在する場合のみ適用）
            form_area = None
            if isinstance(self._form_bounds, dict) and all(k in self._form_bounds for k in ('x','y','width','height')):
                form_area = {
                    'x': float(self._form_bounds['x']),
                    'y': float(self._form_bounds['y']),
                    'width': float(self._form_bounds['width']),
                    'height': float(self._form_bounds['height'])
                }

            # 周辺のテキスト要素を取得
            nearby_texts = await self.page.evaluate(f"""
                () => {{
                    const searchArea = {search_area};
                    const elementBounds = {element_bounds};
                    const formArea = {form_area if form_area else 'null'};
                    const texts = [];
                    
                    // テキストノードを含む要素を検索
                    const walker = document.createTreeWalker(
                        document.body,
                        NodeFilter.SHOW_TEXT,
                        null,
                        false
                    );
                    
                    let textNode;
                    while (textNode = walker.nextNode()) {{
                        const text = textNode.textContent?.trim();
                        if (!text || text.length < 2 || text.length > 200) continue;
                        
                        const parent = textNode.parentElement;
                        if (!parent) continue;
                        
                        // 入力要素は除外
                        const tag = parent.tagName.toLowerCase();
                        if (['input', 'textarea', 'select', 'script', 'style'].includes(tag)) continue;
                        
                        const bounds = parent.getBoundingClientRect();
                        if (bounds.width === 0 || bounds.height === 0) continue;

                        // フォーム境界が指定されている場合は交差チェック
                        if (formArea) {{
                            const intersectH = Math.min(bounds.right, formArea.x + formArea.width) - Math.max(bounds.left, formArea.x);
                            const intersectV = Math.min(bounds.bottom, formArea.y + formArea.height) - Math.max(bounds.top, formArea.y);
                            if (intersectH <= 0 || intersectV <= 0) continue;
                        }}
                        
                        // 検索範囲内かチェック
                        if (bounds.left < searchArea.x + searchArea.width && 
                            bounds.right > searchArea.x &&
                            bounds.top < searchArea.y + searchArea.height &&
                            bounds.bottom > searchArea.y) {{
                            
                            // 相対位置を計算
                            const centerX = elementBounds.x + elementBounds.width / 2;
                            const centerY = elementBounds.y + elementBounds.height / 2;
                            const textCenterX = bounds.left + bounds.width / 2;
                            const textCenterY = bounds.top + bounds.height / 2;
                            
                            const distance = Math.sqrt(
                                Math.pow(centerX - textCenterX, 2) + 
                                Math.pow(centerY - textCenterY, 2)
                            );
                            
                            let position = 'nearby';
                            if (textCenterY < centerY - 20) position = 'above';
                            else if (textCenterY > centerY + 20) position = 'below';
                            else if (textCenterX < centerX - 20) position = 'left';
                            else if (textCenterX > centerX + 20) position = 'right';
                            
                            texts.push({{
                                text: text,
                                position: position,
                                distance: distance,
                                tag: tag
                            }});
                        }}
                    }}
                    
                    return texts.slice(0, 10); // 最大10個まで
                }}
            """)
            
            for text_info in (nearby_texts or []):
                if self._is_valid_text(text_info['text']):
                    position = text_info['position']
                    distance = text_info['distance']
                    
                    # 位置と距離による信頼度計算
                    position_weight = self.settings['position_weight'].get(position, 0.5)
                    distance_factor = max(0.1, 1 - (distance / search_radius))
                    confidence = position_weight * distance_factor
                    
                    if confidence >= self.settings['confidence_threshold']:
                        contexts.append(TextContext(
                            text=text_info['text'],
                            source_type=f'position_{position}',
                            confidence=confidence,
                            position_relative=position,
                            distance=distance
                        ))
        
        except Exception as e:
            logger.debug(f"Error extracting by position: {e}")
        
        return contexts
    
    def _filter_and_score_contexts(self, contexts: List[TextContext]) -> List[TextContext]:
        """コンテキストのフィルタリングと信頼度調整"""
        filtered_contexts = []
        
        for context in contexts:
            # ノイズテキストの除去
            if self._is_noise_text(context.text):
                continue
            
            # 日本語フィールドパターンとのマッチングで信頼度向上
            field_match_bonus = self._calculate_field_match_bonus(context.text)
            context.confidence = min(1.0, context.confidence + field_match_bonus)
            
            # 信頼度閾値チェック
            if context.confidence >= self.settings['confidence_threshold']:
                filtered_contexts.append(context)
        
        # 信頼度順でソート
        filtered_contexts.sort(key=lambda x: x.confidence, reverse=True)
        
        # 重複除去
        seen_texts = set()
        unique_contexts = []
        for context in filtered_contexts:
            text_key = context.text.lower().strip()
            if text_key not in seen_texts:
                seen_texts.add(text_key)
                unique_contexts.append(context)
        
        return unique_contexts[:5]  # 最大5個まで
    
    def _is_valid_text(self, text: str) -> bool:
        """有効なテキストかどうかチェック"""
        if not text or not text.strip():
            return False
        
        text = text.strip()
        
        # 長さチェック
        if (len(text) < self.settings['min_text_length'] or 
            len(text) > self.settings['max_text_length']):
            return False
        
        # ノイズパターンチェック
        return not self._is_noise_text(text)
    
    def _is_noise_text(self, text: str) -> bool:
        """ノイズテキストかどうかチェック"""
        text_lower = text.lower()
        
        for pattern in self.noise_patterns:
            if re.search(pattern, text_lower, re.IGNORECASE):
                return True
        
        return False
    
    def _calculate_field_match_bonus(self, text: str) -> float:
        """フィールドパターンマッチによるボーナス計算（強化版）"""
        text_lower = text.lower()
        bonus = 0.0
        
        # 1. 基本フィールドパターンマッチ
        for field_name, patterns in self.japanese_field_patterns.items():
            for pattern in patterns:
                if pattern.lower() in text_lower:
                    # FAX フィールドは低いボーナス（除外対象として認識）
                    if field_name == 'FAX番号':
                        bonus = max(bonus, 0.1)
                    else:
                        bonus = max(bonus, 0.3)
                    break
        
        # 2. 必須項目インジケータによる追加ボーナス
        required_bonus = 0.0
        for indicator in self.japanese_context_patterns['required_indicators']:
            if indicator in text:
                required_bonus = 0.2
                break
        
        # 3. 入力ガイドによる追加ボーナス
        guide_bonus = 0.0
        for guide in self.japanese_context_patterns['input_guides']:
            if guide in text:
                guide_bonus = 0.15
                break
        
        # 4. 丁寧語による追加ボーナス
        polite_bonus = 0.0
        for polite in self.japanese_context_patterns['polite_forms']:
            if polite in text:
                polite_bonus = 0.1
                break
        
        return min(1.0, bonus + required_bonus + guide_bonus + polite_bonus)
    
    async def _get_element_text_content(self, element_locator: Locator) -> str:
        """要素のテキストコンテンツを取得"""
        try:
            return await element_locator.evaluate("""
                el => {
                    if (el.tagName.toLowerCase() === 'input') {
                        return el.value || '';
                    } else if (el.tagName.toLowerCase() === 'textarea') {
                        return el.textContent || el.value || '';
                    }
                    return '';
                }
            """) or ''
        except:
            return ''
    
    def get_best_context_text(self, contexts: List[TextContext]) -> str:
        """最も信頼度の高いコンテキストテキストを取得"""
        if not contexts:
            return ''
        
        # 信頼度順にソート済みなので最初の要素を返す
        return contexts[0].text
    
    def get_combined_context_text(self, contexts: List[TextContext], max_contexts: int = 3) -> str:
        """複数のコンテキストを結合したテキストを取得"""
        if not contexts:
            return ''
        
        # 上位N個のコンテキストを結合
        selected_contexts = contexts[:max_contexts]
        combined_text = ' '.join(context.text for context in selected_contexts)
        
        return combined_text
    
    def get_context_summary(self, contexts: List[TextContext]) -> Dict[str, Any]:
        """コンテキスト抽出のサマリーを取得"""
        if not contexts:
            return {'total': 0, 'sources': {}, 'max_confidence': 0}
        
        source_counts = {}
        for context in contexts:
            source_counts[context.source_type] = source_counts.get(context.source_type, 0) + 1
        
        return {
            'total': len(contexts),
            'sources': source_counts,
            'max_confidence': max(context.confidence for context in contexts),
            'avg_confidence': sum(context.confidence for context in contexts) / len(contexts)
        }
    
    def detect_required_indicator(self, contexts: List[TextContext]) -> bool:
        """コンテキストから必須項目インジケータを検出"""
        combined_text = ' '.join(context.text for context in contexts)
        
        for indicator in self.japanese_context_patterns['required_indicators']:
            if indicator in combined_text:
                return True
        
        return False
    
    def detect_optional_indicator(self, contexts: List[TextContext]) -> bool:
        """コンテキストから任意項目インジケータを検出"""
        combined_text = ' '.join(context.text for context in contexts)
        
        for indicator in self.japanese_context_patterns['optional_indicators']:
            if indicator in combined_text:
                return True
        
        return False
    
    def detect_field_type_from_context(self, contexts: List[TextContext]) -> str:
        """コンテキストからフィールドタイプを推定"""
        combined_text = ' '.join(context.text for context in contexts).lower()
        
        # 最も信頼度の高いマッチを見つける
        best_match = ''
        best_score = 0.0
        
        for field_name, patterns in self.japanese_field_patterns.items():
            score = 0.0
            match_count = 0
            
            for pattern in patterns:
                if pattern.lower() in combined_text:
                    match_count += 1
                    # パターンの長さを考慮した重み付け
                    pattern_weight = len(pattern) / 10.0
                    score += pattern_weight
            
            # マッチ数による追加ボーナス
            if match_count > 1:
                score *= 1.2
            
            if score > best_score:
                best_score = score
                best_match = field_name
        
        return best_match
    
    def get_format_hints(self, contexts: List[TextContext]) -> List[str]:
        """コンテキストからフォーマットヒントを抽出"""
        combined_text = ' '.join(context.text for context in contexts)
        hints = []
        
        for hint in self.japanese_context_patterns['format_hints']:
            if hint in combined_text:
                hints.append(hint)
        
        return hints
    
    def is_fax_field_context(self, contexts: List[TextContext]) -> bool:
        """コンテキストからFAXフィールドかどうかを判定"""
        combined_text = ' '.join(context.text for context in contexts).lower()
        
        fax_patterns = self.japanese_field_patterns.get('FAX番号', [])
        for pattern in fax_patterns:
            if pattern.lower() in combined_text:
                return True
        
        return False
    
    def _should_skip_position_search(self, contexts: List[TextContext]) -> bool:
        """
        位置探索をスキップするかどうかを判定（高速化）
        
        強いラベル系コンテキスト（label、dt、th）で十分な手がかりがある場合、
        重い位置探索（TreeWalker）をスキップする
        
        Args:
            contexts: 既に抽出されたコンテキストリスト
            
        Returns:
            True: 位置探索をスキップする
            False: 位置探索を実行する
        """
        if not contexts:
            return False
        
        # 強いコンテキストソース（ラベル系）の信頼度閾値
        strong_confidence_threshold = 0.7
        strong_sources = ['label', 'dt', 'th', 'list_label']
        
        # 高信頼度の強いコンテキストが存在するか
        strong_contexts = [
            ctx for ctx in contexts 
            if ctx.source_type in strong_sources and ctx.confidence >= strong_confidence_threshold
        ]
        
        if not strong_contexts:
            return False
        
        # フィールドパターンマッチがあるか追加チェック
        for ctx in strong_contexts:
            for field_patterns in self.japanese_field_patterns.values():
                for pattern in field_patterns:
                    if pattern.lower() in ctx.text.lower():
                        logger.debug(f"Strong context found: {ctx.source_type}, text: '{ctx.text[:50]}...', confidence: {ctx.confidence}")
                        return True
        
        return False
