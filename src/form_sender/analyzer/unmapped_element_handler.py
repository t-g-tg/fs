import logging
from typing import Dict, List, Any, Optional, Callable, Awaitable, Tuple
from playwright.async_api import Page, Locator, TimeoutError as PlaywrightTimeoutError

from .element_scorer import ElementScorer
from .context_text_extractor import ContextTextExtractor
from .field_combination_manager import FieldCombinationManager
from .form_structure_analyzer import FormStructure
from config.manager import get_prefectures, get_choice_priority_config
from ..utils.privacy_consent_handler import PrivacyConsentHandler

logger = logging.getLogger(__name__)


class UnmappedElementHandler:
    """未マッピング要素の自動処理を担当するクラス"""

    def __init__(
        self,
        page: Page,
        element_scorer: ElementScorer,
        context_text_extractor: ContextTextExtractor,
        field_combination_manager: FieldCombinationManager,
        settings: Dict[str, Any],
        generate_playwright_selector_func: Callable[[Locator], Awaitable[str]],
        get_element_details_func: Callable[[Locator], Awaitable[Dict[str, Any]]],
        field_patterns,
    ):
        self.page = page
        self.element_scorer = element_scorer
        self.context_text_extractor = context_text_extractor
        self.field_combination_manager = field_combination_manager
        self.settings = settings
        self._generate_playwright_selector = generate_playwright_selector_func
        self._get_element_details = get_element_details_func
        self.field_patterns = field_patterns
        # 近傍コンテナの必須検出結果キャッシュ
        self._container_required_cache: Dict[str, bool] = {}

        # 必須マーカー（『※』は注記用途が多く誤検出の原因になるため除外）
        self.REQUIRED_MARKERS = [
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

    async def _detect_group_required_via_container(self, first_radio: Locator) -> bool:
        """見出しコンテナ側の必須マーカーを探索して判定（設定化・キャッシュ付）。"""
        try:
            key = await self._generate_playwright_selector(first_radio)
        except Exception:
            key = str(first_radio)
        if key in self._container_required_cache:
            return self._container_required_cache[key]

        max_depth = int(self.settings.get("radio_required_max_container_depth", 6))
        sib_depth = int(self.settings.get("radio_required_max_sibling_depth", 2))
        js = """
            (el) => {{
              const MARKERS = ['*','必須','Required','Mandatory','Must','(必須)','（必須）','[必須]','［必須］'];
              const CLASS_HINTS = ['must','required','require','need','mandatory','必須'];
              const hasMarker = (node) => {{
                if (!node) return false;
                const txt = (node.innerText || node.textContent || '').trim();
                const cls = (node.getAttribute && ((node.getAttribute('class')||'').toLowerCase())) || '';
                if (!txt) return false;
                if (MARKERS.some(m => txt.includes(m))) return true;
                if (cls && CLASS_HINTS.some(k => cls.includes(k))) return true;
                return false;
              }};
              let p = el; let depth = 0;
              while (p && depth < {max_depth}) {{
                const tag = (p.tagName || '').toLowerCase();
                if (['p','div','li','fieldset','dd','td','ul'].includes(tag)) {{
                  if (hasMarker(p)) return true;
                  let sib = p.previousElementSibling; let sdepth = 0;
                  while (sib && sdepth < {sib_depth}) {{
                    if (hasMarker(sib)) return true;
                    sib = sib.previousElementSibling; sdepth++;
                  }}
                  // テーブル構造: td/dd に対する直前の th/dt を確認
                  if (tag === 'td' || tag === 'dd') {{
                    let ps = p.previousElementSibling;
                    while (ps) {{
                      const stag = (ps.tagName || '').toLowerCase();
                      if ((tag === 'td' && stag === 'th') || (tag === 'dd' && stag === 'dt')) {{
                        if (hasMarker(ps)) return true;
                        break;
                      }}
                      ps = ps.previousElementSibling;
                    }}
                  }}
                }}
                p = p.parentElement; depth++;
              }}
              return false;
            }}
        """.format(max_depth=max_depth, sib_depth=sib_depth)
        try:
            found = bool(await first_radio.evaluate(js))
        except Exception as e:
            logger.debug(f"Container required detection failed: {e}")
            found = False
        self._container_required_cache[key] = found
        return found

    async def handle_unmapped_elements(
        self,
        classified_elements: Dict[str, List[Locator]],
        field_mapping: Dict[str, Dict[str, Any]],
        client_data: Optional[Dict[str, Any]],
        form_structure: Optional[FormStructure],
    ) -> Dict[str, Dict[str, Any]]:
        if not self.settings.get("enable_auto_handling", True):
            return {}

        auto_handled = {}

        # 先に『統合氏名/統合氏名カナ』が name1/name2 / kana1/kana2 のような分割ペアに誤割当てされていないかを確認し、
        # 該当する場合は統合マッピングを降格して分割入力を優先できるようにする（汎用・安全）。
        try:
            self._demote_unified_name_for_indexed_pairs(
                classified_elements, field_mapping
            )
            self._demote_unified_kana_for_indexed_pairs(
                classified_elements, field_mapping
            )
        except Exception as e:
            logger.debug(f"unified demotion skipped: {e}")

        mapped_element_ids = {
            id(info.get("element"))
            for info in field_mapping.values()
            if isinstance(info, dict) and info.get("element")
        }

        checkbox_handled = await self._auto_handle_checkboxes(
            classified_elements.get("checkboxes", []), mapped_element_ids
        )
        auto_handled.update(checkbox_handled)
        mapped_element_ids.update(
            {
                id(v.get("element"))
                for v in checkbox_handled.values()
                if isinstance(v, dict) and v.get("element")
            }
        )

        radio_handled = await self._auto_handle_radios(
            classified_elements.get("radios", []), mapped_element_ids, client_data
        )
        auto_handled.update(radio_handled)
        mapped_element_ids.update(
            {
                id(v.get("element"))
                for v in radio_handled.values()
                if isinstance(v, dict) and v.get("element")
            }
        )

        select_handled = await self._auto_handle_selects(
            classified_elements.get("selects", []), mapped_element_ids, client_data
        )
        auto_handled.update(select_handled)
        mapped_element_ids.update(
            {
                id(v.get("element"))
                for v in select_handled.values()
                if isinstance(v, dict) and v.get("element")
            }
        )

        # 汎用昇格: 都道府県フィールド（select/text）が未マッピングなら field_mapping に昇格
        try:
            promoted_pref = await self._promote_prefecture_field(
                (classified_elements.get("selects", []) or []),
                (classified_elements.get("text_inputs", []) or []),
                field_mapping,
            )
            if promoted_pref:
                auto_handled.update(promoted_pref)
        except Exception as e:
            logger.debug(f"Promote prefecture select skipped: {e}")

        # 汎用昇格: 部署名フィールド（text）。未マッピングなら field_mapping に昇格。
        try:
            promoted_dept = await self._promote_department_field(
                (classified_elements.get("text_inputs", []) or []),
                field_mapping,
            )
            if promoted_dept:
                auto_handled.update(promoted_dept)
        except Exception as e:
            logger.debug(f"Promote department text skipped: {e}")

        email_conf = await self._auto_handle_email_confirmation(
            classified_elements.get("email_inputs", [])
            + classified_elements.get("text_inputs", []),
            mapped_element_ids,
            field_mapping,
            form_structure,
        )
        auto_handled.update(email_conf)

        # 統合氏名が既にマッピングされている場合は、auto_fullname 系の生成を抑止
        if "統合氏名" not in field_mapping:
            fullname_handled = await self._auto_handle_unified_fullname(
                classified_elements.get("text_inputs", []),
                mapped_element_ids,
                client_data,
                form_structure,
            )
            auto_handled.update(fullname_handled)

        # カナ処理（順序調整版）: 配列/インデックス処理で '姓カナ'/'名カナ' を先に確定させ、
        # その後に必要であれば統合カナの救済を行う。
        has_unified_kana = "統合氏名カナ" in field_mapping
        has_split_kana = ("姓カナ" in field_mapping) and ("名カナ" in field_mapping)
        if not (has_unified_kana or has_split_kana):
            text_inputs = classified_elements.get("text_inputs", [])
            # まず分割カナの自動処理を試みる（セイ/メイ のヒント検索）
            last_like, first_like = None, None
            indexed_kana_pair_present = False
            for el in text_inputs:
                try:
                    if id(el) in mapped_element_ids:
                        continue
                    info = await self.element_scorer._get_element_info(el)
                    if not info.get("visible", True):
                        continue
                    name_id_cls = " ".join(
                        [
                            info.get("name", ""),
                            info.get("id", ""),
                            info.get("class", ""),
                        ]
                    ).lower()
                    nm = (info.get("name", "") or "").lower()
                    if nm in ("kana1", "kana_1", "kana2", "kana_2"):
                        # どちらか一方でも見つかれば候補
                        indexed_kana_pair_present = indexed_kana_pair_present or (
                            nm in ("kana1", "kana_1")
                        )
                    contexts = (
                        await self.context_text_extractor.extract_context_for_element(
                            el
                        )
                    )
                    best = (
                        self.context_text_extractor.get_best_context_text(contexts)
                        or ""
                    ).lower()
                    kana_like = (
                        ("kana" in name_id_cls)
                        or ("furigana" in name_id_cls)
                        or ("katakana" in name_id_cls)
                        or ("カナ" in best)
                        or ("フリガナ" in best)
                        or ("ふりがな" in best)
                    )
                    if not kana_like:
                        continue
                    if any(
                        t in (best + " " + name_id_cls) for t in ["sei", "姓", "セイ"]
                    ):
                        last_like = el if last_like is None else last_like
                    if any(
                        t in (best + " " + name_id_cls) for t in ["mei", "名", "メイ"]
                    ):
                        first_like = el if first_like is None else first_like
                except Exception:
                    continue
            if last_like and first_like:
                kana_split = await self._auto_handle_split_kana(
                    text_inputs, mapped_element_ids, client_data
                )
                auto_handled.update(kana_split)
                mapped_element_ids.update(
                    {
                        id(v.get("element"))
                        for v in kana_split.values()
                        if isinstance(v, dict) and v.get("element")
                    }
                )
            elif not indexed_kana_pair_present:
                kana_handled = await self._auto_handle_unified_kana(
                    text_inputs,
                    mapped_element_ids,
                    client_data,
                    form_structure,
                )
                auto_handled.update(kana_handled)
                mapped_element_ids.update(
                    {
                        id(v.get("element"))
                        for v in kana_handled.values()
                        if isinstance(v, dict) and v.get("element")
                    }
                )
            # indexed_kana_pair_present の場合は、この後のインデックス処理で安全に分割カナを割当てる

        # 任意のFAXフィールドがある場合、電話番号で補完（必須でない場合のみ）
        try:
            fax_handled = await self._auto_handle_fax(
                classified_elements.get("text_inputs", []) or [],
                mapped_element_ids,
                client_data,
            )
            auto_handled.update(fax_handled)
        except Exception as e:
            logger.debug(f"Auto handle fax skipped: {e}")

        # 配列形式の姓名・カナ（name[0]/name[1], kana[0]/kana[1]）の汎用処理
        try:
            split_name = await self._auto_handle_split_name_arrays(
                classified_elements.get("text_inputs", []) or [], mapped_element_ids
            )
            auto_handled.update(split_name)
            mapped_element_ids.update(
                {
                    id(v.get("element"))
                    for v in split_name.values()
                    if isinstance(v, dict) and v.get("element")
                }
            )
        except Exception as e:
            logger.debug(f"Auto handle split name arrays skipped: {e}")

        # name1/name2, kana1/kana2 形式の汎用処理
        try:
            indexed_pairs = await self._auto_handle_indexed_name_pairs(
                classified_elements.get("text_inputs", []) or [], mapped_element_ids
            )
            auto_handled.update(indexed_pairs)
            mapped_element_ids.update(
                {
                    id(v.get("element"))
                    for v in indexed_pairs.values()
                    if isinstance(v, dict) and v.get("element")
                }
            )
        except Exception as e:
            logger.debug(f"Auto handle indexed name pairs skipped: {e}")

        # family_name / given_name の汎用処理
        try:
            fam_given = await self._auto_handle_family_given_names(
                classified_elements.get("text_inputs", []) or [], mapped_element_ids
            )
            auto_handled.update(fam_given)
            mapped_element_ids.update(
                {
                    id(v.get("element"))
                    for v in fam_given.values()
                    if isinstance(v, dict) and v.get("element")
                }
            )
        except Exception as e:
            logger.debug(f"Auto handle family/given skipped: {e}")

        # 電話番号の3分割（tel1/tel2/tel3 等）の汎用処理
        try:
            phone_split = await self._auto_handle_split_phone(
                (classified_elements.get("tel_inputs", []) or [])
                + (classified_elements.get("text_inputs", []) or []),
                mapped_element_ids,
                field_mapping,
                client_data,
            )
            auto_handled.update(phone_split)
            mapped_element_ids.update(
                {
                    id(v.get("element"))
                    for v in phone_split.values()
                    if isinstance(v, dict) and v.get("element")
                }
            )
        except Exception as e:
            logger.debug(f"Auto handle split phone skipped: {e}")

        # 確認用メールアドレスの汎用処理（強シグナルがある場合にコピー入力）
        try:
            email_conf = await self._auto_handle_email_confirmation(
                (classified_elements.get("email_inputs", []) or [])
                + (classified_elements.get("text_inputs", []) or []),
                mapped_element_ids,
                field_mapping,
                form_structure,
            )
            auto_handled.update(email_conf)
            mapped_element_ids.update(
                {
                    id(v.get("element"))
                    for v in email_conf.values()
                    if isinstance(v, dict) and v.get("element")
                }
            )
        except Exception as e:
            logger.debug(f"Auto handle email confirmation skipped: {e}")

        # 汎用救済: 未マッピングの必須テキスト入力に全角空白を入力
        try:
            req_texts = await self._auto_handle_required_texts(
                classified_elements.get("text_inputs", []) or [],
                mapped_element_ids,
                field_mapping,
            )
            auto_handled.update(req_texts)
            mapped_element_ids.update(
                {
                    id(v.get("element"))
                    for v in req_texts.values()
                    if isinstance(v, dict) and v.get("element")
                }
            )
        except Exception as e:
            logger.debug(f"Auto handle required texts skipped: {e}")

        # 追加: 選択フォーム外の必須（評価用の最小カバー）
        try:
            if form_structure and getattr(form_structure, "form_locator", None):
                outside = await self._auto_handle_required_outside_selected_form(
                    form_structure.form_locator, mapped_element_ids
                )
                auto_handled.update(outside)
        except Exception as e:
            logger.debug(f"outside required auto-handle skipped: {e}")

        logger.info(
            f"Auto-handled elements: checkboxes={len(checkbox_handled)}, radios={len(radio_handled)}, selects={len(select_handled)}"
        )
        return auto_handled

    async def _auto_handle_required_outside_selected_form(
        self, selected_form: Locator, mapped_element_ids: set
    ) -> Dict[str, Dict[str, Any]]:
        """選択フォーム外の必須 input/textarea/checkbox を最小限自動処理（汎用）。"""
        handled: Dict[str, Dict[str, Any]] = {}
        try:
            candidates = await self.page.locator("input, textarea, select").all()
        except Exception:
            return handled

        idx_cb = 1
        # 選択フォームのセレクタ文字列を取得（closest("form").matches で使用）
        try:
            selected_form_selector = await self._generate_playwright_selector(selected_form)
        except Exception:
            selected_form_selector = "form"
        for el in candidates:
            try:
                if id(el) in mapped_element_ids:
                    continue
                # 選択フォームに属するものは対象外
                try:
                    in_selected = await el.evaluate(
                        "(el, sel) => { const cf = el.closest('form'); return !!cf && cf.matches(sel); }",
                        selected_form_selector,
                    )
                except Exception:
                    in_selected = False
                if in_selected:
                    continue

                info = await self.element_scorer._get_element_info(el)
                if not info.get("visible", True):
                    continue

                # 必須判定（属性 or コンテナ）
                required = await self.element_scorer._detect_required_status(el)
                if not required:
                    try:
                        required = await self._detect_group_required_via_container(el)
                    except Exception:
                        required = False
                if not required:
                    continue

                tag = (info.get("tag_name") or "").lower()
                typ = (info.get("type") or "").lower()
                selector = await self._generate_playwright_selector(el)

                # 罠/認証/確認は除外
                nic = " ".join(
                    [
                        (info.get("name") or ""),
                        (info.get("id") or ""),
                        (info.get("class") or ""),
                    ]
                ).lower()
                if any(
                    b in nic
                    for b in [
                        "captcha",
                        "token",
                        "otp",
                        "verification",
                        "confirm",
                        "re_email",
                        "re-mail",
                    ]
                ):
                    continue

                if tag == "input" and typ == "checkbox":
                    # 同意/規約系に限定（副作用を最小化）
                    try:
                        label_ctx = " ".join(
                            [
                                (info.get("name") or ""),
                                (info.get("id") or ""),
                                (info.get("class") or ""),
                            ]
                        ).lower()
                        contexts = await self.context_text_extractor.extract_context_for_element(el)
                        label_ctx += " " + " ".join([getattr(c, 'text', '') or '' for c in (contexts or [])]).lower()
                    except Exception:
                        label_ctx = ""
                    if not any(t in label_ctx for t in ["privacy", "同意", "規約", "ポリシー", "consent", "agree"]):
                        continue
                    handled[f"outside_required_checkbox_{idx_cb}"] = {
                        "element": el,
                        "selector": selector,
                        "tag_name": tag,
                        "type": typ,
                        "input_type": "checkbox",
                        "auto_action": "check",
                        "required": True,
                        "auto_handled": True,
                    }
                    idx_cb += 1
                elif tag == "input" and typ == "radio":
                    # ラジオは選択肢依存のためスキップ（ページ固有）
                    continue
                else:
                    # select は副作用が大きいので扱わない
                    continue
            except Exception as e:
                logger.debug(f"outside required handle element skipped: {e}")

        return handled

    def _demote_unified_name_for_indexed_pairs(
        self,
        classified_elements: Dict[str, List[Locator]],
        field_mapping: Dict[str, Dict[str, Any]],
    ) -> None:
        """name1/name2 のような分割姓名が存在する場合、統合氏名の誤割当てを降格（削除）する。

        条件（汎用）:
        - field_mapping に『統合氏名』が存在
        - text_inputs に visible な name 属性が name1/name2 もしくは name_1/name_2 の2つが存在
        - 『統合氏名』がそのどちらかの要素に割り当てられている
        """
        try:
            if "統合氏名" not in field_mapping:
                return
            text_inputs = classified_elements.get("text_inputs", []) or []

            def _name_key(n: str) -> str:
                return (n or "").strip().lower()

            pairs = {}
            for el in text_inputs:
                info = self.element_scorer._shared_cache.get(str(el)) or {}
                nm = _name_key(info.get("name", ""))
                if not nm:
                    continue
                if nm in ("name1", "name_1"):
                    pairs[1] = el
                elif nm in ("name2", "name_2"):
                    pairs[2] = el
            if 1 in pairs and 2 in pairs:
                mapped_name_attr = (field_mapping.get("統合氏名", {}) or {}).get(
                    "name", ""
                )
                # 片方の name 属性が統合氏名の割当先と一致していれば降格
                for idx in (1, 2):
                    info = self.element_scorer._shared_cache.get(str(pairs[idx])) or {}
                    if mapped_name_attr and mapped_name_attr == (
                        info.get("name", "") or ""
                    ):
                        field_mapping.pop("統合氏名", None)
                        logger.info(
                            "Demoted unified fullname in favor of indexed split name fields"
                        )
                        return
        except Exception:
            pass

    def _demote_unified_kana_for_indexed_pairs(
        self,
        classified_elements: Dict[str, List[Locator]],
        field_mapping: Dict[str, Dict[str, Any]],
    ) -> None:
        """kana1/kana2 のような分割カナが存在する場合、統合氏名カナの誤割当てを降格（削除）する。"""
        try:
            if "統合氏名カナ" not in field_mapping:
                return
            text_inputs = classified_elements.get("text_inputs", []) or []

            def _name_key(n: str) -> str:
                return (n or "").strip().lower()

            pairs = {}
            for el in text_inputs:
                info = self.element_scorer._shared_cache.get(str(el)) or {}
                nm = _name_key(info.get("name", ""))
                if not nm:
                    continue
                if nm in ("kana1", "kana_1"):
                    pairs[1] = el
                elif nm in ("kana2", "kana_2"):
                    pairs[2] = el
            if 1 in pairs and 2 in pairs:
                mapped_name_attr = (field_mapping.get("統合氏名カナ", {}) or {}).get(
                    "name", ""
                )
                for idx in (1, 2):
                    info = self.element_scorer._shared_cache.get(str(pairs[idx])) or {}
                    if mapped_name_attr and mapped_name_attr == (
                        info.get("name", "") or ""
                    ):
                        field_mapping.pop("統合氏名カナ", None)
                        logger.info(
                            "Demoted unified kana in favor of indexed split kana fields"
                        )
                        return
        except Exception:
            pass

    async def _auto_handle_indexed_name_pairs(
        self,
        text_inputs: List[Locator],
        mapped_element_ids: set,
    ) -> Dict[str, Dict[str, Any]]:
        """name1/name2, kana1/kana2 形式を汎用的に『姓/名』『姓カナ/名カナ』として扱う。"""
        handled: Dict[str, Dict[str, Any]] = {}
        try:
            buckets = {"name": {}, "kana": {}}
            for el in text_inputs:
                info = await self.element_scorer._get_element_info(el)
                if not info.get("visible", True):
                    continue
                nm = (info.get("name", "") or "").lower()
                key = None
                idx = None
                if nm in ("name1", "name_1"):
                    key, idx = "name", 1
                elif nm in ("name2", "name_2"):
                    key, idx = "name", 2
                elif nm in ("kana1", "kana_1"):
                    key, idx = "kana", 1
                elif nm in ("kana2", "kana_2"):
                    key, idx = "kana", 2
                if key and idx:
                    buckets[key][idx] = (el, info)
            # 姓名
            if 1 in buckets["name"] and 2 in buckets["name"]:
                for field_name, idx in [("姓", 1), ("名", 2)]:
                    el, info = buckets["name"][idx]
                    selector = await self._generate_playwright_selector(el)
                    required = await self.element_scorer._detect_required_status(el)
                    handled[field_name] = {
                        "element": el,
                        "selector": selector,
                        "tag_name": info.get("tag_name", "input") or "input",
                        "type": info.get("type", "text") or "text",
                        "name": info.get("name", ""),
                        "id": info.get("id", ""),
                        "input_type": "text",
                        "default_value": "",
                        "required": required,
                        "visible": info.get("visible", True),
                        "enabled": info.get("enabled", True),
                        "auto_handled": True,
                    }
            # カナ
            if 1 in buckets["kana"] and 2 in buckets["kana"]:
                for field_name, idx in [("姓カナ", 1), ("名カナ", 2)]:
                    el, info = buckets["kana"][idx]
                    selector = await self._generate_playwright_selector(el)
                    required = await self.element_scorer._detect_required_status(el)
                    handled[field_name] = {
                        "element": el,
                        "selector": selector,
                        "tag_name": info.get("tag_name", "input") or "input",
                        "type": info.get("type", "text") or "text",
                        "name": info.get("name", ""),
                        "id": info.get("id", ""),
                        "input_type": "text",
                        "default_value": "",
                        "required": required,
                        "visible": info.get("visible", True),
                        "enabled": info.get("enabled", True),
                        "auto_handled": True,
                    }
        except Exception as e:
            logger.debug(f"indexed name pairs handler error: {e}")
        return handled

    async def _auto_handle_split_phone(
        self,
        text_inputs: List[Locator],
        mapped_element_ids: set,
        field_mapping: Dict[str, Dict[str, Any]],
        client_data: Optional[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """電話番号の分割入力欄（tel1/tel2/tel3等）に対する汎用自動入力。

        - name/id/class に tel/phone を含み、末尾に 1/2/3 を含む3要素を検出
        - 既にマッピング済みの要素は対象外
        - 統合『電話番号』がこれらの要素のいずれかに割当てられている場合は降格（削除）
        - client_data の phone_1/2/3 から値を投入
        """
        handled: Dict[str, Dict[str, Any]] = {}
        try:
            # 1) 候補抽出（既存マッピングの有無に関わらず検出）
            def _infer_phone_part_index(nm: str, ide: str, cls: str) -> Optional[int]:
                """電話番号分割のインデックスを 1..3 で推定する。

                ルール:
                - name/id/class のいずれかに 'tel'/'phone'/'電話' が無ければ None
                - まず配列風の末尾インデックス [0-2] を検出したら +1（0→1, 1→2, 2→3）
                - それ以外で『tel1/phone2/電話3』のような直接数字があれば、その数字を返す（1..3想定）
                - フォールバックで末尾一桁数字があれば、それを返す（0 のみ +1 する）
                - 何も無ければ 1 を仮定
                """
                import re

                nm = (nm or "").lower()
                ide = (ide or "").lower()
                cls = (cls or "").lower()
                blob = nm + " " + ide + " " + cls
                if not (("tel" in blob) or ("phone" in blob) or ("電話" in blob)):
                    return None
                # 配列風 [d] の最終出現を優先
                m_br = re.search(r"\[(\d)\](?!.*\d)", blob)
                if m_br:
                    raw = int(m_br.group(1))
                    return raw + 1  # 0→1, 1→2, 2→3

                # 接頭辞 + 数字（tel1/phone2/電話3）
                for s in (nm, ide, cls):
                    if not s:
                        continue
                    m = re.search(r"(?:tel|phone|電話)[^\d]*([0-9])(?!.*\d)", s)
                    if m:
                        raw = int(m.group(1))
                        return 1 if raw == 0 else raw

                # 拡張: 配列/名付けインデックス first/center/last を [1]/[2]/[3] に対応
                # 例: tel[first]/tel[center]/tel[last]
                if any(k in blob for k in ("[first]", "[center]", "[last]")):
                    if "[first]" in blob:
                        return 1
                    if "[center]" in blob:
                        return 2
                    if "[last]" in blob:
                        return 3

                # フォールバック: 末尾の一桁数字
                tail = re.search(r"(\d)(?!.*\d)$", blob)
                if tail:
                    raw = int(tail.group(1))
                    return 1 if raw == 0 else raw

                return 1

            triples_all: Dict[int, Tuple[Locator, Dict[str, Any]]] = {}
            for el in text_inputs:
                info = await self.element_scorer._get_element_info(el)
                if not info.get("visible", True):
                    continue
                nm = (info.get("name", "") or "").lower()
                ide = (info.get("id", "") or "").lower()
                cls = (info.get("class", "") or "").lower()
                idx = _infer_phone_part_index(nm, ide, cls)
                if idx is None:
                    continue
                if idx in (1, 2, 3):
                    triples_all[idx] = (el, info)
            if not all(k in triples_all for k in (1, 2, 3)):
                return handled

            # 2) 統合『電話番号』がこのグループのいずれかに割当てられているなら降格
            try:
                if "電話番号" in field_mapping:
                    sel_unified = field_mapping.get("電話番号", {}).get("selector", "")
                    if sel_unified:
                        group_selectors = set()
                        for idx in (1, 2, 3):
                            try:
                                group_selectors.add(
                                    await self._generate_playwright_selector(
                                        triples_all[idx][0]
                                    )
                                )
                            except Exception:
                                pass
                        if sel_unified in group_selectors:
                            field_mapping.pop("電話番号", None)
                            logger.info(
                                "Demoted unified phone in favor of split phone fields"
                            )
            except Exception:
                pass

            # 3) 値を割当（client_data から）
            client = (
                client_data.get("client", {}) if isinstance(client_data, dict) else {}
            )
            parts = [
                (client.get("phone_1", "") or "").strip(),
                (client.get("phone_2", "") or "").strip(),
                (client.get("phone_3", "") or "").strip(),
            ]
            labels = {1: "市外局番", 2: "市内局番", 3: "加入者番号"}
            for idx in (1, 2, 3):
                el, info = triples_all[idx]
                selector = await self._generate_playwright_selector(el)
                required = await self.element_scorer._detect_required_status(el)
                if not required:
                    try:
                        # 電話番号グループの見出し側必須表示（th/dt/見出し）を検出
                        required = await self._detect_group_required_via_container(el)
                    except Exception:
                        required = False
                handled[f"auto_phone_part_{idx}"] = {
                    "element": el,
                    "selector": selector,
                    "tag_name": info.get("tag_name", "input") or "input",
                    "type": info.get("type", "text") or "text",
                    "name": info.get("name", ""),
                    "id": info.get("id", ""),
                    "input_type": "text",
                    "auto_action": "fill",
                    "default_value": parts[idx - 1],
                    "required": required,
                    "auto_handled": True,
                    "part_label": labels[idx],
                }
        except Exception as e:
            logger.debug(f"split phone handler error: {e}")
        return handled

    async def _auto_handle_required_texts(
        self,
        text_inputs: List[Locator],
        mapped_element_ids: set,
        field_mapping: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """未マッピングの必須テキスト/テキストエリアを汎用的に入力（全角空白）。

        仕様:
        - required属性/aria-required/周辺コンテキストの必須マーカー(*, 必須 等)のいずれかで必須と判断
        - 既にマッピング済みの要素は対象外
        - 『captcha/token/確認用』等は除外（_is_nonfillable_required に準拠）
        - 値は『　』（全角スペース）を入力
        """
        handled: Dict[str, Dict[str, Any]] = {}
        idx = 1
        for el in text_inputs:
            try:
                if id(el) in mapped_element_ids:
                    continue
                info = await self.element_scorer._get_element_info(el)
                if not info.get("visible", True):
                    continue
                # 罠/スパム対策フィールドは除外
                try:
                    trap_blob = " ".join(
                        [
                            (info.get("name", "") or ""),
                            (info.get("id", "") or ""),
                            (info.get("class", "") or ""),
                        ]
                    ).lower()
                    if any(
                        t in trap_blob
                        for t in [
                            "honeypot",
                            "honey",
                            "trap",
                            "botfield",
                            "no-print",
                            "noprint",
                        ]
                    ):
                        continue
                except Exception:
                    pass
                # 既存のマッピングと同一セレクタはスキップ（重複対策）
                try:
                    sel = await self._generate_playwright_selector(el)
                    if any(
                        (fm.get("selector") == sel)
                        for fm in field_mapping.values()
                        if isinstance(fm, dict)
                    ):
                        continue
                except Exception:
                    pass
                # 除外（確認用/認証等）: ここでは軽量なローカル判定で十分
                try:
                    name_id_cls = " ".join(
                        [
                            (info.get("name") or ""),
                            (info.get("id") or ""),
                            (info.get("class") or ""),
                        ]
                    ).lower()
                    input_type = (info.get("type") or "").lower()
                    tag = (info.get("tag_name") or "").lower()
                    blacklist = [
                        "captcha",
                        "image_auth",
                        "image-auth",
                        "spam-block",
                        "token",
                        "otp",
                        "verification",
                        "email_confirm",
                        "mail_confirm",
                        "email_confirmation",
                        "confirm_email",
                        "confirm",
                        "re_email",
                        "re-mail",
                    ]
                    if any(b in name_id_cls for b in blacklist):
                        continue
                    if input_type in ["checkbox", "radio"] or tag == "select":
                        continue
                except Exception:
                    pass
                # カナ/ふりがな等はここでは扱わない（後段の昇格/assignerに委譲）
                try:
                    blob = " ".join(
                        [
                            (info.get("name", "") or ""),
                            (info.get("id", "") or ""),
                            (info.get("class", "") or ""),
                            (info.get("placeholder", "") or ""),
                        ]
                    ).lower()
                    # context からも簡易取得
                    ctxs = (
                        await self.context_text_extractor.extract_context_for_element(
                            el
                        )
                    )
                    ctx_text = " ".join(
                        [(getattr(c, "text", "") or "") for c in (ctxs or [])]
                    )
                    if any(
                        t in (blob + " " + ctx_text)
                        for t in [
                            "furigana",
                            "kana",
                            "katakana",
                            "カナ",
                            "フリガナ",
                            "ふりがな",
                            "ひらがな",
                        ]
                    ):
                        continue
                except Exception:
                    pass
                # 必須判定
                required = await self.element_scorer._detect_required_status(el)
                if not required:
                    # コンテナ(dt/dd/th/td 等)に必須マーカーが表示されるタイプの検出を追加（汎用）
                    try:
                        required = await self._detect_group_required_via_container(el)
                    except Exception:
                        required = False
                if not required:
                    continue
                selector = await self._generate_playwright_selector(el)
                field_name = f"auto_required_text_{idx}"
                # 既定の自動値（全角空白）。
                auto_value = "　"
                try:
                    nic = (
                        info.get("name", "")
                        + " "
                        + info.get("id", "")
                        + " "
                        + info.get("class", "")
                    ).lower()
                    import re

                    if "tel" in nic or "phone" in nic:
                        m = re.search(r"(?:tel|phone)[^\d]*([123])(?!.*\d)", nic)
                        if m:
                            idxnum = int(m.group(1))
                            if idxnum in (2, 3):
                                auto_value = ""  # 後段の割当（auto_phone_part_*）やassignerで埋まる前提で空文字
                except Exception:
                    pass
                handled[field_name] = {
                    "element": el,
                    "selector": selector,
                    "tag_name": info.get("tag_name", "input") or "input",
                    "type": info.get("type", "text") or "text",
                    "name": info.get("name", ""),
                    "id": info.get("id", ""),
                    "input_type": "text",
                    "auto_action": "fill",
                    "default_value": auto_value,
                    "required": True,
                    "auto_handled": True,
                }
                idx += 1
            except Exception as e:
                logger.debug(f"required text auto-handle failed: {e}")
        return handled

    async def _promote_prefecture_field(
        self,
        selects: List[Locator],
        text_inputs: List[Locator],
        field_mapping: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """select要素の中から『都道府県』を示すものを汎用判定し、未マッピングなら昇格する。

        判定基準（すべて汎用）:
        - tag が select
        - 以下のいずれかを満たす
          * name/id/class に 'pref' または 'prefecture'
          * label/aria-labelledby 等のコンテキストに『都道府県』/『Prefecture』
        - 既に field_mapping に『都道府県』がある場合は処理しない
        """
        handled: Dict[str, Dict[str, Any]] = {}
        if "都道府県" in field_mapping:
            return handled
        tokens_attr = ["pref", "prefecture", "todofuken", "todouhuken"]
        tokens_ctx = ["都道府県", "prefecture"]

        # 1) select を優先
        for el in selects:
            try:
                info = await self.element_scorer._get_element_info(el)
                if not info.get("visible", True):
                    continue
                blob = " ".join(
                    [
                        (info.get("name", "") or "").lower(),
                        (info.get("id", "") or "").lower(),
                        (info.get("class", "") or "").lower(),
                        (info.get("placeholder", "") or "").lower(),
                    ]
                )
                attr_hit = any(t in blob for t in tokens_attr)

                ctx_hit = False
                if not attr_hit:
                    try:
                        contexts = await self.context_text_extractor.extract_context_for_element(
                            el
                        )
                        texts = " ".join(
                            [(c.text or "").lower() for c in (contexts or [])]
                        )
                        ctx_hit = any(t in texts for t in tokens_ctx)
                    except Exception:
                        ctx_hit = False

                # 追加検証: selectのoptionに都道府県名が一定数含まれるか
                option_ok = False
                if not (attr_hit or ctx_hit):
                    try:
                        pref_list = [
                            p.lower() for p in (get_prefectures().get("names") or [])
                        ]
                    except Exception:
                        pref_list = []
                    try:
                        options = await el.evaluate(
                            "el => Array.from(el.querySelectorAll('option')).map(o => (o.textContent||'').trim())"
                        )
                    except Exception:
                        options = []
                    lowered = [str(o).lower() for o in options]
                    hits = (
                        sum(1 for p in pref_list if any(p in o for o in lowered))
                        if pref_list
                        else 0
                    )
                    option_ok = hits >= 5

                if not (attr_hit or ctx_hit or option_ok):
                    continue

                selector = await self._generate_playwright_selector(el)
                required = await self.element_scorer._detect_required_status(el)
                # field_mapping に正式登録（assigner が『都道府県』を特別扱い）
                field_mapping["都道府県"] = {
                    "element": el,
                    "selector": selector,
                    "tag_name": "select",
                    "type": "select",
                    "name": info.get("name", ""),
                    "id": info.get("id", ""),
                    "input_type": "select",
                    "default_value": "",
                    "required": required,
                    "visible": info.get("visible", True),
                    "enabled": info.get("enabled", True),
                    "score": 0,
                }
                handled["都道府県"] = field_mapping["都道府県"]
                # 既に同一要素が『住所』として登録されていれば除去（誤上書き防止）
                try:
                    for k, v in list(field_mapping.items()):
                        if k.startswith("住所") and v.get("selector") == selector:
                            field_mapping.pop(k, None)

                except Exception:
                    pass
                logger.info("Promoted '都道府県' select to field_mapping")
                break
            except Exception as e:
                logger.debug(f"prefecture promotion failed: {e}")
        # 2) input[type=text] でも『都道府県』と確信できるものを昇格（属性必須）
        for el in text_inputs:
            try:
                info = await self.element_scorer._get_element_info(el)
                if not info.get("visible", True):
                    continue
                t = (info.get("type", "") or "").lower()
                if t not in ["", "text"]:
                    continue
                blob = " ".join(
                    [
                        (info.get("name", "") or "").lower(),
                        (info.get("id", "") or "").lower(),
                        (info.get("class", "") or "").lower(),
                        (info.get("placeholder", "") or "").lower(),
                    ]
                )
                # input系は属性に 'pref' がある場合のみ（ラベルの『都道府県』だけでは昇格しない）
                attr_hit = any(k in blob for k in ["pref", "prefecture"])
                if not attr_hit:
                    continue
                selector = await self._generate_playwright_selector(el)
                required = await self.element_scorer._detect_required_status(el)
                field_mapping["都道府県"] = {
                    "element": el,
                    "selector": selector,
                    "tag_name": info.get("tag_name", "input") or "input",
                    "type": info.get("type", "text") or "text",
                    "name": info.get("name", ""),
                    "id": info.get("id", ""),
                    "input_type": "text",
                    "default_value": "",
                    "required": required,
                    "visible": info.get("visible", True),
                    "enabled": info.get("enabled", True),
                    "score": 0,
                }
                handled["都道府県"] = field_mapping["都道府県"]
                # 既に同一要素が『住所』として登録されていれば除去
                try:
                    for k, v in list(field_mapping.items()):
                        if k.startswith("住所") and v.get("selector") == selector:
                            field_mapping.pop(k, None)
                except Exception:
                    pass
                logger.info("Promoted '都道府県' input(text) to field_mapping")
                break
            except Exception as e:
                logger.debug(f"prefecture text promotion failed: {e}")
        return handled

    async def _promote_department_field(
        self,
        text_inputs: List[Locator],
        field_mapping: Dict[str, Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """text要素から『部署名』らしいものを汎用判定し、未マッピングなら昇格する。

        判定基準（汎用）:
        - tag が input[type=text]
        - name/id/class/placeholder もしくはラベル/周辺テキストに以下の語を含む
          * 部署, 部署名, department
        - 既に field_mapping に『部署名』がある場合は処理しない
        """
        handled: Dict[str, Dict[str, Any]] = {}
        if "部署名" in field_mapping:
            return handled
        tokens_attr = ["部署", "部署名", "department"]
        tokens_ctx = ["部署", "部署名"]
        for el in text_inputs:
            try:
                info = await self.element_scorer._get_element_info(el)
                if not info.get("visible", True):
                    continue
                if (info.get("type", "").lower() or "") not in ("text", ""):
                    continue
                blob = " ".join(
                    [
                        (info.get("name", "") or "").lower(),
                        (info.get("id", "") or "").lower(),
                        (info.get("class", "") or "").lower(),
                        (info.get("placeholder", "") or "").lower(),
                    ]
                )
                attr_hit = any(t in blob for t in [s.lower() for s in tokens_attr])
                ctx_hit = False
                if not attr_hit:
                    try:
                        contexts = await self.context_text_extractor.extract_context_for_element(
                            el
                        )
                        texts = " ".join(
                            [(c.text or "").lower() for c in (contexts or [])]
                        )
                        ctx_hit = any(t in texts for t in [s.lower() for s in tokens_ctx])
                    except Exception:
                        ctx_hit = False
                if not (attr_hit or ctx_hit):
                    continue
                selector = await self._generate_playwright_selector(el)
                required = await self.element_scorer._detect_required_status(el)
                field_mapping["部署名"] = {
                    "element": el,
                    "selector": selector,
                    "tag_name": "input",
                    "type": "text",
                    "name": info.get("name", ""),
                    "id": info.get("id", ""),
                    "input_type": "text",
                    "default_value": "",
                    "required": required,
                    "visible": info.get("visible", True),
                    "enabled": info.get("enabled", True),
                    "score": 0,
                }
                handled["部署名"] = field_mapping["部署名"]
                break
            except Exception as e:
                logger.debug(f"department promotion failed: {e}")
        return handled

    async def _auto_handle_checkboxes(
        self, checkboxes: List[Locator], mapped_element_ids: set
    ) -> Dict[str, Dict[str, Any]]:
        handled: Dict[str, Dict[str, Any]] = {}
        try:
            groups: Dict[str, List[Tuple[Locator, Dict[str, Any]]]] = {}
            for cb in checkboxes:
                if id(cb) in mapped_element_ids:
                    continue
                info = await self.element_scorer._get_element_info(cb)
                if not info.get("visible", True):
                    continue
                key = info.get("name") or info.get("id") or f"cb_{id(cb)}"
                groups.setdefault(key, []).append((cb, info))

            # 汎用優先語（従来+法人）
            pri1 = ["営業", "提案", "メール", "法人"]
            pri2 = ["その他"]

            for group_key, items in groups.items():
                group_required = False
                for cb, info in items:
                    if await self.element_scorer._detect_required_status(cb):
                        group_required = True
                        break
                    name_id_class = " ".join(
                        [
                            info.get("name", ""),
                            info.get("id", ""),
                            info.get("class", ""),
                        ]
                    ).lower()
                    if any(
                        k in name_id_class
                        for k in [
                            "acceptance",
                            "consent",
                            "同意",
                            "policy",
                            "privacy",
                            "個人情報",
                            "personal",
                        ]
                    ):
                        group_required = True
                        break
                # コンテナ側の必須マーカー検出（DT/DD, TH/TD, 見出しなど）
                if not group_required:
                    try:
                        group_required = (
                            await self._detect_group_required_via_container(items[0][0])
                        )
                    except Exception as e:
                        logger.debug(
                            f"Container required detection error for checkbox group '{group_key}': {e}"
                        )
                # 追加: 『連絡方法/ご希望の連絡』系の選好チェックボックスは、
                # 必須でなくても汎用的に1つ選択しておく（迷惑の少ない『メール』優先）。
                # 汎用改善: サイト特化ではなく、語彙で判定。
                is_contact_method_group = False
                if not group_required:
                    try:
                        tokens = [
                            "連絡方法",
                            "ご希望連絡",
                            "希望連絡",
                            "連絡手段",
                            "contact method",
                            "preferred contact",
                        ]
                        # group_key だけでなくコンテキストも確認
                        blob_key = (group_key or "").lower()
                        if any(t in blob_key for t in ["連絡", "contact"]):
                            is_contact_method_group = True
                        else:
                            for cb, _info in items:
                                contexts = await self.context_text_extractor.extract_context_for_element(cb)
                                best = (
                                    self.context_text_extractor.get_best_context_text(contexts) or ""
                                ).lower()
                                if any(tok in best for tok in [t.lower() for t in tokens]):
                                    is_contact_method_group = True
                                    break
                    except Exception:
                        is_contact_method_group = False
                # 追加: プライバシー/規約同意の文脈検出（name/id/classに現れないケースの補完）
                is_privacy_group = False
                if not group_required:
                    try:
                        privacy_tokens_primary = [
                            "プライバシー",
                            "プライバシーポリシー",
                            "個人情報",
                            "個人情報保護",
                            "個人情報の取り扱い",
                            "privacy",
                            "privacy policy",
                            "個人情報の取扱い",
                            "個人情報保護方針",
                            "利用規約",
                            "terms",
                            "terms of service",
                        ]
                        agree_tokens = [
                            "同意",
                            "承諾",
                            "同意する",
                            "agree",
                            "確認の上",
                            "に同意",
                        ]

                        lower_priv = [t.lower() for t in privacy_tokens_primary]
                        lower_agree = [t.lower() for t in agree_tokens]

                        for cb, info in items:
                            # 1) 既存の軽量コンテキスト抽出で判定
                            contexts = await self.context_text_extractor.extract_context_for_element(cb)
                            texts = []
                            try:
                                texts = [
                                    c.text for c in (contexts or []) if getattr(c, "text", None)
                                ]
                            except Exception:
                                texts = []
                            best = (
                                self.context_text_extractor.get_best_context_text(contexts) or ""
                            )
                            blob = (" ".join(texts + [best])).lower()
                            if any(tok in blob for tok in lower_priv) and (
                                any(tok in blob for tok in lower_agree) or len(items) == 1
                            ):
                                is_privacy_group = True
                                break

                            # 2) フォールバック: PrivacyConsentHandler のラベル探索＋周辺テキスト抽出を利用
                            try:
                                form_scope = cb.locator("xpath=ancestor::form[1]")
                                scope = (
                                    form_scope if await form_scope.count() else self.page.locator("body")
                                )
                                label = await PrivacyConsentHandler._find_label_for_checkbox(scope, cb)
                                rich_text = await PrivacyConsentHandler._collect_context_text(cb, label)
                                rich = (rich_text or "").lower()
                                if any(tok in rich for tok in lower_priv) and (
                                    any(tok in rich for tok in lower_agree) or len(items) == 1
                                ):
                                    is_privacy_group = True
                                    break
                            except Exception:
                                # 失敗しても継続
                                pass
                    except Exception:
                        is_privacy_group = False

                if not group_required and not is_privacy_group and not is_contact_method_group:
                    continue

                texts: List[str] = []
                for cb, info in items:
                    contexts = (
                        await self.context_text_extractor.extract_context_for_element(
                            cb
                        )
                    )
                    best = (
                        self.context_text_extractor.get_best_context_text(contexts)
                        if contexts
                        else ""
                    )
                    val = info.get("value", "")
                    texts.append(
                        (
                            best or val or info.get("name", "") or info.get("id", "")
                        ).strip()
                    )

                # プライバシー同意系は「同意/agree」を含む選択肢を優先
                if is_privacy_group:
                    idx = 0
                    for i, t in enumerate(texts):
                        tl = (t or "").lower()
                        if any(k in tl for k in ["同意", "agree", "承諾"]):
                            idx = i
                            break
                # 連絡方法グループは『メール優先』（英語/多言語に対応）
                elif is_contact_method_group:
                    idx = self._choose_contact_method_index(texts)
                else:
                    # 第3段階の優先度: 「問い合わせ/問合」を追加
                    idx = self._choose_priority_index(texts, pri1, pri2, ["問い合わせ", "問合"])
                cb, info = items[idx]
                selector = await self._generate_playwright_selector(cb)
                field_name = f"auto_checkbox_{group_key}"
                handled[field_name] = {
                    "element": cb,
                    "selector": selector,
                    "tag_name": "input",
                    "type": "checkbox",
                    "name": info.get("name", ""),
                    "id": info.get("id", ""),
                    "input_type": "checkbox",
                    "auto_action": "check",
                    "selected_index": idx,
                    "selected_option_text": texts[idx],
                    "default_value": True,
                    "required": True,
                    "auto_handled": True,
                }
        except Exception as e:
            logger.debug(f"Error in checkbox auto handling: {e}")
        return handled

    async def _auto_handle_radios(
        self,
        radios: List[Locator],
        mapped_element_ids: set,
        client_data: Optional[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        async def _extract_radio_option_text(radio: Locator) -> str:
            """ラジオ選択肢のラベル抽出（ブロック要素も許容＋フォールバック）。

            優先順:
            1) label[for]
            2) 祖先<label>
            3) 近傍兄弟（テキスト/インライン/小ブロック: span,i,b,strong,em,small,label,div,p,li,dd）
            4) 取れない場合は Python 側でコンテキスト抽出
            """
            try:
                text = await radio.evaluate(
                    """
                    (el) => {
                      const norm = (s) => (s || '').replace(/\s+/g, ' ').trim();
                      const getText = (n) => norm(n && (n.innerText || n.textContent || ''));
                      const isInline = (n) => {
                        const tag = (n.tagName || '').toLowerCase();
                        return ['span','i','b','strong','em','small','label'].includes(tag);
                      };
                      const isSmallBlock = (n) => {
                        const tag = (n.tagName || '').toLowerCase();
                        return ['div','p','li','dd'].includes(tag);
                      };
                      const looksLikeHeading = (t) => {
                        if (!t) return false;
                        const s = t.toLowerCase();
                        if (s.length > 40) return true;
                        if (/[?？]/.test(s)) return true;
                        const stop = ['必須','※','お問い合わせ','問合せ','お問合せ','内容','項目','カテゴリー','カテゴリ','category','subject','type','contact','gender','性別'];
                        return stop.some(k => s.includes(k));
                      };
                      // 1) label[for]
                      const id = el.getAttribute('id');
                      if (id) {
                        const lbl = document.querySelector(`label[for="${id}"]`);
                        if (lbl) {
                          const t = getText(lbl);
                          if (t && !looksLikeHeading(t)) return t;
                        }
                      }
                      // 2) 祖先<label>
                      let p = el; let depth = 0;
                      while (p && depth < 3) {
                        if ((p.tagName || '').toLowerCase() === 'label') {
                          const t = getText(p);
                          if (t && !looksLikeHeading(t)) return t;
                          break;
                        }
                        p = p.parentElement; depth++;
                      }
                      // 3) 近傍兄弟（インライン優先、ついで小ブロック）。
                      const pickFromSiblings = (dir = 'next') => {
                        let sib = dir === 'next' ? el.nextSibling : el.previousSibling;
                        let hops = 0;
                        while (sib && hops < 3) {
                          const isText = sib.nodeType === 3;
                          const isElem = sib.nodeType === 1;
                          if (isText || isElem) {
                            let candidate = '';
                            if (isText) candidate = norm(sib.textContent);
                            else if (isInline(sib) || isSmallBlock(sib)) candidate = getText(sib);
                            if (candidate && !looksLikeHeading(candidate)) return candidate;
                          }
                          sib = dir === 'next' ? sib.nextSibling : sib.previousSibling;
                          hops++;
                        }
                        return '';
                      };
                      const n1 = pickFromSiblings('next');
                      if (n1) return n1;
                      const n2 = pickFromSiblings('prev');
                      if (n2) return n2;
                      return '';
                    }
                    """
                )
                label_text = str(text or "").strip()
            except Exception:
                label_text = ""

            # Python側フォールバック: コンテキスト抽出（質問見出しは除外）
            if not label_text:
                try:
                    contexts = (
                        await self.context_text_extractor.extract_context_for_element(
                            radio
                        )
                    )
                    best = (
                        self.context_text_extractor.get_best_context_text(contexts)
                        if contexts
                        else ""
                    )
                    t = (best or "").strip()
                    t_lower = t.lower()
                    if (
                        t
                        and len(t) <= 40
                        and ("必須" not in t)
                        and ("※" not in t)
                        and not ("?" in t or "？" in t)
                    ):
                        stop = [
                            "お問い合わせ",
                            "問合せ",
                            "お問合せ",
                            "内容",
                            "項目",
                            "category",
                            "subject",
                            "type",
                            "contact",
                            "gender",
                            "性別",
                        ]
                        if not any(s in t_lower for s in stop):
                            label_text = t
                except Exception:
                    label_text = ""

            return label_text

        handled: Dict[str, Dict[str, Any]] = {}
        radio_groups = {}
        for radio in radios:
            if id(radio) in mapped_element_ids:
                continue
            try:
                element_info = await self.element_scorer._get_element_info(radio)
                if not element_info.get("visible", True):
                    continue
                name = element_info.get("name", f"unnamed_radio_{id(radio)}")
                if name not in radio_groups:
                    radio_groups[name] = []
                radio_groups[name].append((radio, element_info))
            except Exception as e:
                logger.debug(f"Error grouping radio: {e}")

        pri1 = ["営業", "提案", "メール", "法人"]
        # 既定の優先語（従来どおり）
        pri2 = ["その他", "other", "該当なし"]

        # クライアント性別の正規化（male/female/other）
        def _normalize_gender(val: str) -> Optional[str]:
            if not val:
                return None
            v = val.strip().lower()
            male_tokens = ["男性", "だんせい", "男", "male", "man"]
            female_tokens = ["女性", "じょせい", "女", "female", "woman"]
            other_tokens = [
                "その他",
                "未回答",
                "無回答",
                "回答しない",
                "other",
                "prefer not",
            ]
            if any(t in v for t in male_tokens):
                return "male"
            if any(t in v for t in female_tokens):
                return "female"
            if any(t in v for t in other_tokens):
                return "other"
            return None

        client_info = (
            client_data.get("client")
            if isinstance(client_data, dict) and "client" in client_data
            else client_data
        ) or {}
        client_gender_norm = _normalize_gender(str(client_info.get("gender", "") or ""))
        for group_name, radio_list in radio_groups.items():
            if not radio_list:
                continue
            is_gender_field = any(
                keyword in group_name.lower()
                for keyword in ["性別", "gender", "sex", "男女"]
            )
            group_required = is_gender_field
            if not group_required:
                for radio, _ in radio_list:
                    if await self.element_scorer._detect_required_status(radio):
                        group_required = True
                        break

            # 追加の汎用判定: コンテキスト上に必須マーカーが存在するか（閾値なしで広く検出）
            # 例: 「お問い合わせ項目 (必須)」のようにグループ見出し側にのみ付くケース
            if not group_required:
                try:
                    group_required = await self._detect_group_required_via_container(
                        radio_list[0][0]
                    )
                except Exception as e:
                    logger.debug(
                        f"Container required detection error for group '{group_name}': {e}"
                    )
            # 任意グループでも送信成功率向上のため一つ選択（『その他』優先）

            texts: List[str] = []
            for radio, info in radio_list:
                # ラジオ選択肢そのもののテキストを優先的に取得
                # （質問見出しなどのグループラベルは使用しない）
                label_text = await _extract_radio_option_text(radio)
                if not label_text:
                    # ラベルが取れない場合は value/name/id をフォールバック
                    fallback = (
                        info.get("value", "")
                        or info.get("name", "")
                        or info.get("id", "")
                    )
                    label_text = str(fallback or "").strip()
                texts.append(label_text)

            # クライアント性別が判明している場合は最優先で一致候補を選択
            idx = None
            if client_gender_norm and is_gender_field:

                def _option_gender(text: str) -> Optional[str]:
                    tl = (text or "").lower()
                    if any(k in tl for k in ["男", "男性", "male"]):
                        return "male"
                    if any(k in tl for k in ["女", "女性", "female"]):
                        return "female"
                    if any(k in tl for k in ["その他", "other"]):
                        return "other"
                    return None

                for i, t in enumerate(texts):
                    if _option_gender(t) == client_gender_norm:
                        idx = i
                        break

            # 会社/個人の汎用判定: ラジオに『法人』『個人』がある場合、会社名の有無で選択
            if idx is None:
                try:
                    has_corporate = any(("法人" in (t or "")) for t in texts)
                    has_personal = any(("個人" in (t or "")) for t in texts)
                    company_name = str(
                        client_info.get("company_name", "") or ""
                    ).strip()
                    if has_corporate and has_personal and company_name:
                        # 『法人』を選択
                        idx = next(
                            (i for i, t in enumerate(texts) if "法人" in (t or "")),
                            None,
                        )
                except Exception:
                    pass

            # フォールバック: 既存優先度ロジック（『その他』を除外して選択）
            if idx is None:
                idx = (
                    self._choose_gender_index(texts)
                    if is_gender_field
                    else self._choose_priority_index(texts, pri1, pri2, ["問い合わせ", "問合"])
                )

            # 『その他』回避フォールバックは撤廃（従来の選択ロジックへ復帰）

            radio, element_info = radio_list[idx]
            selector = await self._generate_playwright_selector(radio)
            field_name = f"auto_radio_{group_name}"
            handled[field_name] = {
                "element": radio,
                "selector": selector,
                "tag_name": "input",
                "type": "radio",
                "name": element_info.get("name", ""),
                "id": element_info.get("id", ""),
                "input_type": "radio",
                "auto_action": "select",
                "selected_index": idx,
                "selected_option_text": texts[idx],
                "default_value": True,
                "required": bool(group_required),
                "auto_handled": True,
                "group_size": len(radio_list),
            }
        return handled

    async def _auto_handle_selects(
        self,
        selects: List[Locator],
        mapped_element_ids: set,
        client_data: Optional[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        handled: Dict[str, Dict[str, Any]] = {}
        pri1 = ["営業", "提案", "メール", "法人"]
        pri2 = ["その他"]
        client_info = (
            client_data.get("client")
            if isinstance(client_data, dict) and "client" in client_data
            else client_data
        ) or {}
        prefecture_target = (client_info.get("address_1") or "").strip()
        client_gender_norm = None

        # 同じ正規化ヘルパー
        def _normalize_gender(val: str) -> Optional[str]:
            if not val:
                return None
            v = val.strip().lower()
            male_tokens = ["男性", "だんせい", "男", "male", "man"]
            female_tokens = ["女性", "じょせい", "女", "female", "woman"]
            other_tokens = [
                "その他",
                "未回答",
                "無回答",
                "回答しない",
                "other",
                "prefer not",
            ]
            if any(t in v for t in male_tokens):
                return "male"
            if any(t in v for t in female_tokens):
                return "female"
            if any(t in v for t in other_tokens):
                return "other"
            return None

        client_gender_norm = _normalize_gender(str(client_info.get("gender", "") or ""))

        for i, select in enumerate(selects):
            if id(select) in mapped_element_ids:
                continue
            try:
                element_info = await self.element_scorer._get_element_info(select)
                if not element_info.get("visible", True):
                    continue

                # 必須判定（フォールバック付き）
                required = await self.element_scorer._detect_required_status(select)
                if not required:
                    # DL構造: <dd> の直前 <dt> に必須マーカーがあるか簡易チェック（JS側も try-catch）
                    try:
                        required = await select.evaluate(
                            """
                            (el, MARKERS) => {
                              try {
                                if (!el || !el.tagName) return false;
                                let p = el;
                                while (p && (p.tagName||'').toLowerCase() !== 'dd') p = p.parentElement;
                                if (!p) return false;
                                let dt = p.previousElementSibling;
                                while (dt && (dt.tagName||'').toLowerCase() !== 'dt') dt = dt.previousElementSibling;
                                if (!dt) return false;
                                const t = (dt.innerText || dt.textContent || '').trim();
                                if (!t) return false;
                                return MARKERS.some(m => t.includes(m));
                              } catch { return false; }
                            }
                            """,
                            list(self.REQUIRED_MARKERS),
                        )
                    except PlaywrightTimeoutError as e:
                        logger.debug(
                            f"Timeout during DT/DD required detection for select: {e}"
                        )
                        required = False
                    except Exception as e:
                        logger.debug(
                            f"Unexpected error in DT/DD required detection for select: {e}"
                        )
                        required = False

                # 追加の汎用フォールバック: デフォルトがダミーの場合は実質必須とみなして選択する
                # 例: 「--- 選択してください ---」や空値がデフォルトのままの場合
                opt_data = await select.evaluate(
                    "el => Array.from(el.options).map(o => ({text: (o.textContent || '').trim(), value: o.value || ''}))"
                )
                if len(opt_data) < 2:
                    continue

                # 既定選択の保持（デフォルト優先）
                try:
                    pre_idx = await select.evaluate("el => el.selectedIndex")
                except Exception:
                    pre_idx = -1
                texts = [d.get("text", "") for d in opt_data]
                values = [d.get("value", "") for d in opt_data]

                # ダミー/プレースホルダー除外トークン（設定優先、未定義や空ならデフォルトにフォールバック）
                DEFAULT_EXCLUDE_TOKENS = [
                    "選択", "選択してください", "ご選択", "お選び", "お選びください", "選んで", "選んでください",
                    "choose", "please choose", "select", "please select", "未選択", "---", "—", "–",
                ]
                exclude_tokens = []
                try:
                    choice_cfg = get_choice_priority_config() or {}
                    raw = (choice_cfg.get("select", {}).get("exclude_keywords", []) or [])
                    exclude_tokens = [str(x).lower() for x in raw if str(x).strip()]
                except Exception:
                    exclude_tokens = []
                if not exclude_tokens:
                    exclude_tokens = [s.lower() for s in DEFAULT_EXCLUDE_TOKENS]

                def _is_dummy_option(idx: int) -> bool:
                    try:
                        t = (texts[idx] or "").strip().lower()
                        v = (values[idx] or "").strip()
                        if v == "":
                            return True
                        if any(tok in t for tok in exclude_tokens):
                            return True
                        if any(tok in v.lower() for tok in ["select", "choose", "---", "none"]):
                            return True
                    except Exception:
                        return False
                    return False

                # 問い合わせ種別系セレクト（ご用件/お問い合わせ内容/目的/purpose/inquiry/category/subject）は
                # 非必須でも安全側で選択（既定が特定カテゴリだと誤った送信内容になるため）。
                try:
                    name_id_cls = " ".join(
                        [
                            (element_info.get("name", "") or ""),
                            (element_info.get("id", "") or ""),
                            (element_info.get("class", "") or ""),
                        ]
                    ).lower()
                    contexts = await self.context_text_extractor.extract_context_for_element(select)
                    best_ctx = (self.context_text_extractor.get_best_context_text(contexts) or "").lower()
                except Exception:
                    name_id_cls = ""
                    best_ctx = ""
                inquiry_tokens_attr = [
                    "purpose",
                    "inquiry",
                    "category",
                    "subject",
                    "topic",
                ]
                inquiry_tokens_ctx = [
                    "お問い合わせ内容",
                    "ご用件",
                    "お問い合わせ種別",
                    "種別",
                    "お問い合わせ",
                ]
                is_inquiry_type = any(t in name_id_cls for t in inquiry_tokens_attr) or any(
                    t in best_ctx for t in inquiry_tokens_ctx
                )
                if is_inquiry_type:
                    required = True
                if not required:
                    try:
                        pre_text = (
                            (texts[pre_idx] or "").strip()
                            if isinstance(pre_idx, int) and pre_idx >= 0
                            else ""
                        )
                        pre_val = (
                            (values[pre_idx] or "").strip()
                            if isinstance(pre_idx, int) and pre_idx >= 0
                            else ""
                        )
                        is_dummy_default = (
                            (not pre_text and not pre_val)
                            or any(tok in pre_text.lower() for tok in exclude_tokens)
                            or pre_val == ""
                        )
                        if is_dummy_default:
                            required = True  # 実質必須として自動選択を適用
                    except Exception:
                        pass
                if not required:
                    continue
                is_pref_select = any("東京都" in tx for tx in texts) and any(
                    "大阪府" in tx for tx in texts
                )
                is_gender_select = any(
                    any(k in (tx or "") for k in ["男", "男性", "male"]) for tx in texts
                ) and any(
                    any(k in (tx or "") for k in ["女", "女性", "female"])
                    for tx in texts
                )
                idx = None
                # 既定値が有効（先頭ダミーでない、かつ値が空でない）ならそれを優先採用
                if isinstance(pre_idx, int) and 0 <= pre_idx < len(values):
                    pre_text = (texts[pre_idx] or "").strip()
                    pre_val = (values[pre_idx] or "").strip()
                    is_dummy = (any(tok in pre_text.lower() for tok in exclude_tokens) or pre_val == "")
                    if not is_dummy:
                        idx = pre_idx
                if is_gender_select and client_gender_norm:
                    # クライアント性別に一致する選択肢を優先
                    targets = {
                        "male": ["男", "男性", "male"],
                        "female": ["女", "女性", "female"],
                        "other": ["その他", "other"],
                    }.get(client_gender_norm, [])
                    for cand_text in texts:
                        pass
                    cand = [
                        k
                        for k, tx in enumerate(texts)
                        if any(
                            t in (tx or "").lower()
                            for t in [s.lower() for s in targets]
                        )
                    ]
                    if cand:
                        idx = cand[0]
                elif is_pref_select:
                    if prefecture_target:
                        cand = [
                            k for k, tx in enumerate(texts) if prefecture_target in tx
                        ]
                        if cand:
                            idx = cand[-1]
                    if idx is None:
                        for fallback in ["東京都", "大阪府"]:
                            cand = [k for k, tx in enumerate(texts) if fallback in tx]
                            if cand:
                                idx = cand[-1]
                                break
                elif is_inquiry_type:
                    # 問い合わせ種別は『その他』を最優先、次に『問い合わせ/問合』を優先
                    pri_other = ["その他", "other"]
                    cand_other = [
                        k for k, tx in enumerate(texts)
                        if any(p.lower() in (tx or "").lower() for p in pri_other)
                    ]
                    if cand_other:
                        idx = cand_other[0]
                    else:
                        pri_inquiry = ["問い合わせ", "問合"]
                        cand_inq = [
                            k for k, tx in enumerate(texts)
                            if (not _is_dummy_option(k)) and any(p.lower() in (tx or "").lower() for p in pri_inquiry)
                        ]
                        if cand_inq:
                            idx = cand_inq[0]
                if idx is None:
                    # 第3段階の優先度: 「問い合わせ/問合」を追加（ダミー/空値は除外）
                    idx = self._choose_priority_index(
                        texts, pri1, pri2, ["問い合わせ", "問合"], exclude_tokens, values
                    )

                selector = await self._generate_playwright_selector(select)
                field_name = f"auto_select_{i+1}"
                handled[field_name] = {
                    "element": select,
                    "selector": selector,
                    "tag_name": "select",
                    "type": "",
                    "name": element_info.get("name", ""),
                    "id": element_info.get("id", ""),
                    "input_type": "select",
                    "auto_action": "select_index",
                    "selected_index": idx,
                    "selected_option_text": texts[idx],
                    "default_value": values[idx] or texts[idx],
                    "required": True,
                    "auto_handled": True,
                    "options_count": len(opt_data),
                }
            except Exception as e:
                logger.debug(f"Error auto-handling select {i}: {e}")
        return handled

    def _choose_priority_index(
        self,
        texts: List[str],
        pri1: List[str],
        pri2: List[str],
        pri3: Optional[List[str]] = None,
        exclude_text_tokens: Optional[List[str]] = None,
        values: Optional[List[str]] = None,
    ) -> int:
        def is_excluded(i: int) -> bool:
            try:
                if values is not None:
                    v = (values[i] or "").strip()
                    if v == "":
                        return True
                    if any(tok in v.lower() for tok in ["select", "choose", "---", "none"]):
                        return True
                if exclude_text_tokens:
                    tl = (texts[i] or "").lower()
                    if any(tok in tl for tok in exclude_text_tokens):
                        return True
            except Exception:
                return False
            return False

        def last_match(keys: List[str]) -> Optional[int]:
            idxs: List[int] = []
            low_keys = [k.lower() for k in (keys or [])]
            for i, t in enumerate(texts):
                tl = (t or "").lower()
                if any(k in tl for k in low_keys):
                    if not is_excluded(i):
                        idxs.append(i)
            return idxs[-1] if idxs else None

        idx = last_match(pri1)
        if idx is not None:
            return idx
        idx = last_match(pri2)
        if idx is not None:
            return idx
        if pri3:
            idx = last_match(pri3)
            if idx is not None:
                return idx
        # 最後のフォールバック: 除外されていない最後の選択肢
        for i in range(len(texts) - 1, -1, -1):
            if not is_excluded(i):
                return i
        return max(0, len(texts) - 1)

    def _choose_contact_method_index(self, texts: List[str]) -> int:
        """連絡方法チェックボックスの優先選択（メール優先）。

        優先順位: Email > Any/Either/No preference > Phone > Fax（最後の手段）
        - 大文字小文字・全角半角を吸収して判定
        """
        email_tokens = [
            "email", "e-mail", "mail", "メール", "eメール", "電子メール", "mail address", "email address"
        ]
        any_tokens = [
            "any", "either", "no preference", "どちらでも", "問いません", "どれでも"
        ]
        phone_tokens = [
            "phone", "tel", "telephone", "call", "携帯", "モバイル", "電話"
        ]
        fax_tokens = ["fax", "ファックス", "ファクス"]

        def find_first(keys: List[str]) -> Optional[int]:
            lk = [k.lower() for k in keys]
            for i, t in enumerate(texts):
                tl = (t or "").lower()
                if any(k in tl for k in lk):
                    return i
            return None

        for keys in (email_tokens, any_tokens, phone_tokens, fax_tokens):
            idx = find_first(keys)
            if idx is not None:
                return idx
        return 0

    def _choose_gender_index(self, texts: List[str]) -> int:
        male_keywords = ["男", "男性", "male", "man", "Men", "Male"]
        for i, text in enumerate(texts):
            if text and any(keyword in text for keyword in male_keywords):
                return i
        return 0

    async def _auto_handle_email_confirmation(
        self,
        candidates: List[Locator],
        mapped_element_ids: set,
        field_mapping: Dict[str, Dict[str, Any]],
        form_structure: Optional[FormStructure] = None,
    ) -> Dict[str, Dict[str, Any]]:
        handled: Dict[str, Dict[str, Any]] = {}
        if "メールアドレス" not in field_mapping:
            return handled
        confirmation_attr_patterns = [
            "email_confirm",
            "mail_confirm",
            "email_confirmation",
            "confirm_email",
            "confirm_mail",
            "mail2",
            "mail_2",
            "email2",
            "email_2",
            "confirm-mail",
            "email-confirm",
            "from2",
            "email_check",
            "mail_check",
            "re_email",
            "re_mail",
        ]
        confirmation_ctx_tokens = ["確認", "確認用", "再入力", "再度", "もう一度"]
        blacklist = [
            "captcha",
            "image_auth",
            "spam-block",
            "token",
            "otp",
            "verification",
        ]

        # 既に確定している主メール欄（論理フィールド『メールアドレス』）の name/id を取得して、
        # そのバリアント（例: "_<name>", "<name>2"）を確認欄として扱う汎用ヒューリスティクスを追加。
        primary_name = ""
        primary_id = ""
        try:
            primary_info = field_mapping.get("メールアドレス", {}) or {}
            primary_name = (primary_info.get("name") or "").strip().lower()
            primary_id = (primary_info.get("id") or "").strip().lower()
        except Exception:
            primary_name = ""
            primary_id = ""
        for i, el in enumerate(candidates):
            if id(el) in mapped_element_ids:
                continue
            info = await self.element_scorer._get_element_info(el)
            if not info.get("visible", True):
                continue
            name_raw = (info.get("name", "") or "").strip()
            id_raw = (info.get("id", "") or "").strip()
            placeholder_raw = (info.get("placeholder", "") or "").strip()
            name_id_class = " ".join(
                [name_raw, id_raw, info.get("class", ""), placeholder_raw]
            ).lower()
            if any(b in name_id_class for b in blacklist):
                continue
            attr_hit = any(p in name_id_class for p in confirmation_attr_patterns)
            # プレースホルダに『確認』『再入力』等が含まれる場合も確認欄とみなす
            if not attr_hit and placeholder_raw:
                low_pl = placeholder_raw.lower()
                if any(t.lower() in low_pl for t in confirmation_ctx_tokens):
                    attr_hit = True

            # バリアント規則:
            # - 主メール name/id に対して、"_<name>" or "<name>2" や "<id>2" を確認欄とみなす
            #   （例: F2M_FROM → _F2M_FROM, email → email2）
            try:
                nm = (name_raw or "").lower()
                ide = (id_raw or "").lower()
                if primary_name:
                    if (
                        nm == f"_{primary_name}"
                        or nm == f"{primary_name}2"
                        or nm == f"{primary_name}_confirm"
                    ):
                        attr_hit = True
                if not attr_hit and primary_id:
                    if (
                        ide == f"_{primary_id}"
                        or ide == f"{primary_id}2"
                        or ide == f"{primary_id}_confirm"
                    ):
                        attr_hit = True
            except Exception:
                pass
            ctx_hit = False
            if not attr_hit:
                try:
                    contexts = (
                        await self.context_text_extractor.extract_context_for_element(
                            el
                        )
                    )
                    best = (
                        self.context_text_extractor.get_best_context_text(contexts)
                        or ""
                    ).lower()
                    ctx_hit = any(
                        tok in best
                        for tok in [t.lower() for t in confirmation_ctx_tokens]
                    )
                except Exception:
                    ctx_hit = False
            if not (attr_hit or ctx_hit):
                continue
            selector = await self._generate_playwright_selector(el)
            required = await self.element_scorer._detect_required_status(el)
            # コンテキスト/属性で確認欄と判断できた場合は、実入力上の必須が検出できなくても
            # 実用上の必須に準じて扱う（未入力だと送信拒否されるサイトが多いため）。
            required = bool(required or ctx_hit or attr_hit)
            field_name = f"auto_email_confirm_{i+1}"
            handled[field_name] = {
                "element": el,
                "selector": selector,
                "tag_name": info.get("tag_name", "input") or "input",
                "type": info.get("type", "email") or "email",
                "name": info.get("name", ""),
                "id": info.get("id", ""),
                "input_type": "email",
                "auto_action": "copy_from",
                "copy_from_field": "メールアドレス",
                "default_value": "",
                "required": required,
                "auto_handled": True,
            }

        # フォールバック: 上記ロジックで検出できなかった場合、
        # 『メール』系の文脈/属性を持つ入力が2つ以上存在し、
        # そのうち1つが既に『メールアドレス』として確定しているとき、
        # もう一方を確認欄としてコピー入力対象にする（Google Forms 等の匿名構造対策）。
        try:
            if not handled:
                primary_sel = (field_mapping.get("メールアドレス", {}) or {}).get("selector", "")
                email_like: list[tuple] = []  # (el, info, best_ctx, sel)
                for el in candidates:
                    if id(el) in mapped_element_ids:
                        continue
                    info = await self.element_scorer._get_element_info(el)
                    if not info.get("visible", True):
                        continue
                    etype = (info.get("type", "") or "").lower()
                    # 型で明確にメールでないものは除外
                    if etype in ("number", "tel", "url", "password", "date", "time"):
                        continue
                    nm = (info.get("name", "") or "").lower()
                    ide = (info.get("id", "") or "").lower()
                    cls = (info.get("class", "") or "").lower()
                    ph = (info.get("placeholder", "") or "").lower()
                    attrs_blob = " ".join([nm, ide, cls, ph])
                    contexts = await self.context_text_extractor.extract_context_for_element(el)
                    best = (self.context_text_extractor.get_best_context_text(contexts) or "").lower()
                    email_tokens = ["email", "e-mail", "mail", "メール"]
                    # 1) メール系の指標
                    email_hit = (etype == "email") or any(t in attrs_blob for t in email_tokens) or any(
                        t in best for t in email_tokens
                    )
                    if not email_hit:
                        continue
                    # 2) 確認系の指標（属性 or コンテキスト or 主メール名/IDのバリアント）
                    confirm_attr = any(p in attrs_blob for p in confirmation_attr_patterns)
                    confirm_ctx = any(t.lower() in best for t in [c.lower() for c in confirmation_ctx_tokens])
                    variant_hit = False
                    try:
                        if primary_name:
                            if nm in {f"_{primary_name}", f"{primary_name}2", f"{primary_name}_confirm"}:
                                variant_hit = True
                        if primary_id and not variant_hit:
                            if ide in {f"_{primary_id}", f"{primary_id}2", f"{primary_id}_confirm"}:
                                variant_hit = True
                    except Exception:
                        variant_hit = False
                    if not (confirm_attr or confirm_ctx or variant_hit):
                        continue
                    sel = await self._generate_playwright_selector(el)
                    # 既に主メールと同一セレクタは除外
                    if primary_sel and sel == primary_sel:
                        continue
                    email_like.append((el, info, best, sel))
                if email_like:
                    # 最初の適合候補のみ採用
                    el, info, best, sel = email_like[0]
                    required = await self.element_scorer._detect_required_status(el)
                    handled["auto_email_confirm_1"] = {
                        "element": el,
                        "selector": sel,
                        "tag_name": info.get("tag_name", "input") or "input",
                        "type": info.get("type", "email") or "email",
                        "name": info.get("name", ""),
                        "id": info.get("id", ""),
                        "input_type": "email",
                        "auto_action": "copy_from",
                        "copy_from_field": "メールアドレス",
                        "default_value": "",
                        "required": bool(required or best),
                        "auto_handled": True,
                    }
        except Exception:
            # フォールバックでの例外は抑制（他ロジックに影響させない）
            pass

        # さらに失敗した場合の最終フォールバック（構造順ベース）
        try:
            if not handled and form_structure and getattr(form_structure, "elements", None):
                primary_sel = (field_mapping.get("メールアドレス", {}) or {}).get("selector", "")
                # フォーム内論理順の一覧を作成
                seq = [(fe.selector, fe) for fe in (form_structure.elements or []) if getattr(fe, 'selector', '')]
                idx = next((i for i,(sel,fe) in enumerate(seq) if sel == primary_sel), -1)
                if idx >= 0:
                    for j in range(idx+1, min(idx+6, len(seq))):
                        sel, fe = seq[j]
                        if fe.tag_name != 'input':
                            continue
                        if fe.element_type in ('checkbox','radio','number','tel','url','password'):
                            continue
                        if any((isinstance(v,dict) and v.get('selector')==sel) for v in field_mapping.values()):
                            continue
                        # メール指標 + 確認指標の双方が必要
                        attrs_blob = " ".join([
                            (fe.name or '').lower(),
                            (fe.id or '').lower(),
                            (fe.class_name or '').lower(),
                            (fe.placeholder or '').lower(),
                            (fe.label_text or '').lower(),
                            (fe.associated_text or '').lower(),
                        ])
                        email_tokens = ["email","e-mail","mail","メール"]
                        email_hit = (fe.element_type or '').lower() == 'email' or any(t in attrs_blob for t in email_tokens)
                        confirm_hit = any(t in attrs_blob for t in [c.lower() for c in [
                            "確認","確認用","再入力","もう一度","confirm","confirmation","re_email","re_mail","email2","mail2"
                        ]])
                        if not (email_hit and confirm_hit):
                            continue
                        handled['auto_email_confirm_structural'] = {
                            'element': fe.locator,
                            'selector': sel,
                            'tag_name': 'input',
                            'type': fe.element_type or 'text',
                            'name': fe.name or '',
                            'id': fe.id or '',
                            'input_type': 'email',
                            'auto_action': 'copy_from',
                            'copy_from_field': 'メールアドレス',
                            'default_value': '',
                            'required': True,
                            'auto_handled': True,
                        }
                        break
        except Exception:
            pass

        return handled

        # Unreachable


    async def promote_email_confirmation_to_mapping(
        self,
        auto_handled: Dict[str, Dict[str, Any]],
        field_mapping: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        """auto_email_confirm_* を field_mapping に昇格（確認欄が必須のサイト対策）。

        - auto_handled で検出済みの確認用メール欄を、重複しない範囲で field_mapping に移す。
        - 値の投入は assigner 側の copy_from ロジックに委譲する。
        """
        promoted_keys: List[str] = []
        try:
            for k, v in list(auto_handled.items()):
                if not k.startswith("auto_email_confirm_"):
                    continue
                if not isinstance(v, dict):
                    continue
                # 既に同一セレクタが field_mapping に存在する場合は昇格不要
                sel = v.get("selector")
                if any((isinstance(fv, dict) and fv.get("selector") == sel) for fv in field_mapping.values()):
                    continue
                field_mapping[k] = v
                promoted_keys.append(k)
        except Exception:
            return promoted_keys
        return promoted_keys

    async def _auto_handle_split_name_arrays(
        self, text_inputs: List[Locator], mapped_element_ids: set
    ) -> Dict[str, Dict[str, Any]]:
        """name[0]/name[1], kana[0]/kana[1] のような配列入力を汎用対応。

        ルール:
        - name[0] -> 姓, name[1] -> 名
        - kana[0] -> 姓カナ, kana[1] -> 名カナ
        - 既に姓/名（またはカナ）がマッピング済みの場合はスキップ
        - 要素が不可視/同一要素に既に割当済みの場合はスキップ
        """
        handled: Dict[str, Dict[str, Any]] = {}
        try:
            pairs = {
                "name": [("姓", 0), ("名", 1)],
                "kana": [("姓カナ", 0), ("名カナ", 1)],
            }
            buckets = {k: {} for k in pairs.keys()}  # base -> {index: (locator, info)}
            order_buckets = {
                k: [] for k in pairs.keys()
            }  # base -> [(locator, info)] (順序用)
            for el in text_inputs:
                if id(el) in mapped_element_ids:
                    continue
                try:
                    info = await self.element_scorer._get_element_info(el)
                except Exception:
                    continue
                if not info.get("visible", True):
                    continue
                nm = (info.get("name", "") or "").lower()
                for base in pairs.keys():
                    if nm.startswith(base + "[") and nm.endswith("]"):
                        try:
                            idx = int(nm[len(base) + 1 : -1])
                        except Exception:
                            continue
                        if idx in (0, 1):
                            buckets[base][idx] = (el, info)
                    elif nm == base + "[]":
                        # インデックス無しの配列は出現順で割当
                        order_buckets[base].append((el, info))
            for base, mapping in buckets.items():
                if 0 in mapping and 1 in mapping:
                    for field_name, idx in pairs[base]:
                        if field_name in handled:
                            continue
                        if field_name in getattr(self, "field_mapping", {}):
                            # UnmappedElementHandler では self.field_mapping 参照不可のため抑制
                            pass
                        el, info = mapping[idx]
                        selector = await self._generate_playwright_selector(el)
                        required = await self.element_scorer._detect_required_status(el)
                        handled[field_name] = {
                            "element": el,
                            "selector": selector,
                            "tag_name": info.get("tag_name", "input") or "input",
                            "type": info.get("type", "text") or "text",
                            "name": info.get("name", ""),
                            "id": info.get("id", ""),
                            "input_type": "text",
                            "default_value": "",
                            "required": required,
                            "visible": info.get("visible", True),
                            "enabled": info.get("enabled", True),
                            "auto_handled": True,
                        }
            # 順序割当（name[] / kana[]）
            for base, items in order_buckets.items():
                if len(items) >= 2:
                    for (field_name, idx), (el, info) in zip(pairs[base], items[:2]):
                        selector = await self._generate_playwright_selector(el)
                        required = await self.element_scorer._detect_required_status(el)
                        handled[field_name] = {
                            "element": el,
                            "selector": selector,
                            "tag_name": info.get("tag_name", "input") or "input",
                            "type": info.get("type", "text") or "text",
                            "name": info.get("name", ""),
                            "id": info.get("id", ""),
                            "input_type": "text",
                            "default_value": "",
                            "required": required,
                            "visible": info.get("visible", True),
                            "enabled": info.get("enabled", True),
                            "auto_handled": True,
                        }
        except Exception as e:
            logger.debug(f"split name arrays handler error: {e}")
        return handled

    async def _auto_handle_family_given_names(
        self, text_inputs: List[Locator], mapped_element_ids: set
    ) -> Dict[str, Dict[str, Any]]:
        handled: Dict[str, Dict[str, Any]] = {}
        try:
            cand = []
            for el in text_inputs:
                if id(el) in mapped_element_ids:
                    continue
                info = await self.element_scorer._get_element_info(el)
                if not info.get("visible", True):
                    continue
                nm = (info.get("name", "") or "").lower()
                idv = (info.get("id", "") or "").lower()
                blob = nm + " " + idv
                kind = None
                if any(
                    t in blob
                    for t in [
                        "family_name",
                        "family-name",
                        "lastname",
                        "last_name",
                        "surname",
                        "family",
                    ]
                ):
                    kind = "姓"
                elif any(
                    t in blob
                    for t in [
                        "given_name",
                        "given-name",
                        "firstname",
                        "first_name",
                        "given",
                    ]
                ):
                    kind = "名"
                if kind:
                    cand.append((kind, el, info))
            for kind, el, info in cand:
                if kind in handled:
                    continue
                selector = await self._generate_playwright_selector(el)
                required = await self.element_scorer._detect_required_status(el)
                handled[kind] = {
                    "element": el,
                    "selector": selector,
                    "tag_name": info.get("tag_name", "input") or "input",
                    "type": info.get("type", "text") or "text",
                    "name": info.get("name", ""),
                    "id": info.get("id", ""),
                    "input_type": "text",
                    "default_value": "",
                    "required": required,
                    "visible": info.get("visible", True),
                    "enabled": info.get("enabled", True),
                    "auto_handled": True,
                }
        except Exception as e:
            logger.debug(f"family/given name handler error: {e}")
        return handled

    async def _auto_handle_unified_fullname(
        self,
        text_inputs: List[Locator],
        mapped_element_ids: set,
        client_data: Optional[Dict[str, Any]],
        form_structure: Optional[FormStructure],
    ) -> Dict[str, Dict[str, Any]]:
        handled = {}
        unified_patterns = set(self.field_patterns.get_unified_name_patterns())
        if form_structure and form_structure.elements:
            for i, fe in enumerate(form_structure.elements):
                if id(fe.locator) in mapped_element_ids:
                    continue
                label_text = (fe.label_text or "").lower()
                placeholder_text = (fe.placeholder or "").lower()
                if any(p in label_text for p in unified_patterns) or any(
                    p in placeholder_text for p in unified_patterns
                ):
                    info = await self.element_scorer._get_element_info(fe.locator)
                    if not info.get("visible", True):
                        continue
                    # 追加ガード: email/確認系には統合氏名を適用しない
                    typ = (info.get("type", "") or "").lower()
                    nm = (info.get("name", "") or "").lower()
                    cls = (info.get("class", "") or "").lower()
                    if (
                        (typ == "email")
                        or ("mail" in nm)
                        or ("email" in nm)
                        or ("mail" in cls)
                        or ("email" in cls)
                    ):
                        continue
                    selector = await self._generate_playwright_selector(fe.locator)
                    required = await self.element_scorer._detect_required_status(
                        fe.locator
                    )
                    fullname = self.field_combination_manager.generate_combined_value(
                        "full_name", client_data or {}
                    )
                    if not fullname:
                        continue
                    field_name = f"auto_fullname_label_{i+1}"
                    handled[field_name] = {
                        "element": fe.locator,
                        "selector": selector,
                        "tag_name": info.get("tag_name", "input"),
                        "type": info.get("type", "text") or "text",
                        "name": info.get("name", ""),
                        "id": info.get("id", ""),
                        "input_type": "text",
                        "auto_action": "fill",
                        "default_value": fullname,
                        "required": required,
                        "auto_handled": True,
                    }
        return handled

    async def _auto_handle_unified_kana(
        self,
        text_inputs: List[Locator],
        mapped_element_ids: set,
        client_data: Optional[Dict[str, Any]],
        form_structure: Optional[FormStructure],
    ) -> Dict[str, Dict[str, Any]]:
        handled: Dict[str, Dict[str, Any]] = {}
        try:
            for i, el in enumerate(text_inputs):
                if id(el) in mapped_element_ids:
                    continue
                info = await self.element_scorer._get_element_info(el)
                if not info.get("visible", True):
                    continue
                name_id_cls = " ".join(
                    [info.get("name", ""), info.get("id", ""), info.get("class", "")]
                ).lower()
                # 候補判定: name/id/classに kana/furigana/カナ 等、またはラベルに「フリガナ」
                contexts = (
                    await self.context_text_extractor.extract_context_for_element(el)
                )
                best = (
                    self.context_text_extractor.get_best_context_text(contexts) or ""
                ).lower()
                is_kana_like = (
                    any(k in name_id_cls for k in ["kana", "furigana", "katakana"])
                    or ("フリガナ" in best)
                    or ("ふりがな" in best)
                )
                if not is_kana_like:
                    continue
                # CAPTCHAや認証は除外
                if any(
                    b in name_id_cls for b in ["captcha", "image_auth", "spam-block"]
                ):
                    continue

                # kana/hiragana の種別推定（簡易）
                kana_type = (
                    "hiragana"
                    if any(
                        h in (best + " " + name_id_cls)
                        for h in ["ひらがな", "hiragana"]
                    )
                    else "katakana"
                )
                value = self.field_combination_manager.generate_unified_kana_value(
                    kana_type, client_data or {}
                )
                if not value:
                    continue

                selector = await self._generate_playwright_selector(el)
                required = await self.element_scorer._detect_required_status(el)
                if not required:
                    # コンテキスト内の必須マーカー（* や 必須）が近傍に存在する場合は必須扱い
                    try:
                        texts = [
                            c.text for c in (contexts or []) if getattr(c, "text", None)
                        ]
                        blob_txt = " ".join(texts).strip()
                        if any(m in blob_txt for m in self.REQUIRED_MARKERS) or (
                            "*" in blob_txt
                        ):
                            required = True
                    except Exception:
                        pass
                field_name = f"auto_unified_kana_{i+1}"
                handled[field_name] = {
                    "element": el,
                    "selector": selector,
                    "tag_name": info.get("tag_name", "input"),
                    "type": info.get("type", "text") or "text",
                    "name": info.get("name", ""),
                    "id": info.get("id", ""),
                    "input_type": "text",
                    "auto_action": "fill",
                    "default_value": value,
                    "required": required,
                    "auto_handled": True,
                }
        except Exception as e:
            logger.debug(f"Auto handle unified kana failed: {e}")
        return handled

    async def _auto_handle_fax(
        self,
        text_inputs: List[Locator],
        mapped_element_ids: set,
        client_data: Optional[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """FAX番号入力欄を任意で自動入力（必須ではない場合のみ）。

        - name/id/class/ラベルに fax/ファックス/FAX を含む要素を検出
        - 必須判定が False の場合に限り、電話番号を代用して入力
        """
        handled: Dict[str, Dict[str, Any]] = {}
        # 設定で明示的に有効化された場合のみ実行（デフォルト無効）
        if not bool(self.settings.get("enable_optional_fax_fill", False)):
            return handled
        try:
            client = (
                client_data.get("client", {}) if isinstance(client_data, dict) else {}
            )
            phone = "".join(
                [
                    client.get("phone_1", ""),
                    client.get("phone_2", ""),
                    client.get("phone_3", ""),
                ]
            ).strip()
            if not phone:
                return handled
            for i, el in enumerate(text_inputs):
                if id(el) in mapped_element_ids:
                    continue
                info = await self.element_scorer._get_element_info(el)
                if not info.get("visible", True):
                    continue
                name_id_cls = " ".join(
                    [info.get("name", ""), info.get("id", ""), info.get("class", "")]
                ).lower()
                contexts = (
                    await self.context_text_extractor.extract_context_for_element(el)
                )
                best = (
                    self.context_text_extractor.get_best_context_text(contexts) or ""
                ).lower()
                if not (
                    ("fax" in name_id_cls) or ("ファックス" in best) or ("fax" in best)
                ):
                    continue
                # 必須でない場合のみ自動入力
                if await self.element_scorer._detect_required_status(el):
                    continue
                selector = await self._generate_playwright_selector(el)
                handled[f"auto_fax_{i+1}"] = {
                    "element": el,
                    "selector": selector,
                    "tag_name": info.get("tag_name", "input"),
                    "type": info.get("type", "text") or "text",
                    "name": info.get("name", ""),
                    "id": info.get("id", ""),
                    "input_type": "text",
                    "auto_action": "fill",
                    "default_value": phone,
                    "required": False,
                    "auto_handled": True,
                }
        except Exception as e:
            logger.debug(f"Auto handle fax failed: {e}")
        return handled

    async def _auto_handle_split_kana(
        self,
        text_inputs: List[Locator],
        mapped_element_ids: set,
        client_data: Optional[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """セイ/メイの分割カナ入力欄を自動入力（安全・汎用）。

        判定:
        - name/id/class に kana/furigana/katakana が含まれる、またはラベルに「カナ/フリガナ/ふりがな」
        - かつ『セイ/姓/SEI』『メイ/名/MEI』の指標で last/first を分類
        """
        handled: Dict[str, Dict[str, Any]] = {}
        try:
            last_el, first_el = None, None
            for el in text_inputs:
                if id(el) in mapped_element_ids:
                    continue
                info = await self.element_scorer._get_element_info(el)
                if not info.get("visible", True):
                    continue
                name_id_cls = " ".join(
                    [info.get("name", ""), info.get("id", ""), info.get("class", "")]
                ).lower()
                contexts = (
                    await self.context_text_extractor.extract_context_for_element(el)
                )
                best = (
                    self.context_text_extractor.get_best_context_text(contexts) or ""
                ).lower()
                kana_like = (
                    ("kana" in name_id_cls)
                    or ("furigana" in name_id_cls)
                    or ("katakana" in name_id_cls)
                    or ("カナ" in best)
                    or ("フリガナ" in best)
                    or ("ふりがな" in best)
                )
                if not kana_like:
                    continue
                blob = best + " " + name_id_cls
                if any(t in blob for t in ["sei", "姓", "セイ"]):
                    last_el = last_el or el
                if any(t in blob for t in ["mei", "名", "メイ"]):
                    first_el = first_el or el
            if not (last_el and first_el):
                return handled

            client = (
                client_data.get("client", {}) if isinstance(client_data, dict) else {}
            )
            last_kana = client.get("last_name_kana", "")
            first_kana = client.get("first_name_kana", "")
            if last_el and last_kana:
                selector = await self._generate_playwright_selector(last_el)
                handled["auto_split_kana_last"] = {
                    "element": last_el,
                    "selector": selector,
                    "tag_name": "input",
                    "type": "text",
                    "input_type": "text",
                    "auto_action": "fill",
                    "default_value": last_kana,
                    "required": await self.element_scorer._detect_required_status(
                        last_el
                    ),
                }
            if first_el and first_kana:
                selector = await self._generate_playwright_selector(first_el)
                handled["auto_split_kana_first"] = {
                    "element": first_el,
                    "selector": selector,
                    "tag_name": "input",
                    "type": "text",
                    "input_type": "text",
                    "auto_action": "fill",
                    "default_value": first_kana,
                    "required": await self.element_scorer._detect_required_status(
                        first_el
                    ),
                }
        except Exception as e:
            logger.debug(f"Auto handle split kana failed: {e}")
        return handled

    async def promote_required_fullname_to_mapping(
        self,
        auto_handled: Dict[str, Dict[str, Any]],
        field_mapping: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        promoted_keys: List[str] = []
        if "統合氏名" in field_mapping:
            return promoted_keys
        candidates = [
            (k, v)
            for k, v in (auto_handled or {}).items()
            if k.startswith("auto_fullname") and v.get("required")
        ]
        if not candidates:
            return promoted_keys
        key, info = candidates[0]
        el = info.get("element")
        if not el:
            return promoted_keys
        try:
            element_info = await self._get_element_details(el)
            element_info["score"] = 100
            field_mapping["統合氏名"] = element_info
            promoted_keys.append(key)
        except Exception as e:
            logger.debug(f"Promote required fullname failed: {e}")
        return promoted_keys

    async def promote_required_kana_to_mapping(
        self,
        auto_handled: Dict[str, Dict[str, Any]],
        field_mapping: Dict[str, Dict[str, Any]],
    ) -> List[str]:
        """必須の統合カナ(auto_unified_kana_*)を field_mapping に昇格。

        - 既に『統合氏名カナ』/『姓カナ』『名カナ』が存在する場合は何もしない
        - auto_handled に required=True の auto_unified_kana_* があれば、
          『統合氏名カナ』として field_mapping に追加
        """
        promoted: List[str] = []
        if ("統合氏名カナ" in field_mapping) or (
            ("姓カナ" in field_mapping) and ("名カナ" in field_mapping)
        ):
            return promoted
        for k, v in auto_handled.items():
            if not k.startswith("auto_unified_kana_"):
                continue
            if not v.get("required", False):
                continue
            # フィールド名を昇格
            try:
                el = v.get("element")
                # 可能なら要素詳細を取得してプレースホルダー/属性を含める
                element_info = (
                    await self._get_element_details(el)
                    if el
                    else {
                        **{
                            kk: vv
                            for kk, vv in v.items()
                            if kk not in ["auto_action", "default_value"]
                        }
                    }
                )
                # 入力値はここでは設定せず（assignerが安全に決定）
                element_info.update(
                    {
                        "score": element_info.get("score", 0) or 100,
                    }
                )
                field_mapping["統合氏名カナ"] = {
                    **{
                        kk: vv
                        for kk, vv in element_info.items()
                        if kk not in ["auto_action", "default_value"]
                    },
                    "input_type": "text",
                    # 実際の必須性を要素から再判定して反映（誤必須を防止）
                    "required": (await self.element_scorer._detect_required_status(el))
                    if el
                    else bool(element_info.get("required")),
                    "source": "promoted",
                }
                # 呼び出し側で auto_handled から除去できるよう、昇格元キー（auto_*）を返す
                promoted.append(k)
                break
            except Exception:
                continue
        return promoted
