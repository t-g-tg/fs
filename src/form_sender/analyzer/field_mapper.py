import logging
from typing import Dict, List, Any, Optional, Callable, Awaitable, Tuple
from playwright.async_api import Page, Locator

from .element_scorer import ElementScorer
from .context_text_extractor import ContextTextExtractor
from .field_patterns import FieldPatterns
from .duplicate_prevention import DuplicatePreventionManager
from config.manager import get_prefectures
from .mapping_safeguards import passes_safeguard
from .required_rescue import RequiredRescue
from .candidate_filters import allow_candidate

logger = logging.getLogger(__name__)


class FieldMapper:
    """フィールドのマッピング処理を担当するクラス"""

    # 非必須だが高信頼であれば入力価値が高い項目（汎用・安全）
    OPTIONAL_HIGH_PRIORITY_FIELDS = {
        "件名",
        "電話番号",
        "住所",
        "郵便番号",
        "郵便番号1",
        "郵便番号2",
        "会社名",
        "都道府県",
        "会社名カナ",
    }

    def __init__(
        self,
        page: Page,
        element_scorer: ElementScorer,
        context_text_extractor: ContextTextExtractor,
        field_patterns: FieldPatterns,
        duplicate_prevention: DuplicatePreventionManager,
        settings: Dict[str, Any],
        create_enhanced_element_info_func: Callable[..., Awaitable[Dict[str, Any]]],
        generate_temp_value_func: Callable[..., str],
        field_combination_manager,
    ):
        self.page = page
        self.element_scorer = element_scorer
        self.context_text_extractor = context_text_extractor
        self.field_patterns = field_patterns
        self.duplicate_prevention = duplicate_prevention
        self.settings = settings
        self._create_enhanced_element_info = create_enhanced_element_info_func
        self._generate_temp_field_value = generate_temp_value_func
        self.field_combination_manager = field_combination_manager
        self.unified_field_info: Dict[str, Any] = {}
        self.form_type_info: Dict[str, Any] = {}

        # P1: 必須救済ハンドラの初期化
        # _ensure_required_mappings から呼び出されるため、ここで生成しておく。
        # 依存する関数参照（_create_enhanced_element_info / _generate_temp_field_value /
        # _is_confirmation_field / _infer_logical_field_name_for_required）
        # はインスタンスメソッドとして既存実装済み。
        self._required_rescue = RequiredRescue(
            element_scorer=self.element_scorer,
            context_text_extractor=self.context_text_extractor,
            field_patterns=self.field_patterns,
            settings=self.settings,
            duplicate_prevention=self.duplicate_prevention,
            create_enhanced_element_info=self._create_enhanced_element_info,
            generate_temp_field_value=self._generate_temp_field_value,
            is_confirmation_field_func=self._is_confirmation_field,
            infer_logical_name_func=self._infer_logical_field_name_for_required,
        )

    async def execute_enhanced_field_mapping(
        self,
        classified_elements: Dict[str, List[Locator]],
        unified_field_info: Dict[str, Any],
        form_type_info: Dict[str, Any],
        element_bounds_cache: Dict[str, Any],
        required_analysis: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        self.unified_field_info = unified_field_info
        self.form_type_info = form_type_info
        self._element_bounds_cache = element_bounds_cache
        field_mapping: Dict[str, Any] = {}
        used_elements: set[int] = set()
        essential_fields_completed: set[str] = set()
        sorted_patterns = self.field_patterns.get_sorted_patterns_by_weight()

        required_elements_set = set(
            (required_analysis or {}).get("required_elements", [])
        )

        for field_name, field_patterns in sorted_patterns:
            if self.field_combination_manager.is_deprecated_field(field_name):
                continue
            if self._should_skip_field_for_unified(
                field_name
            ) or self._should_skip_field_for_form_type(field_name):
                continue

            # 追加: カナ分割（セイ/メイ）が存在する場合は『統合氏名カナ』を先行スキップ
            try:
                if field_name == "統合氏名カナ" and await self._has_kana_split_candidates(
                    classified_elements
                ):
                    try:
                        logger.info(
                            "Skip '統合氏名カナ' due to detected split candidates in DOM"
                        )
                    except Exception as e:
                        logger.debug(f"Skip unified kana log failed: {e}")
                    continue
            except Exception:
                pass

            target_element_types = self._determine_target_element_types(field_patterns)
            # メールは一部サイトで type="mail" 等の独自型を用いるため other_inputs も候補に含める
            if field_name == "メールアドレス" and "other_inputs" not in target_element_types:
                target_element_types.append("other_inputs")

            # 汎用改善: 『お問い合わせ本文』は textarea が存在する場合は textarea のみを候補に限定
            # 背景: 一部フォームで context が強い input[type="text"] が誤って本文に選ばれるケースがあるため、
            #       まず textarea を厳格に優先し、textarea が無い場合のみ input を検討する。
            try:
                if field_name == "お問い合わせ本文":
                    textareas = classified_elements.get("textareas", []) or []
                    if len(textareas) > 0:
                        target_element_types = ["textareas"]
            except Exception:
                pass

            best_element, best_score, best_score_details, best_context = (
                await self._find_best_element(
                    field_name,
                    field_patterns,
                    classified_elements,
                    target_element_types,
                    used_elements,
                    essential_fields_completed,
                    required_elements_set,
                )
            )

            # コア項目判定に基づくマッピング決定
            is_core_field = self._is_core_field(field_name)
            base_threshold = self.settings["min_score_threshold"]
            # 非コア項目は品質優先の動的閾値を使用して誤検出を抑制
            dynamic_threshold = self._get_dynamic_quality_threshold(
                field_name, essential_fields_completed
            )

            # 必須フィールドが検出されなかった場合の救済方針
            # fmi一般則: 「必須項目が0のときは原則必須として扱う（FAXは除外）」に整合
            treat_all_as_required = required_analysis and required_analysis.get(
                "treat_all_as_required", False
            )
            # required_analysisで検出された必須要素（name/id一致）に紐づく場合は必ずマッピング対象
            is_required_match = False
            if best_score_details:
                ei = best_score_details.get("element_info", {})
                cand_name = (ei.get("name") or "").strip()
                cand_id = (ei.get("id") or "").strip()
                if (
                    cand_name in required_elements_set
                    or cand_id in required_elements_set
                ):
                    is_required_match = True

            # treat_all_as_required の扱い:
            #  - これまでは essential_fields のみに限定していたが、
            #    一般則に従い『会社名』も必須相当として扱う（FAXは除外）
            #  - サイト特化ではなく汎用の精度向上。スコア/セーフガードは従来通り適用される。
            should_map_field = (
                is_core_field
                or is_required_match
                or (
                    bool(treat_all_as_required)
                    and (
                        (field_name in self.settings.get("essential_fields", []))
                        or (field_name == "会社名")
                    )
                )
            )

            # 高信頼かつ汎用的に安全な任意項目（例: 件名・電話番号）は、
            # 必須一致でなくても動的しきい値を満たせば採用を許可する。
            # 背景: 多くの日本語フォームでは件名/電話は任意だが、入力しても副作用が少なく、
            # 自動送信の成功率向上に寄与するため。
            # 注意: フォーム特化の条件は追加しない（汎用改善のみ）。
            # オプション: 任意高優先度項目のベース閾値許容を追跡（map_ok でも利用）
            allow_optional_base = False
            if (not should_map_field) and best_element:
                if field_name in self.OPTIONAL_HIGH_PRIORITY_FIELDS:
                    # 高優先度任意項目は動的閾値が高すぎて取りこぼすことがあるため、
                    # ベース閾値以上かつ文脈が十分であれば採用を許可（汎用安全）
                    if best_score >= dynamic_threshold:
                        should_map_field = True
                    else:
                        base_threshold = self.settings.get("min_score_threshold", 70)
                        if best_score >= base_threshold:
                            should_map_field = True
                            allow_optional_base = True

            # マッピング判定ロジック（精度向上版）
            map_ok = False
            if best_element and should_map_field:
                if is_core_field:
                    # コア項目のうち『お問い合わせ本文』は誤検出を防ぐため、
                    # 必須一致だけでは採用せず、最低スコアを満たすか textarea の場合のみ採用
                    if field_name == "お問い合わせ本文":
                        tag_name = (
                            (best_score_details.get("element_info", {}) or {})
                            .get("tag_name", "")
                            .lower()
                        )
                        map_ok = (best_score >= base_threshold) or (
                            tag_name == "textarea"
                        )
                    else:
                        # それ以外のコア項目は「必須一致」または最低閾値クリアで採用。
                        # ただし姓/名など一部フィールドはフィールド別しきい値を厳守して安全側に倒す。
                        per_field_thresholds = (
                            self.settings.get("min_score_threshold_per_field", {}) or {}
                        )
                        required_threshold = per_field_thresholds.get(
                            field_name, base_threshold
                        )
                        # コア項目でも required 一致だけでの採用は安全側に制限。
                        # → 姓/名などは required 一致があっても required_threshold を満たさない場合は採用しない。
                        if field_name in per_field_thresholds:
                            map_ok = best_score >= required_threshold
                        else:
                            map_ok = is_required_match or (
                                best_score >= required_threshold
                            )
                else:
                    # 非コア項目でも『必須一致』の場合は採用（偽陰性防止: fmi一般則に整合）。
                    # さらに、任意高優先度項目でベース閾値許容を与えた場合は base_threshold 基準で採否。
                    if is_required_match:
                        map_ok = True
                    elif allow_optional_base and (field_name in self.OPTIONAL_HIGH_PRIORITY_FIELDS):
                        base_threshold = self.settings.get("min_score_threshold", 70)
                        map_ok = best_score >= base_threshold
                    else:
                        map_ok = best_score >= dynamic_threshold

            # フィールド固有の安全ガード（メール）
            if map_ok and field_name == "メールアドレス":
                try:
                    if not passes_safeguard(
                        field_name,
                        best_score_details,
                        best_context,
                        self.context_text_extractor,
                        field_patterns,
                        self.settings,
                    ):
                        map_ok = False
                except Exception:
                    pass

            # フィールド固有の安全ガード（電話）
            if map_ok and field_name == "電話番号":
                try:
                    if not passes_safeguard(
                        field_name,
                        best_score_details,
                        best_context,
                        self.context_text_extractor,
                        field_patterns,
                        self.settings,
                    ):
                        map_ok = False
                except Exception:
                    pass
            # 分割電話（電話1/2/3）にも電話の安全ガードを適用
            if map_ok and field_name in {"電話1", "電話2", "電話3"}:
                try:
                    if not passes_safeguard(
                        "電話番号",
                        best_score_details,
                        best_context,
                        self.context_text_extractor,
                        field_patterns,
                        self.settings,
                    ):
                        map_ok = False
                except Exception:
                    map_ok = False

            # 郵便番号の安全ガード（CAPTCHA誤検出/汎用テキストへの誤割当て対策）
            if map_ok and field_name == "郵便番号":
                try:
                    if not passes_safeguard(
                        field_name,
                        best_score_details,
                        best_context,
                        self.context_text_extractor,
                        field_patterns,
                        self.settings,
                    ):
                        map_ok = False
                except Exception:
                    map_ok = False
            # 分割郵便番号（郵便番号1/2）にも同等の安全ガードを適用
            if map_ok and field_name in {"郵便番号1", "郵便番号2"}:
                try:
                    if not passes_safeguard(
                        "郵便番号",
                        best_score_details,
                        best_context,
                        self.context_text_extractor,
                        field_patterns,
                        self.settings,
                    ):
                        map_ok = False
                except Exception:
                    map_ok = False

            # 都道府県の安全ガード（共通ユーティリティ）
            if map_ok and field_name == "都道府県":
                try:
                    if not passes_safeguard(
                        field_name,
                        best_score_details,
                        best_context,
                        self.context_text_extractor,
                        field_patterns,
                        self.settings,
                    ):
                        map_ok = False
                except Exception:
                    pass

            # 住所の安全ガード（フリガナ/部署等への誤割当て抑止）
            if map_ok and field_name == "住所":
                try:
                    if not passes_safeguard(
                        field_name,
                        best_score_details,
                        best_context,
                        self.context_text_extractor,
                        field_patterns,
                        self.settings,
                    ):
                        map_ok = False
                except Exception:
                    map_ok = False

            # 汎用のセーフガード呼び出し（上記以外のフィールドは常に True だが、将来拡張に備えて共通化）
            if map_ok and field_name not in {"メールアドレス", "電話番号", "郵便番号", "都道府県"}:
                try:
                    if not passes_safeguard(
                        field_name,
                        best_score_details,
                        best_context,
                        self.context_text_extractor,
                        field_patterns,
                        self.settings,
                    ):
                        map_ok = False
                except Exception:
                    pass

            if map_ok:
                element_info = await self._create_enhanced_element_info(
                    best_element, best_score_details, best_context
                )
                try:
                    element_info["source"] = "normal"
                except Exception:
                    pass
                temp_value = self._generate_temp_field_value(field_name)

                if self.duplicate_prevention.register_field_assignment(
                    field_name, temp_value, best_score, element_info
                ):
                    field_mapping[field_name] = element_info
                    used_elements.add(id(best_element))
                    if field_name in self.settings.get("essential_fields", []):
                        essential_fields_completed.add(field_name)
                    logger.info(f"Mapped '{field_name}' with score {best_score}")

        # 必須保証フェーズ（取りこぼし救済）
        try:
            await self._ensure_required_mappings(
                classified_elements, field_mapping, used_elements, required_elements_set
            )
        except Exception as e:
            logger.debug(f"Ensure required mappings failed: {e}")

        # 電話番号の3分割が検出できる場合は統合より分割を優先
        try:
            await self._promote_phone_triplets(classified_elements, field_mapping)
        except Exception as e:
            logger.debug(f"promote phone triplets skipped: {e}")

        # フォールバック: 重要コア項目の取りこぼし救済
        await self._fallback_map_message_field(
            classified_elements, field_mapping, used_elements
        )
        await self._fallback_map_email_field(
            classified_elements, field_mapping, used_elements
        )
        # 追加救済: name/id が 'email' の入力を確実に採用（確認欄は除外）
        await self._salvage_strict_email_by_attr(
            classified_elements, field_mapping, used_elements
        )
        # 追加救済: 郵便番号の2分割（zip1/zip2 等）を name/id から直接補完
        await self._salvage_postal_split_by_attr(
            classified_elements, field_mapping, used_elements
        )
        # 強化救済: ラベル/見出しテキストにメール語が含まれる input[type=text]
        await self._salvage_email_by_label_context(
            classified_elements, field_mapping, used_elements
        )
        await self._fallback_map_postal_field(
            classified_elements, field_mapping, used_elements
        )
        # 追加救済: 都道府県（p-region/region/prefecture）を name/id/class/placeholder/ラベルから補完
        try:
            await self._salvage_prefecture_by_attr(
                classified_elements, field_mapping, used_elements
            )
        except Exception as e:
            logger.debug(f"prefecture salvage skipped: {e}")
        # 住所の取りこぼし救済（placeholder/ラベル/属性から強い住所シグナルを検出）
        await self._fallback_map_address_field(
            classified_elements, field_mapping, used_elements
        )
        # 追加救済: よくある住所分割 name（city/adrs/room）の強化マッピング
        try:
            await self._force_map_common_address_fields(
                classified_elements, field_mapping, used_elements
            )
        except Exception as e:
            logger.debug(f"force map common address skipped: {e}")
        # 最終救済: 旧式テーブル行ラベルからメール欄を強制採用
        await self._force_map_email_from_table_label(
            classified_elements, field_mapping, used_elements
        )
        # 追加フォールバック: 氏名/件名の取りこぼし救済（安全側の厳格条件）
        await self._fallback_map_fullname_field(
            classified_elements, field_mapping, used_elements
        )
        await self._fallback_map_subject_field(
            classified_elements, field_mapping, used_elements
        )
        # 安全補正: 都道府県が非selectに割当てられていたら、都道府県セレクトへ差し替え
        try:
            await self._remap_prefecture_to_select_if_available(
                classified_elements, field_mapping
            )
        except Exception as e:
            logger.debug(f"prefecture remap skipped: {e}")
        return field_mapping

    async def _salvage_prefecture_by_attr(self, classified_elements, field_mapping, used_elements):
        if "都道府県" in field_mapping:
            return
        # 候補: input/select
        cands = (classified_elements.get("text_inputs") or []) + (
            classified_elements.get("selects") or []
        )
        tokens = ["p-region", "prefecture", "pref", "region", "都道府県"]
        best = None
        for el in cands:
            if id(el) in used_elements:
                continue
            try:
                ei = await self.element_scorer._get_element_info(el)
            except Exception:
                continue
            blob = " ".join(
                [
                    (ei.get("name", "") or ""),
                    (ei.get("id", "") or ""),
                    (ei.get("class", "") or ""),
                    (ei.get("placeholder", "") or ""),
                ]
            ).lower()
            ctxs = await self.context_text_extractor.extract_context_for_element(el)
            ctx_text = " ".join([(getattr(c, "text", "") or "").lower() for c in ctxs or []])
            if any(t in blob for t in tokens) or any(t in ctx_text for t in tokens):
                best = (el, ei, ctxs)
                break
        if not best:
            return
        el, ei, ctxs = best
        details = {"element_info": ei, "total_score": 85}
        info = await self._create_enhanced_element_info(el, details, ctxs)
        info["required"] = True
        info["input_type"] = info.get("input_type") or ("select" if (ei.get("tag_name",""))=="select" else "text")
        info["source"] = "salvage_prefecture"
        tmp = self._generate_temp_field_value("都道府県")
        if self.duplicate_prevention.register_field_assignment("都道府県", tmp, 85, info):
            field_mapping["都道府県"] = info
            used_elements.add(id(el))

    async def _has_kana_split_candidates(self, classified_elements: Dict[str, List[Locator]]) -> bool:
        """DOMに『セイ/メイ』系の分割かな入力が存在するか簡易検出。

        属性(name/id/class/placeholder)に以下の双方が見つかれば True:
        - カナ/ふりがな指標（kana/katakana/furigana/カナ/カタカナ/フリガナ/ひらがな）
        - 『セイ/姓』に相当するトークン、及び『メイ/名』に相当するトークン
        """
        try:
            text_inputs = classified_elements.get("text_inputs", []) or []
            if len(text_inputs) < 2:
                return False
            last_found = False
            first_found = False
            for el in text_inputs[:40]:  # 上限で軽量化
                try:
                    ei = await self.element_scorer._get_element_info_quick(el)
                except Exception:
                    continue
                blob = " ".join(
                    [
                        (ei.get("name") or ""),
                        (ei.get("id") or ""),
                        (ei.get("class") or ""),
                        (ei.get("placeholder") or ""),
                    ]
                )
                if not blob:
                    continue
                # ctx_blob は後段の参照に備えてデフォルト初期化（属性でカナ検出時も未定義にならないように）
                ctx_blob = ""
                has_kana = any(
                    t in blob
                    for t in [
                        "kana",
                        "katakana",
                        "furigana",
                        "カナ",
                        "カタカナ",
                        "フリガナ",
                        "ひらがな",
                    ]
                )
                # ラベル/周辺テキストにも手がかりがあれば許容（属性にカナ語が無いフォーム対策）
                if not has_kana:
                    try:
                        ctxs = await self.context_text_extractor.extract_context_for_element(el)
                    except Exception:
                        ctxs = []
                    ctx_blob = " ".join([(getattr(c, 'text', '') or '') for c in (ctxs or [])])
                    has_kana = any(t in ctx_blob for t in ["フリガナ", "ふりがな", "カナ", "ひらがな"]) or False
                    if not has_kana:
                        continue
                last_found = last_found or any(t in blob for t in ["セイ", "せい", "姓", "sei", "lastname", "family"]) or ("セイ" in ctx_blob or "姓" in ctx_blob)
                first_found = first_found or any(
                    t in blob for t in ["メイ", "めい", "名", "mei", "firstname", "given"]
                ) or ("メイ" in ctx_blob or "名" in ctx_blob)
                if last_found and first_found:
                    return True
            return False
        except Exception:
            return False

    async def _promote_phone_triplets(self, classified_elements, field_mapping):
        """tel1/tel2/tel3 形式の3分割電話欄を検出し、
        統合『電話番号』よりも分割（電話番号1/2/3）のマッピングを優先する（汎用）。
        """
        tel_inputs = (classified_elements.get("tel_inputs") or []) + (
            classified_elements.get("text_inputs") or []
        )
        if len(tel_inputs) < 2:
            return
        import re

        # 方式A: name/id/class に 'tel1/tel2/tel3' 等の明示番号を含むケース
        explicit_candidates = {}
        # 方式B: 配列インデックス（[0]/[1]/[2] など）で分割されているケース（例: telnum[data][0]）
        grouped_by_base: dict[str, dict[int, Any]] = {}

        def _extract_array_index(s: str) -> list[int]:
            return [int(x) for x in re.findall(r"\[(\d+)\]", s)]

        def _base_key(s: str) -> str:
            # 配列インデックスを正規化し、グルーピング用のキーを生成
            # 例: "telnum[data][0]" -> "telnum[data][]"
            return re.sub(r"\[\d+\]", "[]", s)

        for el in tel_inputs:
            try:
                info = await self.element_scorer._get_element_info(el)
                nm = (info.get("name", "") or "").lower()
                ide = (info.get("id", "") or "").lower()
                cls = (info.get("class", "") or "").lower()
                blob = f"{nm} {ide} {cls}"
                if "tel" not in blob and "phone" not in blob:
                    continue

                # A) 明示番号（1/2/3）判定
                m = re.search(r"(?:tel|phone)[^\d]*([123])(?!.*\d)", blob)
                if m:
                    idx = int(m.group(1))
                    if idx in (1, 2, 3):
                        explicit_candidates[idx] = el
                        continue

                # B) 配列インデックス判定（[0]/[1]/[2] → 1/2/3にマップ）
                indexes = _extract_array_index(nm) or _extract_array_index(ide)
                if indexes:
                    # base に tel/phone を含むもののみ対象
                    base_source = nm or ide
                    base = _base_key(base_source)
                    if ("tel" in base) or ("phone" in base):
                        # 最後のインデックスだけを採用（多次元でも末尾が分割番号）
                        idx0 = indexes[-1]
                        if idx0 in (0, 1, 2):
                            mapped = {0: 1, 1: 2, 2: 3}[idx0]
                            grouped_by_base.setdefault(base, {})[mapped] = el
            except Exception:
                continue

        # 明示番号が3つ揃っていれば優先採用
        candidates = None
        if all(k in explicit_candidates for k in (1, 2, 3)):
            candidates = explicit_candidates
        else:
            # 配列インデックスで 0/1/2 が揃っている base グループを探す
            for base, parts in grouped_by_base.items():
                if all(k in parts for k in (1, 2, 3)):
                    candidates = parts
                    break

        if not candidates:
            # 追加フォールバック: 既に選定済みの『電話番号』の name を利用して [0]/[1]/[2] 兄弟を探索
            try:
                current = field_mapping.get("電話番号")
                if current and isinstance(current, dict):
                    ei = current.get("score_details", {}).get("element_info", {})
                    nm = (ei.get("name") or "").lower()
                    if nm and ("tel" in nm or "phone" in nm):
                        # 最後の [index] を見付けて 0/1/2 セットを構築
                        arr = re.findall(r"\[(\d+)\]", nm)
                        if arr:
                            base = re.sub(r"\[\d+\]$", "", nm)  # 末尾の [n] を除去
                            targets = [f"{base}[{i}]" for i in (0, 1, 2)]
                            locators = []
                            for t in targets:
                                try:
                                    loc = self.page.locator(f"input[name='{t}']")
                                    count = await loc.count()
                                    if count > 0:
                                        locators.append(loc.first)
                                except Exception:
                                    locators.append(None)
                            if len(locators) == 3 and all(locators):
                                candidates = {1: locators[0], 2: locators[1], 3: locators[2]}
            except Exception:
                candidates = None
            if not candidates:
                return
        # 統合『電話番号』が候補のいずれかを指していれば降格
        # 統合『電話番号』は分割が確定した時点で削除（重複入力防止）
        field_mapping.pop("電話番号", None)
        # 分割マッピング登録
        # スコア詳細を生成して一貫した構造を保つ
        patterns = self.field_patterns.get_pattern("電話番号") or {}
        names = {1: "電話番号1", 2: "電話番号2", 3: "電話番号3"}
        for idx, fname in names.items():
            el = candidates[idx]
            score, details = await self.element_scorer.calculate_element_score(
                el, patterns, "電話番号"
            )
            info = await self._create_enhanced_element_info(el, details, [])
            try:
                info["source"] = "promote_split"
            except Exception:
                pass
            # ここで各反復毎に保存（regression: ループ外代入で3番のみ残る問題を修正）
            field_mapping[fname] = info

    async def _salvage_postal_split_by_attr(self, classified_elements, field_mapping, used_elements):
        """zip1/zip2 等の明示的な2分割郵便番号を直接補完（汎用救済）。

        - 既に『郵便番号1/2』が存在する場合は何もしない。
        - name/id に zip1/zip2, postal1/postal2, postcode1/postcode2 等が含まれる input を検出。
        - 安全ガード（passes_safeguard('郵便番号')）に合格した場合のみ採用。
        """
        if ("郵便番号1" in field_mapping) or ("郵便番号2" in field_mapping):
            return
        cands = (classified_elements.get("tel_inputs") or []) + (
            classified_elements.get("text_inputs") or []
        )
        part1, part2 = None, None
        for el in cands:
            # 既存マッピングで統合『郵便番号』として利用済みの要素も検査対象に含める
            # （差し替えで split に昇格させるケースを許容）
            try:
                ei = await self.element_scorer._get_element_info(el)
            except Exception:
                ei = {}
            nm = (ei.get("name", "") or "").lower()
            ide = (ei.get("id", "") or "").lower()
            blob = f"{nm} {ide}"
            if any(k in blob for k in ["zip1", "postal1", "postcode1", "zipcode1", "zip_1", "postal_code_1", "postcode_1", "zipcode_1"]):
                part1 = (el, ei)
            if any(k in blob for k in ["zip2", "postal2", "postcode2", "zipcode2", "zip_2", "postal_code_2", "postcode_2", "zipcode_2"]):
                part2 = (el, ei)
        if not (part1 and part2):
            return
        from .mapping_safeguards import passes_safeguard
        for idx, (el, ei) in enumerate([part1, part2], start=1):
            contexts = await self.context_text_extractor.extract_context_for_element(el)
            # 簡易スコアで安全ガード判定
            details = {"element_info": ei, "total_score": 85}
            if not passes_safeguard("郵便番号", details, contexts, self.context_text_extractor, {}, self.settings):
                return
        # 採用
        for idx, (el, ei) in enumerate([part1, part2], start=1):
            contexts = await self.context_text_extractor.extract_context_for_element(el)
            details = {"element_info": ei, "total_score": 85}
            info = await self._create_enhanced_element_info(el, details, contexts)
            try:
                info["source"] = "salvage_postal_split"
            except Exception:
                pass
            field_mapping[f"郵便番号{idx}"] = info
            # 重複割当て防止用に使用済みへ登録
            try:
                used_elements.add(id(el))
            except Exception:
                pass
        # 統合『郵便番号』が存在する場合は重複入力を避けるため削除
        try:
            field_mapping.pop("郵便番号", None)
        except Exception:
            pass

    async def _remap_prefecture_to_select_if_available(
        self, classified_elements, field_mapping
    ) -> None:
        """都道府県のマッピングが input に誤って割当てられた場合、
        近傍の select（都道府県名を十分数含む）へ差し替える汎用安全補正。
        """
        target = "都道府県"
        if target not in field_mapping:
            return
        try:
            cur = field_mapping.get(target) or {}
            tag = (cur.get("tag_name") or "").lower()
        except Exception:
            tag = ""
        if tag == "select":
            return  # 既にselectなら補正不要

        # select候補の中から『都道府県』らしいものを選ぶ
        selects = classified_elements.get("selects", []) or []
        if not selects:
            return
        from config.manager import get_prefectures

        pref_cfg = get_prefectures() or {}
        names = pref_cfg.get("names", []) if isinstance(pref_cfg, dict) else []

        best = (None, -1)  # (locator, hits)
        for sel in selects:
            try:
                options = await sel.evaluate(
                    "el => Array.from(el.options).map(o => (o.textContent||'').trim())"
                )
            except Exception:
                options = []
            if not options:
                continue
            low_opts = [str(o).lower() for o in options]
            hits = sum(1 for n in names if str(n).lower() in low_opts)
            if hits > best[1]:
                best = (sel, hits)
        sel, hits = best
        if not sel or hits < 5:  # 十分な都道府県名を含まない select は採用しない
            return

        # 差し替え
        patterns = self.field_patterns.get_pattern(target) or {}
        score, details = await self.element_scorer.calculate_element_score(sel, patterns, target)
        info = await self._create_enhanced_element_info(sel, details, [])
        try:
            info["source"] = "safety_remap"
        except Exception:
            pass
        field_mapping[target] = info

    async def _find_best_element(
        self,
        field_name,
        field_patterns,
        classified_elements,
        target_element_types,
        used_elements,
        essential_fields_completed,
        required_elements_set: set,
    ) -> Tuple[Optional[Locator], float, Dict, List]:
        best_element, best_score, best_score_details, best_context = None, 0.0, {}, []
        candidate_elements = [
            el
            for el_type in target_element_types
            for el in classified_elements.get(el_type, [])
        ]

        elements_to_evaluate = await self._quick_rank_candidates(
            candidate_elements, field_patterns, field_name, used_elements
        )

        early_stopped = False
        for element in elements_to_evaluate:
            score, score_details, contexts = await self._score_element_in_detail(
                element, field_patterns, field_name
            )
            if score <= 0:
                continue

            active_threshold = self._get_dynamic_quality_threshold(
                field_name, essential_fields_completed
            )
            # 候補が必須に該当する場合、しきい値を最小化（必須は落とさない）
            try:
                ei = score_details.get("element_info", {})
                cand_name = (ei.get("name") or "").strip()
                cand_id = (ei.get("id") or "").strip()
                if (
                    cand_name in required_elements_set
                    or cand_id in required_elements_set
                ):
                    active_threshold = self.settings["min_score_threshold"]
            except Exception:
                pass
            if score > best_score and score >= active_threshold:
                best_element, best_score, best_score_details, best_context = (
                    element,
                    score,
                    score_details,
                    contexts,
                )
                if self._check_early_stop(
                    field_name, score, score_details, contexts, field_patterns
                ):
                    early_stopped = True
                    break
        if early_stopped:
            logger.debug(f"Early stop for '{field_name}'")
        return best_element, best_score, best_score_details, best_context

    async def _quick_rank_candidates(
        self, elements, field_patterns, field_name, used_elements
    ):
        if not self.settings.get("quick_ranking_enabled", True):
            return [el for el in elements if id(el) not in used_elements]

        quick_scored = []
        for el in elements:
            if id(el) in used_elements:
                continue
            try:
                q_score = await self.element_scorer.calculate_element_score_quick(
                    el, field_patterns, field_name
                )
                if q_score > -900:
                    quick_scored.append((q_score, el))
            except Exception:
                continue

        quick_scored.sort(key=lambda x: x[0], reverse=True)
        top_k = (
            self.settings["quick_top_k_essential"]
            if field_name in self.settings["essential_fields"]
            else self.settings["quick_top_k"]
        )
        return [el for _, el in quick_scored[:top_k]]

    async def _score_element_in_detail(self, element, field_patterns, field_name):
        element_bounds = self._element_bounds_cache.get(str(element))
        # 情報付与用にコンテキストは取得するが、採点は ElementScorer に一元化する
        contexts = await self.context_text_extractor.extract_context_for_element(
            element, element_bounds
        )
        score, score_details = await self.element_scorer.calculate_element_score(
            element, field_patterns, field_name
        )
        # 候補除外フィルタ（住所/性別 select の誤検出抑止）
        try:
            ei = score_details.get("element_info", {})
            if not await allow_candidate(field_name, element, ei):
                return 0, {}, []
        except Exception:
            pass
        # 住所×select の誤検出抑止（汎用）
        # 都道府県セレクト以外の select を『住所』と誤認しないように、
        # option に都道府県名が十分数含まれない場合はスコアを無効化する。
        try:
            if field_name == "住所":
                ei = score_details.get("element_info", {})
                tag = (ei.get("tag_name") or "").lower()
                if tag == "select":
                    try:
                        options = await element.evaluate(
                            "el => Array.from(el.options).map(o => (o.textContent||'').trim())"
                        )
                    except Exception:
                        options = []
                    # get_prefectures() は {"names": [...]} 構造のため、実際の名称リストを参照する
                    pref_cfg = get_prefectures() or {}
                    names = pref_cfg.get("names", []) if isinstance(pref_cfg, dict) else []
                    hits = 0
                    if options and names:
                        low_opts = [str(o).lower() for o in options]
                        for n in names:
                            if str(n).lower() in low_opts:
                                hits += 1
                                if hits >= 5:
                                    break
                    # option に都道府県語が少ない場合は、select を住所候補から除外
                    if hits < 5 and not any("都道府県" in (o or "") for o in options):
                        return 0, {}, []
            # 性別×select の誤検出抑止（選択肢に男女表現が無ければ対象外）
            if field_name == "性別":
                ei = score_details.get("element_info", {})
                tag = (ei.get("tag_name") or "").lower()
                if tag == "select":
                    try:
                        opt_texts = await element.evaluate(
                            "el => Array.from(el.options).map(o => (o.textContent||'').trim().toLowerCase())"
                        )
                    except Exception:
                        opt_texts = []
                    male = any(
                        k in (t or "")
                        for t in opt_texts
                        for k in ["男", "男性", "male"]
                    )
                    female = any(
                        k in (t or "")
                        for t in opt_texts
                        for k in ["女", "女性", "female"]
                    )
                    if not (male and female):
                        return 0, {}, []
        except Exception:
            pass
        # 必須マーカーのある要素は優先（汎用安全ボーナス）
        # ただし『タグ一致のみ』の弱い候補には適用しない（誤マッピング抑止）
        try:
            if await self.element_scorer._detect_required_status(element):
                bd = score_details.get("score_breakdown", {})
                # 強いシグナル群（tag を除外）。class は誤検出源になりやすいため除外。
                strong_signals = (
                    int(bd.get("type", 0))
                    + int(bd.get("name", 0))
                    + int(bd.get("id", 0))
                    + int(bd.get("placeholder", 0))
                    + int(bd.get("context", 0))
                )
                # いずれかの強いシグナルが存在する場合のみ必須ブーストを適用
                if strong_signals > 0:
                    boost = int(self.settings.get("required_boost", 40))
                    if field_name == "電話番号":
                        boost = int(self.settings.get("required_phone_boost", 200))
                    score += boost
                    score_details["score_breakdown"]["required_boost"] = boost
        except Exception:
            pass
        if score <= 0:
            return 0, {}, []
        # ここでの追加ボーナスは廃止（重複加点防止）
        return score, score_details, contexts

    def _is_confirmation_field(
        self, element_info: Dict[str, Any], contexts: List
    ) -> bool:
        """属性とコンテキスト（ラベル/見出し）から確認用入力欄を判定"""
        confirm_tokens = [
            t.lower()
            for t in self.settings.get(
                "confirm_tokens",
                [
                    "confirm",
                    "confirmation",
                    "確認",
                    "確認用",
                    "再入力",
                    "もう一度",
                    "再度",
                ],
            )
        ]
        try:
            attrs = " ".join(
                [
                    (element_info.get("name") or ""),
                    (element_info.get("id") or ""),
                    (element_info.get("class") or ""),
                    (element_info.get("placeholder") or ""),
                ]
            ).lower()
        except Exception:
            attrs = ""
        if any(tok in attrs for tok in confirm_tokens):
            return True
        try:
            best_txt = (
                self.context_text_extractor.get_best_context_text(contexts) or ""
            ).lower()
        except Exception:
            best_txt = ""
        return any(tok in best_txt for tok in confirm_tokens)

    def _check_early_stop(
        self, field_name, score, score_details, contexts, field_patterns
    ):
        if not self.settings.get(
            "early_stop_enabled", True
        ) or field_name not in self.settings.get("essential_fields", []):
            return False
        ei = score_details.get("element_info", {})
        tag = (ei.get("tag_name") or "").lower()
        typ = (ei.get("type") or "").lower()
        strong_type = (field_name == "メールアドレス" and typ == "email") or (
            field_name == "お問い合わせ本文" and tag == "textarea"
        )
        strict_patterns = field_patterns.get("strict_patterns", [])
        best_txt = (
            self.context_text_extractor.get_best_context_text(contexts) or ""
        ).lower()
        has_strict = any(sp.lower() in best_txt for sp in strict_patterns)
        return (
            strong_type
            and has_strict
            and score >= self.settings.get("early_stop_score", 95)
        )

    async def _fallback_map_message_field(
        self, classified_elements, field_mapping, used_elements
    ):
        """本文取りこぼし救済

        優先度:
        1) textarea があれば textarea のみを対象に厳格に判定
        2) textarea が無い場合に限り、input[type=text] を強い本文ラベルに基づき限定的に救済
        """
        target_field = "お問い合わせ本文"
        if target_field in field_mapping:
            return

        patterns = self.field_patterns.get_pattern(target_field) or {}
        # 優先度を二層化: 主要語群を強優先、備考/ご要望は二次優先
        primary_tokens = {
            "お問い合わせ",
            "本文",
            "メッセージ",
            "ご質問",
            "お問い合わせ内容",
            "ご相談",
        }
        secondary_tokens = {"備考", "ご要望"}
        # 本文候補から除外すべきコンテキスト（代表: その他ご要望/任意/自由記入）
        exclude_tokens = {"その他", "その他の", "その他ご要望", "任意", "自由記入", "自由入力"}

        # 1) textarea 優先（従来ロジック）
        textarea_candidates = classified_elements.get("textareas", []) or []

        def _ctx_has(tokens: set[str], txt: str) -> bool:
            return any(tok in txt for tok in tokens)

        async def _pick_textarea_by_tokens(tokens: set[str]):
            best_local = (None, 0, None, [])
            for el in textarea_candidates:
                if id(el) in used_elements:
                    continue
                el_bounds = (
                    self._element_bounds_cache.get(str(el))
                    if hasattr(self, "_element_bounds_cache")
                    else None
                )
                contexts = await self.context_text_extractor.extract_context_for_element(
                    el, el_bounds
                )
                best_txt = (
                    self.context_text_extractor.get_best_context_text(contexts) or ""
                ).lower()
                # 除外トークンを含む場合は本文として扱わない
                if _ctx_has(exclude_tokens, best_txt):
                    continue
                if not _ctx_has(tokens, best_txt):
                    continue
                score, details = await self.element_scorer.calculate_element_score(
                    el, patterns, target_field
                )
                if score > best_local[1]:
                    best_local = (el, score, details, contexts)
            return best_local

        # 1-a) 主要語で選定
        best = await _pick_textarea_by_tokens(primary_tokens)
        # 1-b) 見つからない場合のみ、二次語群で選定（除外語が混じるケースは弾く）
        if not best[0]:
            best = await _pick_textarea_by_tokens(secondary_tokens)

        el, score, details, contexts = best
        if el and score >= 60:
            info = await self._create_enhanced_element_info(el, details, contexts)
            try:
                info["source"] = "fallback"
            except Exception:
                pass
            tmp = self._generate_temp_field_value(target_field)
            if self.duplicate_prevention.register_field_assignment(
                target_field, tmp, score, info
            ):
                field_mapping[target_field] = info
                logger.info(
                    f"Fallback mapped '{target_field}' via textarea label-context (score {score})"
                )
            return

        # 2) textarea が無い場合のみ、text input を限定救済
        text_inputs = classified_elements.get("text_inputs", []) or []
        if textarea_candidates or not text_inputs:
            return

        best = (None, 0, None, [])
        for el in text_inputs:
            if id(el) in used_elements:
                continue
            try:
                ei = await self.element_scorer._get_element_info(el)
            except Exception:
                ei = {}
            # name/id/class の属性に本文系語が含まれるか（誤検出抑止の補助）
            blob = " ".join(
                [
                    str(ei.get("name") or ""),
                    str(ei.get("id") or ""),
                    str(ei.get("class") or ""),
                ]
            ).lower()
            attr_hint = any(
                k in blob
                for k in ["message", "inquiry", "comment", "content", "details"]
            )

            el_bounds = (
                self._element_bounds_cache.get(str(el))
                if hasattr(self, "_element_bounds_cache")
                else None
            )
            contexts = await self.context_text_extractor.extract_context_for_element(
                el, el_bounds
            )
            best_txt = (
                self.context_text_extractor.get_best_context_text(contexts) or ""
            ).lower()
            # 除外語が含まれていれば本文対象外
            if _ctx_has(exclude_tokens, best_txt):
                continue

            # 主要語優先で判定、無ければ二次語群
            if not (_ctx_has(primary_tokens, best_txt) or _ctx_has(secondary_tokens, best_txt)):
                continue

            # 文脈の強さ + 属性ヒントの双方がある場合のみ採点・救済対象
            if not attr_hint:
                continue

            s, details = await self.element_scorer.calculate_element_score(
                el, patterns, target_field
            )
            # 安全側の救済閾値（email_fallback と同等レベル以上）
            if s > best[1]:
                best = (el, s, details, contexts)

        el, score, details, contexts = best
        if el and score >= int(self.settings.get("message_fallback_min_score", 65)):
            info = await self._create_enhanced_element_info(el, details, contexts)
            try:
                info["source"] = "fallback"
            except Exception:
                pass
            tmp = self._generate_temp_field_value(target_field)
            if self.duplicate_prevention.register_field_assignment(
                target_field, tmp, score, info
            ):
                field_mapping[target_field] = info
                logger.info(
                    f"Fallback mapped '{target_field}' via text-input label+attr (score {score})"
                )

    async def _fallback_map_email_field(
        self, classified_elements, field_mapping, used_elements
    ):
        """メールアドレスの取りこぼし救済

        - type="email" が存在しない/見つからないフォームで、type="text"のメール欄を
          強いラベルコンテキスト（th/dt/label）に基づいて安全に昇格させる。
        - 確認用（confirm/check）や確認入力欄（placeholderに確認を含む）は除外。
        """
        target_field = "メールアドレス"
        if target_field in field_mapping:
            return

        patterns = self.field_patterns.get_pattern(target_field) or {}
        strict_tokens = {"メールアドレス", "メール", "email", "e-mail"}
        # 設定の確認トークンを利用（マジックワード抑制）
        confirm_tokens = set(
            [t.lower() for t in self.settings.get("confirm_tokens", [])]
            or [
                "confirm",
                "confirmation",
                "確認",
                "確認用",
                "再入力",
                "もう一度",
                "再度",
            ]
        )

        candidates = []
        # 優先: email_inputs、その後 text_inputs/other_inputs（type="mail" 等の独自型を含む）
        for bucket in ["email_inputs", "text_inputs", "other_inputs"]:
            for el in classified_elements.get(bucket, []) or []:
                if id(el) in used_elements:
                    continue
                try:
                    ei = await self.element_scorer._get_element_info(el)
                    # 確認用/チェック用を除外
                    blob = " ".join(
                        [
                            (ei.get("name") or ""),
                            (ei.get("id") or ""),
                            (ei.get("class") or ""),
                            (ei.get("placeholder") or ""),
                        ]
                    ).lower()
                    # 確認用の強いシグナルのみで除外（"check" 単独では除外しない）
                    if any(k in blob for k in confirm_tokens):
                        continue
                    # コンテキストに強いメール語が含まれるか
                    el_bounds = (
                        self._element_bounds_cache.get(str(el))
                        if hasattr(self, "_element_bounds_cache")
                        else None
                    )
                    contexts = (
                        await self.context_text_extractor.extract_context_for_element(
                            el, el_bounds
                        )
                    )
                    # 確認用フィールドは除外
                    if self._is_confirmation_field(ei, contexts):
                        continue
                    best_txt = (
                        self.context_text_extractor.get_best_context_text(contexts)
                        or ""
                    ).lower()
                    # 追加: テーブル行の左セルテキストを直接取得（thがない旧式table対応）
                    dom_label = ""
                    try:
                        dom_label = await el.evaluate(
                            "(el) => {\n"
                            "  const td = el.closest('td');\n"
                            "  if (!td) return '';\n"
                            "  const tr = td.closest('tr');\n"
                            "  if (!tr) return '';\n"
                            "  const cells = Array.from(tr.children);\n"
                            "  const idx = cells.indexOf(td);\n"
                            "  if (idx > 0) {\n"
                            "    const prev = cells[idx-1];\n"
                            "    if (prev && prev.tagName && prev.tagName.toLowerCase()==='td') {\n"
                            "      return (prev.textContent||'').trim();\n"
                            "    }\n"
                            "  } else if (cells.length >= 2) {\n"
                            "    const first = cells[0];\n"
                            "    if (first && first !== td && first.tagName && first.tagName.toLowerCase()==='td') {\n"
                            "      return (first.textContent||'').trim();\n"
                            "    }\n"
                            "  }\n"
                            "  return '';\n"
                            "}"
                        )
                    except Exception:
                        dom_label = ""
                    # ラベルに強い語、または属性ヒント（email/mail/@）のいずれかがあれば候補にする
                    label_blob = (best_txt + " " + (dom_label or "").lower())
                    label_ok = any(
                        tok.lower() in label_blob for tok in strict_tokens
                    )
                    attr_ok = ("email" in blob or "mail" in blob or "@" in blob)
                    if not (label_ok or attr_ok):
                        continue
                    # 確認欄の強い語が dom_label に含まれるケースも除外
                    if any(k in label_blob for k in confirm_tokens):
                        continue
                    # input[type=email] は基本的に候補に含める（上の確認用除外に既に通している）
                    # スコア計算
                    score, details = await self.element_scorer.calculate_element_score(
                        el, patterns, target_field
                    )
                    # ラベル強一致時の底上げ（旧式tableでスコアが出ないケースを救済）
                    if label_ok and score < int(self.settings.get("email_fallback_min_score", 55)):
                        details = details or {}
                        details["total_score"] = int(self.settings.get("email_fallback_min_score", 55))
                        score = int(self.settings.get("email_fallback_min_score", 55))
                    if score <= 0:
                        continue
                    candidates.append((score, el, details, contexts))
                except Exception:
                    continue

        if not candidates:
            return

        candidates.sort(key=lambda x: x[0], reverse=True)
        score, el, details, contexts = candidates[0]
        # 設定化した安全側の閾値（旧式サイト対応でやや緩和）
        if score >= int(self.settings.get("email_fallback_min_score", 55)):
            info = await self._create_enhanced_element_info(el, details, contexts)
            try:
                info["source"] = "fallback"
            except Exception:
                pass
            tmp = self._generate_temp_field_value(target_field)
            if self.duplicate_prevention.register_field_assignment(
                target_field, tmp, score, info
            ):
                field_mapping[target_field] = info
                logger.info(
                    f"Fallback mapped '{target_field}' via label-context (score {score})"
                )

    async def _fallback_map_postal_field(
        self, classified_elements, field_mapping, used_elements
    ):
        """郵便番号の取りこぼし救済

        - ラベル/属性に郵便番号系の強いシグナルがある input[type=text/tel] を安全側に昇格
        - 既に『郵便番号』が確定している場合は何もしない
        """
        target_field = "郵便番号"
        # 既に統合/分割いずれかの郵便番号が確定している場合は重複生成しない
        if target_field in field_mapping or any(
            k.startswith("郵便番号") for k in field_mapping.keys()
        ):
            return
        candidates = (classified_elements.get("tel_inputs") or []) + (
            classified_elements.get("text_inputs") or []
        )
        if not candidates:
            return
        patterns = self.field_patterns.get_pattern(target_field) or {}
        best = (None, 0, None, [])
        for el in candidates:
            if id(el) in used_elements:
                continue
            score, details, contexts = await self._score_element_in_detail(
                el, patterns, target_field
            )
            if score > best[1]:
                best = (el, score, details, contexts)
        el, score, details, contexts = best
        if not el:
            return
        # 郵便番号は attr/id/ラベルでの強い安全判定（_passes_postal）があるため、
        # フォールバック閾値はやや緩め（>=50）に設定して取りこぼしを防ぐ。
        threshold = 50
        # 追加の安全ガード: 郵便番号の文脈/属性検証
        try:
            from .mapping_safeguards import passes_safeguard as _passes

            if not _passes(
                target_field,
                details,
                contexts,
                self.context_text_extractor,
                patterns,
                self.settings,
            ):
                return
        except Exception:
            # ガード判定で例外が起きても安全側に倒す（採用しない）
            return

        if score >= threshold:
            info = await self._create_enhanced_element_info(el, details, contexts)
            try:
                info["source"] = "fallback"
            except Exception:
                pass
            tmp = self._generate_temp_field_value(target_field)
            if self.duplicate_prevention.register_field_assignment(
                target_field, tmp, score, info
            ):
                field_mapping[target_field] = info
                used_elements.add(id(el))
                logger.info(f"Fallback mapped '{target_field}' (score {score})")

    async def _fallback_map_address_field(
        self, classified_elements, field_mapping, used_elements
    ):
        """住所の取りこぼし救済。

        条件:
        - まだ『住所』が未確定
        - input[type=text] のうち、placeholder/属性/ラベルに住所系の強いシグナル
        - スコアが安全閾値以上
        """
        target_field = "住所"
        if target_field in field_mapping:
            return
        text_inputs = classified_elements.get("text_inputs", []) or []
        if not text_inputs:
            return
        patterns = self.field_patterns.get_pattern(target_field) or {}

        addr_attr_tokens = [
            "address",
            "addr",
            "adrs",
            "street",
            "building",
            "pref",
            "prefecture",
            "city",
            "town",
            "room",
            "apt",
            "apartment",
        ]
        addr_ctx_tokens = [
            "住所",
            "所在地",
            "都道府県",
            "prefecture",
            "市区町村",
            "番地",
            "丁目",
            "マンション",
            "ビル",
        ]
        # ひらがな/カナなど人名指標が強いものは除外
        name_like_excl = ["ふりがな", "フリガナ", "カナ", "ひらがな"]

        candidates: list[tuple] = []  # (el, score, details, contexts)
        # 注文番号/各種番号など、住所と無関係なトークンを含む要素は除外
        order_like_tokens = [
            "注文番号", "order number", "受注番号", "予約番号", "伝票番号", "受付番号", "お問い合わせ番号", "tracking number",
        ]
        spam_trap_tokens = ["honeypot", "honey", "trap", "botfield", "no-print", "noprint", "hidden", "hid"]

        for el in text_inputs:
            if id(el) in used_elements:
                continue
            try:
                ei = await self.element_scorer._get_element_info(el)
            except Exception:
                continue
            # 確認/認証/非入力系の除外
            try:
                if await self.element_scorer._is_excluded_element_with_context(
                    ei, el, patterns
                ):
                    continue
            except Exception:
                pass
            blob = " ".join(
                [
                    (ei.get("name") or ""),
                    (ei.get("id") or ""),
                    (ei.get("class") or ""),
                    (ei.get("placeholder") or ""),
                ]
            ).lower()
            if any(tok in blob for tok in name_like_excl):
                continue
            el_bounds = (
                self._element_bounds_cache.get(str(el))
                if hasattr(self, "_element_bounds_cache")
                else None
            )
            contexts = await self.context_text_extractor.extract_context_for_element(
                el, el_bounds
            )
            best_ctx = (
                self.context_text_extractor.get_best_context_text(contexts) or ""
            ).lower()
            # 注文番号・トラップ系は除外
            try:
                combined = (blob + " " + best_ctx).lower()
                if any(t.lower() in combined for t in [t.lower() for t in order_like_tokens]):
                    continue
                if any(t in combined for t in spam_trap_tokens):
                    continue
            except Exception:
                pass
            attr_hit = any(t in blob for t in addr_attr_tokens) or any(
                t in (ei.get("placeholder", "") or "") for t in ["県", "市", "区", "丁目", "番地", "-", "ー", "－"]
            )
            ctx_hit = any(t in best_ctx for t in addr_ctx_tokens)
            if not (attr_hit or ctx_hit):
                continue

            score, details = await self.element_scorer.calculate_element_score(
                el, patterns, target_field
            )
            # スコアがゼロ/マイナスは除外
            if score <= 0:
                continue
            candidates.append((el, score, details, contexts, attr_hit, ctx_hit))

        if not candidates:
            return
        # スコア降順で安定ソート
        candidates.sort(key=lambda t: t[1], reverse=True)
        # 住所の誤検出リスクを下げるため、しきい値を基本65とするが、
        # 属性/文脈の双方ヒット時は閾値をやや緩和（汎用安全の範囲内）
        base = int(self.settings.get("email_fallback_min_score", 60)) + 5  # 既存設定と整合
        threshold_base = max(60, base)  # 下限60

        mapped_count = 0
        supplement_idx = 1
        for el, score, details, contexts, attr_hit, ctx_hit in candidates:
            # 属性/文脈のヒット状況に応じて動的なしきい値を決定
            threshold = threshold_base - 5 if (attr_hit and ctx_hit) else threshold_base
            # 強い属性一致（name/id が city/adrs/room 等）の場合はさらに緩和
            try:
                ei_local = details.get("element_info", {}) or {}
                nm = (ei_local.get("name", "") or "").lower()
                ide = (ei_local.get("id", "") or "").lower()
                strong_attr = nm in {"city", "adrs", "room"} or ide in {"city", "adrs", "room"}
            except Exception:
                strong_attr = False
            if strong_attr:
                threshold = max(50, threshold - 10)
            if score < threshold:
                # 動的しきい値は候補ごとに異なるため、
                # 現在の候補が不採用でも後続候補を検査し続ける
                continue
            # 既に他で利用済みの要素はスキップ
            if id(el) in used_elements:
                continue
            # 役割推定（市区町村/詳細）に使う簡易トークン
            try:
                ei = details.get("element_info", {}) or {}
                blob = " ".join([
                    (ei.get("name", "") or ""),
                    (ei.get("id", "") or ""),
                    (ei.get("class", "") or ""),
                    (ei.get("placeholder", "") or ""),
                    (self.context_text_extractor.get_best_context_text(contexts) or ""),
                ]).lower()
            except Exception:
                blob = ""
            city_tokens = ["市区町村", "市区", "郡", "市", "city", "区", "町", "town", "丁目"]
            detail_tokens = [
                "番地", "丁目", "建物", "building", "マンション", "ビル", "部屋", "room", "apt", "apartment", "号室"
            ]

            # フィールド名の決定: 先頭は『住所』、以降は『住所_補助N』
            fname = target_field if (mapped_count == 0 and target_field not in field_mapping) else f"住所_補助{supplement_idx}"
            # 過剰マッピング抑制: 2つまで（市区町村+詳細）
            if fname.startswith("住所_補助") and supplement_idx > 2:
                break

            info = await self._create_enhanced_element_info(el, details, contexts)
            try:
                info["source"] = "fallback"
            except Exception:
                pass
            tmp = self._generate_temp_field_value(fname)
            if self.duplicate_prevention.register_field_assignment(fname, tmp, score, info):
                field_mapping[fname] = info
                used_elements.add(id(el))
                mapped_count += 1
                if fname.startswith("住所_補助"):
                    supplement_idx += 1
                logger.info(
                    f"Fallback mapped '{fname}' via label/attr/placeholder (score {score})"
                )
            # 2フィールドまでで十分
            if mapped_count >= 2:
                break

    async def _salvage_strict_email_by_attr(
        self, classified_elements, field_mapping, used_elements
    ):
        """厳格属性に基づくメール救済。

        - まだ『メールアドレス』が無い場合
        - input の name/id が 'email' で、確認欄のシグナル（confirm/check）を含まない
        """
        target = "メールアドレス"
        if target in field_mapping:
            return
        buckets = ["email_inputs", "text_inputs", "other_inputs"]
        for b in buckets:
            for el in classified_elements.get(b, []) or []:
                if id(el) in used_elements:
                    continue
                try:
                    ei = await self.element_scorer._get_element_info(el)
                except Exception:
                    ei = {}
                nm = (ei.get("name") or "").lower()
                ide = (ei.get("id") or "").lower()
                cls = (ei.get("class") or "").lower()
                if nm == "email" or ide == "email":
                    blob = " ".join([nm, ide, cls])
                    if any(k in blob for k in ["confirm", "確認", "check"]):
                        continue
                    info = await self._create_enhanced_element_info(
                        el, {"element_info": ei, "total_score": 80}, []
                    )
                    try:
                        info["source"] = "salvage_attr"
                    except Exception:
                        pass
                    tmp = self._generate_temp_field_value(target)
                    if self.duplicate_prevention.register_field_assignment(
                        target, tmp, 80, info
                    ):
                        field_mapping[target] = info
                        used_elements.add(id(el))
                        logger.info("Salvaged 'メールアドレス' by strict name/id match = email")
                        return

    async def _force_map_common_address_fields(self, classified_elements, field_mapping, used_elements):
        """一般的な name 属性（city/adrs/room）に対して、しきい値に依存せず安全に住所系としてマッピング。
        - 既に『住所』『住所_補助*』が存在する場合は追加しない
        - 最高2フィールド（優先: city, 次点: adrs）
        - 住所テキスト欄以外は対象外
        """
        if any(k in field_mapping for k in ("住所", "住所_補助1")):
            return
        text_inputs = classified_elements.get("text_inputs", []) or []
        targets = []
        for el in text_inputs:
            try:
                if id(el) in used_elements:
                    continue
                ei = await self.element_scorer._get_element_info(el)
                nm = (ei.get("name", "") or "").lower()
                ide = (ei.get("id", "") or "").lower()
                if nm in {"city", "adrs"} or ide in {"city", "adrs"}:
                    targets.append((nm or ide, el))
            except Exception:
                continue
        if not targets:
            return
        # 優先順位: city -> adrs
        order = {"city": 0, "adrs": 1}
        targets.sort(key=lambda t: order.get(t[0], 99))
        mapped = 0
        for idx, (_, el) in enumerate(targets):
            fname = "住所" if (idx == 0 and "住所" not in field_mapping) else f"住所_補助{idx}"
            try:
                patterns = self.field_patterns.get_pattern("住所") or {}
                score, details = await self.element_scorer.calculate_element_score(el, patterns, "住所")
            except Exception:
                score, details = 90, {"element_info": await self.element_scorer._get_element_info(el), "total_score": 90}
            info = await self._create_enhanced_element_info(el, details, [])
            try:
                info["source"] = "forced_common_address"
            except Exception:
                pass
            tmp = self._generate_temp_field_value(fname)
            if self.duplicate_prevention.register_field_assignment(fname, tmp, score, info):
                field_mapping[fname] = info
                used_elements.add(id(el))
                mapped += 1
            if mapped >= 2:
                break

    async def _salvage_email_by_label_context(
        self, classified_elements, field_mapping, used_elements
    ):
        """ラベル/見出しテキストからメール欄を救済（厳格属性が使えない古いサイト向け）。

        安全条件:
        - まだ『メールアドレス』が未確定
        - input[type=text] などのテキスト入力
        - 近傍の強いラベルテキストに『メール』『e-mail』『email』等が含まれる
        - 『確認』『再入力』等の確認用語は含まれない
        """
        target = "メールアドレス"
        if target in field_mapping:
            return
        text_inputs = (classified_elements.get("text_inputs") or []) + (
            classified_elements.get("other_inputs") or []
        )
        if not text_inputs:
            return
        # 設定の確認トークンを利用（mail2等は追加で補強）
        confirm_tokens = set(
            [t.lower() for t in (self.settings.get("confirm_tokens", []) or [])]
        ) | {"mail2", "re_mail", "re-email", "re-mail", "email2", "確認用", "再入力", "もう一度", "再度"}
        patterns = self.field_patterns.get_pattern(target) or {}
        best = (None, 0, None, [])
        for el in text_inputs:
            if id(el) in used_elements:
                continue
            try:
                ei = await self.element_scorer._get_element_info(el)
            except Exception:
                ei = {}
            typ = (ei.get("type") or "").lower()
            if typ not in ["", "text"]:
                continue
            # 直接DOMからテーブル行の左セルテキストを取得（thが無い旧式table対応）
            dom_label = ""
            try:
                dom_label = await el.evaluate(
                    "(el) => {\n"
                    "  const td = el.closest('td');\n"
                    "  if (!td) return '';\n"
                    "  const tr = td.closest('tr');\n"
                    "  if (!tr) return '';\n"
                    "  const cells = Array.from(tr.children);\n"
                    "  const idx = cells.indexOf(td);\n"
                    "  if (idx > 0) {\n"
                    "    const prev = cells[idx-1];\n"
                    "    if (prev && prev.tagName && prev.tagName.toLowerCase()==='td') {\n"
                    "      return (prev.textContent||'').trim();\n"
                    "    }\n"
                    "  } else if (cells.length >= 2) {\n"
                    "    const first = cells[0];\n"
                    "    if (first && first !== td && first.tagName && first.tagName.toLowerCase()==='td') {\n"
                    "      return (first.textContent||'').trim();\n"
                    "    }\n"
                    "  }\n"
                    "  return '';\n"
                    "}"
                )
            except Exception:
                dom_label = ""
            el_bounds = (
                self._element_bounds_cache.get(str(el))
                if hasattr(self, "_element_bounds_cache")
                else None
            )
            contexts = await self.context_text_extractor.extract_context_for_element(
                el, el_bounds
            )
            best_txt = (self.context_text_extractor.get_best_context_text(contexts) or "").lower()
            # 属性由来のテキスト
            attr_context = " ".join(
                [
                    (ei.get("name") or ""),
                    (ei.get("id") or ""),
                    (ei.get("class") or ""),
                    (ei.get("placeholder") or ""),
                ]
            ).lower()
            label_context = (best_txt + " " + (dom_label or "").lower())
            label_blob = label_context
            if any(tok in label_blob for tok in ["メール", "e-mail", "email", "mail"]):
                # 確認用の強いシグナルを除外（ラベル・属性双方を対象）
                full_context = (label_context + " " + attr_context)
                if any(k in full_context for k in confirm_tokens):
                    continue
                # スコアに依存しない救済（最低限の安全チェックは上で実施済み）
                details = {"element_info": ei, "total_score": 80}
                best = (el, 80, details, contexts)
                break
        el, score, details, contexts = best
        if el:
            info = await self._create_enhanced_element_info(el, details, contexts)
            try:
                info["source"] = "salvage_label"
            except Exception:
                pass
            tmp = self._generate_temp_field_value(target)
            if self.duplicate_prevention.register_field_assignment(
                target, tmp, score, info
            ):
                field_mapping[target] = info
                used_elements.add(id(el))
                logger.info(
                    f"Salvaged '{target}' by label context (score {score})"
                )

    async def _fallback_map_fullname_field(
        self, classified_elements, field_mapping, used_elements
    ):
        """統合氏名の取りこぼし救済（安全条件）。"""
        if any(k in field_mapping for k in ["統合氏名", "姓", "名"]):
            return
        text_inputs = classified_elements.get("text_inputs", []) or []
        if not text_inputs:
            return
        patterns = self.field_patterns.get_pattern("統合氏名") or {}
        pos_tokens = {"氏名", "お名前", "name", "fullname"}
        neg_tokens = {
            "会社", "法人", "団体", "組織", "部署", "役職",
            "住所", "postal", "郵便", "zip", "prefecture", "都道府県",
            "email", "mail", "メール", "tel", "phone", "電話",
        }
        best = (None, 0, None, [])
        for el in text_inputs:
            if id(el) in used_elements:
                continue
            try:
                ei = await self.element_scorer._get_element_info(el)
            except Exception:
                ei = {}
            blob = " ".join([
                (ei.get("name") or ""),
                (ei.get("id") or ""),
                (ei.get("class") or ""),
                (ei.get("placeholder") or ""),
            ]).lower()
            if any(t in blob for t in neg_tokens):
                continue
            el_bounds = (
                self._element_bounds_cache.get(str(el))
                if hasattr(self, "_element_bounds_cache")
                else None
            )
            contexts = await self.context_text_extractor.extract_context_for_element(
                el, el_bounds
            )
            best_txt = (
                self.context_text_extractor.get_best_context_text(contexts) or ""
            ).lower()
            if not (any(t in blob for t in pos_tokens) or any(t in best_txt for t in pos_tokens)):
                continue
            s, details = await self.element_scorer.calculate_element_score(
                el, patterns, "統合氏名"
            )
            if s > best[1]:
                best = (el, s, details, contexts)
        el, score, details, contexts = best
        if el and score >= max(55, int(self.settings.get("email_fallback_min_score", 60)) - 5):
            info = await self._create_enhanced_element_info(el, details, contexts)
            try:
                info["source"] = "fallback"
            except Exception:
                pass
            tmp = self._generate_temp_field_value("統合氏名")
            if self.duplicate_prevention.register_field_assignment(
                "統合氏名", tmp, score, info
            ):
                field_mapping["統合氏名"] = info
                used_elements.add(id(el))
                logger.info(
                    f"Fallback mapped '統合氏名' via label/attr (score {score})"
                )

        # 追加フォールバック: 分割姓名（姓/名）の取りこぼし救済（プレースホルダ/ラベル/属性の強いトークン）
        try:
            await self._fallback_map_split_name_fields(classified_elements, field_mapping, used_elements)
        except Exception as e:
            logger.debug(f"fallback split name mapping skipped: {e}")

    async def _fallback_map_subject_field(
        self, classified_elements, field_mapping, used_elements
    ):
        """件名の取りこぼし救済（安全条件）。"""
        target = "件名"
        if target in field_mapping:
            return
        text_inputs = classified_elements.get("text_inputs", []) or []
        if not text_inputs:
            return
        patterns = self.field_patterns.get_pattern(target) or {}
        pos_tokens = {"件名", "タイトル", "subject", "topic", "heading", "sub", "title"}
        neg_tokens = {
            "会社", "法人", "団体", "組織", "部署", "役職",
            "氏名", "お名前", "name", "fullname", "メール", "email", "mail",
            "tel", "phone", "電話", "住所", "郵便", "postal", "zip",
        }
        # スパム対策/罠フィールドの一般的なトークン（可視/不可視を問わず除外）
        trap_tokens = {"honeypot", "honey", "trap", "botfield", "no-print", "noprint", "hidden", "hid"}
        best = (None, 0, None, [])
        for el in text_inputs:
            if id(el) in used_elements:
                continue
            try:
                ei = await self.element_scorer._get_element_info(el)
            except Exception:
                ei = {}
            blob = " ".join([
                (ei.get("name") or ""),
                (ei.get("id") or ""),
                (ei.get("class") or ""),
                (ei.get("placeholder") or ""),
            ]).lower()
            if any(t in blob for t in neg_tokens):
                continue
            if any(t in blob for t in trap_tokens):
                continue
            el_bounds = (
                self._element_bounds_cache.get(str(el))
                if hasattr(self, "_element_bounds_cache")
                else None
            )
            contexts = await self.context_text_extractor.extract_context_for_element(
                el, el_bounds
            )
            best_txt = (
                self.context_text_extractor.get_best_context_text(contexts) or ""
            ).lower()
            if not (any(t in blob for t in pos_tokens) or any(t in best_txt for t in pos_tokens)):
                continue
            s, details = await self.element_scorer.calculate_element_score(
                el, patterns, target
            )
            if s > best[1]:
                best = (el, s, details, contexts)
        el, score, details, contexts = best
        if el and score >= max(65, int(self.settings.get("email_fallback_min_score", 60))):
            info = await self._create_enhanced_element_info(el, details, contexts)
            try:
                info["source"] = "fallback"
            except Exception:
                pass
            tmp = self._generate_temp_field_value(target)
            if self.duplicate_prevention.register_field_assignment(
                target, tmp, score, info
            ):
                field_mapping[target] = info
                used_elements.add(id(el))
                logger.info(
                    f"Fallback mapped '{target}' via label/attr (score {score})"
                )
            return

    async def _force_map_email_from_table_label(
        self, classified_elements, field_mapping, used_elements
    ):
        target = "メールアドレス"
        if target in field_mapping:
            return
        text_inputs = classified_elements.get("text_inputs", []) or []
        for el in text_inputs:
            if id(el) in used_elements:
                continue
            try:
                ei = await self.element_scorer._get_element_info(el)
            except Exception:
                ei = {}
            if not ei.get("visible", True):
                continue
            typ = (ei.get("type") or "").lower()
            if typ not in ["", "text"]:
                continue
            try:
                label = await el.evaluate(
                    "(el) => {\n"
                    "  const td = el.closest('td'); if (!td) return '';\n"
                    "  const tr = td.closest('tr'); if (!tr) return '';\n"
                    "  const cells = Array.from(tr.children); const idx = cells.indexOf(td);\n"
                    "  const pick = (node) => (node && node.tagName && node.tagName.toLowerCase()==='td') ? (node.textContent||'').trim() : '';\n"
                    "  let t=''; if (idx>0) t = pick(cells[idx-1]); if (!t && cells.length>=2) t = pick(cells[0]); return t;\n"
                    "}"
                )
            except Exception:
                label = ""
            lab = str(label or "").lower()
            if not lab:
                continue
            if not any(tok in lab for tok in ["メール", "e-mail", "email", "mail"]):
                continue
            if any(k in lab for k in ["確認用", "再入力", "もう一度", "再度", "confirm", "confirmation", "mail2", "re-mail", "re_email", "re email"]):
                continue
            info = await self._create_enhanced_element_info(el, {"element_info": ei, "total_score": 80}, [])
            try:
                info["source"] = "force_table_label"
            except Exception:
                pass
            tmp = self._generate_temp_field_value(target)
            if self.duplicate_prevention.register_field_assignment(target, tmp, 80, info):
                field_mapping[target] = info
                used_elements.add(id(el))
                logger.info("Force-mapped 'メールアドレス' from table row label (left TD)")
                return

        # 追加救済: name/id が 'sub' / 'subject' / 'title' の要素を安全に採用（罠トークンは除外）
        try:
            for el in classified_elements.get("text_inputs", []) or []:
                if id(el) in used_elements:
                    continue
                ei = await self.element_scorer._get_element_info(el)
                nm = (ei.get("name") or "").lower()
                ide = (ei.get("id") or "").lower()
                cl = (ei.get("class") or "").lower()
                if any(t in (nm + " " + ide + " " + cl) for t in trap_tokens):
                    continue
                if nm in {"sub", "subject", "title"} or ide in {"sub", "subject", "title"}:
                    info = await self._create_enhanced_element_info(el, {"element_info": ei, "total_score": 60}, [])
                    try:
                        info["source"] = "fallback_attr"
                    except Exception:
                        pass
                    tmp = self._generate_temp_field_value(target)
                    if self.duplicate_prevention.register_field_assignment(
                        target, tmp, 60, info
                    ):
                        field_mapping[target] = info
                        used_elements.add(id(el))
                        logger.info("Fallback mapped '件名' via attribute name/id match (sub/subject/title)")
                        return
        except Exception:
            pass

    def _determine_target_element_types(
        self, field_patterns: Dict[str, Any]
    ) -> List[str]:
        """候補バケットをフィールド定義から厳選

        - tag が input の場合は `text_inputs` を優先し、不要な `textareas` を含めない
        - types が email/tel/url/number を含む場合はそれぞれを追加
        - tag が textarea/select の場合はそのバケットを追加
        - いずれにも該当しない場合のみ汎用フォールバック（ただし textareas は除外）
        """
        target_types = set()
        pattern_types = field_patterns.get("types", [])
        pattern_tags = field_patterns.get("tags", [])

        # types に基づく厳密な候補
        if "email" in pattern_types:
            target_types.add("email_inputs")
        if "tel" in pattern_types:
            target_types.update(["tel_inputs", "text_inputs"])
        if "url" in pattern_types:
            target_types.add("url_inputs")
        if "number" in pattern_types:
            target_types.add("number_inputs")
        if "text" in pattern_types:
            target_types.add("text_inputs")

        # tag による候補
        # 入力タイプが明示されている場合はそれに従う。'input' だからといって
        # 無条件に text_inputs を候補に含めると、性別など本来 radio/select である
        # フィールドが input[type=text] に誤って昇格する温床になる。
        if "input" in pattern_tags and "text" in pattern_types:
            target_types.add("text_inputs")
        if "textarea" in pattern_tags:
            target_types.add("textareas")
        if "select" in pattern_tags:
            target_types.add("selects")

        # フォールバック（textareas は含めない＝精度優先）
        if not target_types:
            target_types.update(["text_inputs", "email_inputs", "tel_inputs"])

        return list(target_types)

    async def _fallback_map_split_name_fields(self, classified_elements, field_mapping, used_elements):
        """分割姓名（姓/名）の取りこぼし救済。

        条件（汎用）:
        - まだ『姓』『名』が未確定
        - text_inputs のうち、placeholder/ラベル/属性に 姓/名 or first/last/fname/lname 系トークン
        - 可視かつ allow_candidate を満たす
        - スコアが最低閾値に近い場合でも、強いトークン一致で昇格
        """
        if ("姓" in field_mapping) and ("名" in field_mapping):
            return
        text_inputs = classified_elements.get("text_inputs", []) or []
        if not text_inputs:
            return
        patterns_last = self.field_patterns.get_pattern("姓") or {}
        patterns_first = self.field_patterns.get_pattern("名") or {}

        def _blob(ei: dict, ctx_best: str) -> str:
            return " ".join([
                (ei.get("name") or ""), (ei.get("id") or ""), (ei.get("class") or ""), (ei.get("placeholder") or ""),
                (ctx_best or "")
            ]).lower()

        candidates_last = []
        candidates_first = []
        for el in text_inputs:
            if id(el) in used_elements:
                continue
            ei = await self.element_scorer._get_element_info(el)
            if not ei.get("visible", True):
                continue
            try:
                from .candidate_filters import allow_candidate as _allow
                if not await _allow("姓", el, ei):
                    continue
            except Exception:
                pass
            ctxs = await self.context_text_extractor.extract_context_for_element(el)
            best_txt = self.context_text_extractor.get_best_context_text(ctxs) or ""
            blob = _blob(ei, best_txt)
            last_hit = any(t in blob for t in ["姓", "lastname", "last_name", "last-name", "family-name", "family_name", "sei", "lname", "l_name"]) or any(
                t in (best_txt or "") for t in ["姓", "苗字"])
            first_hit = any(t in blob for t in ["名", "firstname", "first_name", "first-name", "given-name", "given_name", "mei", "fname", "f_name"]) or any(
                t in (best_txt or "") for t in ["名"])
            if last_hit:
                s, d = await self.element_scorer.calculate_element_score(el, patterns_last, "姓")
                candidates_last.append((s, el, d, ctxs))
            if first_hit:
                s, d = await self.element_scorer.calculate_element_score(el, patterns_first, "名")
                candidates_first.append((s, el, d, ctxs))

        if candidates_last and ("姓" not in field_mapping):
            candidates_last.sort(key=lambda x: x[0], reverse=True)
            s, el, d, ctxs = candidates_last[0]
            info = await self._create_enhanced_element_info(el, d, ctxs)
            tmp = self._generate_temp_field_value("姓")
            if self.duplicate_prevention.register_field_assignment("姓", tmp, s, info):
                field_mapping["姓"] = info
                used_elements.add(id(el))
        if candidates_first and ("名" not in field_mapping):
            candidates_first.sort(key=lambda x: x[0], reverse=True)
            # 既に『姓』に使った要素は避ける
            pick = None
            for s, el, d, ctxs in candidates_first:
                if id(el) not in used_elements:
                    pick = (s, el, d, ctxs)
                    break
            if not pick:
                return
            s, el, d, ctxs = pick
            info = await self._create_enhanced_element_info(el, d, ctxs)
            tmp = self._generate_temp_field_value("名")
            if self.duplicate_prevention.register_field_assignment("名", tmp, s, info):
                field_mapping["名"] = info
                used_elements.add(id(el))

    def _should_skip_field_for_unified(self, field_name: str) -> bool:
        # 統合氏名（漢字）がある場合は「姓」「名」のみスキップ
        if self.unified_field_info.get("has_fullname") and field_name in ["姓", "名"]:
            return True
        # 統合カナがある場合は分割カナをスキップ
        if self.unified_field_info.get("has_kana_unified") and field_name in [
            "姓カナ",
            "名カナ",
        ]:
            return True
        # 統合ひらがながある場合は分割ひらがなをスキップ
        if self.unified_field_info.get("has_hiragana_unified") and field_name in [
            "姓ひらがな",
            "名ひらがな",
        ]:
            return True
        # 統合電話がある場合は分割電話をスキップ
        if self.unified_field_info.get("has_phone_unified") and field_name in [
            "電話1",
            "電話2",
            "電話3",
        ]:
            return True
        # 汎用改善: 分割姓名が存在する場合は、統合氏名をスキップして先に分割を優先
        # 理由:
        #  - 統合氏名が先に確定すると最初の入力欄を占有し、
        #    『姓/名』どちらか一方の取りこぼしや誤マッピングが発生しやすい。
        #  - FormPreProcessor 側ではカナ/ひらがな要素を除外したうえで、
        #    漢字の分割姓名が別要素として存在する場合のみ has_name_split_fields を True にしている。
        if field_name == "統合氏名" and self.unified_field_info.get(
            "has_name_split_fields"
        ):
            try:
                logger.info(
                    "Skip '統合氏名' due to detected split name fields (prefer 分割: 姓/名)"
                )
            except Exception:
                pass
            return True
        # 追加: カナの分割（セイ/メイ）が存在する場合は『統合氏名カナ』をスキップ
        if field_name == "統合氏名カナ" and self.unified_field_info.get(
            "has_name_kana_split_fields"
        ):
            try:
                logger.info(
                    "Skip '統合氏名カナ' due to detected split kana fields (prefer 分割: 姓カナ/名カナ)"
                )
            except Exception:
                pass
            return True
        return False

    async def _ensure_required_mappings(
        self,
        classified_elements: Dict[str, List[Locator]],
        field_mapping: Dict[str, Dict[str, Any]],
        used_elements: set,
        required_elements_set: set,
    ) -> None:
        """必須要素を必ず field_mapping に登録する救済フェーズ（委譲版）。"""
        await self._required_rescue.ensure_required_mappings(
            classified_elements, field_mapping, used_elements, required_elements_set
        )

    def _infer_logical_field_name_for_required(
        self, element_info: Dict[str, Any], contexts: List
    ) -> str:
        tag = (element_info.get("tag_name") or "").lower()
        typ = (element_info.get("type") or "").lower()
        # placeholder も含めて論理名推定のヒントにする（(姓)/(名) 等の明示に対応）
        name_id_cls = " ".join(
            [
                (element_info.get("name") or ""),
                (element_info.get("id") or ""),
                (element_info.get("class") or ""),
                (element_info.get("placeholder") or ""),
            ]
        ).lower()
        try:
            ctx_best = self.context_text_extractor.get_best_context_text(contexts) or ""
        except Exception:
            ctx_best = ""
        # すべてのコンテキストテキストを結合して判定精度を上げる（best のみだと取りこぼしが出る）
        try:
            ctx_all = " ".join([getattr(c, "text", "") or "" for c in (contexts or [])])
        except Exception:
            ctx_all = ""
        ctx_text = (str(ctx_best) + " " + str(ctx_all)).lower()

        if tag == "input" and typ == "email":
            return "メールアドレス"
        # 電話番号よりも先に郵便番号の可能性を評価する（zip系トークンが強いケースが多い）
        # type=tel でも郵便番号用に実装されているサイトが多数あるため順序を変更
        if tag == "textarea":
            return "お問い合わせ本文"

        # 郵便番号の推定（単一/分割どちらにも対応できる統合ラベル）
        # name/id/class/placeholder/コンテキストに郵便関連トークンが含まれるかを確認
        postal_tokens = [
            "郵便番号",
            "郵便",
            "〒",
            "zip",
            "zipcode",
            "zip_code",
            "zip-code",
            "postal",
            "postalcode",
            "postal_code",
            "post_code",
            "post-code",
            "postcode",
            "上3桁",
            "下4桁",
            "前3桁",
            "後4桁",
            "yubin",
            "yuubin",
            "yubinbango",
            "yuubinbango",
        ]
        if tag == "input" and typ in ["", "text", "tel"]:
            blob = f"{name_id_cls} {ctx_text}"
            if any(tok in blob for tok in postal_tokens):
                return "郵便番号"

        # 都道府県の推定（prefecture 専用）
        pref_tokens = ["都道府県", "prefecture", "pref", "p-region", "region"]
        if tag in ["input", "select"] and typ in ["", "text"]:
            blob_pref = f"{name_id_cls} {ctx_text}"
            if any(tok in blob_pref for tok in pref_tokens):
                return "都道府県"

        # 郵便番号でない場合に限り、type=tel を電話番号として扱う
        if tag == "input" and typ == "tel":
            return "電話番号"

        # 住所の推定（都道府県/市区町村/番地/建物名などの語を包括）
        address_tokens = [
            "住所",
            "所在地",
            "address",
            "addr",
            "street",
            "street_address",
            "番地",
            "建物",
            "building",
            "都道府県",
            "prefecture",
            "県",
            "市区町村",
            "市区",
            "city",
            "区",
            "町",
            "town",
            "丁目",
            "マンション",
            "ビル",
            "部屋番号",
            "room",
            "apt",
            "apartment",
        ]
        if tag in ["input", "select"] and typ in ["", "text"]:
            blob = f"{name_id_cls} {ctx_text}"
            if any(tok in blob for tok in address_tokens):
                return "住所"

        # 汎用入力(type=text)でも文脈/属性から論理フィールドを推定（救済判定）
        # 1) メールアドレス: ラベル/見出し/placeholder/属性にメール系語が含まれる
        email_tokens = [
            "メール",
            "e-mail",
            "email",
            "mail",
            "ｅメール",
            "Ｅメール",
            "eﾒｰﾙ",
            "電子メール",
            "E-mail",
            "Eメール",
        ]
        if tag == "input" and typ in ["", "text", "mail", "number", "tel"]:
            if any(tok in ctx_text for tok in email_tokens) or any(
                tok in name_id_cls for tok in ["email", "mail"]
            ):
                if not self._is_confirmation_field(element_info, contexts):
                    return "メールアドレス"
            # 2) 電話番号: 文脈/属性に電話系語が含まれる
            if any(
                tok in ctx_text for tok in ["電話", "tel", "phone", "telephone"]
            ) or any(tok in name_id_cls for tok in ["tel", "phone"]):
                return "電話番号"

        # --- Name field inference (split aware) ---
        kana_tokens = [
            "kana",
            "furigana",
            "katakana",
            "カナ",
            "カタカナ",
            "フリガナ",
            "ふりがな",
        ]
        # 『ふりがな』は実務上カタカナ入力を指すケースが多いため、
        # ひらがなトークンからは除外し、kana 側で扱う（上の kana_tokens に含めている）。
        hira_tokens = ["hiragana", "ひらがな"]
        last_tokens = [
            "lastname",
            "last_name",
            "last-name",
            "last",
            "family-name",
            "family_name",
            "surname",
            "sei",
            "姓",
        ]
        # 『名』単独は住所系の『マンション名』等に誤反応しやすいため除外し、
        # より明確な表現のみを用いる
        first_tokens = [
            "firstname",
            "first_name",
            "first-name",
            "first",
            "given-name",
            "given_name",
            "forename",
            "mei",
            "お名前",
            "名前",
        ]
        # カナ/ひらがなの判定は、要素の属性か直近の文脈(best)に限定し、
        # 離れた位置にある別フィールドの『フリガナ』見出しに引っ張られないようにする。
        has_kana = any(t in name_id_cls for t in kana_tokens) or ("フリガナ" in ctx_best)
        has_hira = any(t in name_id_cls for t in hira_tokens) or ("ひらがな" in ctx_best)
        # 属性(first/last)の手掛かりを最優先に用い、文脈は補助として利用
        attr_last = any(t in name_id_cls for t in last_tokens)
        attr_first = any(t in name_id_cls for t in first_tokens)
        ctx_last = any(t in ctx_text for t in ["姓", "せい", "苗字"])
        ctx_first = any(t in ctx_text for t in ["名", "めい"])

        # 両方の属性シグナルが無い場合のみ、文脈シグナルを採用（かつ相互排他的に）
        if not (attr_last or attr_first):
            is_last = ctx_last and not ctx_first
            is_first_token_hit = ctx_first and not ctx_last
        else:
            is_last = attr_last
            is_first_token_hit = attr_first
        # 非個人名（会社名/商品名/部署名/建物名…）の文脈では『名』の判定を抑止
        from .element_scorer import ElementScorer

        non_personal_ctx = bool(
            ElementScorer.NON_PERSONAL_NAME_PATTERN.search(ctx_text or "")
        )
        is_first = is_first_token_hit and not non_personal_ctx
        # has_kanji は参照箇所が無いため削除（判定ロジックには影響しない）

        # Prioritize split-specific logical names when tokens available
        if has_kana:
            # フリガナの単一入力（セイ/メイのヒントが無い）なら統合カナを優先
            try:
                has_furigana_label = ("フリガナ" in ctx_text) or ("furigana" in ctx_text.lower())
                has_sei = any(t in (ctx_text + " " + name_id_cls) for t in ["セイ", "姓", "sei", "lastname", "family"])
                has_mei = any(t in (ctx_text + " " + name_id_cls) for t in ["メイ", "名", "mei", "firstname", "given"])
                if has_furigana_label and not (has_sei or has_mei):
                    return "統合氏名カナ"
            except Exception:
                pass
            if is_last and not is_first:
                return "姓カナ"
            if is_first and not is_last:
                return "名カナ"
            # ambiguous kana → unified kana
            return "統合氏名カナ"

        if has_hira:
            # 『ふりがな』は実務上カタカナ入力を要求するフォームが多いため、
            # 文脈やプレースホルダにカタカナ指標があればカナ系に寄せる（汎用安全化）。
            try:
                placeholder = str(element_info.get("placeholder", "") or "")
                def _has_hiragana(s: str) -> bool:
                    return any("ぁ" <= ch <= "ゖ" for ch in s)
                def _has_katakana(s: str) -> bool:
                    return any(("ァ" <= ch <= "ヺ") or ch == "ー" for ch in s)
                katakana_hint = (
                    ("カタカナ" in ctx_text)
                    or ("katakana" in ctx_text.lower())
                    or _has_katakana(placeholder)
                    or any(tok in name_id_cls for tok in ["katakana", "kana"])
                )
            except Exception:
                katakana_hint = False

            if katakana_hint:
                if is_last and not is_first:
                    return "姓カナ"
                if is_first and not is_last:
                    return "名カナ"
                return "統合氏名カナ"

            # それ以外は『ひらがな』系として扱う
            if is_last and not is_first:
                return "姓ひらがな"
            if is_first and not is_last:
                return "名ひらがな"
            # ひらがな指標のみ（統合フィールド想定）→ 統合氏名カナとして扱い、
            # 値の生成時にひらがな/カタカナを判定（assigner側でbest_contextを参照）
            return "統合氏名カナ"

        # Kanji or unspecified script
        if is_last and not is_first:
            return "姓"
        if is_first and not is_last:
            return "名"

        # Unified fallbacks
        if any(tok in ctx_text for tok in ["お名前", "氏名", "おなまえ"]) or any(
            t in name_id_cls for t in ["your-name", "fullname", "full_name", "name"]
        ):
            return "統合氏名"

        return "auto_required_text_1"

    def _is_nonfillable_required(self, element_info: Dict[str, Any]) -> bool:
        name_id_cls = " ".join(
            [
                (element_info.get("name") or ""),
                (element_info.get("id") or ""),
                (element_info.get("class") or ""),
            ]
        ).lower()
        input_type = (element_info.get("type") or "").lower()
        tag = (element_info.get("tag_name") or "").lower()

        # 1) 技術的に自動入力しない対象（認証/確認/トークン等）
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
            # 追加: ログイン/認証/パスワード系（汎用安全強化）
            "login", "signin", "sign_in", "auth", "authentication", "login_id",
            "password", "pass", "pswd", "mfa", "totp",
        ]
        if any(b in name_id_cls for b in blacklist):
            return True

        # 2) クリック/選択系は ensure_required で直接マッピングせず、自動ハンドラに委譲
        #    - checkbox, radio は _auto_handle_checkboxes / _auto_handle_radios
        #    - select は _auto_handle_selects
        if input_type in ["checkbox", "radio"]:
            return True
        if tag == "select":
            return True

        return False

    def _should_skip_field_for_form_type(self, field_name: str) -> bool:
        return field_name in self.form_type_info.get("irrelevant_fields", [])

    def _get_dynamic_quality_threshold(
        self, field_name: str, essential_fields_completed: set
    ) -> float:
        from .mapping_thresholds import get_dynamic_quality_threshold as _impl

        return _impl(
            field_name,
            self.settings,
            essential_fields_completed,
            self.OPTIONAL_HIGH_PRIORITY_FIELDS,
        )

    def _is_core_field(self, field_name: str) -> bool:
        """
        コア項目かどうかを判定（入力すべき重要なフィールド）

        Args:
            field_name: フィールド名

        Returns:
            コア項目の場合True
        """
        # コア項目の定義：フォーム送信に必要最小限の項目
        core_fields = {
            # 氏名系
            "統合氏名",
            "姓",
            "名",
            # カナ系（汎用昇格）
            "統合氏名カナ",
            "姓カナ",
            "名カナ",
            # 連絡手段
            "メールアドレス",
            # 本文・内容
            "お問い合わせ本文",
        }

        return field_name in core_fields

    def _calculate_context_bonus(
        self, contexts, field_name: str, field_patterns: Dict[str, Any]
    ) -> float:
        if not contexts:
            return 0.0
        bonus = 0.0
        best_context = max(contexts, key=lambda x: x.confidence)
        strict_patterns = field_patterns.get("strict_patterns", [])
        matched = any(p.lower() in best_context.text.lower() for p in strict_patterns)
        if matched:
            bonus += 20 + best_context.confidence * 15
        return min(bonus, 50)
