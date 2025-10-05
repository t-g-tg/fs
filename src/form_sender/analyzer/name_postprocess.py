from __future__ import annotations

"""氏名系ポストプロセス（RuleBasedAnalyzer からの委譲用、挙動不変）。"""

from typing import Any, Dict, Callable


def prune_suspect_name_mappings(field_mapping: Dict[str, Any], settings: Dict[str, Any]) -> None:
    try:
        if "統合氏名" not in field_mapping:
            return
        negative_ctx_tokens = [
            "住所",
            "マンション名",
            "建物名",
            "ふりがな",
            "フリガナ",
            "カナ",
            "かな",
            "ひらがな",
            "郵便",
            "郵便番号",
            "商品名",
            "部署",
            "部署名",
        ]
        negative_attr_tokens = ["kana", "furigana", "katakana", "hiragana"]
        for k in ["姓", "名"]:
            info = field_mapping.get(k)
            if not info:
                continue
            ctx = (info.get("best_context_text") or "").lower()
            blob = " ".join(
                [
                    str(info.get("name", "")).lower(),
                    str(info.get("id", "")).lower(),
                    str(info.get("class", "")).lower(),
                    str(info.get("placeholder", "")).lower(),
                ]
            )
            if any(t.lower() in ctx for t in negative_ctx_tokens) or any(
                t in blob for t in negative_attr_tokens
            ):
                field_mapping.pop(k, None)
        try:
            per_field = (settings.get("min_score_threshold_per_field", {}) or {})
            min_name_score = int(per_field.get("名", 85))
            min_last_score = int(per_field.get("姓", 85))
        except Exception:
            min_name_score = 85
            min_last_score = 85
        for k, th in [("姓", min_last_score), ("名", min_name_score)]:
            info = field_mapping.get(k)
            if info and int(info.get("score", 0)) < th:
                field_mapping.pop(k, None)
    except Exception:
        pass


def fix_name_mapping_mismatch(field_mapping: Dict[str, Any]) -> None:
    def blob(key: str) -> str:
        info = (field_mapping.get(key, {}) or {})
        return " ".join(
            [
                str(info.get("selector", "")),
                str(info.get("name", "")),
                str(info.get("id", "")),
                str(info.get("class", "")),
            ]
        ).lower()

    def ctx(key: str) -> str:
        info = (field_mapping.get(key, {}) or {})
        return str(info.get("best_context_text", "")).lower()

    def swap(a: str, b: str) -> None:
        if (a in field_mapping) and (b in field_mapping):
            field_mapping[a], field_mapping[b] = (field_mapping[b], field_mapping[a])

    def mismatch(sei_blob: str, mei_blob: str) -> bool:
        if not sei_blob or not mei_blob:
            return False
        return ("first" in sei_blob and "last" in mei_blob) or ("mei" in sei_blob and "sei" in mei_blob)

    # 1) 属性由来の明確な入れ違い
    if mismatch(blob("姓"), blob("名")):
        swap("姓", "名")

    # 2) ラベル/見出しテキスト由来の入れ違い（Wix 等の混在表記 '姓 / First Name' 対策）
    try:
        sei_ctx = ctx("姓")
        mei_ctx = ctx("名")
        # 『姓側に first name 』『名側に last name』の痕跡がある場合は入れ違いとみなす
        if (sei_ctx and "first" in sei_ctx) and (mei_ctx and "last" in mei_ctx):
            swap("姓", "名")
        # 日本語の並列表記（例: '名 / Last Name', '姓 / First Name'）にも限定的に対応
        elif ("名" in sei_ctx and "last" in sei_ctx) and ("姓" in mei_ctx and "first" in mei_ctx):
            swap("姓", "名")
    except Exception:
        pass
    if mismatch(blob("姓カナ"), blob("名カナ")) and ("kana" in blob("姓カナ") and "kana" in blob("名カナ")):
        swap("姓カナ", "名カナ")


def align_name_by_placeholder(field_mapping: Dict[str, Any]) -> None:
    """プレースホルダに基づき『姓/名』および『セイ/メイ』『せい/めい』の入れ違いを是正。

    汎用ルール:
    - 両方のフィールドが存在し、かつ両者の placeholder が互いに逆の語（例: 姓側が『名』・名側が『姓』）を
      含む場合はスワップする。
    - カナ/ひらがなについても『セイ/メイ』『せい/めい』の入れ違いを検出してスワップする。
    - placeholder が取得できない/空文字の場合は何もしない。
    """

    def _pl(key: str) -> str:
        try:
            return str((field_mapping.get(key, {}) or {}).get("placeholder", "") or "")
        except Exception:
            return ""

    def _swap(a: str, b: str) -> None:
        if (a in field_mapping) and (b in field_mapping):
            field_mapping[a], field_mapping[b] = (field_mapping[b], field_mapping[a])

    sei_pl, mei_pl = _pl("姓"), _pl("名")
    if sei_pl and mei_pl:
        lc_sei = sei_pl.lower()
        lc_mei = mei_pl.lower()
        # 英日双方の代表的語彙で判定（境界は単語レベルでの曖昧一致を許容）
        sei_has_mei = ("名" in sei_pl) or ("first name" in lc_sei) or ("given name" in lc_sei)
        mei_has_sei = ("姓" in mei_pl) or ("last name" in lc_mei) or ("family name" in lc_mei)
        if sei_has_mei and mei_has_sei:
            _swap("姓", "名")

    sei_kana_pl, mei_kana_pl = _pl("姓カナ"), _pl("名カナ")
    if sei_kana_pl and mei_kana_pl:
        sei_has_mei = ("メイ" in sei_kana_pl) or ("名" in sei_kana_pl)
        mei_has_sei = ("セイ" in mei_kana_pl) or ("姓" in mei_kana_pl)
        if sei_has_mei and mei_has_sei:
            _swap("姓カナ", "名カナ")

    sei_hira_pl, mei_hira_pl = _pl("姓ひらがな"), _pl("名ひらがな")
    if sei_hira_pl and mei_hira_pl:
        sei_has_mei = ("めい" in sei_hira_pl) or ("名" in sei_hira_pl)
        mei_has_sei = ("せい" in mei_hira_pl) or ("姓" in mei_hira_pl)
        if sei_has_mei and mei_has_sei:
            _swap("姓ひらがな", "名ひらがな")


async def normalize_kana_hiragana_fields(
    field_mapping: Dict[str, Any],
    form_structure: Any,
    get_element_details: Callable[..., Any],
) -> None:
    def _is_hiragana_like(info: dict) -> bool:
        blob = " ".join(
            [
                str(info.get("name", "")),
                str(info.get("id", "")),
                str(info.get("class", "")),
                str(info.get("placeholder", "")),
            ]
        )
        return any(k in blob for k in ["ひらがな", "ふりがな"]) and not any(
            k in blob for k in ["カナ", "カタカナ", "フリガナ"]
        )

    def _is_katakana_like(info: dict) -> bool:
        blob = " ".join(
            [
                str(info.get("name", "")),
                str(info.get("id", "")),
                str(info.get("class", "")),
                str(info.get("placeholder", "")),
            ]
        )
        return any(k in blob for k in ["カナ", "カタカナ", "フリガナ"])

    for kana_field, hira_field in [("姓カナ", "姓ひらがな"), ("名カナ", "名ひらがな")]:
        kinfo = field_mapping.get(kana_field)
        hinfo = field_mapping.get(hira_field)
        if kinfo and _is_hiragana_like(kinfo) and not hinfo:
            field_mapping[hira_field] = kinfo
            field_mapping.pop(kana_field, None)
        if hinfo and _is_katakana_like(hinfo) and not kinfo:
            field_mapping[kana_field] = hinfo
            field_mapping.pop(hira_field, None)

    if ("姓ひらがな" in field_mapping) and ("名ひらがな" in field_mapping):
        if "統合氏名カナ" in field_mapping:
            field_mapping.pop("統合氏名カナ", None)

    uinfo = field_mapping.get("統合氏名カナ")
    if uinfo and _is_hiragana_like(uinfo):
        try:
            has_split_hira = ("姓ひらがな" in field_mapping) and ("名ひらがな" in field_mapping)
            if has_split_hira:
                pass
            else:
                pass
        except Exception:
            pass

    try:
        if form_structure and getattr(form_structure, "elements", None):
            used_selectors = {
                (v or {}).get("selector", "") for v in field_mapping.values() if isinstance(v, dict)
            }
            for need, token in [("姓ひらがな", "姓"), ("名ひらがな", "名")]:
                if need in field_mapping:
                    continue
                for fe in form_structure.elements:
                    try:
                        if (fe.tag_name or "").lower() != "input":
                            continue
                        if (fe.element_type or "").lower() not in ("text", ""):
                            continue
                        if not fe.is_visible:
                            continue
                        blob = " ".join(
                            [
                                (fe.name or ""),
                                (fe.id or ""),
                                (fe.class_name or ""),
                                (fe.placeholder or ""),
                                (fe.label_text or ""),
                                (fe.associated_text or ""),
                            ]
                        )
                        if ("ふりがな" in blob or "ひらがな" in blob) and (token in blob):
                            info = await get_element_details(fe.locator)
                            if info.get("selector", "") not in used_selectors:
                                field_mapping[need] = info
                                used_selectors.add(info.get("selector", ""))
                                break
                    except Exception:
                        continue
    except Exception:
        pass
