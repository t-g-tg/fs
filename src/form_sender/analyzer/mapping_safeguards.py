"""FieldMapper のフィールド固有ガードを分離（振る舞い不変）。

各フィールド（メール/電話/郵便番号/都道府県）に対し、
要素属性やコンテキストに基づいて『採用してよいか』を判定する。

Public API:
 - passes_safeguard(field_name, best_score_details, best_context, context_text_extractor, field_patterns, settings) -> bool
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def _passes_kana_like(field_name: str, ei: Dict[str, Any], best_txt: str) -> bool:
    """カナ/ひらがな系フィールドの安全ガード。

    汎用方針:
    - 属性(name/id/class/placeholder) もしくはラベル/見出し(best_txt)のいずれかに
      カナ/ふりがな指標が存在しない場合は不採用。
    - 明確な非対象語（性別/sex/gender）が含まれる場合は不採用。

    これにより、必須ブーストにより type=text の汎用入力へ誤って割当てられる
    事象を抑止する。
    """
    attrs = _attrs_blob(ei)
    neg_tokens = ["性別", "sex", "gender"]
    if any(t in attrs for t in neg_tokens) or any(t in best_txt for t in neg_tokens):
        return False

    kana_tokens = [
        "kana",
        "katakana",
        "furigana",
        "カナ",
        "カタカナ",
        "フリガナ",
        "ふりがな",
        # 氏名カナで用いられやすい表記
        "セイ",
        "メイ",
    ]
    hira_tokens = [
        "hiragana",
        "ひらがな",
    ]

    # フィールド種別に応じた指標セット
    if field_name in {"統合氏名カナ", "姓カナ", "名カナ", "会社名カナ"}:
        indicators = kana_tokens
    elif field_name in {"姓ひらがな", "名ひらがな"}:
        # ふりがな（カタカナ）のケースも現実には混在するため、
        # ひらがな専用トークンに加えて『ふりがな』も許容
        indicators = hira_tokens + ["ふりがな", "フリガナ"]
    else:
        return True  # 対象外フィールド

    has_indicator = any(t.lower() in attrs for t in indicators) or any(
        t.lower() in best_txt for t in indicators
    )
    return bool(has_indicator)


def _passes_unified_fullname(ei: Dict[str, Any], best_txt: str) -> bool:
    """統合氏名の安全ガード。

    - 住所/郵便/都道府県 等の住所系トークンを含む場合は不許可（誤割当て抑止）
    - ラベル/属性に『氏名/お名前/name/担当者』等のポジティブトークンがあれば積極許可
    """
    attrs = _attrs_blob(ei)
    neg_tokens = [
        # 住所系
        "住所", "address", "addr", "street", "city", "prefecture", "都道府県", "市区町村",
        "郵便", "郵便番号", "zip", "postal", "postcode", "zipcode",
    ]
    if any(t in attrs for t in neg_tokens) or any(t in best_txt for t in neg_tokens):
        return False
    # 『会社名』を含む複合ラベル（例: 会社名または氏名）は統合氏名の採用を控える（会社名優先）
    if ("会社名" in attrs) or ("会社名" in best_txt):
        return False
    pos_tokens = ["氏名", "お名前", "name", "your-name", "担当者", "担当者名"]
    has_pos = any(t in attrs for t in pos_tokens) or any(t in best_txt for t in pos_tokens)
    return True if has_pos else True  # ポジティブは任意。ネガティブのみで除外。


def _best_context_text(best_context: Optional[List], ctx_extractor) -> str:
    try:
        return (ctx_extractor.get_best_context_text(best_context) or "").lower()
    except Exception:
        return ""


def _attrs_blob(ei: Dict[str, Any]) -> str:
    try:
        return " ".join(
            [
                str(ei.get("name") or ""),
                str(ei.get("id") or ""),
                str(ei.get("class") or ""),
                str(ei.get("placeholder") or ""),
            ]
        ).lower()
    except Exception:
        return ""


def _passes_email(ei: Dict[str, Any], best_txt: str) -> bool:
    etype = (ei.get("type") or "").lower()
    attrs_blob = _attrs_blob(ei)
    email_tokens = ["email", "e-mail", "mail", "メール"]
    is_semantic_email = any(t in attrs_blob for t in email_tokens) or any(
        t in best_txt for t in email_tokens
    )
    return bool(etype == "email" or is_semantic_email)


def _passes_phone(ei: Dict[str, Any], best_txt: str) -> bool:
    etype = (ei.get("type") or "").lower()
    attrs_blob = _attrs_blob(ei)
    # 属性: tel/phone に加え、日本語の『電話』『携帯』も許可（type=text な和名属性でも通す）
    pos_attr = any(t in attrs_blob for t in ["tel", "phone", "電話", "携帯"])
    pos_ctx = any(t in best_txt for t in ["電話", "tel", "phone", "携帯", "mobile", "cell"])
    neg_ctx = any(t in best_txt for t in ["時", "時頃", "午前", "午後", "連絡方法"]) or any(
        t in attrs_blob for t in ["timeno", "h1", "h2"]
    )
    return bool(etype == "tel" or pos_attr or (pos_ctx and not neg_ctx))


def _passes_postal(ei: Dict[str, Any], best_txt: str) -> bool:
    attrs_blob = _attrs_blob(ei)
    pos_attr = any(
        t in attrs_blob
        for t in [
            "zip",
            "postal",
            "postcode",
            "zipcode",
            "郵便",
            "〒",
            # 日本語表記のローマ字（ゆうびん）
            "yubin",
            "yuubin",
            "yubinbango",
            "yuubinbango",
        ]
    )
    pos_ctx = any(t in best_txt for t in ["郵便番号", "郵便", "〒", "postal", "zip"])
    neg = any(
        t in attrs_blob
        for t in [
            "captcha",
            "image_auth",
            "token",
            "otp",
            "verification",
            "confirm",
            "確認",
        ]
    )
    return bool((pos_attr or pos_ctx) and not neg)


def _passes_prefecture(ei: Dict[str, Any], best_txt: str) -> bool:
    """都道府県フィールドの安全ガード。

    ポイント:
    - 無条件許可はしない。
    - select であっても、属性/ラベルに都道府県系トークンが無ければ不許可（オプション検証は候補フィルタ側で実施）。
    - 非select の場合は、属性/コンテキストに強いトークンが必要。
    """
    tag = (ei.get("tag_name") or "").lower()
    attrs_blob = _attrs_blob(ei)

    # 属性/ラベルに『都道府県』、または 'pref' 'prefecture' が含まれることを要求
    pos_attr = ("都道府県" in attrs_blob) or ("prefecture" in attrs_blob) or ("pref" in attrs_blob)
    pos_ctx = any(t in best_txt for t in ["都道府県", "prefecture"])

    if tag == "select":
        return bool(pos_attr or pos_ctx)
    # 非selectはより厳格（属性または強いラベル）
    return bool(pos_attr or pos_ctx)

def _passes_message(ei: Dict[str, Any], best_txt: str) -> bool:
    """お問い合わせ本文テキストエリアの安全ガード。

    - 住所系の強いトークン（microformats や address/住所 等）を含む場合は不許可
    - 可能であれば『お問い合わせ/内容/メッセージ』系の文脈を優先
    """
    attrs = _attrs_blob(ei)
    # 住所系・microformatsトークン
    address_like = [
        "住所", "address", "addr", "street", "city", "prefecture", "都道府県", "市区町村",
        "p-region", "p-locality", "p-street-address", "p-extended-address",
    ]
    # 旅行/予約系の専用要望欄（部屋/宿泊/レンタカー 等）は除外
    travel_like = ["宿泊", "宿泊地", "ホテル", "旅館", "部屋", "客室", "レンタカー", "旅程", "便名", "航空券", "予約"]
    if any(t in best_txt for t in travel_like) or any(t in attrs for t in travel_like):
        return False
    if any(t in attrs for t in address_like) or any(t in best_txt for t in address_like):
        # メッセージ系の強い指標が同時にある場合のみ許容
        msg_tokens = ["お問い合わせ", "メッセージ", "本文", "内容", "message", "inquiry", "contact"]
        has_msg = any(t in best_txt for t in msg_tokens) or any(t in attrs for t in msg_tokens)
        if not has_msg:
            return False
    return True


def _passes_address(ei: Dict[str, Any], best_txt: str) -> bool:
    """住所フィールドの安全ガード。

    許可条件（いずれか）:
    - 属性(name/id/class/placeholder)に住所系トークン（address/住所/addr/street/city/pref 等）
    - ラベル/周辺テキストに住所系トークン

    不許可条件（いずれか）:
    - カナ/ふりがな系トークン（フリガナ/カナ/ひらがな/セイ/メイ）
    - 部署/部門/課 等の部署系トークン（department/dept/division/section/team/部署/部門/課/係）
    - 認証/確認系（captcha/verification/token/confirm 等）
    """
    attrs = _attrs_blob(ei)
    # 許可トークン
    pos_tokens = [
        "住所", "所在地", "address", "addr", "street", "city", "prefecture", "都道府県", "市区町村",
        "p-region", "p-locality", "p-street-address", "p-extended-address",
    ]
    # 不許可トークン（かな/部署/認証系）
    kana_like = [
        "フリガナ", "ふりがな", "カナ", "kana", "katakana", "hiragana", "セイ", "メイ", "furi",
    ]
    dept_like = [
        "部署", "部門", "課", "係", "department", "dept", "division", "section", "team",
    ]
    auth_like = [
        # 認証/確認系（CAPTCHA/クイズ/トークン等）
        "captcha", "verification", "token", "otp", "confirm", "確認", "認証",
        "quiz", "wpcf7-quiz", "security", "セキュリティ", "画像認証",
        # 文字入力を要求する一般的ラベル文言
        "文字を入力", "次の文字", "表示されている文字", "上の文字",
    ]

    # 許可判定
    has_pos = any(t in attrs for t in pos_tokens) or any(t in best_txt for t in pos_tokens)
    # 不許可判定
    has_kana = any(t in attrs for t in kana_like) or any(t in best_txt for t in kana_like)
    has_dept = any(t in attrs for t in dept_like) or any(t in best_txt for t in dept_like)
    has_auth = any(t in attrs for t in auth_like) or any(t in best_txt for t in auth_like)

    # 旅行/予約系のコンテキストには住所を割り当てない（宿泊地/部屋/レンタカー等の誤割当て抑止）
    travel_like = [
        "宿泊", "宿泊地", "ホテル", "旅館", "部屋", "客室", "レンタカー", "旅行", "出発", "到着", "旅程", "便名", "航空券", "予約"
    ]
    if any(t in best_txt for t in travel_like) or any(t in attrs for t in travel_like):
        return False

    if has_kana or has_dept or has_auth:
        return False
    return bool(has_pos)


def _passes_company_name(ei: Dict[str, Any], best_txt: str) -> bool:
    """会社名フィールドの安全ガード。

    - 『カナ/フリガナ/ひらがな』系の指標が強い場合は不許可（ふりがな欄の誤割当て抑止）
    """
    attrs = _attrs_blob(ei)
    kana_like = ["カナ", "フリガナ", "ふりがな", "hiragana", "katakana", "kana", "セイ", "メイ"]
    if any(t.lower() in attrs for t in [s.lower() for s in kana_like]) or any(
        t.lower() in best_txt for t in [s.lower() for s in kana_like]
    ):
        return False
    # 追加: 個人名を強く示す“明確な語”のみで判定（単漢字『名』『姓』は除外）
    personal_name_tokens = [
        "お名前", "氏名", "姓名", "full name", "first name", "given name", "last name", "family name",
    ]
    if any(t.lower() in best_txt for t in [s.lower() for s in personal_name_tokens]):
        return False
    return True


def _passes_personal_name(ei: Dict[str, Any], best_txt: str) -> bool:
    """姓/名（個人名）の安全ガード。

    - ラベル/属性に『会社名』『法人名』『団体名』等が含まれる場合は不許可（複合欄は会社名優先）
    """
    attrs = _attrs_blob(ei)
    org_tokens = ["会社名", "法人名", "団体名", "組織名", "企業名", "会社"]
    if any(t in attrs for t in org_tokens) or any(t in best_txt for t in org_tokens):
        return False
    return True


def passes_safeguard(
    field_name: str,
    best_score_details: Dict[str, Any],
    best_context: Optional[List],
    context_text_extractor,
    field_patterns: Dict[str, Any],  # 互換のため受け取るが未使用の場合あり
    settings: Dict[str, Any],  # 互換のため受け取る
) -> bool:
    """フィールド固有の採用ガードに合格するか。

    元の FieldMapper 実装の if/guard ロジックをそのまま移植し、
    True=採用可 / False=不採用 を返す。
    """
    ei = (best_score_details or {}).get("element_info", {})
    best_txt = _best_context_text(best_context, context_text_extractor)

    if field_name == "メールアドレス":
        return _passes_email(ei, best_txt)
    if field_name == "電話番号":
        return _passes_phone(ei, best_txt)
    if field_name == "郵便番号":
        return _passes_postal(ei, best_txt)
    if field_name == "都道府県":
        return _passes_prefecture(ei, best_txt)
    if field_name == "お問い合わせ本文":
        return _passes_message(ei, best_txt)
    if field_name == "住所":
        return _passes_address(ei, best_txt)
    if field_name == "会社名":
        return _passes_company_name(ei, best_txt)
    if field_name in {"姓", "名"}:
        return _passes_personal_name(ei, best_txt)
    if field_name == "件名":
        attrs = _attrs_blob(ei)
        pos = any(t in attrs for t in ["件名", "subject", "題名"]) or any(
            t in best_txt for t in ["件名", "subject", "題名"]
        )
        neg = any(
            t in attrs or t in best_txt
            for t in ["フリガナ", "ふりがな", "カナ", "kana", "furigana", "セイ", "メイ"]
        )
        return bool(pos and not neg)
    if field_name == "役職":
        # 役職/職位/position/job title のいずれかの指標が属性/ラベルに必要
        attrs = _attrs_blob(ei)
        pos_tokens = [
            "役職", "職位", "position", "job title", "title", "役割", "ポジション"
        ]
        neg_ctx_tokens = ["知ったきっかけ", "きっかけ", "how did you hear", "referrer"]
        if any(t in attrs for t in pos_tokens) or any(t in best_txt for t in pos_tokens):
            if not any(t in best_txt for t in neg_ctx_tokens):
                return True
        return False

    # カナ/ひらがな系の安全ガード
    if field_name in {"統合氏名カナ", "姓カナ", "名カナ", "姓ひらがな", "名ひらがな", "会社名カナ"}:
        return _passes_kana_like(field_name, ei, best_txt)

    if field_name == "統合氏名":
        return _passes_unified_fullname(ei, best_txt)

    return True
