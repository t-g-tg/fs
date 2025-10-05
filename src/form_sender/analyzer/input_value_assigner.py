import logging
from typing import Dict, Any, Optional

from .field_combination_manager import FieldCombinationManager
from .split_field_detector import SplitFieldDetector

logger = logging.getLogger(__name__)


class InputValueAssigner:
    """入力値の生成と割り当てを担当するクラス"""

    def __init__(
        self,
        field_combination_manager: FieldCombinationManager,
        split_field_detector: SplitFieldDetector,
    ):
        self.field_combination_manager = field_combination_manager
        self.split_field_detector = split_field_detector
        self.required_analysis: Dict[str, Any] = {}
        self.unified_field_info: Dict[str, Any] = {}

    async def assign_enhanced_input_values(
        self,
        field_mapping: Dict[str, Dict[str, Any]],
        auto_handled: Dict[str, Dict[str, Any]],
        split_groups,
        client_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        input_assignments = {}
        client_data = client_data or {}

        split_assignments = self.split_field_detector.generate_field_assignments(
            split_groups, client_data
        )

        # 予定されている『その他/other』ラジオのセレクタ一覧（auto_handledの選択予定から抽出）
        planned_other_radio_selectors = []
        try:
            for _k, _v in (auto_handled or {}).items():
                try:
                    if not isinstance(_v, dict):
                        continue
                    if str(_v.get("input_type", "")) != "radio":
                        continue
                    sel = _v.get("selector", "")
                    if not sel:
                        continue
                    txt = str(_v.get("selected_option_text", "") or "").lower()
                    if any(t in txt for t in ["その他", "other"]):
                        planned_other_radio_selectors.append(sel)
                except Exception:
                    continue
        except Exception:
            planned_other_radio_selectors = []

        for field_name, field_info in field_mapping.items():
            if not self._should_input_field(field_name, field_info):
                continue

            input_type = field_info.get("input_type")
            # 住所/都道府県は分割割当よりも文脈ヒューリスティクスを優先
            if field_name == "都道府県":
                input_value = self._handle_prefecture_assignment(
                    field_info, client_data
                )
            elif field_name.startswith("住所"):
                input_value = self._handle_address_assignment(
                    field_name, field_info, client_data
                )
            else:
                input_value = split_assignments.get(field_name)
                if input_value is None or str(input_value).strip() == "":
                    input_value = self._generate_enhanced_input_value(
                        field_name, field_info, client_data
                    )

            # 選択式（select/checkbox/radio）のクライアント値割り当て制約
            # 方針: クライアント情報を当てはめる可能性があるのは address_1（都道府県）と gender のみ
            # ここでは select のみを対象にし、性別以外はアルゴリズム選択に委譲する
            auto_action = None
            extra = {}
            if input_type == "select":
                allowed = field_name in {"性別", "都道府県"}
                if not allowed:
                    # 値は使わず、3段階アルゴリズムで選択させる
                    input_value = ""
                    auto_action = "select_by_algorithm"
                else:
                    # 許可フィールドでも値が無ければアルゴリズム選択を有効化
                    if not (input_value or "").strip():
                        auto_action = "select_by_algorithm"

            assign = {
                "selector": field_info["selector"],
                "input_type": input_type,
                "value": input_value,
                "required": field_info.get("required", False),
            }
            if auto_action:
                assign["auto_action"] = auto_action
            # field_infoに自動動作指定がある場合は引き継ぐ（確認用メール等）
            if field_info.get("auto_action"):
                assign["auto_action"] = field_info.get("auto_action")
            if field_info.get("copy_from_field"):
                assign["copy_from_field"] = field_info.get("copy_from_field")
            assign.update(extra)

            input_assignments[field_name] = assign

        for field_name, field_info in auto_handled.items():
            value = field_info.get("default_value", True)
            if field_info.get("auto_action") == "copy_from":
                src = field_info.get("copy_from_field", "")
                value = input_assignments.get(src, {}).get("value", "")

            # P2: auto_required_text_* は UnmappedElementHandler からの auto_handled 経路に乗るため、
            #     ここで『ラジオの “その他” 選択に紐づく追加入力』と推定できる場合のみ、
            #     値を空文字に抑止してダミー全角スペース等の投入を避ける。
            # 判定要件（全て満たす）:
            #   - フィールド名が auto_required_text_*
            #   - 同一 form 内に checked な input[type=radio] があり、その表示ラベルが『その他/other』を含む
            #   - そのラジオと当該入力欄の垂直距離が一定以内（近接, 例: 320px）
            try:
                if str(field_name).startswith("auto_required_text_") and field_info.get("selector"):
                    el = field_info.get("element")
                    if el is not None:
                        is_other_linked = await el.evaluate(
                            """
                            (inputEl, selectors) => {
                              try {
                                const form = inputEl.closest('form') || document.body;
                                const rectIn = inputEl.getBoundingClientRect();
                                const pickEl = (sel) => {
                                  try { return form.querySelector(sel) || document.querySelector(sel); } catch { return null; }
                                };
                                for (const sel of (selectors || [])) {
                                  const r = pickEl(sel);
                                  if (!r) continue;
                                  const rectR = r.getBoundingClientRect();
                                  const dy = Math.abs((rectR.top + rectR.bottom)/2 - (rectIn.top + rectIn.bottom)/2);
                                  if (dy <= 320) return true;
                                }
                                return false;
                              } catch { return false; }
                            }
                            """,
                            planned_other_radio_selectors,
                        )
                        if is_other_linked:
                            value = ""
            except Exception:
                pass

            input_assignments[field_name] = {
                "selector": field_info["selector"],
                "input_type": field_info["input_type"],
                "value": value,
                "required": field_info.get("required", False),
                "auto_action": field_info.get("auto_action", "default"),
            }

        # 追加救済: 必須フィールド情報から電話番号の分割欄（tel2/tel3等）を検出して、
        # クライアントの phone_2/phone_3 を直接割当（フィールド名に依存しない汎用処理）。
        try:
            req = self.required_analysis or {}
            required_elems = req.get("required_elements") or []
            client = (
                client_data.get("client", {}) if isinstance(client_data, dict) else {}
            )
            p2 = (client.get("phone_2", "") or "").strip()
            p3 = (client.get("phone_3", "") or "").strip()
            for elem in required_elems:
                nm = str(elem.get("name", "") or "").lower()
                ide = str(elem.get("id", "") or "").lower()
                selector = ""
                # id優先
                if ide:
                    selector = f'[id="{ide}"]'
                elif nm:
                    # name セレクタはクォート付きで指定
                    selector = f'input[name="{nm}"]'
                else:
                    continue
                # 既に割当済みのセレクタには追加しない
                if any(
                    v.get("selector") == selector
                    for v in input_assignments.values()
                    if isinstance(v, dict)
                ):
                    continue
                blob = nm + " " + ide
                if "tel" in blob or "phone" in blob:
                    import re

                    m = re.search(r"(?:tel|phone)[^\d]*([123])(?!.*\d)", blob)
                    if not m:
                        continue
                    idx = int(m.group(1))
                    if idx == 2 and p2:
                        input_assignments[f"auto_phone_part_{idx}"] = {
                            "selector": selector,
                            "input_type": "text",
                            "value": p2,
                            "required": True,
                            "auto_action": "fill",
                        }
                    if idx == 3 and p3:
                        input_assignments[f"auto_phone_part_{idx}"] = {
                            "selector": selector,
                            "input_type": "text",
                            "value": p3,
                            "required": True,
                            "auto_action": "fill",
                        }
        except Exception as e:
            logger.debug(f"phone split assignment fallback skipped: {e}")
        # 分割電話の自動値を電話番号1/2/3にも同期（評価用JSONの明確化）
        try:
            for idx in (1, 2, 3):
                auto_key = f"auto_phone_part_{idx}"
                target_key = f"電話番号{idx}"
                if auto_key in input_assignments:
                    aval = str(input_assignments[auto_key].get("value", "") or "").strip()
                    if not aval:
                        continue
                    if target_key in input_assignments:
                        input_assignments[target_key]["value"] = aval
                    elif target_key in field_mapping:
                        fi = field_mapping.get(target_key, {})
                        input_assignments[target_key] = {
                            "selector": fi.get("selector", input_assignments[auto_key].get("selector", "")),
                            "input_type": fi.get("input_type", "text"),
                            "value": aval,
                            "required": bool(fi.get("required", False)),
                        }
        except Exception:
            pass
        # フォールバック: 単一フィールドの郵便番号にも7桁を投入
        try:
            if "郵便番号1" in input_assignments:
                v = str(input_assignments["郵便番号1"].get("value", "") or "").strip()
                if not v:
                    client = (
                        client_data.get("client", {})
                        if isinstance(client_data, dict)
                        else {}
                    )
                    combined = (client.get("postal_code_1", "") or "") + (
                        client.get("postal_code_2", "") or ""
                    )
                    if combined.strip():
                        input_assignments["郵便番号1"]["value"] = combined
        except Exception:
            pass
        # 追加補正: メールアドレスに '@' が含まれない場合は統合値を適用
        try:
            assign = input_assignments.get("メールアドレス")
            if isinstance(assign, dict):
                v = str(assign.get("value", "") or "")
                def _is_valid_email(addr: str) -> bool:
                    # 依存追加なしの軽量検証（local@domain.tld の形を要求）
                    import re
                    if not addr or '@' not in addr:
                        return False
                    # かなり緩いが実運用上の誤判定を避けつつ最低限の構造を担保
                    email_re = re.compile(r'^[^\s@]+@[^\s@]+\.[^\s@]{2,}$')
                    return bool(email_re.match(addr))

                if v and not _is_valid_email(v):
                    full = self.field_combination_manager.generate_combined_value(
                        "email", client_data
                    )
                    if full and _is_valid_email(full):
                        assign["value"] = full
                        logger.info("Patched incomplete email local-part to full address")
            # メール確認フィールド（copy_from）の値も最新のメールに合わせて再同期
            for k, v in list(input_assignments.items()):  # 反復中変更の安全化
                try:
                    if isinstance(v, dict) and v.get("auto_action") == "copy_from":
                        src = v.get("copy_from_field", "メールアドレス")
                        src_val = input_assignments.get(src, {}).get("value", "")
                        if src_val:
                            v["value"] = src_val
                            logger.info(f"Synced value for '{k}' from '{src}' after email patch")
                except Exception:
                    continue
        except (KeyError, AttributeError, TypeError) as e:
            logger.debug(f"email patch skipped: {e}")
        # 共通の取り違えを補正（例: sei/mei の入れ違い、sei_kana/mei_kanaの入れ違い）
        try:
            self._fix_name_selector_mismatch(field_mapping, input_assignments)
            self._enforce_name_values(input_assignments, client_data)
        except Exception as e:
            logger.debug(f"name selector mismatch fix skipped: {e}")

        # 追加の安全弁: 都道府県の空値補完（text/selectを問わず）
        try:
            if "都道府県" in input_assignments:
                v = str(input_assignments["都道府県"].get("value", "") or "").strip()
                if not v:
                    pref = self._handle_prefecture_assignment({}, client_data)
                    if pref:
                        input_assignments["都道府県"]["value"] = pref
                        logger.info(
                            "Filled empty '都道府県' from client address_1 (fallback)"
                        )
        except Exception:
            pass

        # 重複アサイン解消: 統合氏名カナと分割カナ（姓/名）が同一セレクタに向いている場合、
        # 分割側（姓/名）のアサインを削除して重複入力を避ける。
        try:
            sel_u = (input_assignments.get("統合氏名カナ", {}) or {}).get("selector")
            if sel_u:
                for k in ["姓", "名"]:
                    sel_k = (input_assignments.get(k, {}) or {}).get("selector")
                    if sel_k and sel_k == sel_u:
                        input_assignments.pop(k, None)
                        logger.info(f"Removed duplicate assignment '{k}' sharing selector with 統合氏名カナ")
            # 統合氏名（auto_fullname_label_1 等）と分割姓名（姓/名）が同一セレクタなら、分割側を削除
            sel_full = (input_assignments.get("統合氏名", {}) or {}).get("selector") or (
                input_assignments.get("auto_fullname_label_1", {}) or {}
            ).get("selector")
            if sel_full:
                for k in ["姓", "名"]:
                    sel_k = (input_assignments.get(k, {}) or {}).get("selector")
                    if sel_k and sel_k == sel_full:
                        input_assignments.pop(k, None)
                        logger.info(f"Removed duplicate assignment '{k}' sharing selector with 統合氏名/auto_fullname")
        except Exception:
            pass

        return input_assignments

    def _should_input_field(self, field_name: str, field_info: Dict[str, Any]) -> bool:
        if self._is_fax_field(field_name, field_info):
            return False
        if self.required_analysis.get("treat_all_as_required", False):
            return True
        # コア項目の拡張: カナ系も常に入力対象に含める（汎用安全）
        core_fields = [
            "件名",
            "お問い合わせ本文",
            "メールアドレス",
            "姓",
            "名",
            "氏名",
            "お名前",
            "統合氏名",
            "統合氏名カナ",
            "姓カナ",
            "名カナ",
            "電話番号",
            "会社名",
            "郵便番号",
            "住所",
        ]
        # 分割電話（電話番号1/2/3）と分割郵便番号（郵便番号1/2）は統合と同等に扱う
        if field_name in core_fields or field_name in {"電話番号1", "電話番号2", "電話番号3", "郵便番号1", "郵便番号2"}:
            return True
        return field_info.get("required", False)

    def _is_fax_field(self, field_name: str, field_info: Dict[str, Any]) -> bool:
        return (
            "fax" in field_name.lower()
            or "fax" in field_info.get("selector", "").lower()
        )

    def _generate_enhanced_input_value(
        self, field_name: str, field_info: Dict[str, Any], client_data: Dict[str, Any]
    ) -> str:
        # Get value from field combination manager
        value = self.field_combination_manager.get_field_value_for_type(
            field_name, "single", client_data
        )

        # 住所/郵便/電話/都道府県の汎用整形・割当（追加）
        def _client() -> Dict[str, Any]:
            return (
                client_data.get("client")
                if isinstance(client_data, dict) and "client" in client_data
                else client_data
            )

        def _ctx_blob() -> str:
            try:
                best = field_info.get("best_context_text") or ""
            except Exception:
                best = ""
            parts = [
                field_info.get("name", ""),
                field_info.get("id", ""),
                field_info.get("class", ""),
                field_info.get("placeholder", ""),
                best,
            ]
            return " ".join([p for p in parts if p]).lower()

        def _format_postal(v: str) -> str:
            vv = (v or "").replace("-", "").strip()
            ph = field_info.get("placeholder", "") or ""
            if len(vv) == 7 and ("-" in ph or "〒" in ph or "〒" in _ctx_blob()):
                return f"{vv[:3]}-{vv[3:]}"
            return vv

        def _format_phone(v: str) -> str:
            vv = (v or "").replace("-", "").strip()
            ph = (field_info.get("placeholder", "") or "").lower()
            if "-" in ph and vv.isdigit() and len(vv) in (10, 11):
                if len(vv) == 10:
                    return f"{vv[:2]}-{vv[2:6]}-{vv[6:]}"
                else:
                    return f"{vv[:3]}-{vv[3:7]}-{vv[7:]}"
            return vv

        blob = _ctx_blob()

        # P1対応: 『その他の理由』等の自由記述欄に自動値は入れない（テキスト入力リスク回避）
        try:
            if str(field_name).startswith("auto_required_text_"):
                reason_tokens = ["その他の理由", "その他", "理由", "詳細", "備考", "remarks", "remark", "reason", "specify"]
                if any(t in blob for t in reason_tokens):
                    return ""  # 値を設定しない
        except Exception:
            pass

        if field_name == "郵便番号":
            pv = self.field_combination_manager.get_field_value_for_type(
                "郵便番号", "single", client_data
            )
            return _format_postal(pv)

        # 汎用: 本文フィールドの文脈に応じた安全なテンプレート選択
        # 例: 査定/買取/見積/修理/予約/採用 などの専用フォームでは、
        #     汎用の営業文ではなく簡潔でニュートラルな一文を用いる（偽陽性低減）
        if field_name == "お問い合わせ本文":
            try:
                blob_lower = blob.lower()
                # 日本語キーワードはそのまま判定
                if any(k in blob for k in ["査定", "買取", "買い取り"]):
                    return "査定のご相談です。詳細は追ってご連絡いたします。"
                if any(k in blob for k in ["見積", "お見積", "御見積"]):
                    return "お見積りのご相談です。詳細は追ってご連絡いたします。"
                if any(k in blob for k in ["修理", "修繕", "サポート"]):
                    return "修理・サポートに関するお問い合わせです。"
                if any(k in blob for k in ["予約", "来店予約", "アポイント"]):
                    return "予約に関するお問い合わせです。"
                if any(k in blob for k in ["採用", "応募", "エントリー"]):
                    return "採用に関するお問い合わせです。"
                # 英語系の簡易トークン
                if any(k in blob_lower for k in ["estimate", "quotation"]):
                    return "Requesting a quotation. Details to follow."
                if any(k in blob_lower for k in ["repair", "support"]):
                    return "Requesting repair/support. We will share details."
            except Exception:
                pass

        # auto_required_text_* が電話番号/住所相当である場合の救済（必須検出由来の匿名テキスト欄）
        if field_name.startswith("auto_required_text_"):
            try:
                blob = " ".join(
                    [
                        str(field_info.get("name", "") or ""),
                        str(field_info.get("id", "") or ""),
                        str(field_info.get("class", "") or ""),
                        str(field_info.get("placeholder", "") or ""),
                        str(field_info.get("best_context_text", "") or ""),
                    ]
                ).lower()
                phone_hints = ["tel", "phone", "telephone", "mobile", "携帯", "電話"]
                if any(h in blob for h in phone_hints):
                    phv = self.field_combination_manager.get_field_value_for_type(
                        "電話番号", "single", client_data
                    )
                    return _format_phone(phv or value)
                # 住所（市区町村/番地・建物など）の簡易判定
                client = (
                    client_data.get("client")
                    if isinstance(client_data, dict) and "client" in client_data
                    else client_data
                )
                city_hints = ["市区町村", "市区", "city", "郡", "区", "町", "town"]
                detail_hints = [
                    "番地",
                    "丁目",
                    "建物",
                    "building",
                    "マンション",
                    "ビル",
                    "部屋",
                    "room",
                    "apt",
                    "apartment",
                    "号室",
                    "詳細",
                    # 属性名・英語表記による分割住所ヒント
                    "addr2",
                    "addr_2",
                    "addr",
                    "address2",
                    "address_2",
                    "address-line2",
                    "addressline2",
                    "line2",
                    "line_2",
                    "street2",
                    "street",
                ]
                # None 安全化ユーティリティ
                def _nz(x: Any) -> str:
                    try:
                        return "" if x is None else str(x)
                    except Exception:
                        return ""

                # 衝突回避: city/detail 両ヒット時はヒット強度で判定（同点なら city 優先）
                _has_city = any(h.lower() in blob for h in city_hints)
                _has_detail = any(h.lower() in blob for h in detail_hints)
                if _has_city or _has_detail:
                    def _hits(tokens):
                        bl = blob
                        return sum(bl.count(t.lower()) for t in tokens)
                    if _has_city and _has_detail:
                        c_hits = _hits([t.lower() for t in city_hints])
                        d_hits = _hits([t.lower() for t in detail_hints])
                        prefer_city = c_hits >= d_hits  # 同点は city
                        if prefer_city:
                            a2 = _nz((client or {}).get("address_2"))
                            a3 = _nz((client or {}).get("address_3"))
                            composed = (a2 + a3).strip()
                            if composed:
                                return composed
                        # detail 優先
                        a4 = _nz((client or {}).get("address_4"))
                        a5 = _nz((client or {}).get("address_5"))
                        composed = (a4 + ("　" if a4 and a5 else "") + a5).strip()
                        if composed:
                            return composed
                    elif _has_city:
                        a2 = _nz((client or {}).get("address_2"))
                        a3 = _nz((client or {}).get("address_3"))
                        composed = (a2 + a3).strip()
                        if composed:
                            return composed
                    else:  # _has_detail only
                        a4 = _nz((client or {}).get("address_4"))
                        a5 = _nz((client or {}).get("address_5"))
                        composed = (a4 + ("　" if a4 and a5 else "") + a5).strip()
                        if composed:
                            return composed
            except Exception:
                pass

        if field_name == "電話番号":
            phv = self.field_combination_manager.get_field_value_for_type(
                "電話番号", "single", client_data
            )
            return _format_phone(phv or value)

        # 部署名はクライアントデータから直接割当
        if field_name == "部署名":
            try:
                client = (
                    client_data.get("client")
                    if isinstance(client_data, dict) and "client" in client_data
                    else client_data
                )
                dep = str((client or {}).get("department", "") or "").strip()
                return dep
            except Exception:
                return value

        if field_name == "都道府県":
            return self._handle_prefecture_assignment(field_info, client_data)

        # 本文は常に確定値を適用（フォールバック条件に依存しない）
        if field_name == "お問い合わせ本文":
            if isinstance(client_data, dict):
                t = client_data.get("targeting", {})
                msg = t.get("message", "")
                if msg:
                    return msg
        # 統合氏名は組み合わせ値を使用
        if field_name == "統合氏名":
            full = self.field_combination_manager.generate_combined_value(
                "full_name", client_data
            )
            if full:
                return full
        # 統合氏名カナは種別判定のうえ確定値を生成
        if field_name == "統合氏名カナ":
            kana_type = "katakana"
            try:
                # 1) コンテキスト
                ctx = field_info.get("best_context_text") or ""
                if not ctx and isinstance(field_info.get("context"), list):
                    ctx = next(
                        (
                            c.get("text", "")
                            for c in field_info["context"]
                            if isinstance(c, dict) and c.get("text")
                        ),
                        "",
                    )
                ctx_blob = str(ctx)
                if ("ひらがな" in ctx_blob) or ("hiragana" in ctx_blob.lower()):
                    kana_type = "hiragana"
                else:
                    # 2) プレースホルダーの文字種
                    placeholder = str(field_info.get("placeholder", "") or "")

                    def _has_hiragana(s: str) -> bool:
                        return any("ぁ" <= ch <= "ゖ" for ch in s)

                    def _has_katakana(s: str) -> bool:
                        return any("ァ" <= ch <= "ヺ" or ch == "ー" for ch in s)

                    if placeholder:
                        if _has_hiragana(placeholder) and not _has_katakana(
                            placeholder
                        ):
                            kana_type = "hiragana"
                        elif _has_katakana(placeholder) and not _has_hiragana(
                            placeholder
                        ):
                            kana_type = "katakana"
                    # 3) name/id/class のヒント
                    if kana_type == "katakana":
                        blob = " ".join(
                            [
                                str(field_info.get("name", "") or ""),
                                str(field_info.get("id", "") or ""),
                                str(field_info.get("class", "") or ""),
                            ]
                        ).lower()
                        if "hiragana" in blob:
                            kana_type = "hiragana"
            except UnicodeError as e:
                logger.warning(f"Unicode error in kana detection: {e}")
                kana_type = "katakana"
            except Exception as e:
                logger.error(f"Unexpected error in kana type detection: {e}")
                kana_type = "katakana"
            # 早期returnせず、生成値を value に格納してフォールバック判定へ進める
            value = self.field_combination_manager.generate_unified_kana_value(
                kana_type, client_data
            )

        # 住所/住所_補助* の文脈に応じた割当
        if field_name.startswith("住所"):
            addr = self._handle_address_assignment(field_name, field_info, client_data)
            if addr:
                return addr

        # 共通フォールバック（P1: 早期returnにより未実行だった問題を解消）
        if not value:
            # 住所補助は必須時のみ全角スペース、任意は空文字
            if str(field_name).startswith("住所_補助"):
                if self.required_analysis.get("treat_all_as_required", False) or field_info.get("required", False):
                    value = "　"  # 全角スペース（送信ブロック回避）
                else:
                    value = ""
            else:
                # すべて必須扱い、または当該フィールドが必須なら全角スペース
                if self.required_analysis.get("treat_all_as_required", False) or field_info.get("required", False):
                    value = "　"
                else:
                    value = ""

        return value

    def _handle_prefecture_assignment(
        self, field_info: Dict[str, Any], client_data: Dict[str, Any]
    ) -> str:
        """都道府県フィールドへの値割り当て。
        - クライアントデータの `address_1` を最優先
        - 異常時は空文字を返す
        """
        try:
            client = (
                client_data.get("client")
                if isinstance(client_data, dict) and "client" in client_data
                else client_data
            )
            pref = (client or {}).get("address_1", "")
            return str(pref or "").strip()
        except Exception as e:
            logger.debug(f"prefecture assignment skipped: {e}")
            return ""

    def _handle_address_assignment(
        self, field_name: str, field_info: Dict[str, Any], client_data: Dict[str, Any]
    ) -> str:
        """住所関連フィールドへの値割り当て（文脈駆動）。
        - 住所_補助*, 市区町村、番地/建物などの文脈を見て適切に構成
        - デフォルトは住所全体
        """
        try:

            def _client() -> Dict[str, Any]:
                return (
                    client_data.get("client")
                    if isinstance(client_data, dict) and "client" in client_data
                    else client_data
                )

            client = _client()
            blob = ""
            try:
                best = field_info.get("best_context_text") or ""
            except Exception:
                best = ""
            parts = [
                field_info.get("name", ""),
                field_info.get("id", ""),
                field_info.get("class", ""),
                field_info.get("placeholder", ""),
                best,
            ]
            blob = " ".join([p for p in parts if p]).lower()

            city_tokens = [
                "市区町村", "市区", "郡", "市", "city", "locality", "p-locality",
                "区", "町", "town"
            ]  # 『丁目』は番地系（詳細）に属するため除外
            detail_tokens = [
                "番地",
                "丁目",
                "建物",
                "building",
                "マンション",
                "ビル",
                "部屋",
                "room",
                "apt",
                "apartment",
                "号室",
                "詳細",
                # 属性名・英語表記による分割住所ヒント（addr/address2/line2 等）
                "adrs",
                "addr2",
                "addr_2",
                "address2",
                "address_2",
                "street-address",
                "extended-address",
                "p-street-address",
                "p-extended-address",
                "address-line2",
                "addressline2",
                "line2",
                "line_2",
                "street2",
                "street",
            ]
            pref_tokens = ["都道府県", "prefecture", "県", "都", "府"]
            # 都道府県専用でない場合を除外するキーワード（一般住所欄を示すもの）
            non_pref_keywords = ["以下", "以降", "から", "まで", "を入力", "番地", "丁目"]

            def join_nonempty(parts, sep=""):
                return sep.join([p for p in parts if p])

            # 都道府県トークンがあっても、一般住所欄の可能性がある場合は除外
            if any(t in blob for t in pref_tokens):
                # 除外キーワードがある場合は都道府県専用ではない
                is_general_address = any(keyword in blob for keyword in non_pref_keywords)
                if not is_general_address:
                    v = client.get("address_1", "")
                    if v:
                        return v
            # 衝突回避: city/detail 両ヒット時はヒット強度で判定（同点なら city 優先）
            _has_city = any(t in blob for t in city_tokens)
            _has_detail_flag = field_name.startswith("住所_補助") or any(
                t in blob for t in detail_tokens
            )
            if _has_city or _has_detail_flag:
                def _hits(tokens):
                    return sum(blob.count(t) for t in tokens)
                if _has_city and _has_detail_flag:
                    c_hits = _hits(city_tokens)
                    d_hits = _hits(detail_tokens)
                    prefer_city = c_hits >= d_hits  # 同点は city
                    if prefer_city:
                        v = join_nonempty(
                            [client.get("address_2", ""), client.get("address_3", "")]
                        )
                        if v:
                            return v
                    v = join_nonempty(
                        [client.get("address_4", ""), client.get("address_5", "")], "　"
                    )
                    if v:
                        return v
                elif _has_city:
                    v = join_nonempty(
                        [client.get("address_2", ""), client.get("address_3", "")]
                    )
                    if v:
                        return v
                else:
                    v = join_nonempty(
                        [client.get("address_4", ""), client.get("address_5", "")], "　"
                    )
                    if v:
                        return v
            # デフォルトは住所全体
            full_addr = self.field_combination_manager.generate_combined_value(
                "address", client_data
            )
            return full_addr or ""
        except Exception as e:
            logger.debug(f"address assignment skipped: {e}")
            return ""

    def _fix_name_selector_mismatch(
        self,
        field_mapping: Dict[str, Dict[str, Any]],
        input_assignments: Dict[str, Any],
    ) -> None:
        """
        一般的なフォームで見られる 'sei' / 'mei'（および *_kana）入れ違いを補正する。
        - 例: 『姓』が #mei、『名』が #sei を指しているケース
        - 例: 『姓カナ』が #mei_kana、『名カナ』が #sei_kana を指しているケース
        汎用ヒューリスティクスのみを用い、他のケースに影響しないよう限定的に適用。
        """

        def sel(name: str) -> str:
            return (field_mapping.get(name, {}).get("selector") or "").lower()

        def swap(a: str, b: str) -> None:
            if a in input_assignments and b in input_assignments:
                input_assignments[a]["value"], input_assignments[b]["value"] = (
                    input_assignments[b]["value"],
                    input_assignments[a]["value"],
                )
                logger.info(f"Auto-corrected value assignment swap: {a} <-> {b}")

        def _is_sei_mei_mismatch(sei_sel: str, mei_sel: str) -> bool:
            # より厳格な判定（典型トークンの相互混入）
            sei_patterns = ["sei", "last", "family"]
            mei_patterns = ["mei", "first", "given"]
            if not sei_sel or not mei_sel:
                return False
            sei_in_mei = any(p in mei_sel for p in sei_patterns)
            mei_in_sei = any(p in sei_sel for p in mei_patterns)
            return (
                sei_in_mei
                and mei_in_sei
                and ("kana" not in sei_sel and "kana" not in mei_sel)
            )

        def _is_sei_mei_kana_mismatch(sei_sel: str, mei_sel: str) -> bool:
            # カナの入れ違い（厳格に *_kana を含むか確認）
            if not sei_sel or not mei_sel:
                return False
            return ("mei" in sei_sel and "sei" in mei_sel) and (
                "kana" in sei_sel and "kana" in mei_sel
            )

        # 1) 漢字の姓/名
        last_sel, first_sel = sel("姓"), sel("名")
        if _is_sei_mei_mismatch(last_sel, first_sel):
            swap("姓", "名")

        # 2) カナの姓/名
        last_kana_sel, first_kana_sel = sel("姓カナ"), sel("名カナ")
        if _is_sei_mei_kana_mismatch(last_kana_sel, first_kana_sel):
            swap("姓カナ", "名カナ")

    def _enforce_name_values(
        self, input_assignments: Dict[str, Any], client_data: Dict[str, Any]
    ) -> None:
        """
        姓/名/カナの値はクライアントデータからの確定値を採用して上書きする。
        - マッピング段階の軽微な取り違えの影響を排除（安全・汎用）
        """
        client = client_data.get("client", {}) if isinstance(client_data, dict) else {}
        mapping = {
            "姓": client.get("last_name", ""),
            "名": client.get("first_name", ""),
            "姓カナ": client.get("last_name_kana", ""),
            "名カナ": client.get("first_name_kana", ""),
        }
        for k, v in mapping.items():
            if k in input_assignments and v:
                input_assignments[k]["value"] = v
                logger.info(f"Enforced canonical value for '{k}' from client data")
