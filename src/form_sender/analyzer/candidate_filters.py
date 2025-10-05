from __future__ import annotations

"""候補要素フィルタ（FieldMapper._score_element_in_detail での早期除外を分離）。"""

from typing import Any, Dict
import re
from playwright.async_api import Locator
from config.manager import get_prefectures


async def allow_candidate(field_name: str, element: Locator, element_info: Dict[str, Any]) -> bool:
    """フィールド固有の早期不採用判定。

    - 住所×select の誤検出抑止: 都道府県名が十分含まれない select は除外
    - 性別×select の誤検出抑止: 男女表現が両方なければ除外
    """
    try:
        # 共通除外: 罠/スパム対策系フィールドや不可視要素は早期除外
        name_id_cls = " ".join([
            str(element_info.get("name") or ""),
            str(element_info.get("id") or ""),
            str(element_info.get("class") or ""),
        ]).lower()
        # 罠/ダミー系の典型トークン（属性に含まれていれば早期除外）
        trap_tokens = [
            "honeypot",
            "honey",
            "trap",
            "botfield",
            "no-print",
            "noprint",
            # よくあるダミーフィールド表現
            "dummy",
        ]
        if any(t in name_id_cls for t in trap_tokens):
            return False
        if not bool(element_info.get("visible", True)):
            return False

        # スタイル属性に基づく不可視/非操作要素の早期除外
        # 代表例: pointer-events:none / opacity:0 （数値0のみ厳密判定）/ display:none / visibility:hidden / 画面外配置
        try:
            raw_style = (element_info.get("style") or "")
            style = raw_style.replace(" ", "").lower()
        except Exception:
            raw_style = style = ""
        # opacity は 0.5 などの部分一致を避け、数値を抽出して 0 のときだけマークする
        _OPACITY_RE = re.compile(r"opacity\s*:\s*([0-9]*\.?[0-9]+)", re.IGNORECASE)
        def _opacity_is_zero(s: str) -> bool:
            try:
                m = _OPACITY_RE.search(s or "")
                return bool(m and float(m.group(1)) == 0.0)
            except Exception:
                return False
        if style:
            hidden_signals = (
                "display:none" in style
                or "visibility:hidden" in style
                or "pointer-events:none" in style
                or _opacity_is_zero(raw_style)
                or "z-index:-1" in style
                or ("position:absolute" in style and ("left:-9999px" in style or "top:-9999px" in style))
            )
            if hidden_signals:
                return False

        # 都道府県: select の場合は option に都道府県名が一定数含まれることを要求
        if field_name == "都道府県":
            tag = (element_info.get("tag_name") or "").lower()
            if tag == "select":
                try:
                    options = await element.evaluate(
                        "el => Array.from(el.options).map(o => (o.textContent||'').trim())"
                    )
                except Exception:
                    options = []
                names = (get_prefectures() or {}).get("names", [])
                hits = 0
                low_opts = [str(o).lower() for o in options]
                for n in names or []:
                    if str(n).lower() in low_opts:
                        hits += 1
                        if hits >= 5:
                            break
                if hits < 5 and not any("都道府県" in (o or "") for o in options):
                    return False

        if field_name == "住所":
            tag = (element_info.get("tag_name") or "").lower()
            if tag == "select":
                try:
                    options = await element.evaluate(
                        "el => Array.from(el.options).map(o => (o.textContent||'').trim())"
                    )
                except Exception:
                    options = []
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
                if hits < 5 and not any("都道府県" in (o or "") for o in options):
                    return False
            # 入力欄の場合、建物名・部屋番号のみを示唆するプレースホルダ/属性は除外
            ph = str(element_info.get("placeholder") or "").lower()
            name_id_cls = " ".join([
                str(element_info.get("name") or ""),
                str(element_info.get("id") or ""),
                str(element_info.get("class") or ""),
            ]).lower()
            neg = [
                "建物名", "建物", "マンション", "アパート", "部屋番号", "号室", "room", "apartment", "building",
                # 部署/部門系（住所の取り違えを抑止）
                "部署", "部門", "課", "係", "department", "dept", "division", "section", "team",
                # ふりがな・カナ系（住所→フリガナの取り違え抑止）
                "フリガナ", "ふりがな", "カナ", "kana", "katakana", "hiragana", "セイ", "メイ",
            ]
            if any(t in ph for t in neg) or any(t in name_id_cls for t in neg):
                return False
            # 住所に無関係な『注文番号』等のトークンは除外
            order_like = ["注文番号", "order number", "受注番号", "予約番号", "伝票番号", "受付番号", "お問い合わせ番号", "tracking number"]
            if any(t in ph for t in order_like) or any(t in name_id_cls for t in order_like):
                return False

        if field_name == "性別":
            tag = (element_info.get("tag_name") or "").lower()
            if tag == "select":
                try:
                    opt_texts = await element.evaluate(
                        "el => Array.from(el.options).map(o => (o.textContent||'').trim().toLowerCase())"
                    )
                except Exception:
                    opt_texts = []
                male = any(k in (t or "") for t in opt_texts for k in ["男", "男性", "male"])
                female = any(k in (t or "") for t in opt_texts for k in ["女", "女性", "female"])
                if not (male and female):
                    return False
    except Exception:
        pass

    return True
