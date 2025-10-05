import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

from playwright.async_api import Locator, Page
from config.manager import get_privacy_consent_config

logger = logging.getLogger(__name__)


@dataclass
class Candidate:
    element: Locator
    label: Optional[Locator]
    role_el: Optional[Locator]
    score: float
    distance: float
    text: str


class PrivacyConsentHandler:
    """プライバシー/個人情報取り扱いへの同意チェックを確実に行うハンドラ

    特徴:
    - マッピングと独立した専用機構（常時オン）
    - 送信ボタン/確認ボタン近傍のチェックボックスに優先度付け
    - ラベル/周辺テキスト/アクセシブルネームから語彙マッチ＋スコアリング
    - Playwright操作失敗時のJSクリック/ラベルクリック/role=checkboxへのフォールバック
    """

    @classmethod
    async def ensure_near_button(
        cls, page: Page, button: Locator, context_hint: str = "submit"
    ) -> bool:
        """ボタン近傍の同意チェックを確実にONにする

        Args:
            page: Playwright Page
            button: 送信/確認ボタンのLocator
            context_hint: ログ用ヒント文字列

        Returns:
            True if at least one likely consent checkbox was successfully checked
        """
        cfg = self.__class__._load_config_safe()

        # feature flags / parameters
        enabled: bool = bool(cfg.get("enabled", True))
        if not enabled:
            logger.debug("Privacy consent handler disabled by config")
            return False

        log_only: bool = bool(cfg.get("log_only_mode", False))
        proximity_px: int = int(cfg.get("proximity_px", 600))
        max_scan: int = int(cfg.get("max_scan_candidates", 20))
        max_to_check: int = int(cfg.get("max_to_check", 2))
        min_score: float = float(cfg.get("min_score", 2.0))
        within_form_only: bool = bool(cfg.get("ensure_within_same_form", True))
        vertical_offset: int = int(cfg.get("vertical_offset_px", 30))

        # 送信ボタンのバウンディングボックスを取得
        try:
            await button.scroll_into_view_if_needed()
            btn_box = await button.bounding_box()
        except Exception:
            btn_box = None

        # 検索範囲の決定
        form_scope: Optional[Locator] = None
        if within_form_only:
            try:
                form_scope = button.locator("xpath=ancestor::form[1]")
                if await form_scope.count() == 0:
                    form_scope = None
            except Exception:
                form_scope = None

        scope = form_scope if form_scope is not None else page.locator("body")

        # 候補の収集: input[type=checkbox], role=checkbox の双方
        input_boxes = scope.locator("input[type=checkbox]")
        role_boxes = scope.get_by_role("checkbox")

        candidates: List[Candidate] = []
        await cls._collect_candidates(scope, input_boxes, btn_box, proximity_px, max_scan, cfg, candidates, is_role=False)
        await cls._collect_candidates(scope, role_boxes, btn_box, proximity_px, max_scan, cfg, candidates, is_role=True)

        if not candidates:
            return False

        # 近さとスコアでソート（スコア優先、同点は距離が近い方）
        candidates.sort(key=lambda c: (-c.score, c.distance))

        checked_any = False
        checked_count = 0
        for cand in candidates:
            try:
                # 垂直方向でボタンの上側を優先
                if btn_box is not None:
                    el_box = await cand.element.bounding_box()
                    if el_box and el_box.get("y", 0) > btn_box.get("y", 0) + vertical_offset:
                        # ボタンより十分下にある要素は低優先（無視）
                        continue

                # 最低スコアを満たさない候補は無視
                if cand.score < min_score:
                    continue

                if await cand.element.is_checked():
                    continue

                await cand.element.scroll_into_view_if_needed()
                if not log_only:
                    await cls._safe_click_with_retry(cand.element, cand.label, max_attempts=int(cfg.get("max_attempts", 3)))

                # 検証
                try:
                    if log_only or await cand.element.is_checked():
                        checked_any = True
                        checked_count += 1
                        logger.info(
                            f"Privacy consent checkbox turned ON near {context_hint} (score={cand.score:.2f}, dist={cand.distance:.0f}px)"
                        )
                        if checked_count >= max_to_check:
                            break
                except Exception:
                    continue
            except Exception:
                continue

        return checked_any

    # ===== helpers =====
    @staticmethod
    def _load_config_safe() -> Dict[str, Any]:
        try:
            cfg = get_privacy_consent_config()
            if not isinstance(cfg, dict):
                raise ValueError("privacy consent config must be dict")
            # 軽量バリデーションとデフォルト
            cfg.setdefault("proximity_px", 600)
            cfg.setdefault("max_scan_candidates", 20)
            cfg.setdefault("min_score", 2.0)
            cfg.setdefault("ensure_within_same_form", True)
            cfg.setdefault("vertical_offset_px", 30)
            cfg.setdefault("enabled", True)
            cfg.setdefault("log_only_mode", False)
            cfg.setdefault("max_to_check", 2)
            return cfg
        except Exception:
            return {
                "keywords": {
                    "must": ["同意", "consent", "agree"],
                    "context": [
                        "個人情報", "プライバシ", "privacy", "policy", "個人データ", "terms", "規約", "取扱"
                    ],
                    "negative": ["メルマガ", "newsletter", "配信", "案内", "広告", "キャンペーン"],
                },
                "proximity_px": 600,
                "max_scan_candidates": 20,
                "ensure_within_same_form": True,
                "vertical_offset_px": 30,
                "enabled": True,
                "log_only_mode": False,
                "max_to_check": 2,
                "log_level": "info",
            }

    @classmethod
    async def _collect_candidates(
        cls,
        scope: Locator,
        src: Locator,
        btn_box: Optional[Dict[str, Any]],
        proximity_px: int,
        max_scan: int,
        cfg: Dict[str, Any],
        out: List[Candidate],
        is_role: bool = False,
    ) -> None:
        try:
            count = await src.count()
            for i in range(min(count, max_scan)):
                el = src.nth(i)
                try:
                    if not await el.is_visible():
                        continue
                except Exception:
                    continue

                if is_role:
                    label = None
                    name = await el.get_attribute("aria-label")
                    text = name or (await el.text_content() or "")
                    if len(text) < 6:
                        more = await cls._collect_context_text(el, None)
                        text = (text + " " + more).strip()
                else:
                    label = await cls._find_label_for_checkbox(scope, el)
                    text = await cls._collect_context_text(el, label)

                dist = await cls._distance_to(btn_box, el)
                score = cls._score(text, dist, proximity_px, cfg)
                if score > 0:
                    out.append(Candidate(el, label, el if is_role else None, score, dist, text))
        except Exception:
            return

    @staticmethod
    async def _safe_click_with_retry(element: Locator, label: Optional[Locator], max_attempts: int = 3) -> None:
        attempts = 0
        while attempts < max_attempts:
            attempts += 1
            try:
                await element.check()
                if await element.is_checked():
                    return
            except Exception:
                pass
            if label is not None:
                try:
                    await label.click()
                    if await element.is_checked():
                        return
                except Exception:
                    pass
            try:
                await element.evaluate("el => el.click()")
                if await element.is_checked():
                    return
            except Exception:
                pass
        # 非ブロッキング: 最終失敗でも例外を投げない
    @staticmethod
    async def _find_label_for_checkbox(scope: Locator, checkbox: Locator) -> Optional[Locator]:
        # for 属性
        try:
            el_id = await checkbox.get_attribute("id")
            if el_id:
                lbl = scope.locator(f"label[for='{el_id}']")
                if await lbl.count():
                    return lbl.first
        except Exception:
            pass

        # 親がlabel
        try:
            parent_label = checkbox.locator("xpath=ancestor::label[1]")
            if await parent_label.count():
                return parent_label.first
        except Exception:
            pass

        # 近接テキスト: 同じ行/周辺
        try:
            sib = checkbox.locator("xpath=following-sibling::*[1]")
            if await sib.count():
                return sib.first
        except Exception:
            pass
        return None

    @staticmethod
    async def _collect_context_text(checkbox: Locator, label: Optional[Locator]) -> str:
        text_parts: List[str] = []
        try:
            if label is not None:
                t = await label.inner_text()
                if t:
                    text_parts.append(t)
        except Exception:
            pass
        try:
            t = await checkbox.get_attribute("aria-label")
            if t:
                text_parts.append(t)
        except Exception:
            pass
        # さらに親要素の短いテキストも取り込む
        try:
            parent_text = await checkbox.evaluate(
                "(el) => {\n"
                "  const maxLen = 160;\n"
                "  let cur = el.parentElement;\n"
                "  while (cur && cur !== document.body) {\n"
                "    const txt = (cur.innerText||'').trim();\n"
                "    if (txt && txt.length < maxLen) return txt;\n"
                "    cur = cur.parentElement;\n"
                "  }\n"
                "  return '';\n"
                "}"
            )
            if parent_text:
                text_parts.append(parent_text)
        except Exception:
            pass
        return " ".join(text_parts)[:320]

    @staticmethod
    async def _distance_to(btn_box: Optional[Dict[str, Any]], el: Locator) -> float:
        try:
            if not btn_box:
                return 99999.0
            box = await el.bounding_box()
            if not box:
                return 99999.0
            # 垂直方向重視
            dy = abs((btn_box.get("y", 0) or 0) - (box.get("y", 0) or 0))
            dx = abs((btn_box.get("x", 0) or 0) - (box.get("x", 0) or 0))
            return dy + min(dx * 0.2, 50)
        except Exception:
            return 99999.0

    @staticmethod
    def _score(text: str, distance: float, proximity_px: int, cfg: Dict[str, Any]) -> float:
        text_l = (text or "").lower()
        kws = cfg.get("keywords", {})
        must: Sequence[str] = [str(x).lower() for x in kws.get("must", [])]
        context: Sequence[str] = [str(x).lower() for x in kws.get("context", [])]
        negative: Sequence[str] = [str(x).lower() for x in kws.get("negative", [])]

        # negativeワードに強く反応（メルマガなどを除外）
        if any(n in text_l for n in negative):
            return 0.0

        base = 0.0
        if any(m in text_l for m in must):
            base += 2.5
        ctx_hits = sum(1 for c in context if c in text_l)
        base += min(ctx_hits * 1.2, 3.0)

        # リンクが含まれると加点（policy等へのリンク）
        if re.search(r"privacy|policy|個人情報|プライバシ", text_l):
            base += 0.5

        # 近接スコア（距離0で+2.0、proximity_pxで0）
        if distance < proximity_px:
            prox = max(0.0, 1.0 - (distance / max(proximity_px, 1)))
            base += prox * 2.0

        return base
