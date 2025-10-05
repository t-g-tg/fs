"""
営業禁止文言検出システム（Form Sender版）

Form Analyzerから移植した高度な営業禁止文言検出機能。
74種類のキーワードパターンと48種類の除外パターンによる精密な検出を実現。
"""

import logging
import re
import time
import unicodedata
import hashlib
import threading
from collections import OrderedDict
from functools import lru_cache
from typing import List, Tuple, Optional, Dict, Any

from bs4 import BeautifulSoup, Comment
try:
    # 一部の環境でのみ提供
    from bs4 import FeatureNotFound
except Exception:  # pragma: no cover
    FeatureNotFound = Exception

from config.manager import get_worker_config

logger = logging.getLogger(__name__)


# --- Lightweight result cache (HTML-hash keyed, shared across instances) ---
# 目的: Analyzer と SuccessJudge 間で同一HTMLの重複解析を避ける
# キー: 正規化前の HTML の SHA1（十分に高速・衝突耐性は実用上問題なし）
# 値: (detected: bool, phrases: List[str], level: str, score: float)
_RESULT_CACHE: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
_CACHE_LOCK = threading.Lock()


def _get_cache_limits() -> Tuple[int, int]:
    """worker_config からキャッシュ制限を取得（max_entries, ttl_sec）。"""
    try:
        det = get_worker_config().get('detectors', {}).get('prohibition', {})
        c = det.get('cache', {}) if isinstance(det.get('cache', {}), dict) else {}
        max_entries = int(c.get('max_entries', 256))
        ttl_sec = int(c.get('ttl_seconds', 120))
        # 下限安全弁
        return max(32, max_entries), max(10, ttl_sec)
    except Exception:
        return 256, 120


def _make_cache_key(html: str) -> str:
    try:
        h = hashlib.sha1(html.encode('utf-8', errors='ignore')).hexdigest()
        return h
    except Exception:
        return str(len(html) if html is not None else 0)


def _cache_get(key: str) -> Optional[Tuple[bool, List[str], str, float]]:
    max_entries, ttl_sec = _get_cache_limits()
    now = time.time()
    with _CACHE_LOCK:
        ent = _RESULT_CACHE.get(key)
        if not ent:
            return None
        ts = ent.get('ts', 0)
        if now - ts > ttl_sec:
            # 期限切れ
            try:
                _RESULT_CACHE.pop(key, None)
            except Exception:
                pass
            return None
        # LRU 更新
        try:
            _RESULT_CACHE.move_to_end(key)
        except Exception:
            pass
        val = ent.get('val')
        # バリデーション
        if not isinstance(val, (tuple, list)) or len(val) != 4:
            return None
        return val  # type: ignore[return-value]


def _cache_set(key: str, value: Tuple[bool, List[str], str, float]) -> None:
    max_entries, _ = _get_cache_limits()
    with _CACHE_LOCK:
        try:
            _RESULT_CACHE[key] = {'val': value, 'ts': time.time()}
            _RESULT_CACHE.move_to_end(key)
            # 上限超過時は古いものから削除
            while len(_RESULT_CACHE) > max_entries:
                try:
                    _RESULT_CACHE.popitem(last=False)
                except Exception:
                    break
        except Exception:
            # キャッシュ失敗は無視して続行
            pass


class ProhibitionDetector:
    """営業禁止文言の検出ロジックをカプセル化するクラス（Form Sender統合版）"""

    def __init__(self):
        """営業禁止検出器の初期化"""
        self.EXCLUSION_PATTERNS = [
            # 営業関連の正当な使用パターン（除外対象）
            "営業日", "営業時間", "営業所", "営業部", "営業課", "営業担当", "営業マン", "営業員", "営業職",
            "営業実績", "営業成績", "営業利益", "営業収益", "営業報告", "営業会議", "営業戦略", "営業方針",
            "営業ノウハウ", "営業スキル", "営業力", "営業中", "営業再開", "営業停止", "営業休止",
            "営業開始", "営業終了", "営業年数", "営業経験", "営業歴", "営業拠点", "営業店舗", "営業エリア",
            "営業地域", "営業範囲", "営業区域", "営業車", "営業車両", "営業用", "営業向け", "営業秘密",
            "営業機密", "営業情報", "営業データ", "営業資料", "営業ツール", "営業支援", "営業システム",
            "営業管理", "営業統計", "営業分析", "営業指標", "営業目標", "営業計画", "営業予算", "営業費用",
            "営業コスト", "営業効率", "営業生産性", "営業品質", "営業サービス", "営業対応", "営業窓口",
            "営業チーム", "営業組織", "営業体制", "営業強化", "営業拡大", "営業促進", "営業推進",
            "営業改善", "営業革新", "営業改革", "営業最適化", "営業効果", "営業結果", "営業成果",
            "営業業績", "営業実態", "営業状況", "営業環境", "営業市場", "営業競争", "営業優位",
            "営業価値", "営業価格", "営業単価", "営業金額", "営業売上", "営業収入", "営業損益",
            "営業黒字", "営業赤字", "営業キャッシュフロー",

            # 詐欺防止・セキュリティ関連（正当な注意喚起）
            "なりすまし", "詐欺", "偽サイト", "フィッシング", "悪質", "不審", "偽装", "模倣",
            "違法", "不正", "注意喚起", "警戒", "被害", "トラブル", "セキュリティ",

            # サービス案内・顧客対応関連（正当なサービス説明）
            "お客様", "カスタマー", "サポート", "ヘルプ", "サービス", "お問い合わせ窓口",
            "相談窓口", "受付窓口", "案内", "説明", "ガイド", "マニュアル", "手順", "方法",
            "利用方法", "使用方法", "操作方法", "設定方法",

            # プライバシー・法務関連（正当な規約・方針）
            "個人情報", "プライバシー", "プライバシーポリシー", "個人情報保護", "データ保護",
            "利用規約", "サービス利用規約", "約款", "規約", "方針", "ポリシー", "ガイドライン",
            "法的", "法律", "法令", "規則", "条例", "コンプライアンス",

            # 通常業務・運営関連（正当な業務説明）
            "運営", "管理", "システム", "メンテナンス", "更新", "改善", "品質", "向上",
            "サービス向上", "利便性", "機能", "特徴", "メリット", "効果", "実績"
        ]

        self.PROHIBITION_KEYWORDS = [
            # === 営業目的系 ===
            "営業目的", "営業を目的", "営業による", "営業のため", "営業に関する",
            "営業活動", "営業行為", "営業案内", "営業電話", "営業メール", "営業連絡", "営業訪問",
            # === セールス系 ===
            "セールス目的", "セールスを目的", "セールスのため", "セールスに関する",
            "セールス活動", "セールス行為", "セールス案内", "セールス電話", "セールスメール",
            "セールス連絡", "セールス訪問",
            # === 販売系 ===
            "販売目的", "販売を目的", "販売のため", "販売に関する",
            "販売活動", "販売行為",
            # === 勧誘系 ===
            "勧誘目的", "勧誘を目的", "勧誘による", "勧誘のため", "勧誘に関する",
            "勧誘活動", "勧誘行為", "勧誘案内", "勧誘電話", "勧誘メール", "勧誘連絡",
            # === 宣伝・広告系 ===
            "宣伝目的", "宣伝を目的", "宣伝のための", "宣伝に関する",
            "宣伝活動", "宣伝行為", "広告目的", "広告宣伝", "PR目的", "プロモーション目的",
            # === 売り込み系 ===
            "売り込み", "売込",
            # === 商業・ビジネス系 ===
            "商業目的", "商業利用", "商業的利用", "ビジネス目的", "ビジネス利用", "営利目的", "営利利用",

            # === 迷惑行為系 ===
            "迷惑行為", "迷惑電話", "スパム", "spam", "SPAM",
        ]

        self.compiled_patterns = self._build_prohibition_patterns()
        self._pattern_cache = {}

    def _build_prohibition_patterns(self):
        """営業禁止パターンを構築（否定文検出強化版）"""
        exclusion_patterns = self._get_exclusion_patterns()

        SALES_KEYWORDS = "営業|セールス|勧誘|販売"
        CONTACT_KEYWORDS = "問い合わせ|お問い合わせ|連絡|ご連絡|メール|電話|訪問"
        PROHIBITION_KEYWORDS = "お断り|断り|遠慮|禁止"
        
        # 否定文パターンを強化
        DECLINE_KEYWORDS = "できません|いたしかねます|しておりません|お受けしておりません|対応しておりません|受け付けておりません"
        POLITE_DECLINE = "お控えください|ご遠慮ください|お断りします|お断りいたします|控えていただけ|遠慮していただけ"
        NEGATIVE_FORMS = "ません|ませんので|ないため|いたしません|いたしかねます"
        
        patterns = []

        sales_with_exclusion = f"営業(?!{exclusion_patterns})"

        # 基本的な営業禁止パターン（従来版強化）
        patterns.extend([
            f"{sales_with_exclusion}.*?(?:{CONTACT_KEYWORDS}).*?(?:{PROHIBITION_KEYWORDS})",
            f"(?:{PROHIBITION_KEYWORDS}).*?{sales_with_exclusion}.*?(?:{CONTACT_KEYWORDS})",
            f"セールス.*?(?:{CONTACT_KEYWORDS}).*?(?:{PROHIBITION_KEYWORDS})",
            f"(?:{PROHIBITION_KEYWORDS}).*?セールス.*?(?:{CONTACT_KEYWORDS})",
            f"勧誘.*?(?:{CONTACT_KEYWORDS}).*?(?:{PROHIBITION_KEYWORDS})",
            f"(?:{PROHIBITION_KEYWORDS}).*?勧誘.*?(?:{CONTACT_KEYWORDS})",
            f"販売.*?(?:{CONTACT_KEYWORDS}).*?(?:{PROHIBITION_KEYWORDS})",
            f"(?:{PROHIBITION_KEYWORDS}).*?販売.*?(?:{CONTACT_KEYWORDS})",
        ])

        # 売り込み関連パターン
        patterns.extend([
            f"売り?込み.*?(?:{PROHIBITION_KEYWORDS})",
            f"(?:{PROHIBITION_KEYWORDS}).*?売り?込み",
        ])

        # 強化された否定文パターン
        patterns.extend([
            # 基本的な対応否定パターン
            f"{sales_with_exclusion}.*?(?:お受け|対応).*?(?:{DECLINE_KEYWORDS})",
            f"セールス.*?(?:お受け|対応).*?(?:{DECLINE_KEYWORDS})",
            f"勧誘.*?(?:お受け|対応).*?(?:{DECLINE_KEYWORDS})",
            f"販売.*?(?:お受け|対応).*?(?:{DECLINE_KEYWORDS})",
            
            # 丁寧な断り表現パターン
            f"{sales_with_exclusion}.*?(?:{CONTACT_KEYWORDS}).*?(?:{POLITE_DECLINE})",
            f"(?:{POLITE_DECLINE}).*?{sales_with_exclusion}.*?(?:{CONTACT_KEYWORDS})",
            f"セールス.*?(?:{CONTACT_KEYWORDS}).*?(?:{POLITE_DECLINE})",
            f"勧誘.*?(?:{CONTACT_KEYWORDS}).*?(?:{POLITE_DECLINE})",
            
            # 否定文の一般形パターン
            f"{sales_with_exclusion}.*?(?:{CONTACT_KEYWORDS}).*?(?:{NEGATIVE_FORMS})",
            f"セールス.*?(?:{CONTACT_KEYWORDS}).*?(?:{NEGATIVE_FORMS})",
            f"勧誘.*?(?:{CONTACT_KEYWORDS}).*?(?:{NEGATIVE_FORMS})",
        ])

        # 電話・メール・連絡特化パターン
        sales_keywords_with_exclusion = f"(?:{sales_with_exclusion}|セールス|勧誘|販売)"
        patterns.extend([
            f"{sales_keywords_with_exclusion}(?:電話|メール|連絡).*?(?:{PROHIBITION_KEYWORDS}|{DECLINE_KEYWORDS}|{POLITE_DECLINE})",
            f"(?:{PROHIBITION_KEYWORDS}|{DECLINE_KEYWORDS}|{POLITE_DECLINE}).*?{sales_keywords_with_exclusion}(?:電話|メール|連絡)",
        ])

        # 目的関連パターン
        commercial_keywords = f"(?:{sales_with_exclusion}|セールス|勧誘|販売|商業|営利)"
        patterns.extend([
            f"{commercial_keywords}.*?目的.*?(?:{PROHIBITION_KEYWORDS}|{DECLINE_KEYWORDS}|{POLITE_DECLINE})",
            f"(?:{PROHIBITION_KEYWORDS}|{DECLINE_KEYWORDS}|{POLITE_DECLINE}).*?{commercial_keywords}.*?目的",
        ])

        # 迷惑行為関連パターン
        patterns.extend([
            f"迷惑.*?(?:電話|連絡|行為).*?(?:{PROHIBITION_KEYWORDS})",
            f"(?:{PROHIBITION_KEYWORDS}).*?迷惑.*?(?:電話|連絡|行為)",
        ])
        
        # 新規：業務関連の間接的禁止表現
        patterns.extend([
            f"業務.*?(?:{CONTACT_KEYWORDS}).*?(?:{POLITE_DECLINE}|{DECLINE_KEYWORDS})",
            f"(?:取材|営業等?).*?(?:電話|連絡|お問い合わせ).*?(?:{DECLINE_KEYWORDS}|{POLITE_DECLINE})",
            f"(?:営業|勧誘|セールス).*?(?:等|など).*?(?:{DECLINE_KEYWORDS}|{POLITE_DECLINE})",
        ])

        # 英語サイト向けの直接禁止/丁寧否定パターンを追加（偽陰性低減）
        patterns.extend([
            # 直接的禁止
            r"\bno\s+(sales|solicitations?|cold\s*calls?|telemarketing|vendor\s+solicitations?)\b",
            r"\bno\s+vendor(s)?\s*(contact|calls|emails)\b",
            r"\bno\s+cold\s*calls?\b",
            # 受け付けない/許可しない
            r"\b(do\s*not|don't|we\s*do\s*not|we\s*don't|not)\s+(accept|take|allow|permit)\s+(sales|solicitations?|vendor\s+(contacts?|inquiries?)|cold\s*calls?|telemarketing)\b",
            r"\b(sales|solicitations?|telemarketing|cold\s*calls?|vendor\s+inquiries?)\s+(are|is)\s+(not\s+accepted|prohibited|forbidden)\b",
            r"\bunsolicited\s+(sales|offers|proposals|marketing)\s+(are|is)\s+(not\s+accepted|prohibited|forbidden)\b",
            r"\bplease\s+do\s+not\s+contact\s+us\s+for\s+(sales|marketing|business\s+proposals?)\b",
            r"\bdo\s+not\s+use\s+this\s+form\s+for\s+(sales|solicitations?)\b",
            # 緩やかな拒否
            r"\bwe\s+are\s+not\s+(accepting|taking)\s+(sales|solicitations?|vendor\s+inquiries?)\b",
        ])

        # パフォーマンス最適化：LRUキャッシュでコンパイル済みパターンをキャッシュ
        # リストをタプルに変換（ハッシュ可能にするため）
        # 並び順の違いによるキャッシュミスを避けるためソート
        patterns_tuple = tuple(sorted(patterns))
        return self._get_cached_compiled_patterns(patterns_tuple)

    @lru_cache(maxsize=256)
    def _get_cached_compiled_patterns(self, patterns_tuple):
        """
        LRUキャッシュ付き正規表現パターンコンパイル（パフォーマンス最適化）
        
        Args:
            patterns_tuple: パターンのタプル（ハッシュ可能にするため）
            
        Returns:
            list: コンパイル済み正規表現パターンのリスト
        """
        compiled_patterns = []
        for pattern in patterns_tuple:
            try:
                compiled_patterns.append(re.compile(pattern, re.IGNORECASE))
            except re.error as e:
                logger.warning(f"正規表現パターンのコンパイルに失敗: {pattern} - {e}")
                
        logger.info(f"Compiled {len(compiled_patterns)} prohibition patterns with LRU cache")
        return compiled_patterns

    def _get_exclusion_patterns(self) -> str:
        """「営業」の除外パターンを返す"""
        time_related = "日|時間|中|再開|停止|休止|開始|終了|年数|経験|歴"
        org_related = "所|部|課|担当|マン|員|職|窓口|チーム|組織|体制"
        location_related = "拠点|店舗|エリア|地域|範囲|区域"
        asset_related = "車|車両|用|向け"
        data_related = "秘密|機密|情報|データ|資料|ツール|支援|システム|管理|統計|分析"
        metrics_related = "実績|成績|利益|収益|報告|会議|指標|目標|計画|予算|費用|コスト|効率|生産性|品質"
        strategy_related = "戦略|方針|ノウハウ|スキル|力|サービス|対応|強化|拡大|促進|推進|改善|革新|改革|最適化"
        result_related = "効果|結果|成果|業績|実態|状況|環境|市場|競争|優位"
        financial_related = "価値|価格|単価|金額|売上|収入|損益|黒字|赤字|キャッシュフロー"

        return f"{time_related}|{org_related}|{location_related}|{asset_related}|{data_related}|{metrics_related}|{strategy_related}|{result_related}|{financial_related}"

    def detect(self, html_content: str) -> Tuple[bool, List[str]]:
        """営業禁止文言の検出（HTML要素限定検索対応）
        
        Args:
            html_content: 検出対象のHTMLコンテンツ
            
        Returns:
            Tuple[bool, List[str]]: (検出有無, 検出された文言のリスト)
        """
        detected, texts, _level, _score = self.detect_with_confidence(html_content)
        return detected, texts
    
    def detect_with_confidence(self, html_content: str) -> Tuple[bool, List[str], str, float]:
        """営業禁止文言の検出（信頼度付き）
        
        Args:
            html_content: 検出対象のHTMLコンテンツ
            
        Returns:
            Tuple[bool, List[str], str, float]: (検出有無, 検出された文言のリスト, 信頼度レベル, 信頼度スコア)
        """
        if not html_content:
            return False, [], "none", 0.0

        try:
            _t0 = time.perf_counter()
            # キャッシュ参照
            try:
                key = _make_cache_key(html_content)
                cached = _cache_get(key)
            except Exception:
                cached = None
            if cached is not None:
                detected_texts_cached = cached[1]
                _elapsed = (time.perf_counter() - _t0) * 1000.0
                logger.debug(
                    f"prohibition_detect_with_confidence: cache_hit texts={len(detected_texts_cached)}, conf={cached[2]}/{cached[3]:.1f}, elapsed_ms={_elapsed:.1f}"
                )
                return cached
            # オプション: 高速プリチェック（設定で有効化。偽陰性防止のためデフォルト無効）
            try:
                det_cfg = get_worker_config().get('detectors', {}).get('prohibition', {})
                fast_precheck = bool(det_cfg.get('fast_precheck_enabled', False))
            except Exception:
                fast_precheck = False
            if fast_precheck and not self._fast_precheck(html_content):
                return False, [], "none", 0.0

            # Phase 1: 重要HTML要素での限定検索（高速・高精度）
            detected_result = self._detect_context_texts_targeted_with_confidence(html_content)
            
            # Phase 2: 限定検索で見つからない場合のフォールバック（全体検索）
            if not detected_result['texts']:
                logger.debug("限定検索で検出なし、全体検索にフォールバック")
                detected_result = self._detect_context_texts_fallback_with_confidence(html_content)
            else:
                logger.debug(f"限定検索で検出完了: {len(detected_result['texts'])}件")
                
            detected_texts = detected_result['texts']
            confidence_level = detected_result['confidence']
            confidence_score = detected_result['score']
            
            if detected_texts:
                logger.info(f"営業禁止文言を検出: {len(detected_texts)}件 (信頼度: {confidence_level}, スコア: {confidence_score:.1f}%)")
                for i, text in enumerate(detected_texts[:3]):  # 最初の3件をログ出力
                    logger.info(f"検出文言{i+1}: {text[:100]}...")
            result_tuple = (len(detected_texts) > 0, detected_texts, confidence_level, confidence_score)
            # キャッシュ格納（失敗しても続行）
            try:
                _cache_set(key, result_tuple)
            except Exception:
                pass
            _elapsed = (time.perf_counter() - _t0) * 1000.0
            logger.debug(
                f"prohibition_detect_with_confidence: texts={len(detected_texts)}, conf={confidence_level}/{confidence_score:.1f}, elapsed_ms={_elapsed:.1f}"
            )
            return result_tuple
        except FeatureNotFound as e:
            logger.error(f"HTML解析エラー(FeatureNotFound): {e}", exc_info=True)
            return False, [], "error", 0.0
        except Exception as e:
            logger.error(f"HTML解析エラー({type(e).__name__}): {e}", exc_info=True)
            return False, [], "error", 0.0
    
    def _detect_context_texts_targeted_with_confidence(self, html_content: str) -> dict:
        """重要HTML要素での限定検索（信頼度付き）"""
        if not html_content:
            return {'texts': [], 'confidence': 'none', 'score': 0.0}
        
        try:
            result = self._detect_context_texts_targeted(html_content)
            if result:
                # 限定検索で発見した場合は信頼度を高く設定
                confidence_info = self._calculate_confidence_score(result, source_type="targeted")
                return {
                    'texts': result,
                    'confidence': confidence_info['level'],
                    'score': confidence_info['score']
                }
            return {'texts': [], 'confidence': 'none', 'score': 0.0}
        except Exception as e:
            logger.warning(f"限定検索（信頼度付き）でエラー: {e}")
            return {'texts': [], 'confidence': 'error', 'score': 0.0}
    
    def _detect_context_texts_fallback_with_confidence(self, html_content: str) -> dict:
        """フォールバック検索（信頼度付き）"""
        try:
            result = self._detect_context_texts_fallback(html_content)
            if result:
                # 全体検索の場合は信頼度を中程度に設定
                confidence_info = self._calculate_confidence_score(result, source_type="fallback")
                return {
                    'texts': result,
                    'confidence': confidence_info['level'],
                    'score': confidence_info['score']
                }
            return {'texts': [], 'confidence': 'none', 'score': 0.0}
        except Exception as e:
            logger.warning(f"フォールバック検索（信頼度付き）でエラー: {e}")
            return {'texts': [], 'confidence': 'error', 'score': 0.0}
    
    def _calculate_confidence_score(self, detected_texts: List[str], source_type: str = "unknown") -> dict:
        """信頼度スコアを計算"""
        if not detected_texts:
            return {'level': 'none', 'score': 0.0}
        
        base_score = 0.0
        multiplier_factors = []
        
        # ベーススコア設定
        if source_type == "targeted":
            base_score = 75.0  # 限定検索は高信頼度ベース
            multiplier_factors.append("targeted_search")
        elif source_type == "fallback":
            base_score = 60.0  # 全体検索は中信頼度ベース
        
        # 検出されたテキストの品質分析
        for text in detected_texts:
            # 否定文の存在チェック（英語も含めて強化）
            negative_patterns = [
                # 日本語
                'ません', 'できません', 'しておりません', 'お断り', 'ご遠慮', 'お控え',
                # 英語
                "do not", "don't", 'no ', 'not accept', 'not be accepted',
                'not allowed', 'not permitted', 'no cold call', 'no solicitation', 'no sales'
            ]
            if any(pattern in text for pattern in negative_patterns):
                base_score += 10.0
                if "negative_structure" not in multiplier_factors:
                    multiplier_factors.append("negative_structure")
            
            # 複数キーワードの組み合わせチェック
            sales_terms = ['営業', 'セールス', '勧誘', '販売', 'sales', 'solicitation', 'telemarketing']
            contact_terms = ['問い合わせ', '連絡', '電話', 'メール', 'contact', 'call', 'phone', 'email']
            
            sales_count = sum(1 for term in sales_terms if term in text)
            contact_count = sum(1 for term in contact_terms if term in text)
            
            if sales_count >= 1 and contact_count >= 1:
                base_score += 15.0
                if "keyword_combination" not in multiplier_factors:
                    multiplier_factors.append("keyword_combination")
        
        # 検出件数による信頼度調整
        if len(detected_texts) >= 3:
            base_score += 10.0
            multiplier_factors.append("multiple_detections")
        elif len(detected_texts) >= 2:
            base_score += 5.0
        
        # スコアの上限設定
        final_score = min(base_score, 100.0)
        
        # 信頼度レベルの決定
        if final_score >= 90.0:
            confidence_level = "high"
        elif final_score >= 70.0:
            confidence_level = "medium"
        elif final_score >= 50.0:
            confidence_level = "low"
        else:
            confidence_level = "very_low"
        
        logger.debug(f"信頼度計算: スコア={final_score:.1f}, レベル={confidence_level}, 要因={multiplier_factors}")
        
        return {
            'level': confidence_level,
            'score': final_score,
            'factors': multiplier_factors
        }

    def _detect_context_texts_targeted(self, html_content: str) -> List[str]:
        """重要HTML要素での限定検索（高速・高精度）"""
        if not html_content:
            return []
        
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 営業禁止文言が記載される可能性が高いHTML要素を定義
            target_selectors = [
                # フッター関連
                'footer', '[class*="footer"]', '[id*="footer"]',
                
                # お問い合わせ・コンタクト関連
                '[class*="contact"]', '[id*="contact"]',
                '[class*="inquiry"]', '[id*="inquiry"]',
                
                # ポリシー・規約関連
                '[class*="policy"]', '[id*="policy"]',
                '[class*="terms"]', '[id*="terms"]',
                '[class*="legal"]', '[id*="legal"]',
                
                # フォーム関連
                'form', '[class*="form"]', '[id*="form"]',
                
                # ナビゲーション・サイドバー
                'nav', '[class*="nav"]', '[id*="nav"]',
                'aside', '[class*="side"]', '[id*="side"]',
                
                # 注意・警告関連
                '[class*="notice"]', '[id*="notice"]',
                '[class*="warning"]', '[id*="warning"]',
                '[class*="alert"]', '[id*="alert"]',

                # 見出し・リスト（利用注意や禁止事項が記載されがち）
                'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li'
            ]
            
            # 対象要素からテキストを収集
            target_texts = []
            elements_found = 0
            
            for selector in target_selectors:
                elements = soup.select(selector)
                for element in elements:
                    # script/style/noscriptタグを削除
                    for tag_name in ['script', 'style', 'noscript']:
                        for tag in element.find_all(tag_name):
                            tag.decompose()
                    
                    element_text = element.get_text(separator=' ', strip=True)
                    if element_text and len(element_text) >= 10:
                        target_texts.append(element_text)
                        elements_found += 1
            
            logger.debug(f"限定検索対象要素: {elements_found}個、取得テキスト: {len(target_texts)}件")
            
            if not target_texts:
                return []
            
            # 収集したテキストを結合して検索処理
            combined_text = ' '.join(target_texts)
            cleaned_text = re.sub(r'\s+', ' ', combined_text)
            
            return self._process_text_for_prohibition_detection(cleaned_text, source_type="targeted")
            
        except Exception as e:
            logger.warning(f"限定検索でエラー: {e} - フォールバックに移行")
            return []
    
    def _detect_context_texts_fallback(self, html_content: str) -> List[str]:
        """フォールバック：全体検索（従来方式）"""
        logger.debug("全体検索を実行中...")
        return self._process_text_for_prohibition_detection(
            self._clean_html_content_for_text_extraction(html_content), 
            source_type="fallback"
        )
    
    def _process_text_for_prohibition_detection(self, cleaned_text: str, source_type: str = "unknown") -> List[str]:
        """統一されたテキスト処理による営業禁止文言検出"""
        if not cleaned_text:
            return []
            
        sentences = self._split_into_sentences(cleaned_text)
        prohibition_texts = set()

        logger.debug(f"{source_type}検索: {len(sentences)}個の文章を処理")

        for i, sentence in enumerate(sentences):
            sentence = sentence.strip()
            if len(sentence) < 10:
                continue

            logger.debug(f"文章{i}: '{sentence[:100]}...'")

            # キーワードマッチング
            for keyword in self.PROHIBITION_KEYWORDS:
                if keyword in sentence:
                    logger.debug(f"キーワード '{keyword}' を検出")
                    if not self._should_exclude_keyword(sentence, keyword):
                        logger.info(f"営業禁止文言検出（キーワード・{source_type}）: '{keyword}' in '{sentence[:50]}...'")
                        prohibition_texts.add(sentence)
                        break
                    else:
                        logger.debug(f"除外パターンにより除外: '{keyword}'")
            
            # パターンマッチング
            for pattern in self.compiled_patterns:
                match = pattern.search(sentence)
                if match:
                    matched_text = match.group(0)
                    logger.debug(f"パターンマッチ: '{matched_text}'")
                    if not self._should_exclude_pattern(sentence, matched_text):
                        logger.info(f"営業禁止文言検出（パターン・{source_type}）: '{matched_text}' in '{sentence[:50]}...'")
                        prohibition_texts.add(sentence)
                        break
                    else:
                        logger.debug(f"除外パターンにより除外: '{matched_text}'")

        return self._filter_prohibition_texts(list(prohibition_texts))

    def _detect_context_texts(self, html_content: str) -> List[str]:
        """営業禁止文言を含む文脈テキストを抽出"""
        if not html_content:
            return []

        cleaned_text = self._clean_html_content_for_text_extraction(html_content)
        sentences = self._split_into_sentences(cleaned_text)
        prohibition_texts = set()

        logger.debug(f"文章に分割: {len(sentences)}個の文章を処理")

        for i, sentence in enumerate(sentences):
            sentence = sentence.strip()
            if len(sentence) < 10:
                continue

            logger.debug(f"文章{i}: '{sentence[:100]}...'")

            # キーワードマッチング
            for keyword in self.PROHIBITION_KEYWORDS:
                if keyword in sentence:
                    logger.debug(f"キーワード '{keyword}' を検出")
                    if not self._should_exclude_keyword(sentence, keyword):
                        logger.info(f"営業禁止文言検出（キーワード）: '{keyword}' in '{sentence[:50]}...'")
                        prohibition_texts.add(sentence)
                        break
                    else:
                        logger.debug(f"除外パターンにより除外: '{keyword}'")
            
            # パターンマッチング
            for pattern in self.compiled_patterns:
                match = pattern.search(sentence)
                if match:
                    matched_text = match.group(0)
                    logger.debug(f"パターンマッチ: '{matched_text}'")
                    if not self._should_exclude_pattern(sentence, matched_text):
                        logger.info(f"営業禁止文言検出（パターン）: '{matched_text}' in '{sentence[:50]}...'")
                        prohibition_texts.add(sentence)
                        break
                    else:
                        logger.debug(f"除外パターンにより除外: '{matched_text}'")

        return self._filter_prohibition_texts(list(prohibition_texts))

    def _clean_html_content_for_text_extraction(self, html_content: str) -> str:
        """テキスト抽出用のHTMLクリーニング"""
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            # script/style/noscriptタグを削除
            for tag_name in ['script', 'style', 'noscript']:
                for tag in soup.find_all(tag_name):
                    tag.decompose()
            # コメントを削除
            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                comment.extract()
            # テキストを抽出
            text_content = soup.get_text(separator=' ', strip=True)
            # 文字正規化（全角記号/英数字 → 半角、互換統合）し、英語表現の揺れを低減
            try:
                text_content = unicodedata.normalize('NFKC', text_content)
            except Exception:
                pass
            # 英語パターン検出の頑健化（日本語への影響は軽微）
            text_content = text_content.lower()
            # 空白を正規化
            return re.sub(r'\s+', ' ', text_content)
        except Exception as e:
            logger.warning(f"HTMLクリーニングエラー: {type(e).__name__}: {e} - 空文字で返却", exc_info=True)
            return ""

    def _fast_precheck(self, html: str) -> bool:
        """軽量プリチェック：正規化済みHTMLに対する主要語彙の粗検索。"""
        try:
            text = unicodedata.normalize('NFKC', html).lower()
        except Exception:
            text = (html or '').lower()
        if len(text) < 10:
            return False
        hints = [
            '営業', 'セールス', '勧誘', '販売',
            'no sales', 'no solicitation', 'no solicitations', 'cold call', 'telemarketing', 'unsolicited'
        ]
        return any(h in text for h in hints)

    def _split_into_sentences(self, text: str) -> List[str]:
        """テキストを文章に分割"""
        if not text:
            return []
        sentence_delimiters = r'[。！？\n\r]+'
        sentences = re.split(sentence_delimiters, text)
        return [s.strip() for s in sentences if len(s.strip()) >= 10]

    def _filter_prohibition_texts(self, texts: List[str]) -> List[str]:
        """営業禁止文言テキストを品質でフィルタリング"""
        if not texts:
            return []
        filtered = [text for text in texts if self._is_high_quality_prohibition_text(text)]
        filtered.sort(key=len, reverse=True)
        return self._remove_duplicate_texts(filtered)

    def _is_high_quality_prohibition_text(self, text: str) -> bool:
        """営業禁止文言テキストの品質をチェック"""
        if not text or len(text) < 5 or len(text) > 500:
            return False
        meaningless_patterns = [r'^[\s\d\-_=+*#@\[\]\(\)]+$', r'^[a-zA-Z\s]+$', r'^\d+$']
        if any(re.match(p, text) for p in meaningless_patterns):
            return False
        has_keyword = any(keyword in text for keyword in self.PROHIBITION_KEYWORDS)
        if not has_keyword:
            has_keyword = any(pattern.search(text) for pattern in self.compiled_patterns)
        return has_keyword

    def _remove_duplicate_texts(self, texts: List[str]) -> List[str]:
        """重複や包含関係のあるテキストを除去"""
        if not texts:
            return []
        unique_texts = []
        for current_text in texts:
            is_duplicate = False
            for existing_text in unique_texts:
                if current_text in existing_text or existing_text in current_text:
                    is_duplicate = True
                    break
                if self._calculate_text_similarity(current_text, existing_text) > 0.8:
                    is_duplicate = True
                    break
            if not is_duplicate:
                unique_texts.append(current_text)
        return unique_texts

    def _calculate_text_similarity(self, text1: str, text2: str) -> float:
        """2つのテキスト間の類似度を計算"""
        if not text1 or not text2:
            return 0.0
        set1, set2 = set(text1), set(text2)
        intersection = len(set1.intersection(set2))
        union = len(set1.union(set2))
        return intersection / union if union > 0 else 0.0

    def _should_exclude_keyword(self, text: str, keyword: str) -> bool:
        """キーワードが除外パターンに該当するかチェック"""
        if "営業" not in keyword:
            return False
        for exclusion in self.EXCLUSION_PATTERNS:
            if exclusion in text:
                if not self._has_other_prohibition_keywords(text, exclusion):
                    return True
        return False

    def _should_exclude_pattern(self, text: str, matched_pattern: str) -> bool:
        """正規表現パターンが除外対象かチェック"""
        if "営業" not in matched_pattern:
            return False
        for exclusion in self.EXCLUSION_PATTERNS:
            if exclusion in text:
                if not self._has_other_prohibition_keywords(text, exclusion):
                    return True
        return False

    def _has_other_prohibition_keywords(self, text: str, exclusion_keyword: str) -> bool:
        """除外キーワード以外に営業禁止文言が含まれているかチェック"""
        if not text:
            return False
        temp_text = text.replace(exclusion_keyword, "")
        if any(keyword in temp_text for keyword in self.PROHIBITION_KEYWORDS):
            return True
        if any(pattern.search(temp_text) for pattern in self.compiled_patterns):
            return True
        return False
