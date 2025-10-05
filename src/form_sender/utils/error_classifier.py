"""
エラー分類ユーティリティ（拡張版）

目的:
- 送信失敗時のエラーを、ネットワーク/HTTP/WAF/検証/要素/CSRF/重複/禁止 などに高精度に分類。
- 既存の軽量パターン分類を維持しつつ、詳細カテゴリ/コード/再試行可否のヒントを付与可能な拡張APIを追加。

方針:
- 互換性維持: 既存の classify_error_type / classify_form_submission_error は従来どおり「文字列コード」を返す。
- 機能拡張: 新規 classify_detail は詳細な辞書を返し、将来的なDB拡張に備える（呼び出し側は必須ではない）。
- 定数管理: コアの代表パターンはコード内に保持。追加パターンは config/error_classification.json から任意ロード。

セキュリティ:
- ログ出力では機微情報を含まない（LogSanitizer 側でマスクされる前提だが、本モジュールは原則ロギング最小限）。
"""

import json
import os
import re
import logging
from typing import Dict, Any, List, Pattern, Optional, Tuple


class ErrorClassifier:
    """エラー分類用ユーティリティクラス（性能最適化版）

    目的:
    - 送信失敗時に「未入力/入力してください」などの画面上の検証メッセージを検知し、
      送信ボタン未検出系に紐づけず MAPPING/VALIDATION 系へ正しく分類する。
    - 既存の軽量パターン分類に加えて、ページコンテンツも加味した詳細分類を提供。
    """
    
    # 最適化：パターンとエラータイプの統合管理
    # (patterns, error_type, priority) のタプル形式で管理
    ERROR_PATTERN_RULES = [
        # 最高優先度: 外部要因パターン（最適化済み）
        ([
            re.compile(r'network[\s\w]*timeout', re.IGNORECASE),
            re.compile(r'server[\s\w]*error', re.IGNORECASE),
            re.compile(r'connection[\s\w]*refused', re.IGNORECASE),
            re.compile(r'site[\s\w]*maintenance', re.IGNORECASE),
            re.compile(r'cloudflare[\s\w]*protection', re.IGNORECASE),
            re.compile(r'access[\s\w]*denied', re.IGNORECASE),
            re.compile(r'page\s+load[\s\w]*timeout', re.IGNORECASE)
        ], 'EXTERNAL', 1),
        
        # 指示書構造問題パターン（最適化済み）
        ([
            re.compile(r'instruction_json[\s\w]*invalid', re.IGNORECASE),
            re.compile(r'json[\s\w]*decode[\s\w]*error', re.IGNORECASE),
            re.compile(r'placeholder[\s\w]*not[\s\w]*found', re.IGNORECASE),
            re.compile(r'missing[\s\w]*instruction', re.IGNORECASE),
            re.compile(r'invalid[\s\w]*json', re.IGNORECASE)
        ], 'INSTRUCTION', 2),
        
        # 送信ボタン関連エラーパターン（最適化済み）
        ([
            re.compile(r'submit\s*button[\s\w]*not\s*found', re.IGNORECASE),
            re.compile(r'no\s*submit\s*button[\s\w]*selector', re.IGNORECASE),
            re.compile(r'submit[\s\w]*selector[\s\w]*not[\s\w]*provided', re.IGNORECASE),
            re.compile(r'button[\s\w]*type[\s\w]*submit[\s\w]*not[\s\w]*found', re.IGNORECASE)
        ], 'SUBMIT_BUTTON', 3),
        
        # 成功判定関連エラーパターン（最適化済み）
        ([
            re.compile(r'cannot\s*determine\s*success', re.IGNORECASE),
            re.compile(r'no[\s\w]*success[\s\w]*indicators', re.IGNORECASE),
            re.compile(r'success[\s\w]*determination[\s\w]*failed', re.IGNORECASE),
            re.compile(r'no[\s\w]*clear[\s\w]*success[\s\w]*error[\s\w]*indicators', re.IGNORECASE)
        ], 'SUCCESS_DETERMINATION_FAILED', 4),
        
        # コンテンツ分析関連エラーパターン（最適化済み）
        ([
            re.compile(r'error[\s\w]*indicators[\s\w]*found[\s\w]*in[\s\w]*content', re.IGNORECASE),
            re.compile(r'no[\s\w]*url[\s\w]*change[\s\w]*detected', re.IGNORECASE),
            re.compile(r'content[\s\w]*analysis[\s\w]*failed', re.IGNORECASE),
            re.compile(r'error[\s\w]*analyzing[\s\w]*page[\s\w]*content', re.IGNORECASE)
        ], 'CONTENT_ANALYSIS', 5),
        
        # フィールド要素関連エラーパターン（最適化済み）
        ([
            re.compile(r'element[\s\w]*not[\s\w]*found[\s\w]*for', re.IGNORECASE),
            re.compile(r'selector[\s\w]*not[\s\w]*found', re.IGNORECASE),
            re.compile(r'element[\s\w]*timeout', re.IGNORECASE),
            re.compile(r'locator[\s\w]*not[\s\w]*found', re.IGNORECASE)
        ], 'ELEMENT_NOT_FOUND', 6),
        
        # 入力タイプ不一致エラーパターン（最適化済み）
        ([
            re.compile(r'cannot\s*type[\s\w]*into\s*input[\s\w]*type', re.IGNORECASE),
            re.compile(r'input[\s\w]*type[\s\w]*mismatch', re.IGNORECASE),
            re.compile(r'cannot[\s\w]*fill[\s\w]*field[\s\w]*type', re.IGNORECASE),
            re.compile(r'error[\s\w]*filling[\s\w]*field', re.IGNORECASE)
        ], 'INPUT_TYPE_MISMATCH', 7),
        
        # フォーム検証エラーパターン（最適化済み）
        ([
            re.compile(r'validation[\s\w]*error', re.IGNORECASE),
            re.compile(r'required[\s\w]*field[\s\w]*failed', re.IGNORECASE),
            re.compile(r'form[\s\w]*validation[\s\w]*failed', re.IGNORECASE),
            re.compile(r'invalid[\s\w]*input[\s\w]*value', re.IGNORECASE)
        ], 'FORM_VALIDATION_ERROR', 8)
    ]

    # フラット化したパターン配列（優先度順）
    _FLATTENED_ERROR_PATTERNS: List[Tuple[Pattern[str], str, int]] = []

    # === 追加: 詳細カテゴリ/コード判定用のパターン群 ==============================
    # ネットワーク/ブラウザ/Playwright 系
    NETWORK_TIMEOUT = re.compile(r'(timeout|timed\s*out|navigation\s*timeout|Timeout\s*\d+ms\s*exceeded)', re.IGNORECASE)
    DNS_ERROR = re.compile(r'(ERR_NAME_NOT_RESOLVED|ENOTFOUND|DNS\s*lookup\s*failed)', re.IGNORECASE)
    TLS_ERROR = re.compile(r'(SSL|TLS|CERT|CERTIFICATE|certificate\s*verify\s*failed|CERT_)', re.IGNORECASE)
    CONN_RESET = re.compile(r'(ECONNRESET|Connection\s*reset|net::ERR_CONNECTION_RESET)', re.IGNORECASE)
    PAGE_CLOSED = re.compile(r'(Target\s*closed|Execution\s*context\s*was\s*destroyed|frame\s*was\s*detached)', re.IGNORECASE)
    NOT_INTERACTABLE = re.compile(r'(not\s*visible|zero\s*size|not\s*interactable|is\s*disabled)', re.IGNORECASE)
    BLOCKED_BY_CLIENT = re.compile(r'(ERR_BLOCKED_BY_CLIENT)', re.IGNORECASE)

    # HTTP / レートリミット / 認可
    HTTP_STATUS = re.compile(r'\bHTTP\s*(\d{3})\b|\b(\d{3})\s*(Forbidden|Unauthorized|Not\s*Found|Too\s*Many\s*Requests|Service\s*Unavailable|Bad\s*Gateway)', re.IGNORECASE)
    RATE_LIMIT = re.compile(r'(rate\s*limit|too\s*many\s*requests|429)', re.IGNORECASE)
    HTTP_FORBIDDEN = re.compile(r'(403|forbidden|アクセス拒否|権限がありません)', re.IGNORECASE)
    HTTP_UNAUTHORIZED = re.compile(r'(401|unauthorized|認証が必要)', re.IGNORECASE)

    # WAF/ボット/チャレンジ
    CLOUDFLARE = re.compile(r'(cloudflare|just\s*a\s*moment|checking\s*your\s*browser|ddos\s*protection)', re.IGNORECASE)
    AKAMAI = re.compile(r'(akamai|Reference\s*#\d+\.\w+\.\w+)', re.IGNORECASE)
    INCAPSULA = re.compile(r'(incapsula|imperva)', re.IGNORECASE)
    PERIMETERX = re.compile(r'(perimeterx|px-)', re.IGNORECASE)
    HUMAN_VERIF = re.compile(r'(are\s*you\s*a\s*human|human\s*verification|verify\s*you\s*are\s*human)', re.IGNORECASE)

    # 既存: コンテンツ語彙（必須/フォーマット/CSRF/重複 など）
    # 最適化：キーワードをコンパイル済み正規表現に変更
    BOT_PATTERN = re.compile(r'\b(?:recaptcha|cloudflare|bot)\b', re.IGNORECASE)
    INSTRUCTION_KEYWORD_PATTERN = re.compile(r'\b(?:parse|decode|invalid|missing)\b', re.IGNORECASE)
    ELEMENT_KEYWORD_PATTERN = re.compile(r'\b(?:element|selector|locator)\b', re.IGNORECASE)
    INSTRUCTION_JSON_PATTERN = re.compile(r'\b(?:instruction|json)\b', re.IGNORECASE)

    # 追加: 日本語・英語の入力必須/未入力バリエーション（ページテキスト用）
    REQUIRED_TEXT_PATTERNS: List[Pattern[str]] = [
        re.compile(p) for p in [
            r"未入力",
            r"入力\s*してください",
            r"入力されていません",
            r"必須\s*項目",
            r"必須です",
            r"選択\s*してください",
            r"チェック\s*してください",
            r"空白|空欄",
            r"\bfield\s+is\s+required\b",
            r"\brequired\s+field\b",
            r"\bplease\s+(enter|select|fill)\b",
            r"\b(cannot\s+be\s+blank|must\s+not\s+be\s+empty)\b",
        ]
    ]

    # 追加: フォーマット不正バリエーション
    FORMAT_TEXT_PATTERNS: List[Pattern[str]] = [
        re.compile(p, re.IGNORECASE) for p in [
            r"形式が正しくありません",
            r"正しく入力してください",
            r"invalid\s+format",
            r"invalid\s+(email|phone|url)",
            r"メール.*(形式|正しく|無効)",
            r"phone.*(invalid|format)",
        ]
    ]

    # 追加: その他代表的エラー
    CAPTCHA_TEXT_PATTERNS: List[Pattern[str]] = [
        re.compile(r, re.IGNORECASE)
        for r in [
            r"captcha",
            r"recaptcha",
            r"私はロボットではありません",
            # 追加: reCAPTCHA UI/DOMの代表パターン（英語文言が出ないケースも検知）
            r"\brc-anchor(?:-[a-z0-9_-]+)?\b",   # .rc-anchor, .rc-anchor-pt など
            r"\bg-recaptcha\b",                  # <div class="g-recaptcha" ...>
            r"grecaptcha",                         # JS API オブジェクト/関数名
            r"recaptcha/api2/anchor",              # iframe src の一部
            r"recaptcha/api\.js",                 # スクリプトURLの一部
            r"g-recaptcha-response"                # hidden/textarea の応答フィールド
        ]
    ]
    # CSRF は誤判定防止のため「token」単独では判定しない。エラー語と近接している場合のみ検出。
    CSRF_NEAR_ERROR_PATTERNS: List[Pattern[str]] = [
        # 英語: CSRF/XSRF/forgery/authenticity + エラー語（invalid/mismatch/expired/missing/failedなど）が近接
        re.compile(r"(csrf|xsrf|forgery|authenticity)[^\n<]{0,80}(invalid|mismatch|expired|missing|required|failed|error)", re.IGNORECASE),
        # 日本語: (CSRF|ワンタイム(キー|トークン)|トークン) + エラー語（無効/不一致/期限/切れ/エラー）
        re.compile(r"(csrf|ワンタイム(?:キー|トークン)|トークン)[^\n<]{0,80}(無効|不一致|期限|切れ|エラー)")
    ]
    DUPLICATE_TEXT_PATTERNS: List[Pattern[str]] = [re.compile(r, re.IGNORECASE) for r in [r"重複", r"既に(送信|登録)", r"duplicate", r"already\s+submitted"]]

    # === 外部設定のロード =========================================================
    _external_rules_loaded: bool = False
    _external_extra_patterns: Dict[str, List[Pattern[str]]] = {}

    # 信頼度の下限（ハード下限）。必要に応じて外部設定へ拡張可能。
    MIN_CONFIDENCE: float = 0.2

    @classmethod
    def _load_external_rules(cls) -> None:
        """config/error_classification.json が存在すれば追加パターンをロード"""
        if cls._external_rules_loaded:
            return
        try:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            config_path = os.path.join(base_dir, 'config', 'error_classification.json')
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                extra: Dict[str, List[str]] = data.get('extra_patterns', {})
                compiled: Dict[str, List[Pattern[str]]] = {}
                for key, patterns in extra.items():
                    compiled[key] = []
                    for p in patterns:
                        try:
                            compiled[key].append(re.compile(p, re.IGNORECASE))
                        except re.error:
                            # 無効な正規表現は黙って無視
                            pass
                cls._external_extra_patterns = compiled
            else:
                cls._external_extra_patterns = {}
        except Exception:
            # 設定読み込みの失敗は致命ではない
            cls._external_extra_patterns = {}
        finally:
            cls._external_rules_loaded = True

        # 事前フラット化（初回のみ）
        if not cls._FLATTENED_ERROR_PATTERNS:
            flattened: List[Tuple[Pattern[str], str, int]] = []
            for patterns, error_type, priority in cls.ERROR_PATTERN_RULES:
                for p in patterns:
                    flattened.append((p, error_type, priority))
            # 既に ERROR_PATTERN_RULES が優先度順に並んでいるが、念のため priority で安定ソート
            flattened.sort(key=lambda x: x[2])
            cls._FLATTENED_ERROR_PATTERNS = flattened

    @classmethod
    def classify_error_type(cls, error_context: Dict[str, Any]) -> str:
        """
        エラータイプの分類（性能最適化版）
        
        フォーム処理の各段階で発生するエラーを詳細に分類
        
        Args:
            error_context: エラーコンテキスト情報
            
        Returns:
            str: エラータイプ
        """
        cls._load_external_rules()
        error_message = (error_context.get('error_message') or '').lower()
        is_bot_detected = error_context.get('is_bot_detected', False)
        is_timeout = error_context.get('is_timeout', False)
        page_content = (error_context.get('page_content') or '').lower()
        http_status = error_context.get('http_status')
        
        try:
            # 1. 特別なケースを先に処理
            special_case = cls._classify_special_cases(error_message, is_bot_detected, is_timeout)
            if special_case:
                return special_case
            
            # 1.5 HTTP ステータス優先（存在すれば）
            if isinstance(http_status, int):
                if http_status == 429:
                    return 'RATE_LIMIT'
                if http_status == 403:
                    # WAF/認可いずれか。コンテンツから補強
                    if cls._contains_any(page_content, [cls.CLOUDFLARE, cls.AKAMAI, cls.INCAPSULA, cls.PERIMETERX, cls.HUMAN_VERIF]):
                        return 'WAF_CHALLENGE'
                    return 'ACCESS'
                if http_status in (500, 502, 503, 504):
                    return 'SERVER_ERROR'
                if http_status == 422:
                    return 'FORM_VALIDATION_ERROR'
                if http_status == 404:
                    return 'NOT_FOUND'
                if http_status == 401:
                    return 'UNAUTHORIZED'
                if http_status == 405:
                    return 'METHOD_NOT_ALLOWED'

            # 2. パターンベースの分類（最適化済み）
            pattern_result = cls._classify_by_patterns(error_message)
            if pattern_result:
                return cls._refine_pattern_result(pattern_result, error_message)
            
            # 2.5 ネットワーク/WAF系の詳細判定
            detail = cls._classify_network_waf_detail(error_message, page_content)
            if detail:
                return detail

            # 3. フォールバック分類
            return cls._classify_fallback(error_message)
            
        except Exception as e:
            # 例外チェーンを保持して再抜出
            raise RuntimeError(f"Error classification failed: {e}") from e
    
    @classmethod
    def _classify_special_cases(cls, error_message: str, is_bot_detected: bool, is_timeout: bool) -> Optional[str]:
        """特別なケースの分類（Bot検知、タイムアウトなど）"""
        # Bot検知（確実）
        if is_bot_detected or cls.BOT_PATTERN.search(error_message):
            return 'BOT_DETECTED'
        
        # タイムアウト判定
        if is_timeout or cls.NETWORK_TIMEOUT.search(error_message):
            return 'TIMEOUT'
            
        return None
    
    @classmethod
    def _classify_by_patterns(cls, error_message: str) -> Optional[str]:
        """パターンベースの最適化された分類"""
        # 事前フラット化配列で単一ループ検索
        if not cls._FLATTENED_ERROR_PATTERNS:
            # 念のためロード（初回）
            cls._load_external_rules()
        for pattern, error_type, _priority in cls._FLATTENED_ERROR_PATTERNS:
            if pattern.search(error_message):
                return error_type
        return None
    
    @classmethod
    def _refine_pattern_result(cls, pattern_result: str, error_message: str) -> str:
        """パターン結果の細かい分類"""
        if pattern_result == 'EXTERNAL':
            if cls.NETWORK_TIMEOUT.search(error_message):
                return 'TIMEOUT'
            if cls.DNS_ERROR.search(error_message):
                return 'DNS_ERROR'
            if cls.TLS_ERROR.search(error_message):
                return 'TLS_ERROR'
            if cls.CONN_RESET.search(error_message):
                return 'CONNECTION_RESET'
            if cls.BLOCKED_BY_CLIENT.search(error_message):
                return 'BLOCKED_BY_CLIENT'
            return 'ACCESS'
        elif pattern_result == 'SUBMIT_BUTTON':
            return cls._classify_submit_button_error(error_message)
        elif pattern_result == 'CONTENT_ANALYSIS':
            return cls._classify_content_analysis_error(error_message)
        else:
            return pattern_result
    
    @classmethod
    def _classify_submit_button_error(cls, error_message: str) -> str:
        """送信ボタンエラーの細かい分類"""
        if 'not found' in error_message or 'selector' not in error_message:
            return 'SUBMIT_BUTTON_NOT_FOUND'
        elif 'selector' in error_message and ('not provided' in error_message or 'missing' in error_message):
            return 'SUBMIT_BUTTON_SELECTOR_MISSING'
        else:
            return 'SUBMIT_BUTTON_ERROR'
    
    @classmethod
    def _classify_content_analysis_error(cls, error_message: str) -> str:
        """コンテンツ分析エラーの細かい分類"""
        if 'error indicators found' in error_message:
            return 'FORM_VALIDATION_ERROR'
        else:
            return 'CONTENT_ANALYSIS_FAILED'
    
    @classmethod
    def _classify_fallback(cls, error_message: str) -> str:
        """フォールバック分類（従来ロジック）"""
        if cls.INSTRUCTION_KEYWORD_PATTERN.search(error_message):
            # より厳密に指示書問題かチェック
            if cls.INSTRUCTION_JSON_PATTERN.search(error_message):
                return 'INSTRUCTION'
            else:
                return 'SYSTEM'  # 曖昧な場合はSYSTEMに分類
        elif cls.ELEMENT_KEYWORD_PATTERN.search(error_message):
            # サイト変更の可能性が高い
            return 'ELEMENT_EXTERNAL'  # 外部要因による要素問題
        elif 'input' in error_message:
            # 入力制限の可能性が高い
            return 'INPUT_EXTERNAL'  # 外部要因による入力問題
        elif 'submit' in error_message:
            return 'SUBMIT'
        elif 'access' in error_message:
            return 'ACCESS'
        else:
            return 'SYSTEM'

    # === 追加: ネットワーク/WAF 詳細分類 ========================================
    @classmethod
    def _contains_any(cls, text: str, patterns: List[Pattern[str]]) -> bool:
        if not text:
            return False
        for p in patterns:
            if p.search(text):
                return True
        return False

    @classmethod
    def _classify_network_waf_detail(cls, error_message: str, page_content: str) -> Optional[str]:
        # ネットワーク系
        if cls.DNS_ERROR.search(error_message):
            return 'DNS_ERROR'
        if cls.TLS_ERROR.search(error_message):
            return 'TLS_ERROR'
        if cls.CONN_RESET.search(error_message):
            return 'CONNECTION_RESET'
        if cls.PAGE_CLOSED.search(error_message):
            return 'PAGE_CLOSED'
        if cls.NOT_INTERACTABLE.search(error_message):
            return 'ELEMENT_NOT_INTERACTABLE'
        if cls.BLOCKED_BY_CLIENT.search(error_message):
            return 'BLOCKED_BY_CLIENT'

        # HTTP/レートリミット（メッセージのみで判断）
        if cls.RATE_LIMIT.search(error_message):
            return 'RATE_LIMIT'
        if cls.HTTP_FORBIDDEN.search(error_message):
            if cls._contains_any(page_content, [cls.CLOUDFLARE, cls.AKAMAI, cls.INCAPSULA, cls.PERIMETERX, cls.HUMAN_VERIF]):
                return 'WAF_CHALLENGE'
            return 'ACCESS'

        # WAF/ボット（コンテンツ）
        if cls._contains_any(page_content, [cls.CLOUDFLARE, cls.AKAMAI, cls.INCAPSULA, cls.PERIMETERX, cls.HUMAN_VERIF]):
            return 'WAF_CHALLENGE'

        # 外部追加パターン
        if cls._external_extra_patterns:
            try:
                for code, patterns in cls._external_extra_patterns.items():
                    if cls._contains_any(error_message, patterns) or cls._contains_any(page_content, patterns):
                        return code
            except Exception:
                pass

        return None
    
    @classmethod
    def should_update_instruction_valid(cls, error_type: str) -> bool:
        """
        instruction_valid を更新すべきかどうかの判定（廃止済み）
        
        RuleBasedAnalyzerのリアルタイム解析ではinstruction_validフラグを使用しないため、
        常にFalseを返す
        
        Args:
            error_type: エラータイプ
            
        Returns:
            bool: 常にFalse（instruction_valid更新は不要）
        """
        # RuleBasedAnalyzerリアルタイム解析ではDBのinstruction_validフラグを更新しない
        return False
    
    @classmethod
    def is_recoverable_error(cls, error_type: str, error_message: str) -> bool:
        """
        復旧可能なエラーかどうかの判定（拡張版）
        
        Args:
            error_type: エラータイプ
            error_message: エラーメッセージ
            
        Returns:
            bool: 復旧可能な場合 True
        """
        # 復旧可能なエラータイプ（従来＋新規）
        recoverable_types = [
            'TIMEOUT', 'ACCESS', 'ELEMENT_EXTERNAL', 
            'INPUT_EXTERNAL', 'SYSTEM',
            'ELEMENT_NOT_FOUND',            # サイト変更の可能性
            'CONTENT_ANALYSIS_FAILED',     # 一時的な問題の可能性
            'SUBMIT_BUTTON_NOT_FOUND',     # ページ変更の可能性
            # 追加: ネットワーク/WAF/HTTP系
            'DNS_ERROR', 'TLS_ERROR', 'CONNECTION_RESET', 'RATE_LIMIT', 'SERVER_ERROR',
        ]
        
        # 復旧不可能なエラータイプ（構造的問題）
        non_recoverable_types = [
            'INSTRUCTION', 'SUBMIT_BUTTON_SELECTOR_MISSING',
            'SUCCESS_DETERMINATION_FAILED', 'INPUT_TYPE_MISMATCH',
            'FORM_VALIDATION_ERROR', 'BOT_DETECTED',
            # 追加: マッピング/検証起因は自動復旧不可
            'MAPPING', 'VALIDATION_FORMAT', 'CSRF_ERROR', 'DUPLICATE_SUBMISSION',
            # WAF系はクールダウンや人的対応を推奨（自動復旧対象外）
            'WAF_CHALLENGE'
        ]
        
        if error_type in non_recoverable_types:
            return False
        
        if error_type not in recoverable_types:
            return False
        
        # 特定のエラーメッセージパターンは復旧不可能
        non_recoverable_patterns = [
            'instruction_valid', 'placeholder', 'json decode',
            'invalid selector', 'malformed', 'selector missing',
            'not provided', 'type mismatch', 'validation error'
        ]
        
        if any(pattern in error_message.lower() for pattern in non_recoverable_patterns):
            return False
        
        return True
    
    # フォーム処理段階に特化した分類メソッド（最適化版）
    
    @classmethod
    def classify_form_submission_error(cls, error_message: str, has_url_change: bool = False, 
                                     page_content: str = "", submit_selector: str = "") -> str:
        """
        フォーム送信段階のエラーを詳細分類（最適化版）
        
        Args:
            error_message: エラーメッセージ
            has_url_change: URL変更があったかどうか
            page_content: ページコンテンツ（オプション）
            submit_selector: 送信ボタンセレクタ（オプション）
            
        Returns:
            str: 詳細なエラータイプ
        """
        try:
            cls._load_external_rules()
            # 最適化されたパターンマッチング
            error_message_lower = (error_message or '').lower()
            pattern_result = cls._classify_by_patterns(error_message_lower)
            
            if pattern_result:
                if pattern_result == 'SUBMIT_BUTTON':
                    return 'SUBMIT_BUTTON_NOT_FOUND'
                elif pattern_result == 'CONTENT_ANALYSIS':
                    return cls._classify_content_analysis_error(error_message_lower)
                elif pattern_result in ['SUCCESS_DETERMINATION_FAILED', 'FORM_VALIDATION_ERROR']:
                    return pattern_result

            # まずはページ本文・メッセージの検証系を優先判定（selector有無より前）
            content_lower = (page_content or '').lower()
            for p in cls.REQUIRED_TEXT_PATTERNS:
                if p.search(content_lower) or p.search(error_message_lower):
                    return 'MAPPING'
            for p in cls.FORMAT_TEXT_PATTERNS:
                if p.search(content_lower) or p.search(error_message_lower):
                    return 'VALIDATION_FORMAT'

            # ネットワーク/WAF 詳細
            detail = cls._classify_network_waf_detail(error_message_lower, content_lower)
            if detail:
                return detail

            # 従来の分類にフォールバック
            error_context = {
                'error_message': error_message,
                'error_location': 'form_submission',
                'has_url_change': has_url_change,
                'page_content': page_content,
                'submit_selector': submit_selector
            }
            refined = cls._classify_from_page_content(error_context)
            if refined:
                return refined
            # submit_selector が無い場合でも検証系に該当しないなら最後に不足扱いへフォールバック
            if not submit_selector or submit_selector.strip() == "":
                # エラーメッセージに「not found」が含まれていれば NOT_FOUND を優先
                if 'not found' in error_message_lower or 'no submit button' in error_message_lower:
                    return 'SUBMIT_BUTTON_NOT_FOUND'
                return 'SUBMIT_BUTTON_SELECTOR_MISSING'

            return cls.classify_error_type(error_context)
            
        except Exception as e:
            raise RuntimeError(f"Form submission error classification failed: {e}") from e

    # 追加: ページテキスト/HTMLからの詳細分類（必須/フォーマット/ボット/CSRF/重複など）
    @classmethod
    def _classify_from_page_content(cls, context: Dict[str, Any]) -> Optional[str]:
        try:
            content = (context.get('page_content') or '').lower()
            if not content:
                return None

            # 必須未入力 → MAPPING
            for p in cls.REQUIRED_TEXT_PATTERNS:
                if p.search(content):
                    return 'MAPPING'

            # 形式不正 → VALIDATION_FORMAT
            for p in cls.FORMAT_TEXT_PATTERNS:
                if p.search(content):
                    return 'VALIDATION_FORMAT'

            # reCAPTCHA/Cloudflare → BOT_DETECTED（UI/DOMパターンも含め広めに検知）
            for p in cls.CAPTCHA_TEXT_PATTERNS:
                try:
                    if p.search(content):
                        return 'BOT_DETECTED'
                except Exception as _re_err:
                    # 正規表現エラー等はDEBUGに記録（本番は沈黙）
                    logging.getLogger(__name__).debug(
                        f"CAPTCHA pattern check error: {type(_re_err).__name__}: {_re_err}"
                    )
                    continue

            # CSRF/トークン → CSRF_ERROR（近接条件を満たす場合のみ）
            for p in cls.CSRF_NEAR_ERROR_PATTERNS:
                if p.search(content):
                    return 'CSRF_ERROR'

            # 重複送信 → DUPLICATE_SUBMISSION
            for p in cls.DUPLICATE_TEXT_PATTERNS:
                if p.search(content):
                    return 'DUPLICATE_SUBMISSION'

            # HTML的手掛かり
            if 'aria-invalid="true"' in content or 'required' in content:
                return 'FORM_VALIDATION_ERROR'

            return None
        except Exception as e:
            logging.getLogger(__name__).debug(f"Page content classification error: {e}")
            return None
    
    @classmethod
    def classify_form_input_error(cls, error_message: str, field_name: str = "",
                                field_type: str = "", selector: str = "") -> str:
        """
        フィールド入力段階のエラーを詳細分類（最適化版）
        
        Args:
            error_message: エラーメッセージ
            field_name: フィールド名（オプション）
            field_type: フィールドタイプ（オプション）
            selector: セレクタ（オプション）
            
        Returns:
            str: 詳細なエラータイプ
        """
        try:
            error_message_lower = (error_message or '').lower()
            
            # 最適化されたパターンマッチング
            pattern_result = cls._classify_by_patterns(error_message_lower)
            
            if pattern_result in ['ELEMENT_NOT_FOUND', 'INPUT_TYPE_MISMATCH', 'FORM_VALIDATION_ERROR']:
                return pattern_result
            
            # 'not found' の特別チェック
            if 'not found' in error_message_lower:
                return 'ELEMENT_NOT_FOUND'
            
            # 従来の分類にフォールバック
            error_context = {
                'error_message': error_message,
                'error_location': 'form_input',
                'field_name': field_name,
                'field_type': field_type,
                'selector': selector
            }
            return cls.classify_error_type(error_context)
            
        except Exception as e:
            raise RuntimeError(f"Form input error classification failed: {e}") from e

    # === 追加: 詳細分類API（後方互換のため任意で使用可能） ========================
    @classmethod
    def classify_detail(
        cls,
        *,
        error_message: str = "",
        page_content: str = "",
        http_status: Optional[int] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        詳細な分類情報を辞書で返す（後方互換のため任意で利用可能）

        Returns:
            {
              'code': 'RATE_LIMIT',
              'category': 'HTTP',
              'retryable': True,
              'cooldown_seconds': 300,
              'confidence': 0.7,
            }
        """
        cls._load_external_rules()
        msg = (error_message or '').lower()
        content = (page_content or '').lower()
        code = None

        # 1) まず既存/拡張ロジックでコードを決める
        if http_status is not None:
            code = cls.classify_error_type({'error_message': msg, 'http_status': http_status, 'page_content': content})
        else:
            # form submission 文脈を仮定
            # submit_selector は未判明のため空文字を渡す（ハードコード値回避）
            code = cls.classify_form_submission_error(error_message=msg, page_content=content, submit_selector="")

        # 2) カテゴリ/再試行可否/クールダウンのヒント
        category_map = {
            'RATE_LIMIT': 'HTTP',
            'SERVER_ERROR': 'HTTP',
            'ACCESS': 'HTTP',
            'UNAUTHORIZED': 'HTTP',
            'NOT_FOUND': 'HTTP',
            'METHOD_NOT_ALLOWED': 'HTTP',
            'DNS_ERROR': 'NETWORK',
            'TLS_ERROR': 'NETWORK',
            'CONNECTION_RESET': 'NETWORK',
            'BLOCKED_BY_CLIENT': 'NETWORK',
            'PAGE_CLOSED': 'BROWSER',
            'TIMEOUT': 'NETWORK',
            'WAF_CHALLENGE': 'WAF',
            'BOT_DETECTED': 'WAF',
            'CSRF_ERROR': 'SECURITY',
            'MAPPING': 'VALIDATION',
            'VALIDATION_FORMAT': 'VALIDATION',
            'FORM_VALIDATION_ERROR': 'VALIDATION',
            'DUPLICATE_SUBMISSION': 'BUSINESS',
            'PROHIBITION_DETECTED': 'BUSINESS',
            'NO_MESSAGE_AREA': 'FORM_STRUCTURE',
        }
        category = category_map.get(code, 'GENERAL')

        retryable = code in {
            'TIMEOUT', 'DNS_ERROR', 'TLS_ERROR', 'CONNECTION_RESET', 'BLOCKED_BY_CLIENT',
            'RATE_LIMIT', 'SERVER_ERROR', 'ACCESS', 'ELEMENT_EXTERNAL', 'INPUT_EXTERNAL', 'SYSTEM'
        }
        cooldown = 300 if code in {'RATE_LIMIT', 'WAF_CHALLENGE'} else (60 if code in {'SERVER_ERROR', 'ACCESS'} else 0)

        # 3) 信頼度の簡易推定（ヒューリスティック）
        confidence = cls._calculate_confidence(code, msg, content)

        return {
            'code': code,
            'category': category,
            'retryable': retryable and code != 'WAF_CHALLENGE',
            'cooldown_seconds': cooldown,
            'confidence': confidence,
        }

    @classmethod
    def _calculate_confidence(cls, code: str, error_message: str, page_content: str) -> float:
        """信頼度スコア（0..1）を簡易ヒューリスティックで算出"""
        msg = error_message or ""
        content = page_content or ""
        score = 0.0

        # 強いエビデンス: 明示エラーパターン
        strong_signals = [
            (code == 'DNS_ERROR' and bool(cls.DNS_ERROR.search(msg))),
            (code == 'TLS_ERROR' and bool(cls.TLS_ERROR.search(msg))),
            (code == 'CONNECTION_RESET' and bool(cls.CONN_RESET.search(msg))),
            (code == 'RATE_LIMIT' and bool(cls.RATE_LIMIT.search(msg) or '429' in msg)),
            (code == 'WAF_CHALLENGE' and cls._contains_any(content, [cls.CLOUDFLARE, cls.AKAMAI, cls.INCAPSULA, cls.PERIMETERX, cls.HUMAN_VERIF])),
            (code == 'CSRF_ERROR' and any(p.search(msg) or p.search(content) for p in cls.CSRF_NEAR_ERROR_PATTERNS)),
            (code in {'MAPPING', 'VALIDATION_FORMAT'} and (
                any(p.search(content) or p.search(msg) for p in cls.REQUIRED_TEXT_PATTERNS + cls.FORMAT_TEXT_PATTERNS)
            )),
        ]
        score += 0.6 if any(strong_signals) else 0.0

        # 補助シグナル: CAPTCHA/WAF語彙・HTTP語
        if any(word in msg for word in ['http', 'status', 'error', 'forbidden', 'unauthorized']):
            score += 0.1
        if cls._contains_any(content, cls.CAPTCHA_TEXT_PATTERNS):
            score += 0.1

        # 低確信ケース: SYSTEM や 汎用カテゴリ
        if code in {'SYSTEM', 'CONTENT_ANALYSIS_FAILED', 'SUBMIT'}:
            score -= 0.2

        # メッセージ/本文無しは減点
        if not msg and not content:
            score -= 0.2

        # スコアクリップ + 最低保証
        score = max(0.0, min(1.0, score))
        if score < cls.MIN_CONFIDENCE:
            score = cls.MIN_CONFIDENCE
        return score
