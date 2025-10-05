"""
送信成功判定システム

参考: ListersFormの6段階送信成功判定システム
Playwrightベースでの実装（トレーシング機能付き）
"""

import asyncio
import re
from typing import Optional, Dict, Any
from playwright.async_api import Page, Response
from urllib.parse import urlparse

from ..utils.judgment_tracer import JudgmentTracer, JudgmentStage, JudgmentResult
from ..utils.secure_logger import get_secure_logger
from ..detection.prohibition_detector import ProhibitionDetector
from ..detection.bot_detector import BotDetectionSystem
from ..detection.pattern_matcher import FormDetectionPatternMatcher

logger = get_secure_logger(__name__)

class SuccessJudge:
    """
    6段階送信成功判定システム + 高度営業禁止検出
    
    判定順序:
    0. 事前営業禁止検出 (95-100% accuracy) ※新機能
    1. URL変更判定 (90-95% accuracy)
    2. 成功メッセージ判定 (85-90% accuracy)  
    3. フォーム消失判定 (80-85% accuracy)
    4. 兄弟要素解析判定 (75-80% accuracy)
    5. エラーパターン判定 (70-75% accuracy)
    6. 失敗パターン判定 (65-70% accuracy)
    
    新機能: Form Analyzer準拠の高度な営業禁止文言検出
    - 74種類のキーワードパターン
    - 48種類の除外パターン
    - 文脈理解に基づく精密検出
    """
    
    def __init__(self, page: Page, enable_tracing: bool = True):
        self.page = page
        self.original_url = None
        self.original_form_elements = []
        self.response_history = []
        self._bot_detector = BotDetectionSystem()
        self._matcher = FormDetectionPatternMatcher()
        
        # 高度営業禁止検出機能（Form Analyzer準拠）
        self.prohibition_detector = ProhibitionDetector()
        self.prohibition_detected = False
        self.prohibition_phrases = []
        self.prohibition_confidence_level = 'none'
        self.prohibition_confidence_score = 0.0
        
        # トレーシング機能
        self.enable_tracing = enable_tracing
        self.tracer: Optional[JudgmentTracer] = None
        
        # 成功メッセージパターン (参考システム準拠+拡張)
        self.success_patterns = [
            # 基本成功メッセージ (参考リポジトリ由来)
            r'送信.*完了|送信.*成功|送信.*ました|送信いたしました|送信致しました',
            r'お問い合わせ.*完了|お問い合わせ.*ました|お問い合わせを受け付けました',
            r'ありがとう.*ございました|ありがとう.*ます|Thank.*you|thanks.*for',
            r'受付.*完了|受付.*ました|受け付け.*完了|受け付け.*ました|受付いたしました',
            r'確認.*完了|確認.*ました|お申し込み.*完了|お申し込み.*ました',
            
            # 具体的な成功表現 (参考リポジトリパターン拡張)
            r'メッセージ.*送信.*完了|メール.*送信.*完了|フォーム.*送信.*完了',
            r'お問い合わせ内容.*送信|内容.*送信.*完了|データ.*送信.*完了',
            r'正常.*送信|正常.*受付|正常.*完了|成功.*送信|成功.*受付',
            r'message.*sent|successfully.*sent|submission.*complete',
            
            # 日本語特有の敬語表現パターン
            r'送信させていただきました|受付させていただきました|確認させていただきました',
            r'お預かりいたしました|承りました|拝受いたしました|頂戴いたしました',
            r'恐れ入ります.*ありがとう|お忙しい.*ありがとう|ご多忙.*ありがとう',
            
            # 後続アクション案内 (参考リポジトリ拡張)
            r'後日.*連絡|後日.*返信|確認.*メール|自動.*返信|返信.*メール',
            r'担当者.*連絡|担当.*から|営業.*連絡|営業.*から|スタッフ.*連絡',
            r'折り返し.*連絡|追って.*連絡|改めて.*連絡|近日中.*連絡',
            
            # 感謝・完了表現 (参考リポジトリ拡張)
            r'お忙しい中.*ありがとう|貴重.*時間.*ありがとう|ご協力.*ありがとう',
            r'手続き.*完了|処理.*完了|登録.*完了|申請.*完了|予約.*完了',
            r'ご相談.*ありがとう|ご質問.*ありがとう|ご要望.*ありがとう',
            
            # CSSクラス名ベースの成功パターン (参考リポジトリより)
            r'class.*success|class.*complete|class.*thanks|class.*done',
            r'class.*finish|class.*sent|class.*submitted|class.*confirmation'
        ]
        
        # エラーメッセージパターン (参考システム準拠+詳細分類)  
        self.error_patterns = {
            # reCAPTCHA関連エラー (参考リポジトリ由来)
            'reCAPTCHA認証があるため送信不可': [
                r'recaptcha|re-captcha|captcha|CAPTCHA',
                r'画像認証|認証.*確認|ロボット.*確認|人間.*確認',
                r'チェック.*してください|認証.*してください'
            ],
            
            # 営業拒否関連エラー (ProhibitionDetectorによる高度検出にアップグレード予定)
            # NOTE: この基本パターンは後方互換性のために残すが、主な検出はProhibitionDetectorで実行
            '営業お断りを検出（基本パターン）': [
                r'営業.*お断り|営業.*禁止|営業.*NG|勧誘.*お断り|勧誘.*禁止',
                r'セールス.*お断り|セールス.*禁止|セールス.*NG',
                r'広告.*お断り|宣伝.*お断り|PR.*お断り'
            ],
            
            # 入力形式エラー (参考リポジトリ準拠)
            'メール形式エラー': [
                r'メール.*形式|メール.*正しく|メール.*無効|email.*format|email.*invalid',
                r'メールアドレス.*正しく|メールアドレス.*形式|メールアドレス.*無効',
                r'@.*正しく|@.*形式|メール.*@'
            ],
            
            # 必須項目エラー (参考リポジトリ拡張)
            '必須項目未入力': [
                r'必須.*項目|必須.*入力|必須.*フィールド|required.*field',
                r'未入力|空白|blank|empty|入力.*してください|入力されていません',
                r'項目.*入力|フィールド.*入力|値.*入力',
                r'選択.*してください|チェック.*してください'
            ],
            
            # システムエラー (参考リポジトリ拡張)
            'システムエラー': [
                r'システム.*エラー|system.*error|サーバー.*エラー|server.*error',
                r'内部.*エラー|internal.*error|処理.*エラー|process.*error',
                r'データベース.*エラー|database.*error|DB.*エラー',
                r'接続.*エラー|connection.*error|ネットワーク.*エラー'
            ],
            
            # 一般的なエラーパターン (統合)
            '一般エラー': [
                r'エラー|ERROR|error|失敗|FAILED|failed|不正|無効|Invalid|invalid',
                r'入力.*エラー|送信.*エラー|登録.*エラー|処理.*失敗',
                r'正しく.*入力|適切.*入力|有効.*入力|形式.*正しく|フォーマット.*エラー',
                r'文字数.*超過|文字数.*不足|長さ.*エラー|サイズ.*エラー',
                r'電話番号.*正しく|電話.*形式|電話.*無効|phone.*invalid'
            ],
            
            # 操作・再試行エラー (参考リポジトリ拡張)
            '再試行要請': [
                r'再度.*お試し|もう一度.*お試し|やり直し|retry|Retry|再送信',
                r'一時的.*エラー|temporary.*error|しばらく.*お待ち|try.*again',
                r'タイムアウト|timeout|Timeout|時間.*切れ|制限時間.*超過'
            ]
        }

        # 早期失敗ゲート用の簡易正規表現（高速・広範囲）
        self._early_failure_regex = re.compile(
            r"(" \
            r"入力\s*してください|入力されていません|未入力|必須|必須です|必須項目|不正|無効|正しく入力|選択してください|チェックしてください|エラー|送信に失敗|送信できません|失敗しました|もう一度|やり直し" \
            r"|recaptcha|captcha|i\'m not a robot|not\s*a\s*robot|please\s*verify|human\s*verification|認証してください|画像認証|ロボットでは|人間であること" \
            r")",
            re.IGNORECASE
        )
        
    async def initialize_before_submission(self):
        """送信前の初期化 - 現在のURL・フォーム要素を記録"""
        try:
            self.original_url = self.page.url
            
            # トレーサー初期化
            if self.enable_tracing:
                self.tracer = JudgmentTracer(self.original_url)
                
                # ブラウザ情報を設定
                try:
                    user_agent = await self.page.evaluate("navigator.userAgent")
                    viewport = self.page.viewport_size
                    self.tracer.set_browser_info("playwright", "unknown", user_agent, viewport)
                except Exception:
                    pass
                
                # ページメタデータを設定
                try:
                    title = await self.page.title()
                    ready_state = await self.page.evaluate("document.readyState")
                    dom_count = await self.page.evaluate("document.querySelectorAll('*').length")
                    self.tracer.set_page_metadata(title, self.original_url, ready_state, dom_count)
                except Exception:
                    pass
            
            # フォーム要素を記録
            form_elements = await self.page.query_selector_all('form input, form textarea, form select')
            self.original_form_elements = []
            for element in form_elements:
                try:
                    element_info = {
                        'type': await element.get_attribute('type'),
                        'name': await element.get_attribute('name'),
                        'id': await element.get_attribute('id'),
                        'tag': element.tag_name.lower(),
                        'visible': await element.is_visible()
                    }
                    self.original_form_elements.append(element_info)
                except Exception as e:
                    logger.debug(f"要素情報取得エラー: {e}")
                    
            # レスポンス履歴を初期化
            self.response_history = []
            self.page.on("response", self._track_response)
            
            # 事前営業禁止検出を実行（送信前チェック）
            await self._pre_submission_prohibition_check()
            
            # トレーシング開始
            if self.tracer:
                self.tracer.start_stage(JudgmentStage.STAGE_0_INIT)
                self.tracer.add_stage_detail("original_url", self.original_url)
                self.tracer.add_stage_detail("form_elements_count", len(self.original_form_elements))
                self.tracer.add_stage_detail("prohibition_detected", self.prohibition_detected)
                if self.prohibition_detected:
                    self.tracer.add_stage_detail("prohibition_phrases_count", len(self.prohibition_phrases))
                self.tracer.complete_stage(JudgmentResult.SUCCESS, 1.0, "初期化完了")
            
            logger.info("送信前初期化完了", {
                "url": self.original_url,
                "form_elements_count": len(self.original_form_elements),
                "prohibition_detected": self.prohibition_detected,
                "prohibition_phrases_count": len(self.prohibition_phrases),
                "tracing_enabled": self.enable_tracing
            })
            
        except Exception as e:
            logger.error(f"送信前初期化エラー: {e}")
            if self.tracer:
                self.tracer.complete_stage(JudgmentResult.ERROR, error=str(e))
            
    async def _pre_submission_prohibition_check(self):
        """
        事前営業禁止チェック（送信前実行）
        Form Analyzer準拠の高度な営業禁止文言検出を実行
        """
        try:
            logger.info("事前営業禁止チェック開始")
            
            # ページのHTMLコンテンツを取得
            html_content = await self.page.content()

            # ProhibitionDetector（信頼度付き）で検出
            detected, phrases, conf_level, conf_score = self.prohibition_detector.detect_with_confidence(html_content)

            self.prohibition_detected = detected
            self.prohibition_phrases = phrases
            self.prohibition_confidence_level = conf_level
            try:
                self.prohibition_confidence_score = float(conf_score)
            except Exception:
                self.prohibition_confidence_score = 0.0
            
            if detected:
                logger.warning(f"営業禁止文言を検出しました: {len(phrases)}件")
                for i, phrase in enumerate(phrases[:3]):  # 最初の3件をログ出力
                    logger.warning(f"禁止文言{i+1}: {phrase[:100]}...")
            else:
                logger.info("営業禁止文言は検出されませんでした")
                
        except Exception as e:
            logger.error(f"事前営業禁止チェックエラー: {e}")
            # エラー時はfalseに設定（送信を継続）
            self.prohibition_detected = False
            self.prohibition_phrases = []
            self.prohibition_confidence_level = 'error'
            self.prohibition_confidence_score = 0.0
            
    def _track_response(self, response: Response):
        """レスポンス履歴を記録"""
        try:
            self.response_history.append({
                'url': response.url,
                'status': response.status,
                'headers': dict(response.headers),
                'timestamp': asyncio.get_event_loop().time()
            })
        except Exception as e:
            logger.debug(f"レスポンス記録エラー: {e}")
            
    async def judge_submission_success(self, timeout: int = 10) -> Dict[str, Any]:
        """
        6段階送信成功判定の実行（トレーシング機能付き + 営業禁止検出対応）
        
        Returns:
            Dict[str, Any]: 判定結果
            {
                'success': bool,
                'stage': int (0-6),  # Stage 0: 営業禁止検出
                'stage_name': str,
                'confidence': float (0.0-1.0),
                'details': Dict[str, Any],
                'message': str,
                'prohibition_detected': bool,  # 新規追加
                'prohibition_phrases': List[str],  # 新規追加
                'trace_summary': Dict[str, Any] (トレーシング有効時)
            }
        """
        final_result = None
        try:
            # Stage 0: 営業禁止検出チェック（最優先）
            if self.prohibition_detected:
                logger.warning("営業禁止文言検出のため、送信成功判定をスキップします")
                final_result = {
                    'success': False,
                    'stage': 0,
                    'stage_name': '営業禁止検出',
                    'confidence': 1.0,  # 営業禁止検出は100%信頼度
                    'details': {
                        'prohibition_detected': True,
                        'prohibition_phrases': self.prohibition_phrases,
                        'phrases_count': len(self.prohibition_phrases),
                        'detection_method': 'Form Analyzer準拠高度検出',
                        'confidence_level': self.prohibition_confidence_level,
                        'confidence_score': self.prohibition_confidence_score,
                    },
                    'message': f'営業禁止文言が検出されました ({len(self.prohibition_phrases)}件)',
                    'prohibition_detected': True,
                    'prohibition_phrases': self.prohibition_phrases
                }
                
                if self.tracer:
                    self.tracer.complete_judgment(JudgmentResult.FAILURE, JudgmentStage.STAGE_0_INIT,
                                                1.0, '営業禁止文言検出のため送信中止')
                
                return self._add_trace_to_result(final_result)
            
            # 画面の安定化を待機
            await asyncio.sleep(2)

            # Stage 0.5: 失敗の早期ゲート（Doc準拠: 失敗キーワード優先）
            early_fail = await self._early_failure_gate()
            if early_fail is not None:
                if self.tracer:
                    self.tracer.complete_judgment(
                        JudgmentResult.FAILURE,
                        JudgmentStage.STAGE_1_URL,
                        early_fail.get('confidence', 0.9),
                        early_fail.get('message', '早期失敗ゲートで検出')
                    )
                return self._add_trace_to_result(early_fail)
            
            # Stage 1: URL変更判定
            stage1_result = await self._judge_stage1_url_change()
            if stage1_result['success']:
                # URL変更のみで成功としないためのガード（Bot/エラーの再確認）
                url_guard_fail = await self._post_url_change_guard()
                if url_guard_fail is not None:
                    if self.tracer:
                        self.tracer.complete_judgment(
                            JudgmentResult.FAILURE,
                            JudgmentStage.STAGE_1_URL,
                            url_guard_fail.get('confidence', 0.9),
                            url_guard_fail.get('message', 'URL変更後ガードで失敗検出')
                        )
                    return self._add_trace_to_result(url_guard_fail)

                final_result = stage1_result
                if self.tracer:
                    self.tracer.complete_judgment(JudgmentResult.SUCCESS, JudgmentStage.STAGE_1_URL,
                                                stage1_result['confidence'], stage1_result['message'])
                return self._add_trace_to_result(stage1_result)
                
            # Stage 2: 成功メッセージ判定
            stage2_result = await self._judge_stage2_success_message()
            if stage2_result['success']:
                final_result = stage2_result
                if self.tracer:
                    self.tracer.complete_judgment(JudgmentResult.SUCCESS, JudgmentStage.STAGE_2_MESSAGE,
                                                stage2_result['confidence'], stage2_result['message'])
                return self._add_trace_to_result(stage2_result)
                
            # Stage 3: フォーム消失判定  
            stage3_result = await self._judge_stage3_form_disappearance()
            if stage3_result['success']:
                final_result = stage3_result
                if self.tracer:
                    self.tracer.complete_judgment(JudgmentResult.SUCCESS, JudgmentStage.STAGE_3_FORM,
                                                stage3_result['confidence'], stage3_result['message'])
                return self._add_trace_to_result(stage3_result)
                
            # Stage 4: 兄弟要素解析判定
            stage4_result = await self._judge_stage4_sibling_analysis()
            if stage4_result['success']:
                final_result = stage4_result
                if self.tracer:
                    self.tracer.complete_judgment(JudgmentResult.SUCCESS, JudgmentStage.STAGE_4_SIBLING,
                                                stage4_result['confidence'], stage4_result['message'])
                return self._add_trace_to_result(stage4_result)
                
            # Stage 5: エラーパターン判定
            stage5_result = await self._judge_stage5_error_patterns()
            if not stage5_result['success']:  # エラーが検出されない場合は成功
                final_result = {
                    'success': True,
                    'stage': 5,
                    'stage_name': 'エラーパターン判定 (エラー未検出)',
                    'confidence': 0.72,
                    'details': stage5_result['details'],
                    'message': 'エラーメッセージが検出されませんでした'
                }
                if self.tracer:
                    self.tracer.complete_judgment(JudgmentResult.SUCCESS, JudgmentStage.STAGE_5_ERROR,
                                                0.72, 'エラーメッセージが検出されませんでした')
                return self._add_trace_to_result(final_result)
            
            # Stage 6: 失敗パターン判定 (最終判定)
            stage6_result = await self._judge_stage6_failure_patterns()
            final_result = stage6_result
            
            if self.tracer:
                result_enum = JudgmentResult.SUCCESS if stage6_result['success'] else JudgmentResult.FAILURE
                self.tracer.complete_judgment(result_enum, JudgmentStage.STAGE_6_FAILURE,
                                            stage6_result['confidence'], stage6_result['message'])
            
            return self._add_trace_to_result(stage6_result)
            
        except Exception as e:
            logger.error(f"送信成功判定エラー: {e}")
            final_result = {
                'success': False,
                'stage': 0,
                'stage_name': 'システムエラー',
                'confidence': 0.0,
                'details': {'error': str(e)},
                'message': f'判定処理中にエラーが発生しました: {e}',
                'prohibition_detected': self.prohibition_detected,
                'prohibition_phrases': self.prohibition_phrases
            }
            
            if self.tracer:
                self.tracer.complete_judgment(JudgmentResult.ERROR, JudgmentStage.COMPLETE,
                                            0.0, f'システムエラー: {e}')
            
            return self._add_trace_to_result(final_result)
    
    def _add_trace_to_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """結果にトレース情報と営業禁止検出情報を追加"""
        # 営業禁止検出情報を常に追加
        if 'prohibition_detected' not in result:
            result['prohibition_detected'] = self.prohibition_detected
        if 'prohibition_phrases' not in result:
            result['prohibition_phrases'] = self.prohibition_phrases
            
        # トレーシング情報を追加
        if self.tracer:
            result['trace_summary'] = self.tracer.get_trace_summary()
            result['trace_id'] = self.tracer.trace.trace_id
        return result
    
    def get_detailed_trace_report(self) -> Optional[Dict[str, Any]]:
        """詳細なトレースレポートを取得"""
        if self.tracer:
            return self.tracer.get_detailed_report()
        return None
    
    async def _judge_stage1_url_change(self) -> Dict[str, Any]:
        """Stage 1: URL変更判定 (90-95% accuracy)"""
        if self.tracer:
            self.tracer.start_stage(JudgmentStage.STAGE_1_URL)
        
        try:
            current_url = self.page.url
            
            # URL完全変更 (高信頼度)
            if current_url != self.original_url:
                parsed_original = urlparse(self.original_url)
                parsed_current = urlparse(current_url)
                
                # 成功ページ指標
                success_indicators = [
                    'thanks', 'thank-you', 'success', 'complete', 'done',
                    'confirm', 'finish', 'ok', 'sent', 'submitted'
                ]
                
                url_confidence = 0.95
                current_path = parsed_current.path.lower()
                
                # 成功指標が含まれている場合
                if any(indicator in current_path for indicator in success_indicators):
                    url_confidence = 0.95
                # ドメインが変更されている場合 (外部リダイレクト)
                elif parsed_original.netloc != parsed_current.netloc:
                    url_confidence = 0.85
                # パスのみ変更
                else:
                    url_confidence = 0.90
                    
                return {
                    'success': True,
                    'stage': 1,
                    'stage_name': 'URL変更判定',
                    'confidence': url_confidence,
                    'details': {
                        'original_url': self.original_url,
                        'current_url': current_url,
                        'url_change_type': 'complete_change',
                        'success_indicators_found': [ind for ind in success_indicators if ind in current_path]
                    },
                    'message': 'URLが変更されました (成功ページへの遷移)'
                }
                
            # URL未変更だが、ハッシュやクエリパラメータ変更を確認
            parsed_original = urlparse(self.original_url)
            parsed_current = urlparse(current_url)
            if (parsed_original.fragment != parsed_current.fragment or parsed_original.query != parsed_current.query):
                # 重要: クエリ/ハッシュのみの変更は成功とみなさない（誤判定抑制のため）
                # 次ステージ（成功メッセージ/フォーム状態）での判定に委ねる
                pass
            
            return {
                'success': False,
                'stage': 1,
                'stage_name': 'URL変更判定',
                'confidence': 0.0,
                'details': {'url_unchanged': True},
                'message': 'URL変更は検出されませんでした'
            }
            
        except Exception as e:
            logger.error(f"Stage1 URL変更判定エラー: {e}")
            return {
                'success': False,
                'stage': 1,
                'stage_name': 'URL変更判定',
                'confidence': 0.0,
                'details': {'error': str(e)},
                'message': f'URL変更判定でエラーが発生しました: {e}'
            }

    async def _early_failure_gate(self) -> Optional[Dict[str, Any]]:
        """URL判定前の早期失敗ゲート（厳格化版）。
        次のいずれかを満たす場合のみ早期失敗とする:
        - Bot保護（reCAPTCHA/Cloudflare）の明確な検出
        - 可視なエラー要素（.error, [aria-invalid="true"] など）が存在
        - ページ本文での失敗語検出が「強いシグナル」のとき

        強いシグナルの基準（偽陽性抑制）:
        - 失敗語の異なるカテゴリーから2件以上ヒット、かつ
        - 明確な成功語が本文に出現していない、かつ
        - URLが明確な成功パス（thanks/success/complete等）に遷移していない

        検出なしの場合はNoneを返す。
        """
        try:
            # Bot保護（厳格検出）
            is_bot, bot_type = await self._bot_detector.detect_bot_protection(self.page)
            if is_bot:
                return {
                    'success': False,
                    'stage': 1,
                    'stage_name': '早期失敗ゲート (Bot保護検出)',
                    'confidence': 0.95,
                    'details': {'bot_protection_detected': True, 'bot_type': bot_type},
                    'message': f'Bot保護({bot_type or "unknown"})が検出されました'
                }

            # 代表的なエラー要素
            selectors = [
                '.error', '.alert-danger', '.alert-error', '.is-error',
                '.invalid', '[aria-invalid="true"]', '[role="alert"]',
                '[data-error]', '[data-valmsg-for]', '.field-error', '.help-block.error'
            ]
            for sel in selectors:
                try:
                    el = await self.page.query_selector(sel)
                    if el and await el.is_visible():
                        txt = ''
                        try:
                            txt = (await el.inner_text())[:200]
                        except Exception:
                            pass
                        return {
                            'success': False,
                            'stage': 1,
                            'stage_name': '早期失敗ゲート (エラー要素)',
                            'confidence': 0.9,
                            'details': {'selector': sel, 'text': txt},
                            'message': 'エラー要素が検出されました'
                        }
                except Exception:
                    continue

            # ページ全体テキストの簡易チェック（厳格化）
            try:
                body_text = await self.page.inner_text('body')
            except Exception:
                body_text = ''
            if body_text:
                text_lower = body_text.lower()
                # 成功語の存在チェック（成功ページでの偽陽性回避）
                has_strong_success = any(
                    re.search(pat, body_text, re.IGNORECASE) for pat in self.success_patterns[:6]
                )

                # URLが明確な成功パスかどうか
                try:
                    current_url = self.page.url
                except Exception:
                    current_url = ''
                success_url_signals = ['thanks', 'thank-you', 'success', 'complete', 'done', 'sent', 'submitted']
                url_indicates_success = any(sig in (current_url or '').lower() for sig in success_url_signals)

                # 失敗語のカテゴリー分布（簡易）
                categories = {
                    'required': [r'必須', r'未入力', r'入力\s*してください', r'is\s*required', r'please\s*(enter|select|fill)'],
                    'invalid': [r'不正', r'無効', r'invalid', r'正しく.*入力', r'形式.*(正しく|エラー|不正)'],
                    'retry': [r'もう一度', r'やり直し', r'retry', r'try\s*again'],
                    'bot': [r'recaptcha|captcha|not\s*a\s*robot|human\s*verification|認証してください|画像認証|ロボットでは|人間であること']
                }
                matched_terms = []
                matched_cats = set()
                for cat, pats in categories.items():
                    for pat in pats:
                        if re.search(pat, text_lower, re.IGNORECASE):
                            matched_cats.add(cat)
                            matched_terms.append(pat)
                            break

                # 強いシグナル判定
                is_bot_text = 'bot' in matched_cats
                strong_signal = (len([c for c in matched_cats if c != 'bot']) >= 2) or is_bot_text

                if strong_signal and not has_strong_success and not url_indicates_success:
                    return {
                        'success': False,
                        'stage': 1,
                        'stage_name': '早期失敗ゲート (失敗メッセージ:厳格)',
                        'confidence': 0.9 if is_bot_text else 0.85,
                        'details': {
                            'matched_categories': sorted(list(matched_cats)),
                            'matched_patterns': matched_terms[:8],
                            'has_strong_success_text': has_strong_success,
                            'url_indicates_success': url_indicates_success,
                        },
                        'message': '失敗を示す強いテキストシグナルを検出しました'
                    }

            return None
        except Exception:
            return None

    async def _post_url_change_guard(self) -> Optional[Dict[str, Any]]:
        """URL変更後のガード。URLが変わってもBot/エラーが出ていないか再確認する。"""
        try:
            # Bot保護を再確認
            is_bot, bot_type = await self._bot_detector.detect_bot_protection(self.page)
            if is_bot:
                return {
                    'success': False,
                    'stage': 1,
                    'stage_name': 'URL変更後ガード (Bot保護検出)',
                    'confidence': 0.95,
                    'details': {'bot_protection_detected': True, 'bot_type': bot_type},
                    'message': f'URL変更後にBot保護({bot_type or "unknown"})を検出'
                }

            # エラーメッセージ/エラー要素が出ていないか
            return await self._early_failure_gate()
        except Exception:
            return None
    
    async def _judge_stage2_success_message(self) -> Dict[str, Any]:
        """Stage 2: 成功メッセージ判定 (85-90% accuracy)"""
        try:
            # ページ全体のテキストコンテンツを取得
            page_content = await self.page.inner_text('body')
            
            # 成功メッセージパターンマッチング
            success_matches = []
            for pattern in self.success_patterns:
                matches = re.finditer(pattern, page_content, re.IGNORECASE)
                for match in matches:
                    success_matches.append({
                        'pattern': pattern,
                        'text': match.group(),
                        'start': match.start(),
                        'end': match.end()
                    })
            
            # configベースの成功指標（パターンマッチャ）
            config_success = False
            try:
                config_success = self._matcher.contains_success_indicators(page_content)
            except Exception:
                pass

            if success_matches or config_success:
                # 信頼度計算 (マッチ数とパターンに基づく)
                base_confidence = 0.88
                pattern_bonus = min(len(success_matches) * 0.02, 0.07)  # 最大7%のボーナス
                if config_success:
                    pattern_bonus = max(pattern_bonus, 0.03)  # config命中で最低+3%
                final_confidence = min(base_confidence + pattern_bonus, 0.95)
                
                return {
                    'success': True,
                    'stage': 2,
                    'stage_name': '成功メッセージ判定',
                    'confidence': final_confidence,
                    'details': {
                        'success_matches': success_matches,
                        'config_success_indicator': config_success,
                        'match_count': len(success_matches) + (1 if config_success else 0),
                        'page_content_length': len(page_content)
                    },
                    'message': f'成功メッセージが検出されました ({len(success_matches)}件のマッチ)'
                }
            
            # 成功メッセージが見つからない場合は、特定の要素を追加チェック
            success_elements = await self.page.query_selector_all(
                '[class*="success"], [class*="complete"], [class*="thanks"], '
                '[id*="success"], [id*="complete"], [id*="thanks"], '
                'h1, h2, h3, .message, .alert, .notification'
            )
            
            element_success_matches = []
            for element in success_elements:
                try:
                    if await element.is_visible():
                        element_text = await element.inner_text()
                        for pattern in self.success_patterns:
                            if re.search(pattern, element_text, re.IGNORECASE):
                                element_success_matches.append({
                                    'element': element.tag_name,
                                    'text': element_text,
                                    'pattern': pattern
                                })
                                break
                except Exception:
                    continue
            
            if element_success_matches:
                return {
                    'success': True,
                    'stage': 2,
                    'stage_name': '成功メッセージ判定 (要素ベース)',
                    'confidence': 0.85,
                    'details': {
                        'element_success_matches': element_success_matches,
                        'match_count': len(element_success_matches)
                    },
                    'message': f'成功メッセージ要素が検出されました ({len(element_success_matches)}件)'
                }
            
            return {
                'success': False,
                'stage': 2,
                'stage_name': '成功メッセージ判定',
                'confidence': 0.0,
                'details': {'no_success_message_found': True},
                'message': '成功メッセージは検出されませんでした'
            }
            
        except Exception as e:
            logger.error(f"Stage2 成功メッセージ判定エラー: {e}")
            return {
                'success': False,
                'stage': 2,
                'stage_name': '成功メッセージ判定',
                'confidence': 0.0,
                'details': {'error': str(e)},
                'message': f'成功メッセージ判定でエラーが発生しました: {e}'
            }
    
    async def _judge_stage3_form_disappearance(self) -> Dict[str, Any]:
        """Stage 3: フォーム消失判定 (80-85% accuracy)"""
        try:
            # 現在のフォーム要素を取得
            current_forms = await self.page.query_selector_all('form')
            current_form_elements = await self.page.query_selector_all(
                'form input, form textarea, form select'
            )
            
            # フォーム完全消失の判定
            if len(current_forms) == 0 and len(self.original_form_elements) > 0:
                return {
                    'success': True,
                    'stage': 3,
                    'stage_name': 'フォーム消失判定 (完全消失)',
                    'confidence': 0.85,
                    'details': {
                        'original_form_elements': len(self.original_form_elements),
                        'current_forms': 0,
                        'current_form_elements': 0,
                        'disappearance_type': 'complete'
                    },
                    'message': 'フォームが完全に消失しました'
                }
            
            # フォーム要素の大幅減少判定
            if len(current_form_elements) < len(self.original_form_elements) * 0.5:
                reduction_rate = 1 - (len(current_form_elements) / len(self.original_form_elements))
                confidence = 0.75 + (reduction_rate * 0.1)  # 減少率に応じて信頼度調整
                
                return {
                    'success': True,
                    'stage': 3,
                    'stage_name': 'フォーム消失判定 (大幅減少)',
                    'confidence': min(confidence, 0.83),
                    'details': {
                        'original_form_elements': len(self.original_form_elements),
                        'current_form_elements': len(current_form_elements),
                        'reduction_rate': reduction_rate,
                        'disappearance_type': 'significant_reduction'
                    },
                    'message': f'フォーム要素が大幅に減少しました (減少率: {reduction_rate:.1%})'
                }
            
            # 送信ボタン消失判定
            submit_buttons = await self.page.query_selector_all(
                'input[type="submit"], button[type="submit"], '
                'button:has-text("送信"), button:has-text("確認"), '
                'input[value*="送信"], input[value*="確認"]'
            )
            
            visible_submit_buttons = []
            for button in submit_buttons:
                try:
                    if await button.is_visible():
                        visible_submit_buttons.append(button)
                except Exception:
                    continue
            
            if len(visible_submit_buttons) == 0:
                return {
                    'success': True,
                    'stage': 3,
                    'stage_name': 'フォーム消失判定 (送信ボタン消失)',
                    'confidence': 0.78,
                    'details': {
                        'submit_buttons_disappeared': True,
                        'current_form_elements': len(current_form_elements),
                        'disappearance_type': 'submit_button'
                    },
                    'message': '送信ボタンが消失しました'
                }
            
            return {
                'success': False,
                'stage': 3,
                'stage_name': 'フォーム消失判定',
                'confidence': 0.0,
                'details': {
                    'original_form_elements': len(self.original_form_elements),
                    'current_form_elements': len(current_form_elements),
                    'visible_submit_buttons': len(visible_submit_buttons),
                    'no_significant_disappearance': True
                },
                'message': 'フォームの有意な消失は検出されませんでした'
            }
            
        except Exception as e:
            logger.error(f"Stage3 フォーム消失判定エラー: {e}")
            return {
                'success': False,
                'stage': 3,
                'stage_name': 'フォーム消失判定',
                'confidence': 0.0,
                'details': {'error': str(e)},
                'message': f'フォーム消失判定でエラーが発生しました: {e}'
            }
    
    async def _judge_stage4_sibling_analysis(self) -> Dict[str, Any]:
        """Stage 4: 兄弟要素解析判定 (75-80% accuracy)"""
        try:
            # フォーム周辺の新しい要素を検索
            form_containers = await self.page.query_selector_all(
                'form, .form, #form, .contact, .inquiry, .contact-form'
            )
            
            new_elements_found = []
            
            for container in form_containers:
                try:
                    # コンテナ内の新しい要素をチェック
                    sibling_elements = await container.query_selector_all(
                        '.success, .complete, .thanks, .message, .alert, '
                        '.notification, .confirmation, .result, .status'
                    )
                    
                    for element in sibling_elements:
                        if await element.is_visible():
                            element_text = await element.inner_text()
                            element_classes = await element.get_attribute('class') or ''
                            element_id = await element.get_attribute('id') or ''
                            
                            # 成功指標のクラス/ID名チェック
                            success_class_indicators = [
                                'success', 'complete', 'thanks', 'thank',
                                'confirmation', 'confirmed', 'sent', 'submitted'
                            ]
                            
                            class_match = any(
                                indicator in element_classes.lower() or indicator in element_id.lower()
                                for indicator in success_class_indicators
                            )
                            
                            # テキストベースの成功判定
                            text_match = any(
                                re.search(pattern, element_text, re.IGNORECASE)
                                for pattern in self.success_patterns[:5]  # 主要パターンのみ
                            )
                            
                            if class_match or text_match:
                                new_elements_found.append({
                                    'element': element.tag_name,
                                    'text': element_text,
                                    'classes': element_classes,
                                    'id': element_id,
                                    'class_match': class_match,
                                    'text_match': text_match
                                })
                                
                except Exception:
                    continue
            
            if new_elements_found:
                # 信頼度計算
                base_confidence = 0.77
                element_bonus = min(len(new_elements_found) * 0.01, 0.03)
                final_confidence = min(base_confidence + element_bonus, 0.80)
                
                return {
                    'success': True,
                    'stage': 4,
                    'stage_name': '兄弟要素解析判定',
                    'confidence': final_confidence,
                    'details': {
                        'new_elements_found': new_elements_found,
                        'element_count': len(new_elements_found)
                    },
                    'message': f'成功を示す新しい要素が検出されました ({len(new_elements_found)}件)'
                }
            
            # フォーム状態変化の確認
            disabled_elements = await self.page.query_selector_all(
                'form input:disabled, form textarea:disabled, form select:disabled, '
                'form button:disabled'
            )
            
            hidden_elements = await self.page.query_selector_all(
                'form input[type="hidden"], form .hidden, form [style*="display: none"]'
            )
            
            if len(disabled_elements) > 0:
                return {
                    'success': True,
                    'stage': 4,
                    'stage_name': '兄弟要素解析判定 (要素無効化)',
                    'confidence': 0.75,
                    'details': {
                        'disabled_elements': len(disabled_elements),
                        'hidden_elements': len(hidden_elements),
                        'state_change_detected': True
                    },
                    'message': f'フォーム要素の無効化が検出されました ({len(disabled_elements)}件)'
                }
            
            return {
                'success': False,
                'stage': 4,
                'stage_name': '兄弟要素解析判定',
                'confidence': 0.0,
                'details': {
                    'no_significant_changes': True,
                    'checked_containers': len(form_containers)
                },
                'message': '成功を示す要素変化は検出されませんでした'
            }
            
        except Exception as e:
            logger.error(f"Stage4 兄弟要素解析判定エラー: {e}")
            return {
                'success': False,
                'stage': 4,
                'stage_name': '兄弟要素解析判定',
                'confidence': 0.0,
                'details': {'error': str(e)},
                'message': f'兄弟要素解析判定でエラーが発生しました: {e}'
            }
    
    async def _judge_stage5_error_patterns(self) -> Dict[str, Any]:
        """Stage 5: エラーパターン判定 (70-75% accuracy) - 参考リポジトリの詳細分類システム準拠"""
        try:
            # ページ全体のテキストコンテンツを取得
            page_content = await self.page.inner_text('body')
            
            # エラーメッセージパターンマッチング (分類別)
            detected_errors = {}
            error_matches = []
            
            # configベースのエラーインジケータ（簡易）
            try:
                if self._matcher.contains_error_indicators(page_content):
                    detected_errors.setdefault('一般エラー', []).append({
                        'pattern': 'config:error_indicator',
                        'text': 'config matched'
                    })
            except Exception:
                pass
            
            for error_type, patterns in self.error_patterns.items():
                for pattern in patterns:
                    matches = re.finditer(pattern, page_content, re.IGNORECASE)
                    for match in matches:
                        if error_type not in detected_errors:
                            detected_errors[error_type] = []
                        detected_errors[error_type].append({
                            'pattern': pattern,
                            'text': match.group(),
                            'start': match.start(),
                            'end': match.end()
                        })
                        error_matches.append({
                            'error_type': error_type,
                            'pattern': pattern,
                            'text': match.group(),
                            'start': match.start(),
                            'end': match.end()
                        })
            
            # エラー要素の検索
            error_elements = await self.page.query_selector_all(
                '.error, .alert-danger, .alert-error, .warning, '
                '[class*="error"], [class*="danger"], [class*="warning"], '
                '[id*="error"], [id*="danger"], [id*="warning"]'
            )
            
            visible_error_elements = []
            for element in error_elements:
                try:
                    if await element.is_visible():
                        element_text = await element.inner_text()
                        element_classes = await element.get_attribute('class') or ''
                        
                        # エラーテキストの分類別確認
                        error_type = '一般エラー'  # デフォルト
                        for err_type, patterns in self.error_patterns.items():
                            if any(re.search(pattern, element_text, re.IGNORECASE) for pattern in patterns):
                                error_type = err_type
                                break
                        
                        if element_text.strip() or 'error' in element_classes.lower():
                            visible_error_elements.append({
                                'error_type': error_type,
                                'element': element.tag_name,
                                'text': element_text,
                                'classes': element_classes
                            })
                except Exception:
                    continue
            
            # エラーが検出された場合 (失敗判定)
            if error_matches or visible_error_elements or any(detected_errors.values()):
                total_error_indicators = len(error_matches) + len(visible_error_elements) + sum(len(v) for v in detected_errors.values())
                
                # 最も重要度の高いエラー分類を特定
                priority_order = [
                    'reCAPTCHA認証があるため送信不可',
                    '営業お断りを検出', 
                    'システムエラー',
                    'メール形式エラー',
                    '必須項目未入力',
                    '再試行要請',
                    '一般エラー'
                ]
                
                primary_error_type = '一般エラー'
                for error_type in priority_order:
                    if error_type in detected_errors:
                        primary_error_type = error_type
                        break
                
                return {
                    'success': False,  # エラーが検出されたので失敗
                    'stage': 5,
                    'stage_name': f'エラーパターン判定 ({primary_error_type})',
                    'confidence': min(0.70 + (total_error_indicators * 0.01), 0.75),
                    'details': {
                        'primary_error_type': primary_error_type,
                        'detected_errors': detected_errors,
                        'error_matches': error_matches,
                        'visible_error_elements': visible_error_elements,
                        'total_error_indicators': total_error_indicators
                    },
                    'message': f'{primary_error_type}が検出されました ({total_error_indicators}件の指標)'
                }
            
            # エラーが検出されない場合 (成功の可能性)
            return {
                'success': False,  # Stage5では判定を保留し、次のステージへ
                'stage': 5,
                'stage_name': 'エラーパターン判定 (エラー未検出)',
                'confidence': 0.72,
                'details': {
                    'no_errors_detected': True,
                    'checked_error_types': len(self.error_patterns),
                    'total_patterns_checked': sum(len(patterns) for patterns in self.error_patterns.values()),
                    'checked_elements': len(error_elements)
                },
                'message': 'エラーメッセージは検出されませんでした'
            }
            
        except Exception as e:
            logger.error(f"Stage5 エラーパターン判定エラー: {e}")
            return {
                'success': False,
                'stage': 5,
                'stage_name': 'エラーパターン判定',
                'confidence': 0.0,
                'details': {'error': str(e)},
                'message': f'エラーパターン判定でエラーが発生しました: {e}'
            }
    
    async def _judge_stage6_failure_patterns(self) -> Dict[str, Any]:
        """Stage 6: 失敗パターン判定 (65-70% accuracy) - 最終判定"""
        try:
            # レスポンス履歴の確認
            response_analysis = self._analyze_response_history()
            
            # ページ読み込み状態の確認
            page_state = await self._analyze_page_state()
            
            # 最終的な成功/失敗判定（参考リポジトリの判定基準準拠）
            failure_indicators = []
            
            # 1. URL失敗パターン検出 (参考リポジトリ由来)
            current_url = self.page.url.lower()
            failure_url_patterns = [
                r'error', r'fail', r'invalid', r'エラー', r'失敗',
                r'404', r'403', r'500', r'timeout'
            ]
            
            for pattern in failure_url_patterns:
                if re.search(pattern, current_url):
                    failure_indicators.append(f'URL失敗パターン: {pattern}')
                    break
            
            # 2. HTTP エラーレスポンスの確認
            if response_analysis.get('has_error_responses'):
                failure_indicators.append('HTTP エラーレスポンス検出')
            
            # 3. ページ読み込みエラーの確認
            if page_state.get('loading_errors'):
                failure_indicators.append('ページ読み込みエラー')
                
            # 4. JavaScriptアラートの検出 (参考リポジトリ由来)
            try:
                # ダイアログ検出の試行（可能な範囲で）
                # Note: Playwrightではダイアログハンドラーを事前設定する必要があるため、
                # ここでは基本的な要素ベースの検出を実装
                
                alert_elements = await self.page.query_selector_all(
                    '[role="alert"], .alert, .dialog, .modal, .popup, '
                    '.js-error, .javascript-error, .error-dialog'
                )
                
                for alert_element in alert_elements:
                    if await alert_element.is_visible():
                        alert_text = await alert_element.inner_text()
                        if alert_text.strip():
                            failure_indicators.append(f'アラート/ダイアログ検出: {alert_text[:50]}...')
                            break
                    
            except Exception:
                pass
            
            # 5. JavaScript実行エラーの確認（基本レベル）
            try:
                js_error_elements = await self.page.query_selector_all(
                    '.js-error, .javascript-error, [data-error], .error-message'
                )
                
                for error_elem in js_error_elements:
                    if await error_elem.is_visible():
                        failure_indicators.append('JavaScript エラー要素検出')
                        break
                        
            except Exception:
                pass
            
            # 失敗指標が多い場合は失敗と判定
            if len(failure_indicators) >= 2:
                return {
                    'success': False,
                    'stage': 6,
                    'stage_name': '失敗パターン判定 (失敗)',
                    'confidence': 0.68,
                    'details': {
                        'failure_indicators': failure_indicators,
                        'response_analysis': response_analysis,
                        'page_state': page_state
                    },
                    'message': f'失敗パターンが検出されました: {", ".join(failure_indicators)}'
                }
            
            # 明確な失敗指標がない場合、成功の可能性が高い
            # ただし信頼度は低めに設定 (最終ステージのため)
            success_confidence = 0.65
            
            # 前のステージでエラーが検出されなかった場合は信頼度を上げる
            if not failure_indicators:
                success_confidence = 0.70
            
            return {
                'success': True,
                'stage': 6,
                'stage_name': '失敗パターン判定 (成功推定)',
                'confidence': success_confidence,
                'details': {
                    'failure_indicators': failure_indicators,
                    'response_analysis': response_analysis,
                    'page_state': page_state,
                    'fallback_success': True
                },
                'message': '明確な失敗パターンが検出されないため、成功と推定されます'
            }
            
        except Exception as e:
            logger.error(f"Stage6 失敗パターン判定エラー: {e}")
            return {
                'success': False,
                'stage': 6,
                'stage_name': '失敗パターン判定',
                'confidence': 0.0,
                'details': {'error': str(e)},
                'message': f'失敗パターン判定でエラーが発生しました: {e}'
            }
    
    def _analyze_response_history(self) -> Dict[str, Any]:
        """レスポンス履歴の分析"""
        try:
            error_responses = []
            redirect_responses = []
            success_responses = []
            
            for response in self.response_history:
                status = response['status']
                if status >= 400:
                    error_responses.append(response)
                elif 300 <= status < 400:
                    redirect_responses.append(response)
                elif 200 <= status < 300:
                    success_responses.append(response)
            
            return {
                'total_responses': len(self.response_history),
                'error_responses': error_responses,
                'redirect_responses': redirect_responses,
                'success_responses': success_responses,
                'has_error_responses': len(error_responses) > 0,
                'has_redirects': len(redirect_responses) > 0
            }
            
        except Exception as e:
            logger.debug(f"レスポンス履歴分析エラー: {e}")
            return {'analysis_error': str(e)}
    
    async def _analyze_page_state(self) -> Dict[str, Any]:
        """ページ状態の分析"""
        try:
            # ページタイトルの取得
            page_title = await self.page.title()
            
            # ページのreadyStateを確認
            ready_state = await self.page.evaluate('document.readyState')
            
            # エラーページの特徴を確認
            error_indicators = []
            
            # 404, 500 などのエラーページ指標
            error_title_patterns = [
                r'404|not found|ページが見つかりません',
                r'500|internal server error|サーバーエラー',
                r'403|forbidden|アクセス拒否',
                r'エラー|error|問題が発生'
            ]
            
            for pattern in error_title_patterns:
                if re.search(pattern, page_title, re.IGNORECASE):
                    error_indicators.append(f'エラータイトル: {page_title}')
                    break
            
            return {
                'page_title': page_title,
                'ready_state': ready_state,
                'error_indicators': error_indicators,
                'loading_errors': len(error_indicators) > 0
            }
            
        except Exception as e:
            logger.debug(f"ページ状態分析エラー: {e}")
            return {'analysis_error': str(e)}
