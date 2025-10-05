"""
Playwrightブラウザのライフサイクル管理
"""
import asyncio
import logging
import os
import platform
from typing import Optional, Dict, Any, List

from playwright.async_api import (
    async_playwright,
    Playwright,
    Browser,
    BrowserContext,
    Page,
    TimeoutError as PlaywrightTimeoutError
)
from playwright_stealth import Stealth
from form_sender.utils.cookie_blocker import install_cookie_routes, install_init_script, try_reject_banners

logger = logging.getLogger(__name__)


class BrowserManager:
    """Playwrightブラウザの起動、ページ作成、終了を管理する"""

    def __init__(self, worker_id: int, headless: bool = None, config: Dict[str, Any] = None):
        self.worker_id = worker_id
        self.headless = headless
        self.config = config or {}

        self.playwright: Optional[Playwright] = None
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self._stealth: Optional[Stealth] = None
        self._stealth_applied: bool = False
        self._stealth_cm = None  # async context manager returned by Stealth().use_async(async_playwright())
        self._stealth_enabled: bool = True
        # playwright-stealth バージョン両対応用
        self._stealth_api: str = "unknown"  # 'v2' | 'v1' | 'none'
        self._stealth_async_func = None  # v1 の場合: function(page) -> awaitable
        # コンテキストのライフサイクル用ロック（遅延初期化: 実行中のイベントループに結びつける）
        self._context_lock = None
        self._context_lock_loop = None

        # 設定値
        # デフォルトはやや長め（初回読み込みの安定性重視）
        self.timeout_settings = self.config.get("timeout_settings", {"page_load": 30000})
        worker_cfg = self.config.get("worker_config", {})
        browser_cfg = worker_cfg.get("browser", {})
        rb_cfg = browser_cfg.get("resource_blocking", {})
        stealth_cfg = browser_cfg.get("stealth", {}) if isinstance(browser_cfg, dict) else {}
        # 既定は設計方針に合わせてブロックON（画像/フォント）。設定で上書き可能。
        self._rb_block_images = bool(rb_cfg.get("block_images", True))
        self._rb_block_fonts = bool(rb_cfg.get("block_fonts", True))
        self._rb_block_stylesheets = bool(rb_cfg.get("block_stylesheets", False))
        # ステルス設定
        try:
            self._stealth_enabled = bool(stealth_cfg.get("enabled", True))
            # navigator.languages をより自然にする。指定が無い場合は日本語優先。
            self._navigator_languages = tuple(stealth_cfg.get("languages", ["ja-JP", "ja"]))
        except Exception:
            self._stealth_enabled = True
            self._navigator_languages = ("ja-JP", "ja")

    async def launch(self) -> bool:
        """Playwrightブラウザを初期化して起動する"""
        try:
            logger.info(f"Worker {self.worker_id}: Initializing Playwright browser")
            is_github_actions = os.getenv("GITHUB_ACTIONS") == "true"

            # playwright-stealth のバージョン検出と起動
            if self._stealth_enabled:
                # v2（Stealth/use_async）優先、失敗したら v1（stealth_async）→ 最後にプレーン
                try:
                    from playwright_stealth import Stealth as _S  # type: ignore
                    self._stealth = _S(
                        navigator_languages_override=self._navigator_languages
                    )
                    self._stealth_cm = self._stealth.use_async(async_playwright())
                    self.playwright = await self._stealth_cm.__aenter__()
                    self._stealth_api = 'v2'
                    logger.info(f"Worker {self.worker_id}: Playwright initialized with stealth v2 context manager")
                except Exception as e_v2:
                    # v1: ページ単位で適用（後続で new_page 時に適用）
                    try:
                        from playwright_stealth import stealth_async as _stealth_async  # type: ignore
                        self.playwright = await async_playwright().start()
                        self._stealth_api = 'v1'
                        self._stealth_async_func = _stealth_async
                        logger.info(f"Worker {self.worker_id}: Playwright initialized with stealth v1 (page-level)")
                    except Exception as e_v1:
                        self._stealth_cm = None
                        self._stealth = None
                        self._stealth_api = 'none'
                        self.playwright = await async_playwright().start()
                        logger.warning(f"Worker {self.worker_id}: Stealth unavailable, using plain Playwright (v2 err: {e_v2}; v1 err: {e_v1})")
            else:
                self.playwright = await async_playwright().start()
            if is_github_actions:
                await asyncio.sleep(0.5)

            browser_args = self._get_browser_args(is_github_actions)
            launch_timeout = 60000 if is_github_actions else 30000

            # 環境変数で強制切替を許可（ローカル検証の安定化用）
            env_headless = os.getenv('PLAYWRIGHT_HEADLESS', '').lower()
            use_headless_env = True if env_headless in ['1', 'true', 'yes'] else False if env_headless in ['0', 'false', 'no'] else None

            use_headless = (
                use_headless_env if use_headless_env is not None else (self.headless if self.headless is not None else True)
            )
            mode_desc = "headless" if use_headless else "GUI"
            logger.info(f"Worker {self.worker_id}: Using {mode_desc} mode")

            # slow_mo はデフォルト無効。必要時のみ環境変数で指定（ms）
            slow_env = os.getenv('PLAYWRIGHT_SLOW_MO_MS', '').strip()
            slow_kw = {}
            if slow_env.isdigit() and int(slow_env) > 0:
                slow_kw = {"slow_mo": int(slow_env)}

            # macOS の GUI 実行ではシステムの Chrome を優先利用（安定化）
            use_chrome_channel = (platform.system().lower() == 'darwin' and not use_headless and not is_github_actions)
            launch_succeeded = False
            last_err: Optional[Exception] = None

            if use_chrome_channel:
                try:
                    self.browser = await self.playwright.chromium.launch(
                        headless=False,
                        channel='chrome',  # システム Chrome 経由
                        timeout=launch_timeout,
                        **slow_kw,
                    )
                    launch_succeeded = True
                    logger.info(f"Worker {self.worker_id}: Launched system Chrome via channel")
                except Exception as e:
                    last_err = e
                    logger.warning(f"Worker {self.worker_id}: Failed to launch system Chrome, falling back to bundled Chromium: {e}")

            if not launch_succeeded:
                # 既定: バンドルされた Chromium を利用
                self.browser = await self.playwright.chromium.launch(
                    headless=use_headless,
                    args=browser_args,
                    timeout=launch_timeout,
                    **slow_kw,
                )

            # 環境に関わらず、起動直後は短い待機を入れて安定化
            await asyncio.sleep(0.5 if not is_github_actions else 1.0)

            logger.info(f"Worker {self.worker_id}: Browser initialized successfully")
            return True

        except Exception as e:
            logger.error(f"Worker {self.worker_id}: Browser initialization failed: {e}")
            return False

    def _get_browser_args(self, is_github_actions: bool) -> List[str]:
        """起動時のブラウザ引数を取得する"""
        # ローカル（macOS等）では安定性を最優先し、ブラウザ引数は極力付けない
        if not is_github_actions and platform.system().lower() == 'darwin':
            return []

        base_args = [
            "--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage",
            "--disable-software-rasterizer", "--disable-web-security",
            "--disable-extensions", "--disable-plugins", "--disable-images", "--no-first-run",
            "--disable-background-timer-throttling", "--disable-renderer-backgrounding",
            "--disable-backgrounding-occluded-windows", "--disable-ipc-flooding-protection",
            "--disable-features=VizDisplayCompositor", "--disable-background-networking",
        ]
        if is_github_actions:
            base_args.extend([
                "--memory-pressure-off", "--max_old_space_size=2048",
                "--disable-sync", "--disable-translate", 
                "--force-color-profile=srgb", "--disable-accelerated-2d-canvas",
                "--disable-accelerated-jpeg-decoding", "--disable-accelerated-mjpeg-decode",
                "--disable-accelerated-video-decode", "--disable-threaded-animation",
                "--disable-threaded-scrolling",
            ])
        return base_args

    async def create_new_page(self, form_url: str) -> Page:
        """新しいブラウザコンテキストとページを作成し、指定URLにアクセスする"""
        if not self.browser:
            raise ConnectionError("Browser is not launched. Call launch() first.")
        
        # ブラウザが閉じられていないかチェック
        try:
            # ブラウザの状態を確認
            contexts = self.browser.contexts
        except Exception as e:
            raise ConnectionError(f"Browser connection lost: {e}")

        try:
            # 既存のコンテキストは極力再利用（GUI安定性優先）
            last_err: Optional[Exception] = None
            page: Optional[Page] = None  # retryスコープ外で初期化して参照安全性を確保
            for i in range(2):
                page = None
                try:
                    # 原子的な健全性チェック（並行競合を抑止）
                    try:
                        _lock = self._ensure_context_lock()
                        if _lock is not None:
                            async with _lock:
                                await self._ensure_context_health()
                        else:
                            await self._ensure_context_health()
                    except Exception:
                        # 健全性検査失敗は続行（下で再生成）
                        pass
                    # 二重の健全性チェックは不要（_ensure_context_health 内で実施済み）

                    # コンテキストが無い場合のみ作成
                    context_created_here = False
                    if not self.context:
                        # どの環境でもロケール/タイムゾーン/Accept-Language を固定（JST運用要件 + 自然な言語ヘッダ）
                        context_common = dict(
                            locale="ja-JP",
                            timezone_id="Asia/Tokyo",
                            extra_http_headers={
                                "Accept-Language": "ja, en-US;q=0.8, en;q=0.7",
                            },
                        )
                        # 作成はロック下で二重生成を回避
                        _lock = self._ensure_context_lock()
                        if _lock is not None:
                            async with _lock:
                                if not self.context:
                                    if platform.system().lower() == 'darwin' and (self.headless is False or self.headless is None):
                                        self.context = await self.browser.new_context(**context_common)
                                    else:
                                        # UAはページヘッダと整合するフル文字列を利用（検出回避のため一致させる）
                                        self.context = await self.browser.new_context(
                                            user_agent=(
                                                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                                "Chrome/120.0.0.0 Safari/537.36"
                                            ),
                                            **context_common,
                                        )
                                    context_created_here = True
                        else:
                            if platform.system().lower() == 'darwin' and (self.headless is False or self.headless is None):
                                self.context = await self.browser.new_context(**context_common)
                            else:
                                self.context = await self.browser.new_context(
                                    user_agent=(
                                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                                        "Chrome/120.0.0.0 Safari/537.36"
                                    ),
                                    **context_common,
                                )
                            context_created_here = True
                    # Cookie ブラックホール（任意）を context に注入（new_page より前）
                    try:
                        cookie_cfg = (self.config.get("worker_config", {}).get("browser", {}).get("cookie_control", {}) if isinstance(self.config, dict) else {})
                    except Exception:
                        cookie_cfg = {}
                    # 既定は無効（誤検出/互換性への影響を避ける）。設定で明示有効化時のみON。
                    await install_init_script(self.context, bool(cookie_cfg.get("override_document_cookie", False)))

                    # playwright-stealth を初回のみ適用（v2: context単位）
                    await self._ensure_stealth(self.context)
                    # 新規ページをオープン
                    page = await self.context.new_page()
                    # v1: ページ単位で stealth を適用
                    try:
                        if self._stealth_api == 'v1' and self._stealth_async_func:
                            await self._stealth_async_func(page)
                    except Exception:
                        pass
                    # UAの変更はmacOS GUIでは避けて安定性を優先。
                    # Cookieコントロールのネットワーク層（CMPブロック/Set-Cookie除去 + 資源ブロックも統合）
                    try:
                        rb_rules = {
                            "images": self._rb_block_images,
                            "fonts": self._rb_block_fonts,
                            "stylesheets": self._rb_block_stylesheets,
                        }
                        # 追加オプション（存在しない場合は安全な既定にフォールバック）
                        s_third = bool(cookie_cfg.get("strip_set_cookie_third_party_only", True))
                        s_domains = cookie_cfg.get("strip_set_cookie_domains", [])
                        if not isinstance(s_domains, list):
                            logger.warning("cookie_control.strip_set_cookie_domains must be a list; using empty list")
                            s_domains = []
                        s_exclude = cookie_cfg.get("strip_set_cookie_exclude_domains", [])
                        if not isinstance(s_exclude, list):
                            logger.warning("cookie_control.strip_set_cookie_exclude_domains must be a list; using empty list")
                            s_exclude = []
                        await install_cookie_routes(
                            page,
                            block_cmp_scripts=bool(cookie_cfg.get("block_cmp_scripts", True)),
                            strip_set_cookie=bool(cookie_cfg.get("strip_set_cookie", False)),
                            resource_block_rules=rb_rules,
                            strip_set_cookie_third_party_only=s_third,
                            strip_set_cookie_domains=list(s_domains),
                            strip_set_cookie_exclude_domains=list(s_exclude),
                        )
                    except Exception:
                        pass
                    if not (platform.system().lower() == 'darwin' and (self.headless is False or self.headless is None)):
                        await page.set_extra_http_headers({
                            "User-Agent": (
                                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) "
                                "Chrome/120.0.0.0 Safari/537.36"
                            ),
                            "Accept-Language": "ja, en-US;q=0.8, en;q=0.7",
                        })
                    else:
                        # macOS GUI でも Accept-Language を明示（UAはシステムChromeに委任）
                        try:
                            await page.set_extra_http_headers({
                                "Accept-Language": "ja, en-US;q=0.8, en;q=0.7",
                            })
                        except Exception:
                            pass

                    logger.info(f"Worker {self.worker_id}: Accessing target form page: ***URL_REDACTED***")
                    # 初期ロードは macOS GUI でも安定性重視で 'domcontentloaded' を既定とする
                    # （一部サイトで 'load' 待機中に対象が閉じられる事象を回避）
                    wait_state = 'domcontentloaded'
                    await page.goto(
                        form_url,
                        timeout=int(self.timeout_settings.get("page_load", 30000)),
                        wait_until=wait_state,
                    )
                    # 追加の待機はローカルGUIでは行わない（安定優先）
                    if not (platform.system().lower() == 'darwin' and (self.headless is False or self.headless is None)):
                        try:
                            await page.wait_for_load_state('networkidle', timeout=5000)
                        except Exception:
                            pass
                    # バナーUIの Reject 自動操作（短時間）
                    try:
                        await try_reject_banners(
                            page,
                            enabled=bool(cookie_cfg.get("ui_reject_banners", True)),
                            timeout_ms=int(self.timeout_settings.get("click_timeout", 5000))
                        )
                    except Exception:
                        pass
                    return page
                except PlaywrightTimeoutError as e:
                    last_err = e
                    try:
                        if page:
                            await page.close()
                    except Exception:
                        pass
                    # 当回で作成したcontextがあればクリーンアップ（リーク抑止）
                    if context_created_here:
                        try:
                            if self.context:
                                await self.context.close()
                        except Exception as _ctx_close_err:
                            try:
                                logger.debug(f"Worker {self.worker_id}: context close on timeout failed: {_ctx_close_err}")
                            except Exception:
                                pass
                        finally:
                            self.context = None
                            self._stealth_applied = False
                    logger.error(f"Worker {self.worker_id}: Page load timeout for ***URL_REDACTED*** (attempt {i+1}/2)")
                except Exception as e:
                    last_err = e
                    try:
                        if page:
                            await page.close()
                    except Exception:
                        pass
                    # ターゲット/接続クローズは一度だけ再試行
                    if any(k in str(e) for k in ["Target page", "Connection closed", "Browser connection lost", "Target closed"]):
                        # コンテキストが壊れている可能性が高いので破棄して再生成させる
                        try:
                            if self.context:
                                await self.context.close()
                        except Exception:
                            pass
                        # 破棄後は再適用させるためにフラグを落とす
                        self.context = None
                        self._stealth_applied = False
                        logger.warning(f"Worker {self.worker_id}: Retrying after transient page error (attempt {i+1}/2): {e}")
                        await asyncio.sleep(0.5)
                        continue
                    logger.error(f"Worker {self.worker_id}: Page access error for ***URL_REDACTED*** {e}")
                    break
            # ここまで来たら最後のエラーを送出
            raise last_err or Exception("Unknown page access error")

        except PlaywrightTimeoutError as e:
            # エラー時のクリーンアップ
            try:
                await self._cleanup_context_on_error()
            except Exception:
                pass
            logger.error(f"Worker {self.worker_id}: Page load timeout for ***URL_REDACTED***")
            raise e
        except Exception as e:
            # エラー時のクリーンアップ
            try:
                await self._cleanup_context_on_error()
            except Exception:
                pass
            logger.error(f"Worker {self.worker_id}: Page access error for ***URL_REDACTED*** {e}")
            raise e

    async def _ensure_context_health(self) -> None:
        """コンテキストの健全性を検査し、壊れていれば再生成する。"""
        if not self.context:
            return
        try:
            _ = self.context.pages
        except Exception:
            await self._recreate_context()

    async def _recreate_context(self) -> None:
        """安全にコンテキストを破棄し、次回作成に備える。"""
        old = self.context
        self.context = None
        self._stealth_applied = False
        if old:
            try:
                await old.close()
            except Exception:
                pass

    def _ensure_context_lock(self):
        """現在の実行ループに結びついたLockを遅延生成して返す。ループが無ければNone。

        - Lockが別ループにバインドされている場合は作り直す
        - Python 3.12以降の『別ループへのLock使用』RuntimeErrorを防ぐ
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # 実行中イベントループが無い状況（同期コンテキスト等）
            return None

        try:
            if self._context_lock is not None and self._context_lock_loop is loop:
                return self._context_lock
        except Exception:
            # 属性未定義等は作り直し
            pass

        # 新しいループに結びついたLockを生成
        self._context_lock = asyncio.Lock()
        self._context_lock_loop = loop
        return self._context_lock

    async def _ensure_stealth(self, context: BrowserContext) -> None:
        """playwright-stealth の回避スクリプトを適用（1回のみ）。

        - 既定では常に有効化。将来的に `config.worker_config.browser.stealth.enabled`
          のフラグを見て切り替える拡張を想定。
        """
        try:
            if self._stealth_applied:
                return
            # 設定フラグ（存在しない場合はデフォルト有効）
            enabled = True
            try:
                enabled = bool(self.config.get("worker_config", {}).get("browser", {}).get("stealth", {}).get("enabled", True))
            except Exception:
                enabled = True
            if not enabled:
                self._stealth_applied = True  # 明示的に無効化されている場合は再適用不要
                return

            if self._stealth_api == 'v2':
                if self._stealth is None:
                    try:
                        from playwright_stealth import Stealth as _S  # type: ignore
                        self._stealth = _S()
                    except Exception:
                        self._stealth_api = 'none'
                        return
                await self._stealth.apply_stealth_async(context)
                self._stealth_applied = True
                logger.info(f"Worker {self.worker_id}: Applied playwright-stealth v2 to context")
            else:
                # v1 はページ単位で new_page 後に適用するため、ここではフラグを変更しない
                pass
        except Exception as e:
            # 失敗しても処理は続行（回避が無くても動作自体は可能）
            logger.warning(f"Worker {self.worker_id}: Failed to apply stealth evasions (suppressed): {e}")

    async def _cleanup_context_on_error(self):
        """エラー時のコンテキストクリーンアップ"""
        # ページ起因のエラーではページのみを閉じ、コンテキストは維持して安定化
        try:
            if self.context:
                pages = self.context.pages
                for p in pages:
                    try:
                        await p.close()
                    except Exception:
                        pass
                # ページを全て閉じてもコンテキストに異常がある場合は安全に破棄
                try:
                    _ = self.context.pages
                except Exception:
                    try:
                        await self.context.close()
                    except Exception:
                        pass
                    finally:
                        self.context = None
                        self._stealth_applied = False
        except Exception:
            pass

    async def close(self):
        """ブラウザとPlaywrightインスタンスを閉じる"""
        # コンテキストを先にクローズ
        if self.context:
            try:
                await self.context.close()
                logger.info(f"Worker {self.worker_id}: Context closed.")
            except Exception as e:
                if "Connection closed" in str(e) or "Target closed" in str(e):
                    logger.warning(f"Worker {self.worker_id}: Context was already closed: {e}")
                else:
                    logger.error(f"Worker {self.worker_id}: Error closing context: {e}")
            finally:
                self.context = None
                # 次回新規context作成時に再度ステルス適用を行う
                self._stealth_applied = False

        if self.browser:
            try:
                # ブラウザが既に閉じられているかチェック
                if hasattr(self.browser, '_connection') and self.browser._connection and not self.browser._connection._closed:
                    await self.browser.close()
                    logger.info(f"Worker {self.worker_id}: Browser closed.")
                else:
                    logger.info(f"Worker {self.worker_id}: Browser already closed.")
            except Exception as e:
                # 接続が既に切れている場合は警告レベルでログ出力
                if "Connection closed" in str(e) or "Target closed" in str(e) or "invalid state" in str(e):
                    logger.warning(f"Worker {self.worker_id}: Browser was already closed: {e}")
                else:
                    logger.error(f"Worker {self.worker_id}: Error closing browser: {e}")
            finally:
                self.browser = None

        if self.playwright:
            # ステルス有無に関わらず、最終的に stop() を試みてプロセスリークを防ぐ
            try:
                if self._stealth_cm is not None:
                    try:
                        await self._stealth_cm.__aexit__(None, None, None)
                        logger.info(f"Worker {self.worker_id}: Stealth context manager exited.")
                    finally:
                        # CM 参照は破棄して以降の停止処理に影響しないようにする
                        self._stealth_cm = None
                # 冪等に stop() を呼ぶ（ステルス側で停止済みでも例外を握り潰して継続）
                try:
                    await self.playwright.stop()
                    logger.info(f"Worker {self.worker_id}: Playwright stopped.")
                except Exception as e:
                    if "invalid state" in str(e) or "Connection closed" in str(e):
                        logger.warning(f"Worker {self.worker_id}: Playwright was already stopped: {e}")
                    else:
                        logger.error(f"Worker {self.worker_id}: Error stopping Playwright: {e}")
            finally:
                self.playwright = None
