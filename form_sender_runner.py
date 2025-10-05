#!/usr/bin/env python3
"""
Form Sender Runner (自走4ワーカー版)

GASで事前整列された send_queue から原子的に専有し、
IsolatedFormWorker で送信→結果を mark_done RPC で確定する。

想定起動: GitHub Actionsから
  python src/form_sender_runner.py \
    --targeting-id 1 \
    --config-file "/tmp/client_config_*.json" \
    [--num-workers 4] [--headless auto]
"""

import argparse
import asyncio
import json
import logging
import multiprocessing as mp
import os
import signal
import sys
import time
from datetime import datetime, timezone, timedelta, date
from typing import Optional, Dict, Any, List

from supabase import create_client
import random
import time as _time
import hashlib

from form_sender.worker.isolated_worker import IsolatedFormWorker
from form_sender.security.log_sanitizer import setup_sanitized_logging
from form_sender.utils.error_classifier import ErrorClassifier
from form_sender.utils.client_data_loader import load_client_data_simple
from config.manager import get_worker_config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = setup_sanitized_logging(__name__)

# ============ Table / RPC Switching (companies vs companies_extra) ============
# GitHub Actions から渡される環境変数で切替。既定は従来テーブル。
COMPANY_TABLE = os.environ.get('COMPANY_TABLE', 'companies').strip() or 'companies'
SEND_QUEUE_TABLE = os.environ.get('SEND_QUEUE_TABLE', 'send_queue').strip() or 'send_queue'
USE_EXTRA_TABLE = (COMPANY_TABLE == 'companies_extra') or (SEND_QUEUE_TABLE == 'send_queue_extra')

# RPC名の切替（*_extra 未デプロイ環境では、後段のフォールバックがシグネチャ不一致のみを許容）
FN_CLAIM = 'claim_next_batch_extra' if USE_EXTRA_TABLE else 'claim_next_batch'
FN_MARK_DONE = 'mark_done_extra' if USE_EXTRA_TABLE else 'mark_done'
FN_REQUEUE = 'requeue_stale_assigned_extra' if USE_EXTRA_TABLE else 'requeue_stale_assigned'


class ExtraClientMismatchError(RuntimeError):
    """companies_extra.client と期待値の不一致を表す内部例外。"""

    def __init__(self, company_id: int, expected_client: Optional[str], actual_client: Optional[str], message: str):
        super().__init__(message)
        self.company_id = company_id
        self.expected_client = expected_client
        self.actual_client = actual_client


def _extract_extra_client_name(client_data: Dict[str, Any]) -> Optional[str]:
    """targetingのextra指定時に参照するclientシート上の会社名を抽出。"""
    if not USE_EXTRA_TABLE:
        return None
    try:
        client_section = client_data.get('client')
        if isinstance(client_section, dict):
            raw_name = client_section.get('company_name')
            if isinstance(raw_name, str):
                name = raw_name.strip()
                return name or None
    except Exception:
        pass
    return None


def _apply_extra_client_filter(query_builder, client_name: Optional[str]):
    """companies_extra向けにclient一致条件を適用するヘルパー。"""
    if USE_EXTRA_TABLE and client_name:
        return query_builder.eq('client', client_name)
    return query_builder


def _get_name_policy_exclude_keywords() -> List[str]:
    """企業名による除外ワード一覧を設定から取得（フォールバックあり）。

    - 既定: 医療/法律系に加え「学校」を含める
    - 設定: config/worker_config.json の runner.name_policy_exclude_keywords
    """
    default_words = ['医療法人', '病院', '法律事務所', '弁護士', '税理士', '弁理士', '学校']
    try:
        cfg = get_worker_config().get('runner', {})
        words = cfg.get('name_policy_exclude_keywords')
        if isinstance(words, list):
            cleaned: List[str] = []
            for w in words:
                if isinstance(w, str):
                    s = w.strip()
                    if s:
                        cleaned.append(s)
            # 設定が空配列なら既定にフォールバック
            return cleaned or default_words
    except Exception:
        pass
    return default_words


def _sanitize_field_mapping_for_storage(field_mapping: Dict[str, Any]) -> Dict[str, Any]:
    """submissions.field_mapping に『マッピング結果全体』を保存できるようJSON安全化する。

    仕様変更（保存対象の拡張）:
    - これまでの最小構造保存から方針を改め、スコア詳細・文脈なども含めた
      可能な限り“全体像”を保存する。
    - ただし Playwright の Locator など JSON 非対応のオブジェクトは除去する。
    - dict/list は再帰的に処理し、シリアライズ不能な値は文字列化または無視する。
    """
    import math
    # 最大再帰深さ（安全弁）
    try:
        max_depth = int(get_worker_config().get('storage', {}).get('sanitize_max_depth', 6))
        if max_depth < 1:
            max_depth = 1
    except Exception:
        max_depth = 6

    if not isinstance(field_mapping, dict):
        return {}

    DROP_KEYS = {"element"}  # 非シリアライズ（Playwright Locator など）

    def to_json_safe(value, depth: int = 0):
        if depth >= max_depth:
            return "<max_depth_reached>"
        # プリミティブ
        if value is None or isinstance(value, (str, bool, int)):
            return value
        if isinstance(value, float):
            # NaN/Inf は null に落とす
            return value if math.isfinite(value) else None
        # 配列系
        if isinstance(value, (list, tuple, set)):
            return [to_json_safe(v, depth + 1) for v in list(value)]
        # 連想配列
        if isinstance(value, dict):
            out = {}
            for k, v in value.items():
                # キーは文字列化（JSON仕様）
                try:
                    k_str = str(k)
                except Exception:
                    k_str = "__invalid_key__"
                if k_str in DROP_KEYS:
                    continue
                if k in DROP_KEYS:
                    continue
                out[k_str] = to_json_safe(v, depth + 1)
            return out
        # それ以外は文字列化（代表値として保持）
        try:
            s = str(value)
        except Exception:
            s = "<unserializable>"
        # 文字列が極端に長い場合でもDBのTOASTに任せる（カットはしない）
        return s

    out: Dict[str, Any] = {}
    for fname, info in field_mapping.items():
        if not isinstance(fname, str):
            try:
                fname = str(fname)
            except Exception:
                continue
        if not isinstance(info, dict):
            # 想定外だがそのままJSON化して格納
            out[fname] = to_json_safe(info)
            continue
        # フィールドごとに JSON 安全化（element キーは除去）
        cleaned = to_json_safe({k: v for k, v in info.items() if k not in DROP_KEYS})
        out[fname] = cleaned
    return out


class _LifecycleOnlyFilter(logging.Filter):
    """ワークフローの標準ログを最小化するためのフィルタ。

    - INFO以上は form_sender.lifecycle のみ通す
    - ERROR以上は全ロガー通す（致命的情報は見える化）
    - それ以外は抑制
    """

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        try:
            if record.levelno >= logging.ERROR:
                return True
            name = record.name or ""
            if record.levelno >= logging.INFO and name.startswith("form_sender.lifecycle"):
                return True
            return False
        except Exception:
            # フィルタで例外が出てもログ消失は避け ERROR のみ通す
            return record.levelno >= logging.ERROR


def _get_lifecycle_logger() -> logging.Logger:
    """開始/完了専用のライフサイクルロガーを作成（INFOを必ず表示）。"""
    log = logging.getLogger("form_sender.lifecycle")
    log.setLevel(logging.INFO)
    # 独自ハンドラー（rootに依存しない）
    if not log.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        log.addHandler(handler)
        # サニタイズ適用
        setup_sanitized_logging("form_sender.lifecycle")
        # 親へは伝播しない
        log.propagate = False
    return log


def _install_logging_policy_for_ci():
    """CI/GitHub Actions用のログ抑制ポリシーを適用。

    - rootにフィルタを付与して非ライフサイクルのINFO/WARNを抑制
    - ワーカー配下は WARNING 以上でも出ないように（ERRORは許可）
    """
    try:
        if os.getenv('GITHUB_ACTIONS', '').lower() == 'true':
            root = logging.getLogger()
            # 二重追加防止（idで判定）
            if not any(isinstance(f, _LifecycleOnlyFilter) for f in getattr(root, 'filters', [])):
                root.addFilter(_LifecycleOnlyFilter())

            # ノイズが出やすいロガーはERROR以上のみ通す
            for noisy in [
                'form_sender.worker',
                'form_sender.analyzer',
                'playwright', 'urllib3', 'requests', 'supabase'
            ]:
                logging.getLogger(noisy).setLevel(logging.ERROR)
    except Exception:
        pass


def _should_fallback_on_rpc_error(exc: Exception, fn_name: str, new_param_keys: List[str]) -> bool:
    """新旧RPCのフォールバック可否を判定。

    - フォールバックは『関数が存在しない/シグネチャ不一致』に限定する。
    - それ以外（実行時例外、権限、業務ガード等）はフォールバックしない。
    """
    try:
        msg = (str(exc) or '').lower()
        fn_l = fn_name.lower()
        # 関数未存在/型不一致
        patterns_fn = [
            'does not exist',
            'no function matches',
            'undefined function',
        ]
        if fn_l in msg and any(p in msg for p in patterns_fn):
            return True
        # パラメータ不一致（新規キーがエラーに含まれる）
        if any(k.lower() in msg for k in (new_param_keys or [])) and any(s in msg for s in [
            'parameter', 'argument', 'unexpected', 'unknown', 'named', 'mismatch'
        ]):
            return True
    except Exception:
        return False
    return False


def jst_today() -> date:
    return (datetime.utcnow() + timedelta(hours=9)).date()


def jst_now() -> datetime:
    return datetime.now(timezone(timedelta(hours=9)))

def jst_utc_bounds(d: date):
    """指定JST日付のUTC境界 (start_utc, end_utc) を返す"""
    jst = timezone(timedelta(hours=9))
    start_jst = datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=jst)
    end_jst = start_jst + timedelta(days=1)
    return (start_jst.astimezone(timezone.utc), end_jst.astimezone(timezone.utc))


def _build_failure_classify_detail(error_type: Optional[str], base_detail: Optional[Dict[str, Any]], evidence: Dict[str, Any]) -> Dict[str, Any]:
    """失敗時のclassify_detailを一元生成（PROHIBITION_DETECTEDを優先補正）。"""
    try:
        if isinstance(base_detail, dict):
            bd = dict(base_detail)
            if isinstance(error_type, str) and error_type == 'PROHIBITION_DETECTED':
                bd.update({
                    'code': 'PROHIBITION_DETECTED',
                    'category': 'BUSINESS',
                    'retryable': False,
                    'cooldown_seconds': 0,
                    'confidence': 1.0,
                })
            # 新設: NO_MESSAGE_AREA を明示分類（フォーム構造上の欠落）
            if isinstance(error_type, str) and error_type == 'NO_MESSAGE_AREA':
                bd.update({
                    'code': 'NO_MESSAGE_AREA',
                    'category': 'FORM_STRUCTURE',
                    'retryable': False,
                    'cooldown_seconds': 0,
                    'confidence': 1.0,
                })
            if evidence:
                bd['evidence'] = evidence
            return bd
        # base_detail が無い場合も PROHIBITION_DETECTED を優先補正
        if isinstance(error_type, str) and error_type == 'PROHIBITION_DETECTED':
            return {
                'code': 'PROHIBITION_DETECTED',
                'category': 'BUSINESS',
                'retryable': False,
                'cooldown_seconds': 0,
                'confidence': 1.0,
                'evidence': evidence,
            }
        # base_detail が無い場合: NO_MESSAGE_AREA の明示分類
        if isinstance(error_type, str) and error_type == 'NO_MESSAGE_AREA':
            return {
                'code': 'NO_MESSAGE_AREA',
                'category': 'FORM_STRUCTURE',
                'retryable': False,
                'cooldown_seconds': 0,
                'confidence': 1.0,
                'evidence': evidence,
            }
        return {
            'code': error_type or 'UNKNOWN',
            'category': 'SYSTEM',
            'retryable': False,
            'cooldown_seconds': 0,
            'confidence': 0.0,
            'evidence': evidence,
        }
    except Exception:
        # 最低限のフォールバック
        return {
            'code': error_type or 'UNKNOWN',
            'category': 'GENERAL',
            'retryable': False,
            'cooldown_seconds': 0,
            'confidence': 0.0,
            'evidence': evidence,
        }




def _build_supabase_client():
    url = os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_SERVICE_ROLE_KEY')
    if not url or not key:
        raise RuntimeError('SUPABASE_URL / SUPABASE_SERVICE_ROLE_KEY is required')
    # 基本妥当性検証（https強制）
    if not str(url).startswith('https://'):
        raise ValueError('SUPABASE_URL must start with https://')
    return create_client(url, key)

def _extract_max_daily_sends(client_data: Dict[str, Any]) -> Optional[int]:
    """max_daily_sends を安全に抽出（正の整数のみ有効）"""
    try:
        targeting = client_data.get('targeting', {})
        mds = targeting.get('max_daily_sends')
        if mds is None:
            return None
        if isinstance(mds, str):
            s = mds.strip()
            try:
                mds = int(s)
            except (ValueError, TypeError):
                return None
        else:
            try:
                mds = int(mds)
            except (ValueError, TypeError):
                return None
        return int(mds) if int(mds) > 0 else None
    except Exception:
        return None

_SUCC_CACHE: Dict[str, Any] = {}
# 失敗分類の軽量キャッシュ（同一メッセージの連続多発時の負荷抑制）
_CLASSIFY_CACHE: Dict[str, Any] = {}
CLASSIFY_CACHE_MAX_SIZE = 256
CLASSIFY_CACHE_TTL_SEC = 600  # 10分で自然失効（設定で上書き可）


def _get_classify_cache_limits() -> (int, int):
    """config/worker_config.json の runner から制限値を取得（無ければデフォルト）。"""
    try:
        cfg = get_worker_config().get('runner', {})
        max_size = int(cfg.get('classify_cache_max_size', CLASSIFY_CACHE_MAX_SIZE))
        ttl = int(cfg.get('classify_cache_ttl_sec', CLASSIFY_CACHE_TTL_SEC))
        return max(16, max_size), max(60, ttl)
    except Exception:
        return CLASSIFY_CACHE_MAX_SIZE, CLASSIFY_CACHE_TTL_SEC


def _prune_classify_cache(now_ts: float) -> None:
    """TTL とサイズに基づいて簡易的にキャッシュを整理。"""
    try:
        max_size, ttl = _get_classify_cache_limits()
        # TTL 期限切れを最大16件だけ掃除（安全のためキーを一度リスト化）
        removed = 0
        keys_snapshot = list(_CLASSIFY_CACHE.keys())[:64]
        for k in keys_snapshot:
            ent = _CLASSIFY_CACHE.get(k)
            if not isinstance(ent, dict):
                _CLASSIFY_CACHE.pop(k, None)
                removed += 1
            elif now_ts - ent.get('ts', 0) > ttl:
                _CLASSIFY_CACHE.pop(k, None)
                removed += 1
            if removed >= 16:
                break
        # サイズ超過なら古い順に削除
        while len(_CLASSIFY_CACHE) > max_size:
            try:
                oldest_key = next(iter(_CLASSIFY_CACHE))
                _CLASSIFY_CACHE.pop(oldest_key, None)
            except StopIteration:
                break
    except Exception:
        # キャッシュ管理失敗は無視
        pass


def _classify_failure_detail(err_msg: Optional[str], add_data: Optional[Dict[str, Any]], error_type: Optional[str]) -> (Optional[Dict[str, Any]], Optional[bool]):
    """失敗詳細を分類し、classify_detail と bot_protection補助フラグを返す。"""
    try:
        http_status = None
        page_content = ''
        is_bot_ctx = False
        if isinstance(add_data, dict):
            ctx = add_data.get('classify_context') or {}
            if isinstance(ctx, dict):
                http_status = ctx.get('http_status')
                page_content = ctx.get('page_content_snippet', '')
                is_bot_ctx = bool(ctx.get('is_bot_detected'))

        # 軽量キャッシュ
        em = (err_msg or '')[:160]
        pc = (page_content or '')[:160]
        raw_key = f"{em}|{http_status}|{error_type or ''}|{pc}"
        cache_key = hashlib.sha1(raw_key.encode('utf-8', errors='ignore')).hexdigest()

        now_ts = _time.time()
        ent = _CLASSIFY_CACHE.get(cache_key)
        max_size, ttl = _get_classify_cache_limits()
        if ent and isinstance(ent, dict) and (now_ts - ent.get('ts', 0) <= ttl):
            detail = ent.get('detail')
        else:
            try:
                detail = ErrorClassifier.classify_detail(
                    error_message=err_msg or '',
                    page_content=page_content or '',
                    http_status=http_status,
                    context={'error_type_hint': error_type} if error_type else None,
                )
            except Exception as e:
                # 失敗分類の例外は握りつぶし、処理継続を最優先
                logger.warning(f"detail classification error (suppressed): {type(e).__name__}: {e}")
                return None, None
            _CLASSIFY_CACHE[cache_key] = {'detail': detail, 'ts': now_ts}
            _prune_classify_cache(now_ts)

        # bot 補助判定
        bot_flag = None
        try:
            code = detail.get('code') if isinstance(detail, dict) else None
            if code in {'BOT_DETECTED', 'WAF_CHALLENGE'} or is_bot_ctx:
                bot_flag = True
        except Exception:
            pass

        return detail, bot_flag
    except RuntimeError as e:
        logger.warning(f"detail classification failed: {e}")
        return None, None
    except Exception as e:
        # 予期せぬ例外も抑止し、分類不能として返す
        logger.warning(f"failure detail unexpected error (suppressed): {type(e).__name__}: {e}")
        return None, None


def _extract_evidence_from_additional(add_data: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Workerのadditional_dataから、DB保存用の根拠情報を抽出する。
    - 検出ワード（成功/失敗）
    - HTTPステータス/リダイレクトURL
    - URL変更・最終URL
    - 判定ステージ/信頼度
    機微になり得るページ本文は含めない（サニタイズ済みスニペットは別途保持されうる）。
    """
    evidence: Dict[str, Any] = {}
    try:
        if not isinstance(add_data, dict):
            return evidence

        judgment = add_data.get('judgment') or {}
        if not isinstance(judgment, dict):
            judgment = {}
        details = judgment.get('details') or {}
        if not isinstance(details, dict):
            details = {}

        # 検出ワード（成功側）
        success_words: List[str] = []
        try:
            for m in (details.get('success_matches') or []):
                t = (m.get('text') or '') if isinstance(m, dict) else ''
                if t:
                    success_words.append(t[:80])
            for m in (details.get('element_success_matches') or []):
                t = (m.get('text') or '') if isinstance(m, dict) else ''
                if t:
                    success_words.append(t[:80])
        except Exception:
            pass

        # 検出ワード（失敗側）
        failure_words: List[str] = []
        try:
            for m in (details.get('error_matches') or []):
                t = (m.get('text') or '') if isinstance(m, dict) else ''
                if t:
                    failure_words.append(t[:80])
            for m in (details.get('visible_error_elements') or []):
                t = (m.get('text') or '') if isinstance(m, dict) else ''
                if t:
                    failure_words.append(t[:80])
            # 早期失敗ゲートの厳格パターン
            for p in (details.get('matched_patterns') or []):
                if isinstance(p, str) and p:
                    failure_words.append(p[:80])
        except Exception:
            pass

        # HTTPレスポンス・リダイレクト
        redirect_urls: List[str] = []
        http_status = None
        try:
            resp = details.get('response_analysis') or {}
            if isinstance(resp, dict):
                for r in (resp.get('redirect_responses') or []):
                    if isinstance(r, dict) and r.get('url'):
                        redirect_urls.append(str(r.get('url')))
        except Exception:
            pass

        # classify_context優先のHTTPステータス
        try:
            ctx = add_data.get('classify_context') or {}
            if isinstance(ctx, dict) and isinstance(ctx.get('http_status'), int):
                http_status = ctx.get('http_status')
        except Exception:
            pass

        # URL情報
        try:
            final_url = add_data.get('final_url') if isinstance(add_data.get('final_url'), str) else None
            original_url = add_data.get('original_url') if isinstance(add_data.get('original_url'), str) else None
            if not final_url and isinstance(details.get('current_url'), str):
                final_url = details.get('current_url')
            if not original_url and isinstance(details.get('original_url'), str):
                original_url = details.get('original_url')
        except Exception:
            final_url = None
            original_url = None

        # ステージ/信頼度
        try:
            stage = int(judgment.get('stage')) if isinstance(judgment.get('stage'), (int, float)) else None
        except Exception:
            stage = None
        stage_name = judgment.get('stage_name') if isinstance(judgment.get('stage_name'), str) else None
        confidence = judgment.get('confidence') if isinstance(judgment.get('confidence'), (int, float)) else None

        # 営業禁止検出に関する付加情報（件数/レベル/検出元/信頼度）
        try:
            prohibition_phrases_count = None
            # SuccessJudge 経由（details）
            if isinstance(details.get('phrases_count'), (int, float)):
                try:
                    prohibition_phrases_count = int(details.get('phrases_count'))
                except Exception:
                    prohibition_phrases_count = None
            # Worker 早期検出サマリー
            if prohibition_phrases_count is None:
                ps = add_data.get('prohibition_summary') if isinstance(add_data.get('prohibition_summary'), dict) else None
                if ps and isinstance(ps.get('matches_count'), (int, float)):
                    try:
                        prohibition_phrases_count = int(ps.get('matches_count'))
                    except Exception:
                        prohibition_phrases_count = None
            # 追加メタ（任意）
            prohibition_detection_level = None
            prohibition_detection_source = None
            prohibition_confidence_level = None
            prohibition_confidence_score = None
            ps = add_data.get('prohibition_summary') if isinstance(add_data.get('prohibition_summary'), dict) else None
            if ps:
                if isinstance(ps.get('level'), str):
                    prohibition_detection_level = ps.get('level')
                if isinstance(ps.get('detection_source'), str):
                    prohibition_detection_source = ps.get('detection_source')
                if isinstance(ps.get('confidence_level'), str):
                    prohibition_confidence_level = ps.get('confidence_level')
                try:
                    if isinstance(ps.get('confidence_score'), (int, float)):
                        prohibition_confidence_score = float(ps.get('confidence_score'))
                except Exception:
                    prohibition_confidence_score = None
            if not prohibition_detection_source and isinstance(details.get('detection_method'), str):
                prohibition_detection_source = details.get('detection_method')
            # SuccessJudge 側の信頼度がある場合は優先採用
            if prohibition_confidence_level is None and isinstance(details.get('confidence_level'), str):
                prohibition_confidence_level = details.get('confidence_level')
            try:
                if prohibition_confidence_score is None and isinstance(details.get('confidence_score'), (int, float)):
                    prohibition_confidence_score = float(details.get('confidence_score'))
            except Exception:
                prohibition_confidence_score = None
        except Exception:
            prohibition_phrases_count = None
            prohibition_detection_level = None
            prohibition_detection_source = None
            prohibition_confidence_level = None
            prohibition_confidence_score = None

        evidence.update({
            'detected_success_words': success_words[:5] if success_words else [],
            'detected_failure_words': failure_words[:5] if failure_words else [],
            'http_status': http_status,
            'redirect_urls': redirect_urls[:5] if redirect_urls else [],
            'final_url': final_url,
            'original_url': original_url,
            'stage': stage,
            'stage_name': stage_name,
            'judge_confidence': confidence,
            # 追加: バリデーション/構造系のヒント（機微情報は含めない）
            'validation_issues': (add_data.get('validation_issues') or [])[:5] if isinstance(add_data.get('validation_issues'), list) else [],
            'dom_textareas_count': int(add_data.get('detected_dom_textareas_count') or 0) if isinstance(add_data.get('detected_dom_textareas_count'), (int, float)) else 0,
            # 営業禁止件数など（存在時のみ利用側で参照）
            'prohibition_phrases_count': prohibition_phrases_count,
            'prohibition_detection_level': prohibition_detection_level,
            'prohibition_detection_source': prohibition_detection_source,
            'prohibition_confidence_level': prohibition_confidence_level,
            'prohibition_confidence_score': prohibition_confidence_score,
        })
        return evidence
    except Exception:
        return evidence

def _get_success_count_today_jst(supabase, targeting_id: int, target_date: date) -> int:
    """当日(JST)成功数をUTC境界で集計"""
    try:
        # キャッシュキー（targeting_id + JST日付文字列）
        key = f"{targeting_id}:{target_date.isoformat()}"
        cfg = get_worker_config().get('runner', {})
        cache_sec = int(cfg.get('success_count_cache_seconds', 30))
        now = _time.time()
        ent = _SUCC_CACHE.get(key)
        if ent and (now - ent.get('ts', 0) < cache_sec):
            return int(ent.get('count', 0))

        start_utc, end_utc = jst_utc_bounds(target_date)
        resp = (
            supabase.table('submissions')
            .select('id', count='exact')
            .eq('targeting_id', targeting_id)
            .eq('success', True)
            .gte('submitted_at', start_utc.isoformat().replace('+00:00', 'Z'))
            .lt('submitted_at', end_utc.isoformat().replace('+00:00', 'Z'))
            .execute()
        )
        cnt = getattr(resp, 'count', None)
        if not isinstance(cnt, int):
            data = getattr(resp, 'data', None) or []
            cnt = len(data)
        # 更新
        _SUCC_CACHE[key] = {'count': int(cnt), 'ts': now}
        return int(cnt)
    except Exception:
        return 0


def _resolve_client_config_path(pattern: str) -> str:
    # ワイルドカード対応: 最も新しいファイルを選択
    if '*' in pattern:
        import glob
        files = glob.glob(pattern)
        if not files:
            raise FileNotFoundError(f'No client_config file matches: {pattern}')
        files.sort(key=lambda p: os.path.getmtime(p), reverse=True)
        return files[0]
    return pattern


def _within_business_hours(client_data: Dict[str, Any]) -> bool:
    try:
        targeting = client_data.get('targeting', {})
        days = targeting.get('send_days_of_week')
        start = targeting.get('send_start_time')  # 'HH:MM'
        end = targeting.get('send_end_time')

        if isinstance(days, str):
            try:
                days = json.loads(days)
            except Exception:
                days = None

        now_jst = jst_now()
        if isinstance(days, list) and len(days) > 0:
            # 0=Mon ... 6=Sun（Python weekday互換）
            if now_jst.weekday() not in days:
                return False

        def to_minutes(s: str) -> int:
            hh, mm = s.split(':')
            return int(hh) * 60 + int(mm)

        if not start or not end:
            return True
        cur_min = now_jst.hour * 60 + now_jst.minute
        # GAS側の実装に合わせて終了時刻を含む（<= end）
        return to_minutes(start) <= cur_min <= to_minutes(end)
    except Exception:
        return True


async def _process_one(supabase, worker: IsolatedFormWorker, targeting_id: int, client_data: Dict[str, Any], target_date: date, run_id: str, shard_id: Optional[int] = None, fixed_company_id: Optional[int] = None) -> bool:
    """1件専有→処理→確定。処理が無ければFalseを返す。"""
    expected_extra_client = _extract_extra_client_name(client_data)
    matched_extra_client: Optional[str] = None
    # 1) claim（固定 company_id が指定された場合は claim をスキップ）
    if fixed_company_id is None:
        params = {
            'p_target_date': str(target_date),
            'p_targeting_id': targeting_id,
            'p_run_id': run_id,
            'p_limit': 1,
            'p_shard_id': shard_id,
        }
        try:
            # 日次上限が設定されている場合は、同名の拡張版（p_max_daily付き）で呼び出し。
            max_daily = _extract_max_daily_sends(client_data)
            if max_daily is not None and max_daily > 0:
                cap_params = dict(params)
                cap_params['p_max_daily'] = max_daily
                try:
                    resp = supabase.rpc(FN_CLAIM, cap_params).execute()
                except Exception as e_cap:
                    # 1st: 引数不一致（p_max_daily 未対応）へのフォールバック
                    if _should_fallback_on_rpc_error(e_cap, FN_CLAIM, ['p_max_daily']):
                        try:
                            resp = supabase.rpc(FN_CLAIM, params).execute()
                        except Exception as e_cap2:
                            # 2nd: 関数未存在など → extra指定時は非対応（安全側: companies に触れない）
                            if (not USE_EXTRA_TABLE) and _should_fallback_on_rpc_error(e_cap2, FN_CLAIM, []):
                                resp = supabase.rpc('claim_next_batch', params).execute()
                            else:
                                raise
                    else:
                        # その他エラーはそのまま伝播
                        raise
            else:
                try:
                    resp = supabase.rpc(FN_CLAIM, params).execute()
                except Exception as e_nc:
                    if (not USE_EXTRA_TABLE) and _should_fallback_on_rpc_error(e_nc, FN_CLAIM, []):
                        resp = supabase.rpc('claim_next_batch', params).execute()
                    else:
                        raise
            rows = resp.data or []
        except Exception as e:
            logger.error(f"claim_next_batch RPC error: {e}")
            # バックオフの初期値（設定化）
            try:
                runner_cfg = get_worker_config().get('runner', {})
                sleep_s = int(runner_cfg.get('backoff_initial', 2))
            except Exception:
                sleep_s = 2
            await asyncio.sleep(sleep_s)
            return False

        if not rows:
            return False

        company_id = rows[0].get('company_id')
        # claim 時点の assigned_at（関数戻り値に含まれない環境では None）
        queue_assigned_at = None
        try:
            val = rows[0].get('assigned_at')
            if isinstance(val, str) and val:
                queue_assigned_at = val
        except Exception:
            queue_assigned_at = None
        # 処理開始ログ（最小限、IDのみ）
        try:
            wid = getattr(worker, 'worker_id', 0)
            _get_lifecycle_logger().info(
                f"process_start: company_id={company_id}, worker_id={wid}, targeting_id={targeting_id}"
            )
        except Exception:
            pass
    else:
        company_id = int(fixed_company_id)
        # 固定ID指定時も開始を記録
        try:
            wid = getattr(worker, 'worker_id', 0)
            _get_lifecycle_logger().info(
                f"process_start: company_id={company_id}, worker_id={wid}, targeting_id={targeting_id}"
            )
        except Exception:
            pass

    # 2) fetch company
    try:
        # ブラックリスト回避: companies.black が NULL のもののみ処理対象
        company_columns = 'id, form_url, black, company_name'
        if USE_EXTRA_TABLE:
            company_columns += ', client'
        comp = (
            supabase.table(COMPANY_TABLE)
            .select(company_columns)
            .eq('id', company_id)
            .limit(1)
            .execute()
        )
        if not comp.data:
            raise RuntimeError('company not found')
        company = comp.data[0]
        if USE_EXTRA_TABLE:
            if not expected_extra_client:
                raise ExtraClientMismatchError(
                    company_id,
                    expected_extra_client,
                    company.get('client') if isinstance(company, dict) else None,
                    'expected client name missing for extra targeting'
                )
            actual_client_raw = company.get('client') if isinstance(company, dict) else None
            if not isinstance(actual_client_raw, str):
                raise ExtraClientMismatchError(company_id, expected_extra_client, None, 'companies_extra.client missing')
            actual_client = actual_client_raw.strip()
            if not actual_client:
                raise ExtraClientMismatchError(company_id, expected_extra_client, actual_client_raw, 'companies_extra.client empty')
            if actual_client != expected_extra_client:
                raise ExtraClientMismatchError(company_id, expected_extra_client, actual_client, 'companies_extra.client mismatch')
            matched_extra_client = actual_client
            company['client'] = actual_client
        # まず企業名ポリシーでの除外判定を先行させ、後続の重複チェック(追加クエリ)を省略して負荷を下げる
        try:
            cname = company.get('company_name') or ''
            policy_words = _get_name_policy_exclude_keywords()
            matched = [w for w in policy_words if isinstance(cname, str) and (w in cname)]
            if matched:
                classify_detail = {
                    'code': 'SKIPPED_BY_NAME_POLICY',
                    'category': 'POLICY',
                    'retryable': False,
                    'cooldown_seconds': 0,
                    'confidence': 1.0,
                    'evidence': {
                        'name_policy_keywords': matched
                    }
                }
                _md_args = {
                    'p_target_date': str(target_date),
                    'p_targeting_id': targeting_id,
                    'p_company_id': company_id,
                    'p_success': False,
                    'p_error_type': 'SKIPPED_BY_NAME_POLICY',
                    'p_classify_detail': classify_detail,
                    'p_field_mapping': None,
                    'p_bot_protection': False,
                    'p_submitted_at': jst_now().isoformat(),
                    'p_run_id': run_id,
                }
                try:
                    supabase.rpc(FN_MARK_DONE, _md_args).execute()
                except Exception as e_mdnp:
                    if _should_fallback_on_rpc_error(e_mdnp, FN_MARK_DONE, ['p_run_id']):
                        _md_args.pop('p_run_id', None)
                        try:
                            supabase.rpc(FN_MARK_DONE, _md_args).execute()
                        except Exception as e_mdnp2:
                            if (not USE_EXTRA_TABLE) and _should_fallback_on_rpc_error(e_mdnp2, FN_MARK_DONE, []):
                                supabase.rpc('mark_done', _md_args).execute()
                            else:
                                raise
                    else:
                        raise
                try:
                    wid = getattr(worker, 'worker_id', 0)
                    _get_lifecycle_logger().info(
                        f"process_done: company_id={company_id}, worker_id={wid}, targeting_id={targeting_id}, success=False, reason=SKIPPED_BY_NAME_POLICY"
                    )
                except Exception:
                    pass
                return True
        except Exception:
            # 判定エラー時は通常処理を継続（安全側）
            pass
        # 当日すでに submissions 記録がある場合はスキップ（DB側JOINを外したため、ここで一意性を担保）
        try:
            start_utc, end_utc = jst_utc_bounds(target_date)
            dup = (
                supabase.table('submissions')
                .select('id', count='exact')
                .eq('targeting_id', targeting_id)
                .eq('company_id', company_id)
                .gte('submitted_at', start_utc.isoformat().replace('+00:00', 'Z'))
                .lt('submitted_at', end_utc.isoformat().replace('+00:00', 'Z'))
                .limit(1)
                .execute()
            )
            dup_cnt = getattr(dup, 'count', None)
            if not isinstance(dup_cnt, int):
                dup_cnt = len(getattr(dup, 'data', []) or [])
            if dup_cnt and dup_cnt > 0:
                classify_detail = {
                    'code': 'SKIPPED_ALREADY_SENT_TODAY',
                    'category': 'POLICY',
                    'retryable': False,
                    'cooldown_seconds': 0,
                    'confidence': 1.0,
                }
                _md_args = {
                    'p_target_date': str(target_date),
                    'p_targeting_id': targeting_id,
                    'p_company_id': company_id,
                    'p_success': False,
                    'p_error_type': 'SKIPPED_ALREADY_SENT_TODAY',
                    'p_classify_detail': classify_detail,
                    'p_field_mapping': None,
                    'p_bot_protection': False,
                    'p_submitted_at': jst_now().isoformat(),
                    'p_run_id': run_id,
                }
                try:
                    supabase.rpc(FN_MARK_DONE, _md_args).execute()
                except Exception as e_mdsk:
                    if _should_fallback_on_rpc_error(e_mdsk, FN_MARK_DONE, ['p_run_id']):
                        _md_args.pop('p_run_id', None)
                        try:
                            supabase.rpc(FN_MARK_DONE, _md_args).execute()
                        except Exception as e_mdsk2:
                            if (not USE_EXTRA_TABLE) and _should_fallback_on_rpc_error(e_mdsk2, FN_MARK_DONE, []):
                                supabase.rpc('mark_done', _md_args).execute()
                            else:
                                raise
                    else:
                        raise
                try:
                    wid = getattr(worker, 'worker_id', 0)
                    _get_lifecycle_logger().info(
                        f"process_done: company_id={company_id}, worker_id={wid}, targeting_id={targeting_id}, success=False, reason=SKIPPED_ALREADY_SENT_TODAY"
                    )
                except Exception:
                    pass
                return True
        except Exception as e_dup:
            # Fail-closed: 重複確認が失敗した場合は送信を中止し、キューを即時requeue（可能な範囲で）。
            logger.warning(f"duplicate check failed for company_id={company_id} (suppressed): {e_dup}")
            try:
                # 自分が割り当てた行のみ pending に戻す（競合安全）
                q = (
                    supabase.table(SEND_QUEUE_TABLE)
                    .update({'status': 'pending', 'assigned_by': None, 'assigned_at': None})
                    .eq('target_date_jst', str(target_date))
                    .eq('targeting_id', targeting_id)
                    .eq('company_id', company_id)
                    .eq('status', 'assigned')
                    .eq('assigned_by', run_id)
                )
                # 可能なら assigned_at の一致も条件に含めて競合安全性を高める
                if queue_assigned_at:
                    q = q.eq('assigned_at', queue_assigned_at)
                q.execute()
                try:
                    wid = getattr(worker, 'worker_id', 0)
                    _get_lifecycle_logger().info(
                        f"requeue_on_dupcheck_error: company_id={company_id}, worker_id={wid}, targeting_id={targeting_id}"
                    )
                except Exception:
                    pass
                # ここでは mark_done は呼ばない（再試行のため）。
                return True  # 処理はした（requeue済）と見なす
            except Exception as e_rq:
                # 即時requeueも失敗した場合は、assigned のまま放置し、stale requeue に委ねる
                logger.error(f"requeue after dupcheck failure error (company_id={company_id}): {e_rq}")
                # 送信は行わない。
                return True
        # black が NULL 以外（true/false含む）は処理対象外（要件: false はデータ上ほぼ存在しない想定）
        if company.get('black') is not None:
            raise RuntimeError('company blacklisted')
    except ExtraClientMismatchError as e_client:
        try:
            expected_hash = hashlib.sha256((e_client.expected_client or '').encode('utf-8')).hexdigest()[:10] if e_client.expected_client else 'none'
        except Exception:
            expected_hash = 'error'
        try:
            actual_hash = hashlib.sha256((e_client.actual_client or '').encode('utf-8')).hexdigest()[:10] if e_client.actual_client else 'none'
        except Exception:
            actual_hash = 'error'
        logger.warning(
            f"companies_extra client mismatch (company_id={e_client.company_id}, targeting_id={targeting_id}, expected_hash={expected_hash}, actual_hash={actual_hash}): {e_client}"
        )
        classify_detail = {
            'code': 'SKIPPED_WRONG_CLIENT',
            'category': 'POLICY',
            'retryable': False,
            'cooldown_seconds': 0,
            'confidence': 1.0,
            'evidence': {
                'expected_client': e_client.expected_client,
                'actual_client': e_client.actual_client,
                'note': str(e_client),
            }
        }
        _md_args = {
            'p_target_date': str(target_date),
            'p_targeting_id': targeting_id,
            'p_company_id': e_client.company_id,
            'p_success': False,
            'p_error_type': 'SKIPPED_WRONG_CLIENT',
            'p_classify_detail': classify_detail,
            'p_field_mapping': None,
            'p_bot_protection': False,
            'p_submitted_at': jst_now().isoformat(),
            'p_run_id': run_id,
        }
        try:
            supabase.rpc(FN_MARK_DONE, _md_args).execute()
        except Exception as e_mdmsc:
            if _should_fallback_on_rpc_error(e_mdmsc, FN_MARK_DONE, ['p_run_id']):
                _md_args.pop('p_run_id', None)
                try:
                    supabase.rpc(FN_MARK_DONE, _md_args).execute()
                except Exception as e_mdmsc2:
                    if (not USE_EXTRA_TABLE) and _should_fallback_on_rpc_error(e_mdmsc2, FN_MARK_DONE, []):
                        supabase.rpc('mark_done', _md_args).execute()
                    else:
                        raise
            else:
                raise
        try:
            wid = getattr(worker, 'worker_id', 0)
            _get_lifecycle_logger().info(
                f"process_done: company_id={e_client.company_id}, worker_id={wid}, targeting_id={targeting_id}, success=False, reason=SKIPPED_WRONG_CLIENT"
            )
        except Exception:
            pass
        return True
    except Exception as e:
        logger.error(f"fetch company error ({company_id}): {e}")
        # mark failed quickly（代表コードで詳細分類を付与）
        classify_detail = {
            'code': 'NOT_FOUND',
            'category': 'HTTP',
            'retryable': False,
            'cooldown_seconds': 0,
            'confidence': 1.0,
        }
        # mark_done（run_id検証付き）を呼び出し。未デプロイ時は旧シグネチャへフォールバック。
        _md_args = {
            'p_target_date': str(target_date),
            'p_targeting_id': targeting_id,
            'p_company_id': company_id,
            'p_success': False,
            'p_error_type': 'NOT_FOUND',
            'p_classify_detail': classify_detail,
            'p_field_mapping': None,
            'p_bot_protection': False,
            'p_submitted_at': jst_now().isoformat(),
            'p_run_id': run_id,
        }
        try:
            supabase.rpc(FN_MARK_DONE, _md_args).execute()
        except Exception as e_md:
            # フォールバックはシグネチャ不一致のみ（安全弁）。それ以外は中断。
            if _should_fallback_on_rpc_error(e_md, FN_MARK_DONE, ['p_run_id']):
                _md_args.pop('p_run_id', None)
                try:
                    supabase.rpc(FN_MARK_DONE, _md_args).execute()
                except Exception as e_md_f:
                    if (not USE_EXTRA_TABLE) and _should_fallback_on_rpc_error(e_md_f, FN_MARK_DONE, []):
                        supabase.rpc('mark_done', _md_args).execute()
                    else:
                        raise
            else:
                raise
        # 失敗完了ログ
        try:
            wid = getattr(worker, 'worker_id', 0)
            _get_lifecycle_logger().info(
                f"process_done: company_id={company_id}, worker_id={wid}, targeting_id={targeting_id}, success=False, reason=NOT_FOUND"
            )
        except Exception:
            pass
        return True

    # 3) process via worker
    if not company.get('form_url'):
        # 送信対象外を即確定
        classify_detail = {
            'code': 'NO_FORM_URL',
            'category': 'CONFIG',
            'retryable': False,
            'cooldown_seconds': 0,
            'confidence': 1.0,
        }
        _md_args = {
            'p_target_date': str(target_date),
            'p_targeting_id': targeting_id,
            'p_company_id': company_id,
            'p_success': False,
            'p_error_type': 'NO_FORM_URL',
            'p_classify_detail': classify_detail,
            'p_field_mapping': None,
            'p_bot_protection': False,
            'p_submitted_at': jst_now().isoformat(),
            'p_run_id': run_id,
        }
        try:
            supabase.rpc(FN_MARK_DONE, _md_args).execute()
        except Exception as e_md2:
            if _should_fallback_on_rpc_error(e_md2, FN_MARK_DONE, ['p_run_id']):
                _md_args.pop('p_run_id', None)
                try:
                    supabase.rpc(FN_MARK_DONE, _md_args).execute()
                except Exception as e_md2_f:
                    if (not USE_EXTRA_TABLE) and _should_fallback_on_rpc_error(e_md2_f, FN_MARK_DONE, []):
                        supabase.rpc('mark_done', _md_args).execute()
                    else:
                        raise
            else:
                raise
        # 失敗完了ログ
        try:
            wid = getattr(worker, 'worker_id', 0)
            _get_lifecycle_logger().info(
                f"process_done: company_id={company_id}, worker_id={wid}, targeting_id={targeting_id}, success=False, reason=NO_FORM_URL"
            )
        except Exception:
            pass
        # 軽量なステート掃除（会社間での痕跡最小化）。失敗しても続行。
        try:
            bm = getattr(worker, 'browser_manager', None)
            ctx = getattr(bm, 'context', None) if bm else None
            if ctx and hasattr(ctx, 'clear_cookies'):
                await ctx.clear_cookies()
        except Exception:
            pass
        return True

    # 3-a) 営業禁止事前チェックはワーカー側に統合（同一ページアクセスで実施）

    task_data = {
        'task_id': f'run-{run_id}-{company_id}',
        'task_type': 'process_company',
        'company_data': company,
        'client_data': client_data,
        'targeting_id': targeting_id,
        'worker_id': getattr(worker, 'worker_id', 0)
    }

    try:
        result = await worker.process_company_task(task_data)
    except Exception as e:
        logger.error(f"worker error ({company_id}): {e}")
        result = {
            'status': 'failed',
            'error_type': 'WORKER_ERROR',
            'bot_protection_detected': False,
            'error_message': str(e),
        }

    # WorkerResult dataclass → dict 互換
    status = getattr(result, 'status', None)
    if hasattr(status, 'value'):
        status_val = status.value
    else:
        status_val = result.get('status') if isinstance(result, dict) else None
    is_success = (status_val == 'success')
    error_type = getattr(result, 'error_type', None) if not is_success else None
    bp = getattr(result, 'bot_protection_detected', False)

    # 成功・失敗の根拠情報を抽出
    add_data = getattr(result, 'additional_data', None) if not isinstance(result, dict) else result.get('additional_data')
    evidence = _extract_evidence_from_additional(add_data)

    # 詳細分類（失敗時はErrorClassifierベース + 根拠を追加、成功時も根拠を保存）
    classify_detail = None
    if not is_success:
        # WorkerResult/dataclass 互換: error_message 取得
        err_msg = getattr(result, 'error_message', None) if not isinstance(result, dict) else result.get('error_message')
        base_detail, bot_flag = _classify_failure_detail(err_msg, add_data, error_type)
        if bot_flag:
            bp = True
        classify_detail = _build_failure_classify_detail(error_type, base_detail, evidence)
    else:
        # 成功時も根拠を保存（偽陽性/陰性検証のため）
        conf = None
        try:
            conf = float(evidence.get('judge_confidence')) if isinstance(evidence.get('judge_confidence'), (int, float)) else None
        except Exception:
            conf = None
        classify_detail = {
            'code': 'SUCCESS',
            'category': 'SUBMISSION',
            'retryable': False,
            'cooldown_seconds': 0,
            'confidence': conf if conf is not None else 0.8,
            'evidence': evidence
        }

    # 4) finalize via RPC（固定 company_id の場合も submissions 記録目的で呼ぶ。send_queue更新は0件でも問題なし）
    try:
        # 営業禁止検出時: companies.prohibition_detected=true を反映
        try:
            if (not is_success) and isinstance(error_type, str) and error_type == 'PROHIBITION_DETECTED':
                try:
                    query = supabase.table(COMPANY_TABLE).update({'prohibition_detected': True}).eq('id', company_id)
                    query = _apply_extra_client_filter(query, matched_extra_client)
                    query.execute()
                except Exception as ue:
                    logger.warning(f"companies.prohibition_detected update failed (company_id={company_id}, suppressed): {ue}")
        except Exception:
            pass

        # NO_MESSAGE_AREA 検出時: companies(または companies_extra).black = true を設定
        # - ランナー分類（classify_detail.code）も含めて NO_MESSAGE_AREA と判定された場合に反映
        # - DOMにお問い合わせ本文（textarea）が存在しないフォームは以後の送信対象から除外する
        try:
            code_val = None
            try:
                if isinstance(classify_detail, dict):
                    code_val = classify_detail.get('code')
            except Exception:
                code_val = None
            if (not is_success) and (
                (isinstance(error_type, str) and error_type == 'NO_MESSAGE_AREA') or code_val == 'NO_MESSAGE_AREA'
            ):
                try:
                    query = supabase.table(COMPANY_TABLE).update({'black': True}).eq('id', company_id)
                    query = _apply_extra_client_filter(query, matched_extra_client)
                    query.execute()
                except Exception as ue:
                    logger.warning(f"{COMPANY_TABLE}.black update failed (company_id={company_id}, suppressed): {ue}")
        except Exception:
            pass

        # Bot保護が検出されている場合は error_type を BOT_DETECTED に寄せる（優先）
        if not is_success:
            try:
                if bp and (
                    not error_type or (isinstance(error_type, str) and error_type not in {"BOT_DETECTED", "WAF_CHALLENGE"})
                ):
                    error_type = "BOT_DETECTED"
            except Exception:
                pass

        # submissions.field_mapping へ書き込むマッピング結果を決定（非シリアライズ要素のみ除去し、全体を保存）
        field_mapping_to_store = None
        try:
            ar = getattr(worker, '_current_analysis_result', None)
            if isinstance(ar, dict):
                fm = ar.get('field_mapping')
                if isinstance(fm, dict):
                    sanitized = _sanitize_field_mapping_for_storage(fm)
                    # 空でなく JSON 化できる場合は保存
                    if sanitized is not None:
                        # 念のため JSON シリアライズ検証
                        json.dumps(sanitized, ensure_ascii=False)
                        field_mapping_to_store = sanitized
        except Exception:
            field_mapping_to_store = None

        _md_args = {
            'p_target_date': str(target_date),
            'p_targeting_id': targeting_id,
            'p_company_id': company_id,
            'p_success': bool(is_success),
            'p_error_type': error_type,
            'p_classify_detail': classify_detail,
            'p_field_mapping': field_mapping_to_store,
            'p_bot_protection': bool(bp),
            'p_submitted_at': jst_now().isoformat(),
            'p_run_id': run_id,
        }
        try:
            supabase.rpc(FN_MARK_DONE, _md_args).execute()
        except Exception as e_md3:
            if _should_fallback_on_rpc_error(e_md3, FN_MARK_DONE, ['p_run_id']):
                _md_args.pop('p_run_id', None)
                try:
                    supabase.rpc(FN_MARK_DONE, _md_args).execute()
                except Exception as e_md3_f:
                    if (not USE_EXTRA_TABLE) and _should_fallback_on_rpc_error(e_md3_f, FN_MARK_DONE, []):
                        supabase.rpc('mark_done', _md_args).execute()
                    else:
                        raise
            else:
                raise
        # 成功時は当日成功数キャッシュを無効化（最新値を反映させる）
        if is_success:
            try:
                key = f"{targeting_id}:{target_date.isoformat()}"
                _SUCC_CACHE.pop(key, None)
            except Exception:
                pass
        # 完了ログ（成功/失敗）
        try:
            wid = getattr(worker, 'worker_id', 0)
            if is_success:
                _get_lifecycle_logger().info(
                    f"process_done: company_id={company_id}, worker_id={wid}, targeting_id={targeting_id}, success=True"
                )
            else:
                # 理由はエラー種別のみ（詳細メッセージは出さない）
                _get_lifecycle_logger().info(
                    f"process_done: company_id={company_id}, worker_id={wid}, targeting_id={targeting_id}, success=False, reason={error_type or 'UNKNOWN'}"
                )
        except Exception:
            pass
        # 会社ごとにCookieをクリア（WAF/トラッキング連鎖抑止）。失敗しても続行。
        try:
            bm = getattr(worker, 'browser_manager', None)
            ctx = getattr(bm, 'context', None) if bm else None
            if ctx and hasattr(ctx, 'clear_cookies'):
                await ctx.clear_cookies()
        except Exception:
            pass
    except Exception as e:
        logger.error(f"mark_done RPC error ({company_id}): {e}")

    return True


def _worker_entry(worker_id: int, targeting_id: int, config_file: str, headless_opt: Optional[bool], target_date: date, shard_id: Optional[int], run_id: str, max_processed: Optional[int], fixed_company_id: Optional[int]):
    # child process
    try:
        # 子プロセスにも抑制ポリシーを適用
        _install_logging_policy_for_ci()
        supabase = _build_supabase_client()
        worker = IsolatedFormWorker(worker_id=worker_id, headless=headless_opt)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def _amain():
            ok = await worker.initialize()
            if not ok:
                logger.error(f"Worker {worker_id}: Playwright init failed")
                return
            client_data = load_client_data_simple(config_file, targeting_id)
            max_daily = _extract_max_daily_sends(client_data)
            # バックオフ設定（config/worker_config.json → runner）
            try:
                runner_cfg = get_worker_config().get('runner', {})
                backoff_initial = int(runner_cfg.get('backoff_initial', 2))
                backoff_max = int(runner_cfg.get('backoff_max', 60))
            except Exception:
                backoff_initial, backoff_max = 2, 60
            backoff = backoff_initial
            processed = 0
            # 連続アイドル検出: 一定時間連続で claim が空振りの場合は安全に終了
            try:
                idle_limit = int(get_worker_config().get('runner', {}).get('idle_exit_seconds', 900))  # 既定15分
            except Exception:
                idle_limit = 900
            last_work_ts = _time.time()
            # --- 大量並列時の空シャード対策（全シャードプローブ + シャードローテーション） ---
            try:
                rcfg = get_worker_config().get('runner', {})
                shard_num = int(rcfg.get('shard_num', 8))
                shard_rotation_enabled = bool(rcfg.get('shard_rotation_enabled', True))
                no_work_probe_seconds = int(rcfg.get('shard_no_work_probe_seconds', 45))
                unsharded_probe_attempts = int(rcfg.get('shard_unsharded_probe_attempts', 1))
                shard_rotation_strategy = str(rcfg.get('shard_rotation_strategy', 'sequential'))
            except Exception:
                shard_num = 8
                shard_rotation_enabled = True
                no_work_probe_seconds = 45
                unsharded_probe_attempts = 1
                shard_rotation_strategy = 'sequential'

            current_shard_id = shard_id  # ループ中に動的に変更可能な shard_id
            no_work_start_ts: Optional[float] = None
            # 取り残し再配布の定期実行（worker_id=0 のみ）
            try:
                runner_cfg = get_worker_config().get('runner', {})
                requeue_interval = int(runner_cfg.get('requeue_interval_seconds', 300))  # 既定5分
                requeue_stale_minutes = int(runner_cfg.get('requeue_stale_minutes', 15))  # 既定15分
            except Exception:
                requeue_interval, requeue_stale_minutes = 300, 15
            last_requeue_ts = 0.0
            while True:
                # 取り残し再配布は仕事の有無に関係なく一定間隔で実行（worker_id=0 のみ）
                try:
                    now_ts = _time.time()
                    if worker_id == 0 and (now_ts - last_requeue_ts >= requeue_interval):
                        try:
                            resp = supabase.rpc(FN_REQUEUE, {
                                'p_target_date': str(target_date),
                                'p_targeting_id': targeting_id,
                                'p_stale_minutes': requeue_stale_minutes,
                            }).execute()
                            try:
                                count_val = None
                                if hasattr(resp, 'data'):
                                    count_val = resp.data
                                _get_lifecycle_logger().info(
                                    f"requeue_stale_assigned: targeting_id={targeting_id}, stale_minutes={requeue_stale_minutes}, requeued={count_val}"
                                )
                            except Exception:
                                pass
                        except Exception as e:
                            # 関数未存在等 → extra 指定時はフォールバック禁止（send_queue を触らない）
                            if (not USE_EXTRA_TABLE) and _should_fallback_on_rpc_error(e, FN_REQUEUE, []):
                                try:
                                    resp = supabase.rpc('requeue_stale_assigned', {
                                        'p_target_date': str(target_date),
                                        'p_targeting_id': targeting_id,
                                        'p_stale_minutes': requeue_stale_minutes,
                                    }).execute()
                                except Exception as e2:
                                    logger.warning(f"requeue_stale_assigned fallback error (suppressed): {e2}")
                            else:
                                logger.warning(f"requeue_stale_assigned error (suppressed): {e}")
                        finally:
                            last_requeue_ts = now_ts
                except Exception:
                    pass

                if not _within_business_hours(client_data):
                    await asyncio.sleep(60)
                    continue
                # 当日成功上限（max_daily_sends）をDBのUTC時刻基準でJST境界に合わせて確認
                if max_daily is not None and max_daily > 0:
                    try:
                        success_cnt = _get_success_count_today_jst(supabase, targeting_id, target_date)
                        if success_cnt >= max_daily:
                            logger.info(
                                f"Targeting {targeting_id}: daily success cap reached ({success_cnt}/{max_daily}) - stopping worker {worker_id}"
                            )
                            return
                    except Exception as e:
                        logger.warning(f"daily cap check failed: {e}")
                had_work = await _process_one(
                    supabase, worker, targeting_id, client_data, target_date, run_id, current_shard_id, fixed_company_id
                )
                if not had_work:
                    # ジッター付き指数バックオフ（コンボイ緩和）
                    try:
                        jitter_ratio = float(get_worker_config().get('runner', {}).get('backoff_jitter_ratio', 0.2))
                    except Exception:
                        jitter_ratio = 0.2
                    jitter = backoff * jitter_ratio
                    sleep_for = max(0.1, backoff + random.uniform(-jitter, jitter))
                    # まずは既定のスリープ前に、空シャード対策のフォールバックを評価
                    now_ts = _time.time()
                    if no_work_start_ts is None:
                        no_work_start_ts = now_ts
                    # 指定シャードで一定時間以上無作業なら、全シャードプローブを実施
                    if (current_shard_id is not None) and (now_ts - no_work_start_ts >= no_work_probe_seconds):
                        probed = False
                        try:
                            attempts = max(1, unsharded_probe_attempts)
                        except Exception:
                            attempts = 1
                        for _ in range(attempts):
                            ok_any = await _process_one(
                                supabase, worker, targeting_id, client_data, target_date, run_id, None, fixed_company_id
                            )
                            if ok_any:
                                # 1件でも処理できたら即リセットして継続（シャード固定は維持）
                                probed = True
                                last_work_ts = _time.time()
                                backoff = backoff_initial
                                no_work_start_ts = None
                                processed += 1
                                break
                        if probed:
                            # フォールバックで1件処理できた場合はスリープやアイドル判定をスキップして次ループへ
                            continue
                        if not probed and shard_rotation_enabled and shard_num > 1:
                            # シャードをローテーション（シーケンシャルまたは乱択）
                            prev = current_shard_id
                            if shard_rotation_strategy == 'random':
                                try:
                                    nxt_candidates = [i for i in range(shard_num) if i != (prev or 0)]
                                    current_shard_id = random.choice(nxt_candidates) if nxt_candidates else prev
                                except Exception:
                                    current_shard_id = ((prev or 0) + 1) % shard_num
                            else:
                                current_shard_id = ((prev or 0) + 1) % shard_num
                            try:
                                _get_lifecycle_logger().info(
                                    f"shard_rotate: worker_id={worker_id}, targeting_id={targeting_id}, from={prev}, to={current_shard_id}"
                                )
                            except Exception:
                                pass
                            # 次の試行に備えてタイマーをリセット
                            no_work_start_ts = now_ts

                    await asyncio.sleep(sleep_for)
                    backoff = min(backoff * 2, backoff_max)
                    # アイドル継続判定
                    try:
                        now_ts = _time.time()
                        if idle_limit > 0 and (now_ts - last_work_ts) >= idle_limit:
                            _get_lifecycle_logger().info(
                                f"no_work_timeout: worker_id={worker_id}, targeting_id={targeting_id}, idle_for_sec={int(now_ts - last_work_ts)}"
                            )
                            return
                    except Exception:
                        pass
                else:
                    backoff = backoff_initial
                    processed += 1
                    last_work_ts = _time.time()
                    no_work_start_ts = None
                    # テスト用: 規定数に達したら終了
                    if max_processed is not None and processed >= max_processed:
                        return

        loop.run_until_complete(_amain())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logger.error(f"Worker {worker_id} fatal: {e}")


def main():
    p = argparse.ArgumentParser(description='Form Sender Runner (4 workers, queue driven)')
    p.add_argument('--targeting-id', type=int, required=True)
    p.add_argument('--config-file', required=True)
    p.add_argument('--num-workers', type=int, default=4)
    p.add_argument('--headless', choices=['true','false','auto'], default='auto')
    p.add_argument('--target-date', type=str, default=None, help='JST date YYYY-MM-DD (default: today JST)')
    p.add_argument('--shard-id', type=int, default=None)
    p.add_argument('--max-processed', type=int, default=None, help='Process this many companies then exit (for local testing)')
    p.add_argument('--company-id', type=int, default=None, help='Process only this company id (bypass queue claim)')
    args = p.parse_args()

    config_path = _resolve_client_config_path(args.config_file)

    headless_opt = None
    if args.headless == 'true':
        headless_opt = True
    elif args.headless == 'false':
        headless_opt = False

    t_date = jst_today() if not args.target_date else date.fromisoformat(args.target_date)

    # spawn workers
    try:
        mp.set_start_method('spawn', force=False)
    except RuntimeError:
        pass

    run_id = os.environ.get('GITHUB_RUN_ID') or f'local-{int(time.time())}'

    # 親プロセスにも抑制ポリシーを適用
    _install_logging_policy_for_ci()

    procs: List[mp.Process] = []
    # company_id 指定時は重複処理を避けるためワーカーは1に制限
    # 1〜4にクランプ（外部からの過大指定を抑止）
    worker_count = 1 if args.company_id is not None else min(4, max(1, args.num_workers))
    for wid in range(worker_count):
        pr = mp.Process(
            target=_worker_entry,
            args=(wid, args.targeting_id, config_path, headless_opt, t_date, args.shard_id, run_id, args.max_processed, args.company_id),
            name=f'fs-worker-{wid}'
        )
        pr.daemon = False
        pr.start()
        procs.append(pr)

    # 親はシグナル待ちして子を巻き取る
    def _term(signum, frame):
        for pr in procs:
            try:
                pr.terminate()
            except Exception:
                pass
        for pr in procs:
            try:
                pr.join(timeout=10)
            except Exception:
                pass
        sys.exit(0)

    signal.signal(signal.SIGINT, _term)
    signal.signal(signal.SIGTERM, _term)

    for pr in procs:
        pr.join()


if __name__ == '__main__':
    main()
