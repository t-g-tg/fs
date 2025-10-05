#!/usr/bin/env python3
"""
GitHub Actions用クライアント設定保存スクリプト

GITHUB_EVENT_PATHからclient_configを安全に取得し、一時ファイルに保存する。
YAML内での直接変数展開を使わないため、ログに機密データが漏洩しない。
"""

import json
import logging
import os
import sys
import stat
from pathlib import Path
from typing import Dict, Any, List, Union, Optional, TypedDict
import hashlib
import time
import threading

logger = logging.getLogger(__name__)

# 型定義（2シート構造完全対応版）
class TargetingConfig(TypedDict, total=False):
    """Gas側ターゲティング設定の型定義（完全版）"""
    id: int  # targeting_id
    subject: str
    message: str
    targeting_sql: str
    ng_companies: str
    max_daily_sends: int
    send_start_time: str
    send_end_time: str 
    send_days_of_week: List[int]

class ClientInfo(TypedDict, total=False):
    """Gas側クライアント情報の型定義（完全版）"""
    # 必須フィールド
    company_name: str
    company_name_kana: str
    form_sender_name: str
    last_name: str
    first_name: str
    last_name_kana: str
    first_name_kana: str
    last_name_hiragana: str
    first_name_hiragana: str
    position: str
    gender: str
    email_1: str
    email_2: str
    postal_code_1: str
    postal_code_2: str
    address_1: str
    address_2: str
    address_3: str
    address_4: str
    phone_1: str
    phone_2: str
    phone_3: str
    # オプションフィールド（空文字許可）
    department: str
    website_url: str
    address_5: str

class Gas2SheetConfig(TypedDict):
    """Gas側2シート構造の正規化型定義（下位互換フィールド除去）"""
    targeting_id: int
    client_id: int
    active: bool
    client: ClientInfo
    targeting: TargetingConfig

# 設定キャッシュ（パフォーマンス最適化）
_config_cache: Dict[str, tuple[Dict[str, Any], float]] = {}
_CACHE_TTL = 300  # 5分間のキャッシュ

def _get_cache_key(raw_config: Dict[str, Any]) -> str:
    """設定データのハッシュベースキャッシュキーを生成"""
    config_str = json.dumps(raw_config, sort_keys=True)
    return hashlib.sha256(config_str.encode()).hexdigest()[:16]

def _is_cache_valid(timestamp: float) -> bool:
    """キャッシュが有効かどうかを判定"""
    return time.time() - timestamp < _CACHE_TTL

def _get_config_value(config: Dict[str, Any], key: str, fallback_key: str = None) -> Any:
    """
    2シート構造とフラット構造両対応の設定値取得
    
    Args:
        config: 設定データ
        key: 取得するキー
        fallback_key: フォールバックキー
    
    Returns:
        設定値またはNone
    """
    # フラット構造での取得を試行
    if key in config:
        return config[key]
    
    # 2シート構造での取得を試行
    if 'targeting' in config and key in config['targeting']:
        return config['targeting'][key]
    
    # フォールバック
    if fallback_key and fallback_key in config:
        return config[fallback_key]
        
    return None

def _validate_2sheet_config(config: Dict[str, Any]) -> None:
    """
    2シート構造の完全バリデーション
    """
    # 2シート構造の基本要件チェック
    if 'client' not in config:
        raise ValueError("2シート構造の 'client' セクションが見つかりません")
    if 'targeting' not in config:
        raise ValueError("2シート構造の 'targeting' セクションが見つかりません")
    
    if not isinstance(config['client'], dict):
        raise TypeError("'client' セクションが辞書型ではありません")
    if not isinstance(config['targeting'], dict):
        raise TypeError("'targeting' セクションが辞書型ではありません")
    
    # 基本管理フィールドのチェック
    if 'targeting_id' not in config:
        raise ValueError("必須フィールド 'targeting_id' が見つかりません")
    if 'client_id' not in config:
        raise ValueError("必須フィールド 'client_id' が見つかりません")
    
    # clientセクションの必須フィールドチェック
    client_required_fields = [
        'company_name', 'company_name_kana', 'form_sender_name',
        'last_name', 'first_name', 'last_name_kana', 'first_name_kana',
        'last_name_hiragana', 'first_name_hiragana', 'position',
        'gender', 'email_1', 'email_2',
        'postal_code_1', 'postal_code_2', 'address_1', 'address_2', 'address_3', 'address_4',
        'phone_1', 'phone_2', 'phone_3'
    ]
    
    missing_client_fields = []
    for field in client_required_fields:
        if field not in config['client'] or not config['client'][field]:
            missing_client_fields.append(field)
    
    if missing_client_fields:
        raise ValueError(f"client セクションの必須フィールドが不足: {missing_client_fields}")
    
    # targetingセクションの必須フィールドチェック
    targeting_required_fields = [
        'subject', 'message', 'max_daily_sends', 'send_start_time', 'send_end_time', 'send_days_of_week'
    ]
    
    missing_targeting_fields = []
    for field in targeting_required_fields:
        if field not in config['targeting'] or config['targeting'][field] is None:
            missing_targeting_fields.append(field)
    
    if missing_targeting_fields:
        raise ValueError(f"targeting セクションの必須フィールドが不足: {missing_targeting_fields}")
    
    # 型チェック
    targeting = config['targeting']
    
    if not isinstance(targeting['send_start_time'], str):
        raise TypeError("フィールド 'targeting.send_start_time' の型が不正です。期待: str")
    if not isinstance(targeting['send_end_time'], str):
        raise TypeError("フィールド 'targeting.send_end_time' の型が不正です。期待: str")
    if not isinstance(targeting['send_days_of_week'], list):
        raise TypeError("フィールド 'targeting.send_days_of_week' の型が不正です。期待: list")
    if not isinstance(targeting['max_daily_sends'], int):
        raise TypeError("フィールド 'targeting.max_daily_sends' の型が不正です。期待: int")
    
    # send_days_of_weekの詳細チェック
    if not all(isinstance(d, int) and 0 <= d <= 6 for d in targeting['send_days_of_week']):
        raise ValueError("targeting.send_days_of_weekは0-6の整数リストである必要があります")
    
    # 営業時間フォーマットチェック
    import re
    time_pattern = re.compile(r'^([01]\d|2[0-3]):([0-5]\d)$')
    if not time_pattern.match(targeting['send_start_time']):
        raise ValueError("targeting.send_start_timeは'HH:MM'形式である必要があります (例: '09:00')")
    if not time_pattern.match(targeting['send_end_time']):
        raise ValueError("targeting.send_end_timeは'HH:MM'形式である必要があります (例: '18:00')")

def transform_client_config(raw_config: Union[Gas2SheetConfig, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Gas側の2シート構造専用の検証・変換処理（構造整合性確保版）
    
    Args:
        raw_config: Gas側の2シート構造設定データ
        
    Returns:
        Dict[str, Any]: 検証済みの2シート構造設定データ
        
    Raises:
        ValueError: 必須フィールド不足または不正な値
        TypeError: 型が不正
    """
    # キャッシュチェック（パフォーマンス最適化）
    cache_key = _get_cache_key(dict(raw_config))
    if cache_key in _config_cache:
        cached_config, timestamp = _config_cache[cache_key]
        if _is_cache_valid(timestamp):
            print(f"INFO: キャッシュから2シート構造設定を取得 (key: {cache_key[:8]}...)")
            return cached_config
        else:
            # 期限切れキャッシュを削除
            del _config_cache[cache_key]
    
    try:
        # 2シート構造専用の完全バリデーション
        _validate_2sheet_config(raw_config)
        
        # 構造をそのまま保持してコピーを作成
        result_config: Dict[str, Any] = dict(raw_config)
        
        # 2シート構造の整合性確認ログ
        client_fields = list(raw_config.get('client', {}).keys())
        targeting_fields = list(raw_config.get('targeting', {}).keys())
        
        print(f"INFO: 2シート構造検証完了")
        print("INFO: client セクション - 検証済み")
        print("INFO: targeting セクション - 検証済み")
        print("INFO: 管理情報 - 検証済み")
        
        # データ構造サマリー
        print("INFO: 設定変換完了")
        print(f"INFO: 構造タイプ: Gas 2-Sheet Validated Structure")
        
        # キャッシュに保存
        _config_cache[cache_key] = (result_config, time.time())
        
        return result_config
        
    except (ValueError, TypeError) as e:
        # 2シート構造のバリデーションエラーは致命的エラー
        print(f"ERROR: 2シート構造バリデーションエラー: {e}", file=sys.stderr)
        print("ERROR: Gas側で送信される2シート構造に問題があります", file=sys.stderr)
        raise  # エラーを再発生させてプロセス終了
    except Exception as e:
        print(f"ERROR: 2シート構造処理中の予期しないエラー: {e}", file=sys.stderr)
        print("ERROR: システムエラーのため処理を中断します", file=sys.stderr)
        raise  # 予期しないエラーも致命的として扱う


def _cleanup_temp_file(temp_file_path: str, file_created: bool) -> None:
    """
    一時ファイルの安全なクリーンアップ
    
    Args:
        temp_file_path: 一時ファイルパス
        file_created: ファイルが作成されたかどうか
    """
    if file_created and os.path.exists(temp_file_path):
        try:
            os.unlink(temp_file_path)
            print(f"INFO: 一時ファイルをクリーンアップしました: {temp_file_path}")
        except Exception as cleanup_error:
            print(f"WARNING: 一時ファイルのクリーンアップに失敗: {cleanup_error}", file=sys.stderr)


def clear_config_cache() -> None:
    """設定キャッシュをクリア（テスト用）"""
    global _config_cache
    _config_cache.clear()
    print("INFO: 設定キャッシュをクリアしました")

def main() -> None:
    """メイン処理（強化版）"""
    try:
        # GitHub Actionsイベントファイルのパスを取得
        event_path = os.environ.get('GITHUB_EVENT_PATH')
        if not event_path:
            print("ERROR: GITHUB_EVENT_PATH環境変数が設定されていません", file=sys.stderr)
            sys.exit(1)
        
        if not os.path.exists(event_path):
            print(f"ERROR: イベントファイルが見つかりません: {event_path}", file=sys.stderr)
            sys.exit(1)
        
        # イベントデータを読み込み
        with open(event_path, 'r', encoding='utf-8') as f:
            event_data = json.load(f)
        
        # Workflow Dispatch と Repository Dispatch両対応でclient_configを取得
        client_config_raw = None
        targeting_id = None
        
        # Workflow Dispatch からの取得を試行
        if 'inputs' in event_data and event_data['inputs'].get('client_config'):
            try:
                client_config_raw = json.loads(event_data['inputs']['client_config'])
                targeting_id = event_data['inputs'].get('targeting_id')
                print("INFO: Workflow Dispatch経由でclient_configを取得しました")
            except json.JSONDecodeError as e:
                print(f"ERROR: Workflow Dispatch client_configのJSON解析エラー: {e}", file=sys.stderr)
                sys.exit(1)
        
        # Repository Dispatch からの取得を試行（フォールバック）
        elif 'client_payload' in event_data:
            client_payload = event_data['client_payload']
            client_config_raw = client_payload.get('client_config')
            targeting_id = client_payload.get('targeting_id')
            print("INFO: Repository Dispatch経由でclient_configを取得しました")
        
        # どちらでも取得できない場合はエラー
        if not client_config_raw:
            print("ERROR: client_configが見つかりません（Workflow DispatchまたはRepository Dispatchのいずれでも）", file=sys.stderr)
            print("INFO: Workflow Dispatch inputs または Repository Dispatch client_payload にclient_configが必要です", file=sys.stderr)
            sys.exit(1)
        
        # Gas側の設定をそのまま保持（検証付き）
        print("INFO: client_config検証開始")
        client_config = transform_client_config(client_config_raw)
        print("INFO: client_config検証成功")
        
        # targeting_idの確認（既に取得済み）
        if not targeting_id:
            print("ERROR: targeting_idが見つかりません", file=sys.stderr)
            sys.exit(1)
        
        # targeting_idの型チェック
        try:
            targeting_id = int(targeting_id)
        except (ValueError, TypeError):
            print("ERROR: targeting_idが無効な値です", file=sys.stderr)
            sys.exit(1)
        
        # 改良版プロセス固有・原子的ファイル作成（競合状態完全回避）
        process_id = os.getpid()
        timestamp = int(time.time() * 1000000)  # マイクロ秒タイムスタンプで競合回避強化
        thread_id = threading.current_thread().ident  # スレッドIDも追加
        random_suffix = hashlib.sha256(f"{process_id}_{timestamp}_{thread_id}".encode()).hexdigest()[:8]
        
        config_file_path = f'/tmp/client_config_{process_id}_{timestamp}_{random_suffix}.json'
        
        # 原子的ファイル作成（完全版）
        temp_file_path = f'{config_file_path}.tmp_{random_suffix}'
        
        # セキュアな権限設定
        old_umask = os.umask(0o077)  # 600権限を強制
        file_created = False
        
        try:
            # 一時ファイル存在チェック（念のため）
            if os.path.exists(temp_file_path):
                logger.warning(f"Temporary file already exists: {temp_file_path}")
                os.unlink(temp_file_path)
            
            # 原子的書き込み操作
            with open(temp_file_path, 'w', encoding='utf-8') as f:
                json.dump(client_config, f, ensure_ascii=False, indent=2)
                f.flush()  # バッファフラッシュ
                os.fsync(f.fileno())  # ディスク同期（データ整合性保証）
            
            file_created = True
            
            # ファイルサイズ検証（データ破損チェック）
            temp_file_size = os.path.getsize(temp_file_path)
            if temp_file_size < 50:  # 最小サイズチェック
                raise ValueError(f"Generated config file too small: {temp_file_size} bytes")
            
            # 原子的リネーム（競合状態を完全回避）
            os.rename(temp_file_path, config_file_path)
            
            # ファイル権限の最終確認
            file_stat = os.stat(config_file_path)
            if file_stat.st_mode & 0o077 != 0:
                logger.warning(f"File permissions not properly set: {oct(file_stat.st_mode)}")
                os.chmod(config_file_path, 0o600)
            
            print(f"INFO: 設定ファイル原子的作成完了: {config_file_path}")
            print(f"INFO: ファイルサイズ: {temp_file_size} bytes, 権限: {oct(file_stat.st_mode)}")
            
        except IOError as e:
            print(f"ERROR: 設定ファイルのI/O操作に失敗しました: {e}", file=sys.stderr)
            _cleanup_temp_file(temp_file_path, file_created)
            sys.exit(1)
        except ValueError as e:
            print(f"ERROR: 設定ファイルデータ検証エラー: {e}", file=sys.stderr)
            _cleanup_temp_file(temp_file_path, file_created)
            sys.exit(1)
        except Exception as e:
            print(f"ERROR: 設定ファイル作成中に予期しないエラー: {e}", file=sys.stderr)
            _cleanup_temp_file(temp_file_path, file_created)
            sys.exit(1)
        finally:
            os.umask(old_umask)  # umaskを確実に復元
        
        # targeting_idを環境ファイルに保存（GitHub Actionsの環境変数として使用）
        github_env = os.environ.get('GITHUB_ENV')
        if github_env:
            with open(github_env, 'a', encoding='utf-8') as f:
                f.write(f'TARGETING_ID={targeting_id}\n')
        
        print("✅ クライアント設定ファイルが正常に作成されました")
        print(f"   設定ファイル: {config_file_path}")
        # targeting_id は秘匿対象ではないため、そのまま出力する
        # ワークフロー実行トレース性向上のため明示表示
        print(f"   targeting_id: {targeting_id}")
        print("   設定: 検証済み")
        print(f"   キャッシュエントリ数: {len(_config_cache)}")
        # 構造種類を表示
        if 'targeting' in client_config and 'client' in client_config:
            print("   データ構造: Gas側2シート構造")
        else:
            print("   データ構造: フラット構造")
        
    except (ValueError, TypeError) as e:
        print(f"ERROR: 設定データ検証エラー: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: イベントファイルのJSON解析に失敗: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"ERROR: 予期しないエラー: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
