"""
クライアント設定ローダー（2シート構造対応・安全性チェック付き）

Runner 専用の軽量ローダー。旧 form_sender_worker.py の
`_load_client_data_simple` と等価の挙動を提供する。
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict

from ..security.log_sanitizer import setup_sanitized_logging


logger = logging.getLogger(__name__)
setup_sanitized_logging(__name__)


def load_client_data_simple(config_file: str, targeting_id: int) -> Dict[str, Any]:
    """
    2シート構造対応クライアントデータ読み込み（構造整合性・安全性検証付き）

    Args:
        config_file: 設定ファイルパス（/tmp か tests/tmp のみ許可）
        targeting_id: ターゲティングID

    Returns:
        Dict[str, Any]: クライアントデータ（client/targeting の2セクション含む）
    """
    try:
        # プロジェクトルート（src/form_sender/utils から3階層上）
        current_file = Path(__file__).resolve()
        project_root = current_file.parents[3]
        project_tests_tmp_real = (project_root / 'tests' / 'tmp').resolve()

        # 強化されたセキュリティチェック
        cf_raw = Path(config_file)
        # 明示的に .. を拒否（生パスで検査）
        if '..' in cf_raw.as_posix():
            raise ValueError("Unsafe path sequence in config file path")
        # シンボリックリンクは解決前の生パスで拒否（resolve() 前）
        try:
            import os as _os
            if cf_raw.is_symlink() or _os.path.islink(str(cf_raw)):
                raise ValueError("Symlink is not allowed for config file")
        except Exception:
            # 検証不能な場合は保守的に拒否
            raise ValueError("Unable to verify symlink status for config file")
        # 実体パスを解決（許可ディレクトリ/ファイル名ポリシーは解決後で評価）
        cf = cf_raw.resolve()

        allowed_dirs = [Path('/tmp'), Path('/private/tmp'), project_tests_tmp_real]
        allowed = any(cf.parent == d for d in allowed_dirs)
        if not allowed:
            raise ValueError(f"Config file directory not allowed: {cf}")
        if not cf.name.startswith('client_config_'):
            raise ValueError("Config file name must start with 'client_config_'")

        with open(cf, 'r', encoding='utf-8') as f:
            config_data = json.load(f)

        if not isinstance(config_data, dict) or not config_data:
            raise ValueError("Config data must be a non-empty dictionary")

        # 必須セクションの整合性チェック
        required_sections = ['client', 'targeting']
        missing = [s for s in required_sections if s not in config_data]
        if missing:
            error_msg = f"2シート構造の必須セクションが不足: {missing}"
            logger.error(error_msg)
            return {
                'targeting_id': targeting_id,
                'client_id': config_data.get('client_id', 0),
                'error': error_msg,
            }

        for section in required_sections:
            sec = config_data.get(section)
            if not isinstance(sec, dict) or not sec:
                error_msg = f"Section '{section}' must be a non-empty dictionary"
                logger.error(error_msg)
                return {
                    'targeting_id': targeting_id,
                    'client_id': config_data.get('client_id', 0),
                    'error': error_msg,
                }

        # client_id の基本検証
        client_id = config_data.get('client_id')
        if client_id is not None and (not isinstance(client_id, int) or client_id < 0):
            logger.warning(f"Invalid client_id: {client_id}, using 0")
            client_id = 0

        client_data = {
            'targeting_id': targeting_id,
            'client_id': client_id or 0,
            'client': dict(config_data['client']),
            'targeting': dict(config_data['targeting']),
            'active': bool(config_data.get('active', True)),
            'description': str(config_data.get('description', '')),
        }

        logger.info(
            f"2シート構造クライアントデータ読み込み完了: targeting_id={targeting_id}, client_id={client_data['client_id']}"
        )
        logger.info(
            f"Client fields: {len(client_data['client'])}, Targeting fields: {len(client_data['targeting'])}"
        )
        return client_data

    except json.JSONDecodeError as e:
        error_msg = f"JSON解析エラー: {e}"
        logger.error(error_msg)
        return {'targeting_id': targeting_id, 'error': error_msg}
    except (IOError, OSError) as e:
        error_msg = f"ファイルI/Oエラー: {e}"
        logger.error(error_msg)
        return {'targeting_id': targeting_id, 'error': error_msg}
    except ValueError as e:
        error_msg = f"データ検証エラー: {e}"
        logger.error(error_msg)
        return {'targeting_id': targeting_id, 'error': error_msg}
    except Exception as e:
        error_msg = f"予期しないエラー: {e}"
        logger.error(error_msg)
        return {'targeting_id': targeting_id, 'error': error_msg}
