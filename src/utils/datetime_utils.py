#!/usr/bin/env python3
"""
DateTime Utilities

日本時間（JST）関連のユーティリティ関数を提供するモジュール。
プロジェクト全体で統一した日本時間処理を行うために使用する。
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Union, Optional
import re

logger = logging.getLogger(__name__)

# 日本標準時タイムゾーン
JST = timezone(timedelta(hours=9))

# タイムゾーン情報を含むISO文字列のパターン
TIMEZONE_PATTERN = re.compile(r'[+-]\d{2}:?\d{2}|Z$')


def utc_to_jst(dt: Union[datetime, str, int, float, None]) -> datetime:
    """
    UTCの日時を日本標準時（JST）に変換
    
    Args:
        dt: UTCの日時（datetime型、ISO文字列、またはUNIXタイムスタンプ）
    
    Returns:
        datetime: JST変換後の日時
        
    Raises:
        ValueError: 入力が無効な場合
        TypeError: サポートされていない型の場合
    """
    if dt is None:
        raise ValueError("Input datetime cannot be None")
    
    if isinstance(dt, str):
        if not dt.strip():
            raise ValueError("Input datetime string cannot be empty")
            
        # ISO文字列をdatetime型に変換
        try:
            # Z で終わる場合はUTCとして解釈
            if dt.endswith('Z'):
                dt = datetime.fromisoformat(dt.replace('Z', '+00:00'))
            # タイムゾーン情報の有無を正規表現で判定
            elif TIMEZONE_PATTERN.search(dt):
                dt = datetime.fromisoformat(dt)
            else:
                # タイムゾーン情報がない場合はUTCと仮定（警告ログ出力）
                logger.warning(f"Assuming UTC timezone for naive datetime string: {dt}")
                dt = datetime.fromisoformat(dt).replace(tzinfo=timezone.utc)
        except ValueError as e:
            raise ValueError(f"Invalid datetime string format: {dt}") from e
    elif isinstance(dt, datetime):
        # datetime型の場合はそのまま使用
        pass
    elif isinstance(dt, (int, float)):
        # UNIXタイムスタンプ（秒単位）の場合
        try:
            dt = datetime.fromtimestamp(dt, tz=timezone.utc)
        except (ValueError, OverflowError, TypeError) as e:
            raise ValueError(f"Invalid UNIX timestamp: {dt}") from e
    else:
        raise TypeError(f"Unsupported type for datetime conversion: {type(dt)}")
    
    # タイムゾーン情報がない場合はUTCと仮定（警告ログ出力）
    if dt.tzinfo is None:
        logger.warning(f"Assuming UTC timezone for naive datetime object: {dt}")
        dt = dt.replace(tzinfo=timezone.utc)
    
    # JSTに変換
    return dt.astimezone(JST)


def now_jst() -> datetime:
    """
    現在時刻を日本標準時（JST）で取得
    
    Returns:
        datetime: JSTの現在時刻
    """
    return datetime.now(JST)


def to_jst_isoformat(dt: Union[datetime, str, int, float, None]) -> Optional[str]:
    """
    日時をJSTのISO形式文字列に変換
    
    Args:
        dt: 変換対象の日時（datetime型、ISO文字列、UNIXタイムスタンプ、またはNone可）
    
    Returns:
        str: JST時間のISO形式文字列（dtがNoneの場合はNoneを返す）
        
    Raises:
        ValueError: 入力が無効な場合
        TypeError: サポートされていない型の場合
    """
    if dt is None:
        return None
    
    try:
        jst_dt = utc_to_jst(dt)
        return jst_dt.isoformat()
    except (ValueError, TypeError) as e:
        logger.error(f"Failed to convert to JST ISO format: {dt}, error: {e}")
        raise


def to_jst_timestamp(dt: Union[datetime, str, int, float, None]) -> Optional[str]:
    """
    日時をJSTのタイムスタンプ文字列に変換（データベース保存用）
    
    Args:
        dt: 変換対象の日時（datetime型、ISO文字列、UNIXタイムスタンプ、またはNone可）
    
    Returns:
        str: JST時間のタイムスタンプ文字列（YYYY-MM-DD HH:MM:SS形式、dtがNoneの場合はNoneを返す）
        
    Raises:
        ValueError: 入力が無効な場合
        TypeError: サポートされていない型の場合
    """
    if dt is None:
        return None
    
    try:
        jst_dt = utc_to_jst(dt)
        return jst_dt.strftime('%Y-%m-%d %H:%M:%S')
    except (ValueError, TypeError) as e:
        logger.error(f"Failed to convert to JST timestamp format: {dt}, error: {e}")
        raise