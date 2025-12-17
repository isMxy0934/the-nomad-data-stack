from datetime import datetime, timedelta

import pytz


def get_current_partition_date_str(tz="Asia/Shanghai") -> str:
    """
    Get current partition date string

    Args:
        tz: timezone, default to Beijing timezone

    Returns:
        str: formatted date string (yyyy-MM-dd)
    """
    timezone = pytz.timezone(tz)
    now = datetime.now(timezone)
    return now.strftime("%Y-%m-%d")


def get_current_date_str(tz="Asia/Shanghai") -> str:
    """
    Get current date string

    Args:
        tz: timezone, default to Beijing timezone

    Returns:
        str: formatted date string (yyyy%m%d)
    """
    timezone = pytz.timezone(tz)
    now = datetime.now(timezone)
    return now.strftime("%Y%m%d")


def get_previous_date_str(tz="Asia/Shanghai") -> str:
    """
    Get previous date string

    Args:
        tz: timezone, default to Beijing timezone

    Returns:
        str: formatted date string (yyyy%m%d)
    """
    timezone = pytz.timezone(tz)
    previous_day = datetime.now(timezone) - timedelta(days=1)
    return previous_day.strftime("%Y%m%d")


def get_previous_partition_date_str(tz="Asia/Shanghai") -> str:
    """
    Get previous partition date string

    Args:
        tz: timezone, default to Beijing timezone

    Returns:
        str: formatted date string (yyyy-MM-dd)
    """
    timezone = pytz.timezone(tz)
    previous_day = datetime.now(timezone) - timedelta(days=1)
    return previous_day.strftime("%Y-%m-%d")
