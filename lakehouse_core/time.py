from __future__ import annotations

from datetime import datetime, timedelta

import pytz


def get_current_partition_date_str(tz: str = "Asia/Shanghai") -> str:
    """Return today's date formatted for partition columns (YYYY-MM-DD)."""

    timezone = pytz.timezone(tz)
    now = datetime.now(timezone)
    return now.strftime("%Y-%m-%d")


def get_current_date_str(tz: str = "Asia/Shanghai") -> str:
    """Return today's date in compact form (YYYYMMDD)."""

    timezone = pytz.timezone(tz)
    now = datetime.now(timezone)
    return now.strftime("%Y%m%d")


def get_previous_date_str(tz: str = "Asia/Shanghai") -> str:
    """Return yesterday's date in compact form (YYYYMMDD)."""

    timezone = pytz.timezone(tz)
    previous_day = datetime.now(timezone) - timedelta(days=1)
    return previous_day.strftime("%Y%m%d")


def get_previous_partition_date_str(tz: str = "Asia/Shanghai") -> str:
    """Return yesterday's date formatted for partition columns (YYYY-MM-DD)."""

    timezone = pytz.timezone(tz)
    previous_day = datetime.now(timezone) - timedelta(days=1)
    return previous_day.strftime("%Y-%m-%d")


def get_default_partition_date_str(tz: str = "Asia/Shanghai") -> str:
    """Return yesterday's date (YYYY-MM-DD) in the given timezone.

    Mirrors the previous Airflow/DAG-facing time util behavior (T-1) without importing Airflow code.
    """

    return get_previous_partition_date_str(tz)


def get_partition_date_str(tz: str = "Asia/Shanghai") -> str:
    """Return the default partition date (T-1) in YYYY-MM-DD format."""

    return get_default_partition_date_str(tz)


def get_date_str(tz: str = "Asia/Shanghai") -> str:
    """Return the default compact date (T-1) in YYYYMMDD format."""

    return get_previous_date_str(tz)
