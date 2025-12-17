from datetime import datetime, timedelta

import pytz


def get_current_partition_date_str(tz="Asia/Shanghai") -> str:
    """Return today's date formatted for partition columns (YYYY-MM-DD)."""
    timezone = pytz.timezone(tz)
    now = datetime.now(timezone)
    return now.strftime("%Y-%m-%d")


def get_current_date_str(tz="Asia/Shanghai") -> str:
    """Return today's date in compact form (YYYYMMDD)."""
    timezone = pytz.timezone(tz)
    now = datetime.now(timezone)
    return now.strftime("%Y%m%d")


def get_previous_date_str(tz="Asia/Shanghai") -> str:
    """Return yesterday's date in compact form (YYYYMMDD)."""
    timezone = pytz.timezone(tz)
    previous_day = datetime.now(timezone) - timedelta(days=1)
    return previous_day.strftime("%Y%m%d")


def get_previous_partition_date_str(tz="Asia/Shanghai") -> str:
    """Return yesterday's date formatted for partition columns (YYYY-MM-DD)."""
    timezone = pytz.timezone(tz)
    previous_day = datetime.now(timezone) - timedelta(days=1)
    return previous_day.strftime("%Y-%m-%d")

# set partition date
def get_partition_date_str(tz="Asia/Shanghai") -> str:
    return get_previous_partition_date_str(tz)

def get_date_str(tz="Asia/Shanghai") -> str:
    return get_previous_date_str(tz)