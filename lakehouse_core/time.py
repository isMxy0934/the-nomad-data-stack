from __future__ import annotations

from datetime import datetime, timedelta

import pytz


def get_default_partition_date_str(tz: str = "Asia/Shanghai") -> str:
    """Return yesterday's date (YYYY-MM-DD) in the given timezone.

    Mirrors `dags/utils/time_utils.py#get_partition_date_str()` without importing Airflow code.
    """

    timezone = pytz.timezone(tz)
    previous_day = datetime.now(timezone) - timedelta(days=1)
    return previous_day.strftime("%Y-%m-%d")

