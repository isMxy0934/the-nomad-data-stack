from __future__ import annotations

from datetime import date, datetime, timedelta

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


def normalize_to_partition_format(date_value: date | datetime | str) -> str:
    """
    Normalize date value to partition format (YYYY-MM-DD).

    Supported formats:
    - date/datetime objects: Format to YYYY-MM-DD
    - YYYYMMDD string (e.g., "20251222"): Convert to YYYY-MM-DD
    - YYYY-MM-DD string (e.g., "2025-12-22"): Validate and return as-is

    Args:
        date_value: Date value in supported format

    Returns:
        Date string in YYYY-MM-DD format

    Raises:
        ValueError: If date_value is not in a supported format
        TypeError: If date_value is not a date/datetime/string type

    Examples:
        >>> normalize_to_partition_format(datetime(2025, 12, 22))
        '2025-12-22'
        >>> normalize_to_partition_format(date(2025, 12, 22))
        '2025-12-22'
        >>> normalize_to_partition_format("20251222")
        '2025-12-22'
        >>> normalize_to_partition_format("2025-12-22")
        '2025-12-22'
    """
    # Handle date/datetime objects
    if isinstance(date_value, (date, datetime)):
        return date_value.strftime("%Y-%m-%d")

    # Handle string values
    if isinstance(date_value, str):
        date_str = date_value.strip()

        # Already in YYYY-MM-DD format - validate and return
        if len(date_str) == 10 and date_str[4] == "-" and date_str[7] == "-":
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
                return date_str
            except ValueError as err:
                raise ValueError(
                    f"Invalid date string '{date_value}': not a valid YYYY-MM-DD date"
                ) from err

        # YYYYMMDD format - convert to YYYY-MM-DD
        if len(date_str) == 8 and date_str.isdigit():
            try:
                parsed = datetime.strptime(date_str, "%Y%m%d")
                return parsed.strftime("%Y-%m-%d")
            except ValueError as err:
                raise ValueError(
                    f"Invalid date string '{date_value}': not a valid YYYYMMDD date"
                ) from err

        # Unsupported string format
        raise ValueError(
            f"Unsupported date format '{date_value}'. "
            f"Supported formats: YYYY-MM-DD, YYYYMMDD, or date/datetime objects"
        )

    # Unsupported type
    raise TypeError(
        f"Unsupported type {type(date_value).__name__}. Expected date, datetime, or str"
    )
