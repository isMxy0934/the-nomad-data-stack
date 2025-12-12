from datetime import datetime
import pytz


def get_current_date_str(tz='Asia/Shanghai') -> str:
    """
    Get current date string
    
    Args:
        tz: timezone, default to Beijing timezone
    
    Returns:
        str: formatted date string (yyyy-MM-dd)
    """
    timezone = pytz.timezone(tz)
    now = datetime.now(timezone)
    return now.strftime('%Y-%m-%d')