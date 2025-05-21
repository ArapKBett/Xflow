from datetime import timedelta
from src.entities.timeframe import Timeframe


def timeframe_to_timedelta(timeframe: Timeframe) -> timedelta:
    """
    Convert a Timeframe enum to a timedelta object.

    Args:
        timeframe (Timeframe): Timeframe enum (e.g., M1, M5, H1).

    Returns:
        timedelta: Corresponding timedelta object.

    Raises:
        ValueError: If the timeframe is unknown.
    """
    if timeframe == Timeframe.W1:
        return timedelta(days=7)
    if timeframe == Timeframe.D1:
        return timedelta(days=1)
    if timeframe == Timeframe.H4:
        return timedelta(hours=4)
    if timeframe == Timeframe.H1:
        return timedelta(hours=1)
    if timeframe == Timeframe.M30:
        return timedelta(minutes=30)
    if timeframe == Timeframe.M15:
        return timedelta(minutes=15)
    if timeframe == Timeframe.M5:
        return timedelta(minutes=5)
    if timeframe == Timeframe.M1:
        return timedelta(minutes=1)
    raise ValueError(f'Unknown timeframe {timeframe}')
