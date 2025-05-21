from enum import Enum


class Timeframe(Enum):
    """
    Enum representing candlestick timeframes.

    Values:
        M1 (0): 1 minute
        M5 (1): 5 minutes
        M15 (2): 15 minutes
        M30 (3): 30 minutes
        H1 (4): 1 hour
        H4 (5): 4 hours
        D1 (6): 1 day
        W1 (7): 1 week
    """
    M1 = 0
    M5 = 1
    M15 = 2
    M30 = 3
    H1 = 4
    H4 = 5
    D1 = 6
    W1 = 7
