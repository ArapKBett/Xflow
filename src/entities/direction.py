from enum import Enum


class Direction(Enum):
    """
    Enum representing the side of an order.

    Values:
        BUY (0): Buy order.
        SELL (1): Sell order.
    """
    BUY = 0
    SELL = 1
