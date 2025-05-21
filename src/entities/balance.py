from decimal import Decimal
from dataclasses import dataclass


@dataclass
class Balance:
    """
    Represents an account balance for a specific currency.

    Attributes:
        currency (str): Asset currency (e.g., "USDT").
        available (Decimal): Available balance for trading.
        frozen (Decimal): Frozen balance (e.g., in open orders).
        total (Decimal): Total balance (available + frozen).
    """
    currency: str
    available: Decimal
    frozen: Decimal
    total: Decimal
