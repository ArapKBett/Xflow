from decimal import Decimal
from dataclasses import dataclass, field


@dataclass
class Balance:
    """
    Represents an account balance for a specific currency.

    Attributes:
        currency (str): Asset currency (e.g., "USDT").
        available (Decimal): Available balance for trading.
        frozen (Decimal): Frozen balance (e.g., in open orders).
        total (Decimal): Total balance (calculated as available + frozen).
    """
    currency: str
    available: Decimal = field(default=Decimal("0"))
    frozen: Decimal = field(default=Decimal("0"))
    total: Decimal = field(init=False)

    def __post_init__(self):
        """
        Post-initialization logic to calculate the total balance.
        Ensures total is consistent with available and frozen balances.
        """
        self.validate_types()
        self.recalculate_total()

    def validate_types(self):
        """
        Validate that available and frozen are of type Decimal.
        """
        if not isinstance(self.available, Decimal) or not isinstance(self.frozen, Decimal):
            raise TypeError("available and frozen must be of type 'Decimal'.")

    def recalculate_total(self):
        """
        Recalculate the total balance based on available and frozen balances.
        """
        self.total = self.available + self.frozen

    def update_balance(self, available: Decimal = None, frozen: Decimal = None):
        """
        Update the available or frozen balance and recalculate the total.

        Args:
            available (Decimal): New available balance (optional).
            frozen (Decimal): New frozen balance (optional).
        """
        if available is not None:
            if not isinstance(available, Decimal):
                raise TypeError("available must be of type 'Decimal'.")
            self.available = available

        if frozen is not None:
            if not isinstance(frozen, Decimal):
                raise TypeError("frozen must be of type 'Decimal'.")
            self.frozen = frozen

        self.recalculate_total()