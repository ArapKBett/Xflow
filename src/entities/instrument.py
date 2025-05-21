from decimal import Decimal
from dataclasses import dataclass, field


@dataclass
class Instrument:
    """
    Represents a trading instrument's specifications.

    Attributes:
        symbol (str): Trading pair symbol (e.g., "BTC-USDT-SWAP").
        tick_size (Decimal): Minimum price increment.
        tick_value (Decimal): Value of one tick (derived from tick_size and contract_size).
        min_price (Decimal): Minimum allowable price.
        max_price (Decimal): Maximum allowable price.
        min_lot_size_quoted_asset (Decimal): Minimum lot size in quoted asset.
        min_qty_market (Decimal): Minimum quantity for market orders.
        max_qty_market (Decimal): Maximum quantity for market orders.
        min_qty_limit (Decimal): Minimum quantity for limit orders.
        max_qty_limit (Decimal): Maximum quantity for limit orders.
        lot_step_market (Decimal): Quantity step for market orders.
        lot_step_limit (Decimal): Quantity step for limit orders.
        max_num_algo_orders (Decimal): Maximum number of algorithmic orders.
        fx_spread (Decimal): Spread for FX instruments.
        contract_size (Decimal): Contract size for futures (e.g., 0.01 BTC per contract).
    """
    symbol: str
    tick_size: Decimal = Decimal("0.01")
    tick_value: Decimal = field(init=False)  # Derived from tick_size and contract_size
    min_price: Decimal = Decimal("0.01")
    max_price: Decimal = Decimal("100000")
    min_lot_size_quoted_asset: Decimal = Decimal("0")
    min_qty_market: Decimal = Decimal("0")
    max_qty_market: Decimal = Decimal("0")
    min_qty_limit: Decimal = Decimal("0")
    max_qty_limit: Decimal = Decimal("0")
    lot_step_market: Decimal = Decimal("0.01")
    lot_step_limit: Decimal = Decimal("0.01")
    max_num_algo_orders: Decimal = Decimal("0")
    fx_spread: Decimal = Decimal("0")
    contract_size: Decimal = Decimal("1")

    def __post_init__(self):
        """
        Post-initialization logic to validate attributes and calculate derived values.
        """
        self.validate_attributes()
        self.tick_value = self.calculate_tick_value()

    def validate_attributes(self):
        """
        Validate the instrument's attributes to ensure logical consistency.
        """
        if self.tick_size <= 0:
            raise ValueError("tick_size must be greater than 0.")
        if self.min_price >= self.max_price:
            raise ValueError("min_price must be less than max_price.")
        if self.lot_step_market <= 0 or self.lot_step_limit <= 0:
            raise ValueError("lot_step_market and lot_step_limit must be greater than 0.")

    def calculate_tick_value(self) -> Decimal:
        """
        Calculate the tick value based on tick_size and contract_size.

        Returns:
            Decimal: The value of one tick.
        """
        return self.tick_size * self.contract_size

    def is_price_within_limits(self, price: Decimal) -> bool:
        """
        Check if a given price is within the instrument's price limits.

        Args:
            price (Decimal): The price to check.

        Returns:
            bool: True if the price is within limits, False otherwise.
        """
        return self.min_price <= price <= self.max_price

    def update_lot_sizes(self, market_step: Decimal, limit_step: Decimal):
        """
        Update the lot step sizes for market and limit orders.

        Args:
            market_step (Decimal): New step size for market orders.
            limit_step (Decimal): New step size for limit orders.
        """
        if market_step <= 0 or limit_step <= 0:
            raise ValueError("Lot step sizes must be greater than 0.")
        self.lot_step_market = market_step
        self.lot_step_limit = limit_step