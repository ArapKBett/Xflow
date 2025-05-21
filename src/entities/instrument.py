from decimal import Decimal
from dataclasses import dataclass


@dataclass
class Instrument:
    """
    Represents a trading instrument's specifications.

    Attributes:
        symbol (str): Trading pair symbol (e.g., "BTC-USDT-SWAP").
        tick_size (Decimal): Minimum price increment.
        tick_value (Decimal): Value of one tick.
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
    tick_size: Decimal = Decimal(0)
    tick_value: Decimal = Decimal(0)
    min_price: Decimal = Decimal(0)
    max_price: Decimal = Decimal(0)
    min_lot_size_quoted_asset: Decimal = Decimal(0)
    min_qty_market: Decimal = Decimal(0)
    max_qty_market: Decimal = Decimal(0)
    min_qty_limit: Decimal = Decimal(0)
    max_qty_limit: Decimal = Decimal(0)
    lot_step_market: Decimal = Decimal(0)
    lot_step_limit: Decimal = Decimal(0)
    max_num_algo_orders: Decimal = Decimal(0)
    fx_spread: Decimal = Decimal(0)
    contract_size: Decimal = Decimal(0)
