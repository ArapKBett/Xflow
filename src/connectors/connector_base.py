import asyncio
import traceback
from abc import ABC, abstractmethod
import time
from decimal import Decimal
from enum import Enum
from typing import Dict, Optional, List

from src.entities.instrument import Instrument
from src.entities.balance import Balance
from src.entities.order import Order, OrderType, OrderStatus
from src.entities.timeframe import Timeframe
from src.entities.direction import Direction
from src.logger.logger import get_logger


class ConnectorType(Enum):
    BINANCE_FUTURE = 0
    OKX_FUTURE = 1


class ConnectorBase(ABC):
    """
    Abstract base class for exchange connectors, defining the interface for interacting with trading APIs.
    Provides methods for fetching instruments, candles, prices, orders, and balances, as well as creating and canceling orders.
    """

    def __init__(self, config: dict):
        """
        Initialize the connector with configuration and start background tasks.

        Args:
            config (dict): Configuration dictionary (e.g., API keys, testnet flag).
        """
        self._config = config
        self._logger = get_logger(self.__module__)
        self._start_background_tasks()

    def _start_background_tasks(self):
        """
        Start background tasks for network connections and data updates.
        """
        asyncio.create_task(self._start_network(), name=f"Task_{self.__class__.__name__}_Network")
        asyncio.create_task(self._instruments_spec_polling_loop(), name=f"Task_{self.__class__.__name__}_InstrumentsSpec")
        asyncio.create_task(self._candles_polling_loop(), name=f"Task_{self.__class__.__name__}_Candles")

    @abstractmethod
    async def _start_network(self):
        """
        Start background tasks and initialize network connections (e.g., WebSocket, REST client).
        """
        pass

    @abstractmethod
    async def _stop_network(self):
        """
        Stop background tasks and close network connections.
        """
        pass

    @property
    @abstractmethod
    def tf_map(self) -> Dict[Timeframe, str]:
        """
        Mapping of Timeframe enums to exchange-specific interval strings.

        Returns:
            Dict[Timeframe, str]: Timeframe to interval mapping.
        """
        pass

    async def _instruments_spec_polling_loop(self):
        """
        Periodically update trading rules by fetching instrument specifications from the exchange.
        Runs every 30 minutes.
        """
        while True:
            try:
                await self._update_instruments_spec()
                await asyncio.sleep(60 * 30)
            except Exception as e:
                self._logger.error(f"Error while fetching instruments spec: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(5)

    @abstractmethod
    async def _update_instruments_spec(self):
        """
        Fetch and update instrument specifications from the exchange.
        """
        pass

    @abstractmethod
    async def get_instrument_spec(self, symbol: str) -> Instrument:
        """
        Retrieve the instrument specification for a symbol.

        Args:
            symbol (str): Trading pair symbol (e.g., "BTCUSDT").

        Returns:
            Instrument: Instrument specification.
        """
        pass

    async def _candles_polling_loop(self):
        """
        Periodically update candles for all tracked symbols and timeframes.
        Runs every second.
        """
        while True:
            try:
                await self._update_candles()
                await asyncio.sleep(1)
            except Exception as e:
                self._logger.error(f"Error while fetching candles: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(5)

    @abstractmethod
    async def _update_candles(self):
        """
        Update candle data for all tracked symbols and timeframes.
        """
        pass

    @abstractmethod
    async def get_last_candles(self, symbol: str, timeframe: Timeframe, n: int) -> List[dict]:
        """
        Retrieve the last n candles for a symbol and timeframe.

        Args:
            symbol (str): Trading pair symbol.
            timeframe (Timeframe): Candle timeframe.
            n (int): Number of candles to retrieve.

        Returns:
            List[dict]: List of candle dictionaries with keys Date, O, H, L, C, V.
        """
        pass

    @abstractmethod
    async def get_cur_price(self, symbol: str) -> Dict[str, Decimal]:
        """
        Fetch the current bid, ask, and last price for a symbol.

        Args:
            symbol (str): Trading pair symbol.

        Returns:
            Dict[str, Decimal]: Dictionary with keys "bid", "ask", "last".
        """
        pass

    @abstractmethod
    async def get_order(self, order: Order) -> Order:
        """
        Retrieve or update an order's status.

        Args:
            order (Order): Order object to retrieve.

        Returns:
            Order: Updated order object.
        """
        pass

    @abstractmethod
    async def get_last_balance(self) -> Dict[str, Balance]:
        """
        Fetch the current account balance.

        Returns:
            Dict[str, Balance]: Dictionary of balances keyed by currency.
        """
        pass

    @abstractmethod
    async def create_order(self, symbol: str, side: Direction, order_type: OrderType, price: Optional[Decimal] = None,
                           qty: Optional[Decimal] = None, stop_price: Optional[Decimal] = None) -> Order:
        """
        Create a new order on the exchange.

        Args:
            symbol (str): Trading pair symbol.
            side (Direction): BUY or SELL.
            order_type (OrderType): MARKET, LIMIT, or STOP.
            price (Optional[Decimal]): Price for limit orders.
            qty (Optional[Decimal]): Quantity in base currency.
            stop_price (Optional[Decimal]): Stop price for stop orders.

        Returns:
            Order: Created order object.
        """
        pass

    @abstractmethod
    async def cancel_order(self, symbol: str, order_id: str):
        """
        Cancel an order by ID.

        Args:
            symbol (str): Trading pair symbol.
            order_id (str): Order ID to cancel.
        """
        pass

    # -------------------------------
    # COMMON Order Helpers
    # -------------------------------
    async def create_limit_order(self, symbol: str, side: Direction, price: Decimal, qty: Decimal) -> Order:
        """
        Create a limit order.

        Args:
            symbol (str): Trading pair symbol.
            side (Direction): BUY or SELL.
            price (Decimal): Order price.
            qty (Decimal): Order quantity in base currency.

        Returns:
            Order: Created order object.
        """
        return await self.create_order(symbol, side, OrderType.LIMIT, price=price, qty=qty)

    async def create_market_order(self, symbol: str, side: Direction, qty: Decimal) -> Order:
        """
        Create a market order.

        Args:
            symbol (str): Trading pair symbol.
            side (Direction): BUY or SELL.
            qty (Decimal): Order quantity in base currency.

        Returns:
            Order: Created order object.
        """
        return await self.create_order(symbol, side, OrderType.MARKET, qty=qty)