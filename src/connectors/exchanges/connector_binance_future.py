# Import necessary modules
import asyncio
import json
import traceback
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional

import pandas as pd
import numpy as np
import websockets
from binance import AsyncClient
from binance.enums import (
    HistoricalKlinesType,
    SIDE_BUY,
    SIDE_SELL,
    ORDER_TYPE_LIMIT,
    ORDER_TYPE_MARKET,
    ORDER_TYPE_STOP_MARKET,
)

from src.entities.instrument import Instrument
from src.entities.balance import Balance
from src.entities.order import Order, OrderType, OrderStatus
from src.entities.timeframe import Timeframe
from src.utils.common_functions import timeframe_to_timedelta
from src.logger.logger import get_logger
from .connector_base import ConnectorBase, ConnectorType


class Direction(Enum):
    """Direction of trade (BUY/SELL)."""
    BUY = SIDE_BUY
    SELL = SIDE_SELL


class BinanceConnectorFuture(ConnectorBase):
    """
    Binance Futures connector implementing the ConnectorBase interface.
    Handles instrument specifications, candles, orders, and balances using Binance's REST and WebSocket APIs.
    """

    connector_type: ConnectorType = ConnectorType.BinanceFuture
    _instruments_spec: Dict[str, Instrument] = {}
    _candles: Dict[str, Dict[Timeframe, Optional[pd.DataFrame]]] = {}
    _CANDLES_DEPTH: int = 800

    def __init__(self, config: dict):
        """
        Initialize the Binance Futures connector.

        Args:
            config (dict): Configuration dictionary containing API key and secret.
        """
        super().__init__(config)
        self._logger = get_logger(self.__module__)
        self.api_key = config.get("api_key")
        self.api_secret = config.get("api_secret")
        self._client = None
        self.order_buffer: List[Order] = []

    @property
    def tf_map(self) -> Dict[Timeframe, str]:
        """Return the mapping of Timeframe to Binance kline intervals."""
        return {
            Timeframe.M1: AsyncClient.KLINE_INTERVAL_1MINUTE,
            Timeframe.M5: AsyncClient.KLINE_INTERVAL_5MINUTE,
            Timeframe.M15: AsyncClient.KLINE_INTERVAL_15MINUTE,
            Timeframe.H1: AsyncClient.KLINE_INTERVAL_1HOUR,
            Timeframe.D1: AsyncClient.KLINE_INTERVAL_1DAY,
            Timeframe.W1: AsyncClient.KLINE_INTERVAL_1WEEK,
        }

    async def _start_network(self):
        """
        Start background tasks for the connector, including client initialization.
        """
        self._logger.info("Starting Binance Futures connector...")
        self._client = await AsyncClient.create(
            api_key=self.api_key, api_secret=self.api_secret
        )
        self._logger.info("Binance Futures client initialized.")

    async def _stop_network(self):
        """
        Stop all background tasks and close client connections.
        """
        self._logger.info("Stopping Binance Futures connector...")
        if self._client:
            await self._client.close_connection()
            self._client = None

    async def _update_instruments_spec(self):
        """
        Update instrument specifications by fetching the latest data from Binance.
        """
        self._instruments_spec.clear()
        try:
            res = await self._client.futures_exchange_info()
            for instrument in res["symbols"]:
                instrument_spec = Instrument(
                    symbol=instrument["symbol"],
                    contract_size=Decimal("1"),
                )
                for el in instrument["filters"]:
                    if el["filterType"] == "PRICE_FILTER":
                        instrument_spec.tick_size = Decimal(el["tickSize"])
                self._instruments_spec[instrument_spec.symbol] = instrument_spec
            self._logger.info(
                f"Updated instruments_spec for Binance Futures. Total: {len(self._instruments_spec)}."
            )
        except Exception as e:
            self._logger.error(f"Failed to update instruments: {e}\n{traceback.format_exc()}")

    async def get_instrument_spec(self, symbol: str) -> Instrument:
        """
        Retrieve the cached instrument specification for a symbol.

        Args:
            symbol (str): Trading pair symbol (e.g., "BTCUSDT").

        Returns:
            Instrument: Instrument specification.
        """
        if symbol not in self._instruments_spec:
            raise ValueError(f"Unknown symbol: {symbol}")
        return self._instruments_spec[symbol]

    async def get_cur_price(self, symbol: str) -> dict:
        """
        Fetch the current bid, ask, and last price for a symbol.

        Args:
            symbol (str): Trading pair symbol.

        Returns:
            dict: Dictionary with keys "bid", "ask", and "last".
        """
        try:
            ticker = await self._client.get_ticker(symbol=symbol)
            return {
                "bid": Decimal(str(ticker["bidPrice"])),
                "ask": Decimal(str(ticker["askPrice"])),
                "last": Decimal(str(ticker["lastPrice"])),
            }
        except Exception as e:
            self._logger.error(f"Failed to fetch current price for {symbol}: {e}")
            raise

    async def create_order(
        self,
        symbol: str,
        side: Direction,
        order_type: OrderType,
        price: Optional[Decimal] = None,
        qty: Optional[Decimal] = None,
    ) -> Order:
        """
        Create a new order on Binance Futures.

        Args:
            symbol (str): Trading pair symbol.
            side (Direction): BUY or SELL.
            order_type (OrderType): MARKET or LIMIT.
            price (Decimal, optional): Price for limit orders.
            qty (Decimal, optional): Quantity.

        Returns:
            Order: Created order object.
        """
        params = {
            "symbol": symbol,
            "side": side.value,
            "type": order_type.value,
            "quantity": str(qty) if qty else None,
        }
        if order_type == OrderType.LIMIT and price:
            params["price"] = str(price)
        try:
            response = await self._client.create_order(**params)
            return Order(
                id=str(response["orderId"]),
                symbol=symbol,
                side=side,
                order_type=order_type,
            )
        except Exception as e:
            self._logger.error(f"Failed to create order for {symbol}: {e}")
            raise