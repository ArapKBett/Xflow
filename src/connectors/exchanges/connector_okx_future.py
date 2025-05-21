import asyncio
import json
import traceback
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional

import pandas as pd
from websockets.legacy.client import WebSocketClientProtocol

from src.connectors.clients.okx_client import OKXAsyncClient
from src.entities.instrument import Instrument
from src.entities.timeframe import Timeframe
from src.entities.order import Order, OrderStatus, OrderType
from src.entities.direction import Direction
from src.entities.balance import Balance
from src.utils.common_functions import timeframe_to_timedelta
from src.logger.logger import get_logger
from ..connector_base import ConnectorBase, ConnectorType


class ConnectorOkxFuture(ConnectorBase):
    """
    OKX Futures connector for interacting with OKX's REST and WebSocket APIs.
    Supports fetching live and historical trade data, placing/cancelling orders, tracking open orders, and real-time balances.
    """

    connector_type: ConnectorType = ConnectorType.OkxFuture
    _CANDLES_DEPTH: int = 1000  # Maximum number of candles to store per timeframe

    def __init__(self, config: dict):
        """
        Initialize the OKX Futures connector.

        Args:
            config (dict): Configuration dictionary with API key, secret, passphrase, and testnet flag.
        """
        super().__init__(config)
        self._logger = get_logger(self.__module__)
        self._client = None
        self._api_key = config.get('API_KEY')
        self._api_secret = config.get('API_SEC')
        self._passphrase = config.get('PASSPHRASE')
        self._testnet = config.get('TESTNET', False)

        # Concurrency locks for safe multi-threaded operations
        self._instruments_update_lock = asyncio.Lock()
        self._candles_update_lock = asyncio.Lock()
        self._orders_update_lock = asyncio.Lock()
        self._balances_update_lock = asyncio.Lock()

        # Caches for data
        self._instruments_spec: Dict[str, Instrument] = {}
        self._candles: Dict[str, Dict[Timeframe, Optional[pd.DataFrame]]] = {}
        self._open_orders: Dict[str, Dict[str, Order]] = {}
        self._balances: Dict[str, Balance] = {}

    @property
    def tf_map(self) -> Dict[Timeframe, str]:
        """
        Timeframe-to-OKX interval mapping.

        Returns:
            Dict[Timeframe, str]: Mapping of Timeframe enums to OKX-supported intervals.
        """
        return {
            Timeframe.M1: "1m",
            Timeframe.M5: "5m",
            Timeframe.M15: "15m",
            Timeframe.M30: "30m",
            Timeframe.H1: "1H",
            Timeframe.H4: "4H",
            Timeframe.D1: "1Dutc",
            Timeframe.W1: "1Wutc",
        }

    async def _start_network(self):
        """
        Start the connector's network connections, including REST and WebSocket clients.
        """
        self._logger.info("Starting OKX Futures connector...")
        self._client = await OKXAsyncClient.create(
            api_key=self._api_key,
            api_secret=self._api_secret,
            passphrase=self._passphrase,
            testnet=self._testnet,
        )
        self._logger.info("OKX client initialized successfully.")

        # Start background tasks for polling and WebSocket handling
        await self._start_polling_tasks()

    async def _start_polling_tasks(self):
        """
        Start tasks for polling instrument specifications and other data.
        """
        if not hasattr(self, "_instruments_spec_polling_task"):
            self._instruments_spec_polling_task = asyncio.create_task(
                self._instruments_spec_polling_loop(),
                name="Task_ConnectorOkxFuture_InstrumentsSpecPolling",
            )
            self._logger.info("Started polling task for instrument specifications.")

    async def _stop_network(self):
        """
        Stop all background tasks and close connections.
        """
        self._logger.info("Stopping OKX Futures connector...")

        # Cancel polling tasks
        if hasattr(self, "_instruments_spec_polling_task"):
            self._instruments_spec_polling_task.cancel()
            self._logger.info("Stopped polling task for instrument specifications.")

        # Close OKX client connection
        if self._client:
            await self._client.close()
            self._logger.info("OKX client connection closed.")
            self._client = None

    async def _update_instruments_spec(self):
        """
        Fetch and update instrument specifications from OKX.
        """
        async with self._instruments_update_lock:
            try:
                self._logger.info("Fetching instrument specifications from OKX...")
                response = await self._client.get_instruments()
                instruments_data = response.get("data", [])

                for instrument in instruments_data:
                    symbol = instrument["instId"]
                    self._instruments_spec[symbol] = Instrument(
                        symbol=symbol,
                        tick_size=Decimal(instrument["tickSz"]),
                        contract_size=Decimal(instrument["ctVal"]),
                        min_qty_market=Decimal(instrument["minSz"]),
                        lot_step_market=Decimal(instrument["lotSz"]),
                    )
                self._logger.info(f"Updated {len(self._instruments_spec)} instrument specifications.")
            except Exception as e:
                self._logger.error(f"Failed to update instrument specifications: {e}\n{traceback.format_exc()}")

    async def get_instrument_spec(self, symbol: str) -> Instrument:
        """
        Retrieve the cached instrument specification for a symbol.

        Args:
            symbol (str): Symbol of the trading pair (e.g., BTC-USDT-SWAP).

        Returns:
            Instrument: The instrument specification object.

        Raises:
            ValueError: If the specified symbol is not found.
        """
        async with self._instruments_update_lock:
            if symbol not in self._instruments_spec:
                raise ValueError(f"Instrument {symbol} not found.")
            return self._instruments_spec[symbol]

    async def _fetch_candles_rest_api(self, symbol: str, timeframe: Timeframe, n: int) -> pd.DataFrame:
        """
        Fetch historical candles for a symbol and timeframe using OKX REST API.

        Args:
            symbol (str): Symbol of the trading pair.
            timeframe (Timeframe): The timeframe for the candles.
            n (int): Number of candles to fetch.

        Returns:
            pd.DataFrame: A DataFrame containing the candle data.
        """
        self._logger.info(f"Fetching {n} candles for {symbol} {timeframe.name} via REST API...")
        try:
            bar_interval = self.tf_map[timeframe]
            response = await self._client.get_candles(inst_id=symbol, bar=bar_interval, limit=n)
            data = response.get("data", [])
            df = pd.DataFrame(data, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df["timestamp"] = pd.to_datetime(df["timestamp"].astype(int), unit="ms")
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = df[col].astype(str).apply(Decimal)  # Ensure Decimal precision
            df.set_index("timestamp", inplace=True)
            return df
        except Exception as e:
            self._logger.error(f"Failed to fetch candles for {symbol}: {e}")
            raise

    async def get_last_candles(self, symbol: str, timeframe: Timeframe, n: int = _CANDLES_DEPTH) -> List[dict]:
        """
        Retrieve the last n candles for a symbol and timeframe.

        Args:
            symbol (str): Symbol of the trading pair.
            timeframe (Timeframe): The timeframe for the candles.
            n (int): Number of candles to retrieve.

        Returns:
            List[dict]: A list of candles as dictionaries.
        """
        async with self._candles_update_lock:
            try:
                num_candles = min(n, self._CANDLES_DEPTH)
                df = await self._fetch_candles_rest_api(symbol, timeframe, num_candles)
                return df.tail(num_candles).to_dict(orient="records")
            except Exception as e:
                self._logger.error(f"Error fetching candles for {symbol} {timeframe.name}: {e}")
                return []