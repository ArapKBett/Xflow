import asyncio
import json
import traceback
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional
import pandas as pd
import numpy as np
import uuid
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
    OKX Futures connector implementing the ConnectorBase interface for futures trading.
    Handles instrument specifications, candles, orders, and balances using OKX's REST and WebSocket APIs.
    """
    connector_type: ConnectorType = ConnectorType.OkxFuture
    # Shared cache for instrument specifications, updated periodically
    _instruments_spec: Dict[str, Instrument] = {}
    _instruments_spec_polling_task: Optional[asyncio.Task] = None
    # Shared cache for candles, stored as pandas DataFrames per symbol and timeframe
    _candles: Dict[str, Dict[Timeframe, Optional[pd.DataFrame]]] = {}
    # Tasks for WebSocket candle updates per symbol and timeframe
    _candles_polling_tasks: Dict[str, Dict[Timeframe, Optional[asyncio.Task]]] = {}
    # Tracks last WebSocket response time to detect stale connections
    _candles_ws_last_response_time: Dict[str, Optional[datetime]] = {}
    # Maximum number of candles to store per symbol and timeframe
    _CANDLES_DEPTH: int = 1000
    # Cached open orders, updated via REST and WebSocket
    _open_orders: Dict[str, Dict[str, Order]] = {}
    # Tasks for WebSocket order updates per symbol
    _open_orders_polling_task: Dict[str, asyncio.Task] = {}
    # Buffer for WebSocket order updates before REST sync
    _orders_buffer: List[dict] = []
    # Timestamp of last REST open orders update
    _open_orders_last_update: int = 0
    # Flags to indicate WebSocket readiness for order updates
    _open_orders_ws_ready_flag: Dict[str, asyncio.Event] = {}
    # Cached balance data, updated via REST and WebSocket
    _balance: Dict[str, Balance] = {}
    # Task for WebSocket balance updates
    _balance_polling_task: Optional[asyncio.Task] = None
    # WebSocket for placing and canceling orders
    _place_order_ws: Optional[WebSocketClientProtocol] = None
    # Task for monitoring place order WebSocket
    _place_order_ws_polling_task: Optional[asyncio.Task] = None
    # Mapping of Timeframe to OKX bar intervals
    _tf_map = {
        Timeframe.M1: "1m",
        Timeframe.M5: "5m",
        Timeframe.M15: "15m",
        Timeframe.M30: "30m",
        Timeframe.H1: "1H",
        Timeframe.H4: "4H",
        Timeframe.D1: "1Dutc",
        Timeframe.W1: "1Wutc"
    }

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
        self._instruments_update_lock = asyncio.Lock()
        self._candles_update_lock = asyncio.Lock()
        self._open_orders_update_lock = asyncio.Lock()
        self._balance_update_lock = asyncio.Lock()

    @property
    def tf_map(self) -> Dict[Timeframe, str]:
        """Return the mapping of Timeframe to OKX bar intervals."""
        return self._tf_map

    async def _start_network(self):
        """
        Start background tasks for the connector, including client initialization,
        instrument polling, and WebSocket handling.
        """
        self._logger.info("Starting background tasks for OkxFuture connector...")
        # Initialize OKXAsyncClient with API credentials
        self._client = await OKXAsyncClient.create(
            api_key=self._api_key,
            api_secret=self._api_secret,
            passphrase=self._passphrase,
            testnet=self._testnet
        )
        self._logger.info("OkxFuture async client successfully initialized.")
        # Start instrument specification polling if not already running
        if ConnectorOkxFuture._instruments_spec_polling_task is None:
            ConnectorOkxFuture._instruments_spec_polling_task = asyncio.create_task(
                self._instruments_spec_polling_loop(),
                name="Task_ConnectorOkxFuture_instruments_spec_polling_loop"
            )
            await asyncio.sleep(0.1)
            self._logger.info("_instruments_spec_polling_task OkxFuture started...")

    async def _stop_network(self):
        """
        Stop all background tasks and close connections.
        Cancels polling tasks and closes the OKX client and WebSockets.
        """
        self._logger.info("Stopping OkxFuture connector...")
        # Close OKX client
        if self._client:
            await self._client.close()
            self._client = None
            self._logger.info("OKXAsyncClient closed.")
        # Cancel instrument polling task
        if ConnectorOkxFuture._instruments_spec_polling_task:
            ConnectorOkxFuture._instruments_spec_polling_task.cancel()
            ConnectorOkxFuture._instruments_spec_polling_task = None
            self._logger.info("_instruments_spec_polling_task stopped.")
        # Cancel balance polling task
        if self._balance_polling_task:
            self._balance_polling_task.cancel()
            self._balance_polling_task = None
            self._logger.info("_balance_polling_task stopped.")
        # Cancel open orders polling tasks
        for symbol, task in self._open_orders_polling_task.items():
            if task:
                task.cancel()
                self._logger.info(f"_open_orders_polling_task for {symbol} stopped.")
        self._open_orders_polling_task.clear()
        # Cancel candle polling tasks
        for symbol, timeframe_tasks in self._candles_polling_tasks.items():
            for timeframe, task in timeframe_tasks.items():
                if task:
                    task.cancel()
                    self._logger.info(f"_candles_polling_task for {symbol}_{timeframe.name} stopped.")
        self._candles_polling_tasks.clear()
        # Cancel place order WebSocket task
        if self._place_order_ws_polling_task:
            self._place_order_ws_polling_task.cancel()
            self._place_order_ws_polling_task = None
            self._logger.info("_place_order_ws_polling_task stopped.")
        # Close place order WebSocket
        if self._place_order_ws:
            try:
                await self._place_order_ws.close()
                self._logger.info("Place order WebSocket closed.")
            except Exception as e:
                self._logger.warning(f"Error closing place order WebSocket: {e}")
            self._place_order_ws = None

    async def _update_instruments_spec(self):
        """
        Update instrument specifications by fetching the latest data from OKX.
        Runs periodically as part of _instruments_spec_polling_loop.
        """
        async with self._instruments_update_lock:
            ConnectorOkxFuture._instruments_spec.clear()
            try:
                res = await self._client.get_instruments()  # Fetches perpetual futures by default
                for item in res.get("data", []):
                    symbol = item["instId"]
                    instrument_spec = Instrument(symbol=symbol)
                    instrument_spec.tick_size = Decimal(item["tickSz"])
                    instrument_spec.tick_value = instrument_spec.tick_size
                    instrument_spec.min_qty_market = Decimal(item["minSz"])
                    instrument_spec.lot_step_market = Decimal(item["lotSz"])
                    instrument_spec.max_qty_market = Decimal(item.get("maxMktSz", "9999999"))
                    instrument_spec.fx_spread = Decimal("0")
                    instrument_spec.contract_size = Decimal(item["ctVal"])  # OKX uses contract value (e.g., 0.01 BTC)
                    ConnectorOkxFuture._instruments_spec[symbol] = instrument_spec
                self._logger.info(f"Updated instruments_spec for OkxFuture. Number of symbols: {len(ConnectorOkxFuture._instruments_spec)}")
            except Exception as e:
                self._logger.error(f"Failed to update instruments: {e}\n{traceback.format_exc()}")

    async def get_instrument_spec(self, symbol: str) -> Instrument:
        """
        Retrieve the cached instrument specification for a symbol.

        Args:
            symbol (str): Trading pair symbol (e.g., "BTC-USDT-SWAP").

        Returns:
            Instrument: Instrument specification.

        Raises:
            ValueError: If the symbol is unknown.
        """
        async with self._instruments_update_lock:
            if symbol not in ConnectorOkxFuture._instruments_spec:
                self._logger.error(f"OkxFuture connector: Unknown symbol {symbol}")
                raise ValueError(f"OkxFuture connector: Unknown symbol {symbol}")
            return ConnectorOkxFuture._instruments_spec[symbol]

    async def _update_candles(self):
        """
        Initialize candle tracking for all instruments.
        Creates WebSocket tasks for each symbol and timeframe if not already running.
        """
        for symbol in ConnectorOkxFuture._instruments_spec:
            if symbol not in ConnectorOkxFuture._candles:
                ConnectorOkxFuture._candles[symbol] = {tf: None for tf in self.tf_map}
                ConnectorOkxFuture._candles_polling_tasks[symbol] = {tf: None for tf in self.tf_map}
                ConnectorOkxFuture._candles_ws_last_response_time[f"{symbol}_{tf.name}"] = None
            for tf in self.tf_map:
                if not ConnectorOkxFuture._candles_polling_tasks[symbol].get(tf):
                    ConnectorOkxFuture._candles_polling_tasks[symbol][tf] = asyncio.create_task(
                        self._process_instrument_candles_ws(symbol, tf),
                        name=f"Task_ConnectorOkxFuture_candles_{symbol}_{tf.name}"
                    )

    async def _get_last_candles_rest_api(self, symbol: str, timeframe: Timeframe, n: int) -> pd.DataFrame:
        """
        Fetch historical candles via OKX REST API.

        Args:
            symbol (str): Trading pair symbol.
            timeframe (Timeframe): The timeframe for candles.
            n (int): Number of candles to fetch.

        Returns:
            pd.DataFrame: DataFrame containing candles with columns Date, O, H, L, C, V.
        """
        while self._client is None:
            self._logger.warning("OKXAsyncClient is not initialized yet. Waiting...")
            await asyncio.sleep(0.5)
        bar = self.tf_map[timeframe]
        all_candles = []
        after = None
        try:
            while len(all_candles) < n:
                candles_data = await self._client.get_candles(inst_id=symbol, bar=bar, limit=300, after=after)
                candles = candles_data.get("data", [])
                candles = sorted(candles, key=lambda x: int(x[0]))
                all_candles.extend(candles)
                if len(candles) < 300:
                    break  # No more candles available
                after = int(candles[0][0]) - 1
            all_candles = sorted(all_candles, key=lambda x: int(x[0]))[-n:]
            df = pd.DataFrame([row[:6] for row in all_candles], columns=["Date", "O", "H", "L", "C", "V"])
            df["Date"] = pd.to_datetime(df["Date"].astype("int64"), unit="ms")
            df = df.set_index("Date")
            df = df.apply(lambda x: Decimal(str(x)) if x.name != "Date" else x)  # Convert to Decimal
            self._logger.info(f"Received {df.shape[0]} candles from {df.index[0]} to {df.index[-1]} "
                             f"via OkxFuture REST API for {symbol}_{timeframe.name}.")
            return df
        except Exception as e:
            self._logger.error(f"Failed to fetch candles for {symbol}_{timeframe.name}: {e}\n{traceback.format_exc()}")
            raise

    async def _process_instrument_candles_ws(self, symbol: str, timeframe: Timeframe):
        """
        Process WebSocket updates for candlestick data.
        Updates the _candles cache and falls back to REST API if gaps are detected.

        Args:
            symbol (str): Trading pair symbol.
            timeframe (Timeframe): The timeframe for candles.
        """
        websocket = None
        while True:
            try:
                tf = self.tf_map[timeframe]
                websocket = await self._client.ws_candles(symbol, tf)
                self._logger.info(f"OkxFuture WebSocket started for candlestick updates {symbol}_{timeframe.name}.")
                is_open = False
                while True:
                    try:
                        res = await asyncio.wait_for(websocket.recv(), timeout=5)
                        if res == "pong":
                            continue
                        res = json.loads(res)
                        if "data" not in res or not res["data"]:
                            continue
                        kline = res["data"][0]
                        ws_date = pd.to_datetime(int(kline[0]), unit="ms")
                        o, h, l, c, v = map(Decimal, kline[1:6])
                        is_closed = kline[-1] == "1"  # OKX marks confirmed candles with "1"
                        ConnectorOkxFuture._candles_ws_last_response_time[f"{symbol}_{timeframe.name}"] = datetime.utcnow()
                        async with self._candles_update_lock:
                            if symbol not in ConnectorOkxFuture._candles or ConnectorOkxFuture._candles[symbol][timeframe] is None:
                                candles = await self._get_last_candles_rest_api(symbol, timeframe, self._CANDLES_DEPTH)
                                ConnectorOkxFuture._candles[symbol][timeframe] = candles
                            df = ConnectorOkxFuture._candles[symbol][timeframe]
                            if ws_date == df.index[-1]:
                                df.loc[ws_date, ["O", "H", "L", "C", "V"]] = [o, h, l, c, v]
                                if is_closed:
                                    is_open = True
                            else:
                                tm_delta = timeframe_to_timedelta(timeframe)
                                if (ws_date - df.index[-1]) > tm_delta or not is_open:
                                    if (ws_date - df.index[-1]) > tm_delta:
                                        self._logger.warning(f"{symbol}_{timeframe.name}: Missing candle(s) detected.")
                                    if not is_open:
                                        self._logger.warning(f"{symbol}_{timeframe.name}: Last candle not finalized.")
                                    candles = await self._get_last_candles_rest_api(symbol, timeframe, 1)
                                    ConnectorOkxFuture._candles[symbol][timeframe] = (
                                        pd.concat([df, candles])
                                        .reset_index()
                                        .drop_duplicates(subset=["Date"], keep="last")
                                        .set_index("Date")[-self._CANDLES_DEPTH:]
                                    )
                                    is_open = False
                                    continue
                                candle = pd.DataFrame({
                                    "Date": [ws_date],
                                    "O": [o], "H": [h], "L": [l], "C": [c], "V": [v]
                                }).set_index("Date")
                                ConnectorOkxFuture._candles[symbol][timeframe] = pd.concat([df, candle])[-self._CANDLES_DEPTH:]
                                is_open = False
                    except asyncio.TimeoutError:
                        last_time = ConnectorOkxFuture._candles_ws_last_response_time.get(f"{symbol}_{timeframe.name}", datetime.utcnow())
                        if (datetime.utcnow() - last_time).total_seconds() > 20:
                            try:
                                await websocket.send("ping")
                                self._logger.debug(f"Sent ping to OKX for {symbol}_{timeframe.name}")
                            except Exception as e:
                                self._logger.warning(f"Ping failed for {symbol}_{timeframe.name}: {e}")
                    except Exception as e:
                        self._logger.error(f"WebSocket error in candle processing for {symbol}_{timeframe.name}: {e}")
                        break
            except Exception as e:
                self._logger.error(f"OkxFuture WebSocket error for {symbol}_{timeframe.name}: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(3)
            finally:
                if websocket:
                    try:
                        await websocket.close()
                        self._logger.info(f"OkxFuture WebSocket closed for {symbol}_{timeframe.name}.")
                    except Exception as e:
                        self._logger.warning(f"Error closing WebSocket for {symbol}_{timeframe.name}: {e}")

    async def get_last_candles(self, symbol: str, timeframe: Timeframe, n: int = _CANDLES_DEPTH) -> list:
        """
        Retrieve the last n candles for a symbol and timeframe.

        Args:
            symbol (str): Trading pair symbol.
            timeframe (Timeframe): The timeframe for candles.
            n (int): Number of candles to retrieve (default: _CANDLES_DEPTH).

        Returns:
            list: List of dictionaries containing candle data, or None if unavailable.
        """
        async with self._candles_update_lock:
            if n > self._CANDLES_DEPTH:
                self._logger.warning(f"OkxFuture connector: n={n} > MAX_CANDLES_DEPTH={self._CANDLES_DEPTH}")
            if symbol not in ConnectorOkxFuture._candles:
                ConnectorOkxFuture._candles[symbol] = {}
                ConnectorOkxFuture._candles_polling_tasks[symbol] = {}
                self._logger.info(f"OkxFuture symbol {symbol} added to track candles.")
            if timeframe not in ConnectorOkxFuture._candles[symbol]:
                try:
                    ConnectorOkxFuture._candles[symbol][timeframe] = None
                    ConnectorOkxFuture._candles_polling_tasks[symbol][timeframe] = None
                    ConnectorOkxFuture._candles_ws_last_response_time[f"{symbol}_{timeframe.name}"] = None
                    self._logger.info(f"OkxFuture symbol {symbol}_{timeframe.name} added to track candles.")
                    candles = await self._get_last_candles_rest_api(symbol, timeframe, self._CANDLES_DEPTH)
                    ConnectorOkxFuture._candles[symbol][timeframe] = candles
                    ConnectorOkxFuture._candles_polling_tasks[symbol][timeframe] = asyncio.create_task(
                        self._process_instrument_candles_ws(symbol, timeframe),
                        name=f"Task_ConnectorOkxFuture_candles_{symbol}_{timeframe.name}"
                    )
                    await asyncio.sleep(0.5)
                except Exception as e:
                    self._logger.error(f"OkxFuture get_last_candles error for {symbol}_{timeframe.name}: {e}\n{traceback.format_exc()}")
                    del ConnectorOkxFuture._candles[symbol][timeframe]
                    del ConnectorOkxFuture._candl
