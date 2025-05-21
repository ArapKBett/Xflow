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
                    del ConnectorOkxFuture._candles_ws_last_response_time[f"{symbol}_{timeframe.name}"]
                    if ConnectorOkxFuture._candles_polling_tasks[symbol][timeframe]:
                        ConnectorOkxFuture._candles_polling_tasks[symbol][timeframe].cancel()
                        self._logger.info(f"_candles_polling_task {symbol}_{timeframe.name} canceled.")
                    return None
            if ConnectorOkxFuture._candles[symbol][timeframe] is None:
                return None
            if (f"{symbol}_{timeframe.name}" in ConnectorOkxFuture._candles_ws_last_response_time and
                    ConnectorOkxFuture._candles_ws_last_response_time[f"{symbol}_{timeframe.name}"]):
                tm_delta = timeframe_to_timedelta(timeframe) + timedelta(seconds=60)
                if (datetime.utcnow() - ConnectorOkxFuture._candles_ws_last_response_time[f"{symbol}_{timeframe.name}"]) > tm_delta:
                    self._logger.error(f"OkxFuture WebSocket stale for {symbol}_{timeframe.name}. Reconnecting...")
                    if ConnectorOkxFuture._candles_polling_tasks[symbol][timeframe]:
                        ConnectorOkxFuture._candles_polling_tasks[symbol][timeframe].cancel()
                        self._logger.info(f"_candles_polling_task {symbol}_{timeframe.name} canceled.")
                    ConnectorOkxFuture._candles_polling_tasks[symbol][timeframe] = asyncio.create_task(
                        self._process_instrument_candles_ws(symbol, timeframe),
                        name=f"Task_ConnectorOkxFuture_candles_{symbol}_{timeframe.name}"
                    )
                    await asyncio.sleep(0.5)
                    return None
            return ConnectorOkxFuture._candles[symbol][timeframe][-n:].to_dict(orient="records")

    async def get_last_formed_candles(self, symbol: str, timeframe: Timeframe, n: int) -> list:
        """
        Retrieve the last n formed (closed) candles, excluding the current unformed candle.

        Args:
            symbol (str): Trading pair symbol.
            timeframe (Timeframe): The timeframe for candles.
            n (int): Number of formed candles to retrieve.

        Returns:
            list: List of dictionaries containing candle data.
        """
        candles = await self.get_last_candles(symbol, timeframe, n + 1)
        return candles[:-1] if candles else []

    async def get_cur_price(self, symbol: str) -> dict:
        """
        Fetch the current bid, ask, and last price for a symbol.

        Args:
            symbol (str): Trading pair symbol.

        Returns:
            dict: Dictionary with keys "bid", "ask", and "last" containing Decimal values.
        """
        try:
            ticker = await self._client.get_ticker(symbol)
            data = ticker.get("data", [])[0]
            return {
                "bid": Decimal(data["bidPx"]),
                "ask": Decimal(data["askPx"]),
                "last": Decimal(data["last"])
            }
        except Exception as e:
            self._logger.error(f"Failed to fetch current price for {symbol}: {e}\n{traceback.format_exc()}")
            raise

    def _parse_order_from_okx_data(self, order_data: dict) -> Order:
        """
        Parse OKX order data into an Order object.

        Args:
            order_data (dict): Order data from OKX API.

        Returns:
            Order: Parsed order object with Decimal values and enums.
        """
        ord_id = order_data["ordId"]
        inst_id = order_data["instId"]
        order = Order(position_id=ord_id, symbol=inst_id)
        order.id = ord_id
        order.price = Decimal(order_data.get("avgPx") or "0")  # Use avgPx for executed price
        order.theor_price = Decimal(order_data.get("px") or "0")  # Limit price
        order.volume = Decimal(order_data.get("sz") or "0")
        order.volume_executed = Decimal(order_data.get("accFillSz") or "0")
        order.commission = Decimal(order_data.get("fee") or "0")
        u_time = order_data.get("uTime")
        if u_time:
            order.datetime = datetime.fromtimestamp(int(u_time) / 1000)
        state = order_data.get("state")
        status_map = {
            "live": OrderStatus.ACTIVE,
            "partially_filled": OrderStatus.PARTIALLY_EXECUTED,
            "filled": OrderStatus.EXECUTED,
            "canceled": OrderStatus.CANCELED
        }
        order.status = status_map.get(state, OrderStatus.PLACED)
        ord_type = order_data.get("ordType")
        type_map = {
            "market": OrderType.MARKET,
            "fok": OrderType.MARKET,
            "ioc": OrderType.MARKET,
            "limit": OrderType.LIMIT,
            "post_only": OrderType.LIMIT,
            "conditional": OrderType.STOP
        }
        order.order_type = type_map.get(ord_type, OrderType.MARKET)
        side = order_data.get("side")
        order.side = Direction.BUY if side == "buy" else Direction.SELL
        return order

    async def _get_last_open_orders_rest_api(self, symbol: str) -> Dict[str, Dict[str, Order]]:
        """
        Fetch open orders for a symbol via REST API.

        Args:
            symbol (str): Trading pair symbol.

        Returns:
            Dict[str, Dict[str, Order]]: Dictionary of open orders keyed by symbol and order ID.
        """
        while self._client is None:
            self._logger.warning("OKXAsyncClient is not initialized yet. Waiting...")
            await asyncio.sleep(0.5)
        try:
            res = await self._client.get_open_orders("SWAP", symbol)
            data = res.get("data", [])
            if not data:
                self._logger.info(f"No open orders found for {symbol}.")
                return {symbol: {}}
            data = sorted(data, key=lambda x: int(x.get("uTime", 0)))
            self._open_orders_last_update = int(data[-1].get("uTime", 0)) if data else 0
            orders = {symbol: {}}
            for item in data:
                order = self._parse_order_from_okx_data(item)
                orders[symbol][order.id] = order
            return orders
        except Exception as e:
            self._logger.error(f"Failed to fetch open orders for {symbol}: {e}\n{traceback.format_exc()}")
            raise

    async def _process_open_orders_ws(self, symbol: str):
        """
        Process WebSocket updates for open orders.
        Updates the _open_orders cache and buffers updates before REST sync.

        Args:
            symbol (str): Trading pair symbol.
        """
        websocket = None
        while True:
            try:
                websocket = await self._client.ws_orders()
                self._logger.info(f"OkxFuture WebSocket started for orders updates {symbol}")
                if symbol in self._open_orders_ws_ready_flag:
                    self._open_orders_ws_ready_flag[symbol].set()
                    self._logger.info(f"WebSocket readiness flag set for {symbol}.")
                while True:
                    res = json.loads(await websocket.recv())
                    if res.get("event") == "error":
                        self._logger.error(f"WebSocket error for {symbol}: {res}")
                        continue
                    channel = res.get("arg", {}).get("channel")
                    if channel != "orders":
                        continue
                    for order_data in res.get("data", []):
                        order_symbol = order_data["instId"]
                        order_id = order_data["ordId"]
                        if order_symbol != symbol:
                            continue
                        async with self._open_orders_update_lock:
                            if order_symbol not in ConnectorOkxFuture._open_orders:
                                ConnectorOkxFuture._orders_buffer.append(order_data)
                                continue
                            status = order_data.get("state")
                            if status in ["live", "partially_filled"]:
                                order = self._parse_order_from_okx_data(order_data)
                                ConnectorOkxFuture._open_orders[order_symbol][order_id] = order
                            elif status in ["filled", "canceled"]:
                                ConnectorOkxFuture._open_orders[order_symbol].pop(order_id, None)
            except Exception as e:
                self._logger.error(f"OkxFuture WebSocket order error for {symbol}: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(5)
            finally:
                if websocket:
                    try:
                        await websocket.close()
                        self._logger.info(f"OkxFuture WebSocket closed for order stream: {symbol}.")
                    except Exception as e:
                        self._logger.warning(f"Error closing WebSocket for order stream {symbol}: {e}")

    async def get_last_open_orders(self, order: Order) -> Dict[str, Order]:
        """
        Fetch all open orders for a symbol.

        Args:
            order (Order): Order object containing the symbol to query.

        Returns:
            Dict[str, Order]: Dictionary of open orders keyed by order ID.
        """
        symbol = order.symbol
        async with self._open_orders_update_lock:
            if symbol not in ConnectorOkxFuture._open_orders:
                try:
                    ConnectorOkxFuture._open_orders_polling_task[symbol] = None
                    ConnectorOkxFuture._open_orders_ws_ready_flag[symbol] = asyncio.Event()
                    ConnectorOkxFuture._open_orders_polling_task[symbol] = asyncio.create_task(
                        self._process_open_orders_ws(symbol),
                        name=f"Task_ConnectorOkxFuture_open_orders_{symbol}"
                    )
                    await asyncio.wait_for(self._open_orders_ws_ready_flag[symbol].wait(), timeout=10.0)
                    self._logger.info(f"WebSocket for {symbol} is ready.")
                    orders = await self._get_last_open_orders_rest_api(symbol)
                    ConnectorOkxFuture._open_orders.update(orders)
                    if symbol not in ConnectorOkxFuture._open_orders:
                        ConnectorOkxFuture._open_orders[symbol] = {}
                        self._logger.info(f"OkxFuture symbol {symbol} added to track open orders.")
                    for raw in ConnectorOkxFuture._orders_buffer:
                        u_time = raw.get("uTime") or raw.get("utime")
                        ord_id = raw["ordId"]
                        if u_time and int(u_time) > ConnectorOkxFuture._open_orders_last_update:
                            state = raw.get("state")
                            order_obj = self._parse_order_from_okx_data(raw)
                            if state in ["live", "partially_filled"]:
                                ConnectorOkxFuture._open_orders[symbol][ord_id] = order_obj
                            elif state in ["filled", "canceled"]:
                                ConnectorOkxFuture._open_orders[symbol].pop(ord_id, None)
                    ConnectorOkxFuture._orders_buffer.clear()
                except asyncio.TimeoutError:
                    self._logger.error(f"WebSocket readiness for {symbol} timed out.")
                    return {}
                except Exception as e:
                    self._logger.error(f"OkxFuture get_last_open_orders error for {symbol}: {e}\n{traceback.format_exc()}")
                    ConnectorOkxFuture._open_orders.pop(symbol, None)
                    if ConnectorOkxFuture._open_orders_polling_task.get(symbol):
                        ConnectorOkxFuture._open_orders_polling_task[symbol].cancel()
                        self._logger.info(f"_orders_polling_task {symbol} canceled.")
                    ConnectorOkxFuture._orders_buffer.clear()
                    return {}
            return ConnectorOkxFuture._open_orders.get(symbol, {})

    async def _update_balance_from_data(self, data: List[dict]):
        """
        Update the balance cache from OKX API data.

        Args:
            data (List[dict]): Balance data from REST or WebSocket.
        """
        async with self._balance_update_lock:
            for account_info in data:
                details = account_info.get("details", [])
                for currency_detail in details:
                    currency = currency_detail.get("ccy")
                    try:
                        available = Decimal(currency_detail.get("availBal", "0"))
                        frozen = Decimal(currency_detail.get("frozenBal", "0"))
                        total = available + frozen
                        ConnectorOkxFuture._balance[currency] = Balance(
                            currency=currency,
                            available=available,
                            frozen=frozen,
                            total=total
                        )
                    except (ValueError, ArithmeticError) as e:
                        self._logger.warning(f"Failed to parse balance for {currency}: {e}")
                        continue

    async def _get_last_balance_rest_api(self):
        """
        Fetch the current account balance via REST API.
        """
        while self._client is None:
            self._logger.warning("OKXAsyncClient is not initialized yet. Waiting...")
            await asyncio.sleep(0.5)
        try:
            res = await self._client.get_balance()
            data = res.get("data")
            if data:
                await self._update_balance_from_data(data)
            else:
                self._logger.warning("Failed to fetch balance data via REST API.")
        except Exception as e:
            self._logger.error(f"Failed to fetch balance: {e}\n{traceback.format_exc()}")
            raise

    async def _process_balance_ws(self):
        """
        Process WebSocket updates for account balance.
        """
        websocket = None
        while True:
            try:
                websocket = await self._client.ws_balance()
                self._logger.info("OkxFuture WebSocket started for balance updates.")
                while True:
                    res = await websocket.recv()
                    res = json.loads(res)
                    if res.get("event") == "error":
                        self._logger.error(f"WebSocket error for balance: {res}")
                        continue
                    channel = res.get("arg", {}).get("channel")
                    data = res.get("data")
                    if channel != "account" or not data:
                        continue
                    await self._update_balance_from_data(data)
            except Exception as e:
                self._logger.error(f"OkxFuture WebSocket error for balance: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(3)
            finally:
                if websocket:
                    try:
                        await websocket.close()
                        self._logger.info("OkxFuture WebSocket closed for balance.")
                    except Exception as e:
                        self._logger.warning(f"Error closing WebSocket for balance: {e}")

    async def get_last_balance(self) -> Dict[str, Balance]:
        """
        Fetch the current account balance, initializing WebSocket updates if needed.

        Returns:
            Dict[str, Balance]: Dictionary of Balance objects keyed by currency.
        """
        async with self._balance_update_lock:
            if not ConnectorOkxFuture._balance:
                try:
                    await self._get_last_balance_rest_api()
                    if not ConnectorOkxFuture._balance_polling_task or ConnectorOkxFuture._balance_polling_task.done():
                        ConnectorOkxFuture._balance_polling_task = asyncio.create_task(
                            self._process_balance_ws(),
                            name="Task_ConnectorOkxFuture_balance_ws"
                        )
                    await asyncio.sleep(0.5)
                except Exception as e:
                    self._logger.error(f"OkxFuture get_last_balance error: {e}\n{traceback.format_exc()}")
                    ConnectorOkxFuture._balance.clear()
                    if ConnectorOkxFuture._balance_polling_task:
                        ConnectorOkxFuture._balance_polling_task.cancel()
                        self._logger.info("_balance_polling_task canceled.")
                    return {}
            return ConnectorOkxFuture._balance

    async def _monitor_place_order_ws(self):
        """
        Monitor the place order WebSocket connection, reconnecting if needed.
        """
        while True:
            try:
                if self._place_order_ws is None:
                    self._logger.warning("OkxFuture place order WebSocket disconnected. Reconnecting...")
                    self._place_order_ws = await self._client.ws_private()
                    self._logger.info("OkxFuture place order WebSocket reconnected.")
                try:
                    pong_waiter = await self._place_order_ws.ping()
                    await asyncio.wait_for(pong_waiter, timeout=10)
                except Exception as e:
                    self._logger.warning(f"OkxFuture place order WebSocket ping failed: {e}. Reconnecting...")
                    await self._place_order_ws.close()
                    self._place_order_ws = None
                await asyncio.sleep(10)
            except Exception as e:
                self._logger.error(f"OkxFuture place order WebSocket monitor error: {e}\n{traceback.format_exc()}")
                await asyncio.sleep(5)

    async def _process_place_order_ws(self):
        """
        Initialize and monitor the place order WebSocket for order creation and cancellation.
        """
        try:
            if self._place_order_ws is None:
                self._place_order_ws = await self._client.ws_private()
                self._logger.info("OkxFuture place order WebSocket connected.")
            if not self._place_order_ws_polling_task or self._place_order_ws_polling_task.done():
                self._place_order_ws_polling_task = asyncio.create_task(
                    self._monitor_place_order_ws(),
                    name="Task_ConnectorOkxFuture_monitor_place_order_ws"
                )
                self._logger.info("OkxFuture place order WebSocket monitor started.")
        except Exception as e:
            self._logger.error(f"Failed to initialize place order WebSocket: {e}\n{traceback.format_exc()}")
            if self._place_order_ws_polling_task:
                self._place_order_ws_polling_task.cancel()
                self._place_order_ws_polling_task = None
            if self._place_order_ws:
                try:
                    await self._place_order_ws.close()
                except Exception as e:
                    self._logger.warning(f"Error closing place order WebSocket: {e}")
                self._place_order_ws = None

    async def _handle_response_place_order_ws(self, request_id: str) -> str:
        """
        Handle WebSocket response for order placement or cancellation.

        Args:
            request_id (str): Unique request ID for the WebSocket message.

        Raises:
            Exception: If the request fails or response is invalid.
        """
        while True:
            res = await asyncio.wait_for(self._place_order_ws.recv(), timeout=30)
            res = json.loads(res)
            if res.get("id") != request_id:
                continue
            if res.get("code") != "0":
                raise Exception(f"Request failed: {res}")
            data = res.get("data", [])
            if not data:
                raise Exception(f"Missing data in response: {res}")
            first_item = data[0]
            s_code = first_item.get("sCode")
            ord_id = first_item.get("ordId")
            msg = first_item.get("sMsg", "Unknown error")
            if s_code == "0":
                return ord_id
            raise Exception(f"OkxFuture WebSocket request failed: {msg}")

    async def create_order(self, symbol: str, side: Direction, order_type: OrderType, price: Optional[Decimal] = None, qty: Optional[Decimal] = None, stop_price: Optional[Decimal] = None) -> Order:
        """
        Create a new order on OKX Futures.
      
        Args:
            symbol (str): Trading pair symbol (e.g., "BTC-USDT-SWAP").
            side (Direction): BUY or SELL.
            order_type (OrderType): MARKET, LIMIT, or STOP.
            price (Decimal, optional): Price for limit orders.
            qty (Decimal, optional): Quantity in base currency (e.g., BTC).
            stop_price (Decimal, optional): Stop price for stop orders.

        Returns:
            Order: Created order object.

        Raises:
            ValueError: If the instrument or quantity is invalid.
        """
        try:
            instrument = await self.get_instrument_spec(symbol)
        except ValueError as e:
            self._logger.error(f"Invalid symbol {symbol}: {e}")
            raise
        if qty:
            contracts = qty / instrument.contract_size
            if contracts % instrument.lot_step_market != 0:
                raise ValueError(f"Order quantity must be a multiple of lot size {instrument.lot_step_market}")
            qty_str = str(int(contracts))
        else:
            qty_str = None
        market_prices = await self.get_cur_price(symbol)
        market_price = market_prices["last"]
        ord_type = order_type.value
        params = {
            "instId": symbol,
            "tdMode": "cross",
            "side": side.value.lower(),
            "ordType": ord_type,
            "sz": qty_str
        }
        if order_type == OrderType.LIMIT and price:
            price_limits = await self._get_price_limits(symbol)
            max_buy, min_sell = price_limits["max_buy"], price_limits["min_sell"]
            if (side == Direction.BUY and price > max_buy) or (side == Direction.SELL and price < min_sell):
                self._logger.warning(f"Converting invalid limit order to market order: {symbol}, side={side.value}, price={price}")
                params["ordType"] = "market"
            else:
                params["px"] = str(price)
        elif order_type == OrderType.STOP and stop_price:
            params["ordType"] = "conditional"  # OKX uses "conditional" for stop orders
            if (side == Direction.BUY and stop_price <= market_price) or (side == Direction.SELL and stop_price >= market_price):
                self._logger.warning(f"Converting stop order to market order: {symbol}, side={side.value}, stop_price={stop_price}")
                params["ordType"] = "market"
            else:
                params["tpTriggerPx"] = str(stop_price)
                params["tpOrdPx"] = "-1"  # Market order on trigger
        elif order_type == OrderType.MARKET:
            params.pop("px", None)
        try:
            request_id = str(uuid.uuid4())[:8]
            payload = {
                "id": request_id,
                "op": "order",
                "args": [params]
            }
            await self._process_place_order_ws()
            await self._place_order_ws.send(json.dumps(payload))
            ord_id = await self._handle_response_place_order_ws(request_id)
            order_data = await self._client.get_order(symbol, ord_id)
            order = self._parse_order_from_okx_data(order_data.get("data", [])[0])
            ConnectorOkxFuture._open_orders.setdefault(symbol, {})[ord_id] = order
            self._logger.info(f"OkxFuture order placed successfully. Order ID: {ord_id}")
            return order
        except Exception as e:
            self._logger.error(f"Failed to create order for {symbol}: {e}\n{traceback.format_exc()}")
            raise

    async def cancel_order(self, symbol: str, order_id: str):
        """
        Cancel an order by ID.

        Args:
            symbol (str): Trading pair symbol.
            order_id (str): Order ID to cancel.
        """
        try:
            await self.get_instrument_spec(symbol)
        except ValueError as e:
            self._logger.error(f"Invalid symbol {symbol}: {e}")
            raise
        try:
            request_id = str(uuid.uuid4())[:8]
            payload = {
                "id": request_id,
                "op": "cancel-order",
                "args": [{"instId": symbol, "ordId": order_id}]
            }
            await self._process_place_order_ws()
            await self._place_order_ws.send(json.dumps(payload))
            ord_id = await self._handle_response_place_order_ws(request_id)
            async with self._open_orders_update_lock:
                ConnectorOkxFuture._open_orders.get(symbol, {}).pop(order_id, None)
            self._logger.info(f"OkxFuture order canceled successfully. Order ID: {ord_id}")
        except Exception as e:
            self._logger.error(f"Failed to cancel order {order_id} for {symbol}: {e}\n{traceback.format_exc()}")
            raise

    async def get_order(self, order: Order) -> Order:
        """
        Retrieve or update an order, using the local cache or REST API.

        Args:
            order (Order): Order object to retrieve.

        Returns:
            Order: Updated order object.
        """
        symbol = order.symbol
        order_id = order.id
        async with self._open_orders_update_lock:
            if symbol in ConnectorOkxFuture._open_orders and order_id in ConnectorOkxFuture._open_orders[symbol]:
                return ConnectorOkxFuture._open_orders[symbol][order_id]
        try:
            order_data = await self._client.get_order(symbol, order_id)
            parsed_order = self._parse_order_from_okx_data(order_data.get("data", [])[0])
            async with self._open_orders_update_lock:
                if parsed_order.status in [OrderStatus.ACTIVE, OrderStatus.PARTIALLY_EXECUTED]:
                    ConnectorOkxFuture._open_orders.setdefault(symbol, {})[order_id] = parsed_order
                else:
                    ConnectorOkxFuture._open_orders.get(symbol, {}).pop(order_id, None)
            return parsed_order
        except Exception as e:
            self._logger.error(f"Failed to fetch order {order_id} for {symbol}: {e}\n{traceback.format_exc()}")
            raise

    async def _get_price_limits(self, symbol: str) -> dict:
        """
        Fetch price limits for a symbol from OKX API.

        Args:
            symbol (str): Trading pair symbol.

        Returns:
            dict: Dictionary with "max_buy" and "min_sell" as Decimal values.
        """
        try:
            res = await self._client.get_price_limits(symbol)
            data = res.get("data", [])[0]
            return {
                "max_buy": Decimal(data["buyLmt"]),
                "min_sell": Decimal(data["sellLmt"])
            }
        except Exception as e:
            self._logger.error(f"Failed to fetch price limits for {symbol}: {e}\n{traceback.format_exc()}")
            # Fallback to market prices
            prices = await self.get_cur_price(symbol)
            return {
                "max_buy": prices["ask"] * Decimal("1.05"),
                "min_sell": prices["bid"] * Decimal("0.95")
            }

    # Override helper methods with Decimal and Direction
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

    async def create_limit_buy_order(self, symbol: str, price: Decimal, qty: Decimal) -> Order:
        """
        Create a limit buy order.

        Args:
            symbol (str): Trading pair symbol.
            price (Decimal): Order price.
            qty (Decimal): Order quantity in base currency.

        Returns:
            Order: Created order object.
        """
        return await self.create_limit_order(symbol, Direction.BUY, price, qty)

    async def create_limit_sell_order(self, symbol: str, price: Decimal, qty: Decimal) -> Order:
        """
        Create a limit sell order.

        Args:
            symbol (str): Trading pair symbol.
            price (Decimal): Order price.
            qty (Decimal): Order quantity in base currency.

        Returns:
            Order: Created order object.
        """
        return await self.create_limit_order(symbol, Direction.SELL, price, qty)

    async def create_market_buy_order(self, symbol: str, qty: Decimal) -> Order:
        """
        Create a market buy order.

        Args:
            symbol (str): Trading pair symbol.
            qty (Decimal): Order quantity in base currency.

        Returns:
            Order: Created order object.
        """
        return await self.create_market_order(symbol, Direction.BUY, qty)

    async def create_market_sell_order(self, symbol: str, qty: Decimal) -> Order:
        """
        Create a market sell order.

        Args:
            symbol (str): Trading pair symbol.
            qty (Decimal): Order quantity in base currency.

        Returns:
            Order: Created order object.
        """
        return await self.create_market_order(symbol, Direction.SELL, qty)