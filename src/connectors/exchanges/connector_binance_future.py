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
from binance.enums import HistoricalKlinesType, SIDE_BUY, SIDE_SELL, ORDER_TYPE_LIMIT, ORDER_TYPE_MARKET, ORDER_TYPE_STOP_MARKET
from src.entities.instrument import Instrument
from src.entities.balance import Balance
from src.entities.order import Order, OrderType, OrderStatus
from src.entities.timeframe import Timeframe
from src.utils.common_functions import timeframe_to_timedelta
from src.logger.logger import get_logger
from .connector_base import ConnectorBase, ConnectorType

# Binance-specific Enums
class Direction(Enum):
    BUY = SIDE_BUY
    SELL = SIDE_SELL

class BinanceConnectorFuture(ConnectorBase):
    """
    Binance Futures connector implementing the ConnectorBase interface for futures trading.
    Handles instrument specifications, candles, orders, and balances using Binance's REST and WebSocket APIs.
    """
    connector_type: ConnectorType = ConnectorType.BinanceFuture
    # Shared cache for instrument specifications, updated every 30 minutes
    _instruments_spec: Dict[str, Instrument] = {}
    _instruments_spec_polling_task: Optional[asyncio.Task] = None
    # Shared cache for candles, stored as pandas DataFrames per symbol and timeframe
    _candles: Dict[str, Dict[Timeframe, Optional[pd.DataFrame]]] = {}
    # Tasks for WebSocket candle updates per symbol and timeframe
    _candles_polling_tasks: Dict[str, Dict[Timeframe, Optional[asyncio.Task]]] = {}
    # Tracks last WebSocket response time to detect stale connections
    _candles_ws_last_response_time: Dict[str, Optional[datetime]] = {}
    # Maximum number of candles to store per symbol and timeframe
    _CANDLES_DEPTH: int = 800
    # Mapping of Timeframe to Binance kline intervals
    _tf_map = {
        Timeframe.M1: AsyncClient.KLINE_INTERVAL_1MINUTE,
        Timeframe.M5: AsyncClient.KLINE_INTERVAL_5MINUTE,
        Timeframe.M15: AsyncClient.KLINE_INTERVAL_15MINUTE,
        Timeframe.M30: AsyncClient.KLINE_INTERVAL_30MINUTE,
        Timeframe.H1: AsyncClient.KLINE_INTERVAL_1HOUR,
        Timeframe.H4: AsyncClient.KLINE_INTERVAL_4HOUR,
        Timeframe.D1: AsyncClient.KLINE_INTERVAL_1DAY,
        Timeframe.W1: AsyncClient.KLINE_INTERVAL_1WEEK
    }

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
        self.order_buffer: List[Order] = []  # Buffer for order updates
        self._instruments_update_lock = asyncio.Lock()
        self._candles_update_lock = asyncio.Lock()

    @property
    def tf_map(self) -> Dict[Timeframe, str]:
        """Return the mapping of Timeframe to Binance kline intervals."""
        return self._tf_map

    async def _start_network(self):
        """
        Start background tasks for the connector, including client initialization,
        instrument polling, and WebSocket handling.
        """
        self._logger.info("Starting background tasks for BinanceFuture connector...")
        # Initialize Binance AsyncClient with API credentials
        self._client = await AsyncClient.create(api_key=self.api_key, api_secret=self.api_secret)
        self._logger.info("BinanceFuture async client successfully initialized.")
        
        # Start instrument specification polling if not already running
        if BinanceConnectorFuture._instruments_spec_polling_task is None:
            BinanceConnectorFuture._instruments_spec_polling_task = asyncio.create_task(
                self._instruments_spec_polling_loop(),
                name="Task_BinanceConnectorFuture_instruments_spec_polling_loop"
            )
            await asyncio.sleep(0.1)
            self._logger.info("_instruments_spec_polling_task BinanceFuture started...")
        
        # Start WebSocket handler for order updates
        asyncio.create_task(self._websocket_handler(), name="Task_BinanceConnectorFuture_websocket_handler")

    async def _stop_network(self):
        """
        Stop all background tasks and close connections.
        Cancels polling tasks and closes the Binance client.
        """
        self._logger.info("Stopping BinanceFuture connector...")
        # Close Binance client connection
        if self._client:
            await self._client.close_connection()
            self._client = None
        # Cancel instrument polling task
        if BinanceConnectorFuture._instruments_spec_polling_task:
            BinanceConnectorFuture._instruments_spec_polling_task.cancel()
            BinanceConnectorFuture._instruments_spec_polling_task = None
            self._logger.info("_instruments_spec_polling_task BinanceFuture stopped.")
        # Cancel all candle polling tasks
        for symbol, tasks in BinanceConnectorFuture._candles_polling_tasks.items():
            for tf, task in tasks.items():
                if task:
                    task.cancel()
                    self._logger.info(f"Candles polling task for {symbol}_{tf.name} stopped.")

    async def _update_instruments_spec(self):
        """
        Update instrument specifications by fetching the latest data from Binance.
        Runs every 30 minutes as part of _instruments_spec_polling_loop.
        """
        async with self._instruments_update_lock:
            BinanceConnectorFuture._instruments_spec.clear()
            try:
                # Fetch exchange info for futures
                res = await self._client.futures_exchange_info()
                for instrument in res["symbols"]:
                    instrument_spec = Instrument(
                        symbol=instrument["symbol"],
                        contract_size=Decimal("1"),  # Binance futures use 1 contract = 1 unit of base asset
                    )
                    # Parse filters for trading rules
                    for el in instrument["filters"]:
                        if el["filterType"] == "MARKET_LOT_SIZE":
                            instrument_spec.max_qty_market = Decimal(el["maxQty"])
                            instrument_spec.lot_step_market = Decimal(el["stepSize"])
                            instrument_spec.min_qty_market = Decimal(el["stepSize"])
                        if el["filterType"] == "PRICE_FILTER":
                            instrument_spec.tick_size = Decimal(el["tickSize"])
                    # Set tick_value and fx_spread (assumed for USDT pairs)
                    instrument_spec.tick_value = instrument_spec.tick_size
                    instrument_spec.fx_spread = Decimal("0")
                    BinanceConnectorFuture._instruments_spec[instrument_spec.symbol] = instrument_spec
                self._logger.info(f"Updated instruments_spec for BinanceFuture. Number of symbols: {len(BinanceConnectorFuture._instruments_spec)}")
            except Exception as e:
                self._logger.error(f"Failed to update instruments: {str(e)}\n{traceback.format_exc()}")

    async def get_instrument_spec(self, symbol: str) -> Instrument:
        """
        Retrieve the cached instrument specification for a symbol.

        Args:
            symbol (str): Trading pair symbol (e.g., "BTCUSDT").

        Returns:
            Instrument: Instrument specification.

        Raises:
            ValueError: If the symbol is unknown.
        """
        async with self._instruments_update_lock:
            if symbol not in BinanceConnectorFuture._instruments_spec:
                self._logger.error(f"BinanceFuture connector: Unknown symbol {symbol}")
                raise ValueError(f"BinanceFuture connector: Unknown symbol {symbol}")
            return BinanceConnectorFuture._instruments_spec[symbol]

    async def _update_candles(self):
        """
        Initialize candle tracking for all instruments.
        Creates WebSocket tasks for each symbol and timeframe if not already running.
        """
        for symbol in BinanceConnectorFuture._instruments_spec:
            if symbol not in BinanceConnectorFuture._candles:
                BinanceConnectorFuture._candles[symbol] = {tf: None for tf in self.tf_map}
                BinanceConnectorFuture._candles_polling_tasks[symbol] = {tf: None for tf in self.tf_map}
                BinanceConnectorFuture._candles_ws_last_response_time[f"{symbol}_{tf.name}"] = None
            for tf in self.tf_map:
                if not BinanceConnectorFuture._candles_polling_tasks[symbol].get(tf):
                    BinanceConnectorFuture._candles_polling_tasks[symbol][tf] = asyncio.create_task(
                        self._process_instrument_candles_ws(symbol, tf),
                        name=f"Task_BinanceConnectorFuture_candles_{symbol}_{tf.name}"
                    )

    def _get_start_utc_date_by_candles_number_and_timeframe(self, timeframe: Timeframe, n: int) -> datetime:
        """
        Calculate the start date for fetching historical candles based on timeframe and number of candles.

        Args:
            timeframe (Timeframe): The timeframe for candles (e.g., M1, H1).
            n (int): Number of candles to fetch.

        Returns:
            datetime: Start date in UTC.

        Raises:
            ValueError: If the timeframe is not supported.
        """
        time_deltas = {
            Timeframe.W1: timedelta(days=7 * n),
            Timeframe.D1: timedelta(days=n),
            Timeframe.H4: timedelta(hours=4 * n),
            Timeframe.H1: timedelta(hours=n),
            Timeframe.M30: timedelta(minutes=30 * n),
            Timeframe.M15: timedelta(minutes=15 * n),
            Timeframe.M5: timedelta(minutes=5 * n),
            Timeframe.M1: timedelta(minutes=n),
        }
        if timeframe not in time_deltas:
            self._logger.error(f"Timeframe {timeframe.name} is not available.")
            raise ValueError(f"Timeframe {timeframe.name} is not available.")
        return datetime.utcnow() - time_deltas[timeframe]

    def _ws_binance_tf_to_timeframe(self, ws_tf: str) -> Timeframe:
        """
        Convert Binance WebSocket timeframe to Timeframe enum.

        Args:
            ws_tf (str): Binance WebSocket timeframe (e.g., "1m", "5m").

        Returns:
            Timeframe: Corresponding Timeframe enum value.

        Raises:
            ValueError: If the timeframe is not supported.
        """
        tf_map = {
            "1m": Timeframe.M1,
            "5m": Timeframe.M5,
            "15m": Timeframe.M15,
            "30m": Timeframe.M30,
            "1h": Timeframe.H1,
            "4h": Timeframe.H4,
            "1d": Timeframe.D1,
            "1w": Timeframe.W1,
        }
        if ws_tf not in tf_map:
            self._logger.error(f"Websocket timeframe {ws_tf} is not available.")
            raise ValueError(f"Websocket timeframe {ws_tf} is not available.")
        return tf_map[ws_tf]

    async def _get_last_candles_rest_api(self, symbol: str, timeframe: Timeframe, n: int, date_start: Optional[datetime] = None) -> pd.DataFrame:
        """
        Fetch historical candles via Binance REST API.

        Args:
            symbol (str): Trading pair symbol (e.g., "BTCUSDT").
            timeframe (Timeframe): The timeframe for candles.
            n (int): Number of candles to fetch.
            date_start (datetime, optional): Start date for fetching candles.

        Returns:
            pd.DataFrame: DataFrame containing candles with columns Date, O, H, L, C, V.
        """
        while not self._client:
            self._logger.warning("BinanceFuture Client is not initialized yet. Waiting...")
            await asyncio.sleep(0.5)
        if date_start is None:
            date_start = self._get_start_utc_date_by_candles_number_and_timeframe(timeframe, n)
        try:
            klines = await self._client.get_historical_klines(
                symbol=symbol,
                interval=self.tf_map[timeframe],
                start_str=str(date_start),
                klines_type=HistoricalKlinesType.FUTURES
            )
            klines = np.array(klines).astype(float)
            candles = pd.DataFrame(klines[:, :6], columns=["Date", "O", "H", "L", "C", "V"])
            candles["Date"] = pd.to_datetime(candles["Date"], unit="ms")
            candles = candles.set_index("Date")
            candles = candles.apply(lambda x: Decimal(str(x)) if x.name != "Date" else x)  # Convert to Decimal
            self._logger.info(f"Received {candles.shape[0]} candles from {candles.index[0]} to {candles.index[-1]} "
                              f"via BinanceFuture REST API for {symbol}_{timeframe.name}.")
            return candles
        except Exception as e:
            self._logger.error(f"Failed to fetch candles via REST API for {symbol}_{timeframe.name}: {str(e)}\n{traceback.format_exc()}")
            raise

    async def _process_instrument_candles_ws(self, symbol: str, timeframe: Timeframe):
        """
        Process WebSocket updates for candlestick data.
        Updates the _candles cache and falls back to REST API if gaps are detected.

        Args:
            symbol (str): Trading pair symbol.
            timeframe (Timeframe): The timeframe for candles.
        """
        while True:
            try:
                async with websockets.connect(f"wss://fstream.binance.com/ws/{symbol.lower()}_perpetual@continuousKline_{self.tf_map[timeframe]}") as websocket:
                    self._logger.info(f"BinanceFuture WebSocket started for candlestick updates {symbol}_{timeframe.name}.")
                    is_open = False
                    while True:
                        res = await asyncio.wait_for(websocket.recv(), timeout=30)
                        res = json.loads(res)
                        ws_symbol = res["ps"]
                        ws_tf = res["k"]["i"]
                        ws_date = pd.to_datetime(res["k"]["t"], unit="ms")
                        o, h, l, c, v = map(Decimal, [res["k"]["o"], res["k"]["h"], res["k"]["l"], res["k"]["c"], res["k"]["v"]])
                        is_closed = res["k"]["x"]
                        # Update last response time
                        BinanceConnectorFuture._candles_ws_last_response_time[f"{symbol}_{timeframe.name}"] = datetime.utcnow()
                        # Validate symbol and timeframe
                        if symbol != ws_symbol or timeframe != self._ws_binance_tf_to_timeframe(ws_tf):
                            self._logger.error(f"BinanceFuture WebSocket mismatch: expected {symbol}_{timeframe}, got {ws_symbol}_{ws_tf}")
                            raise ValueError(f"WebSocket mismatch: expected {symbol}_{timeframe}, got {ws_symbol}_{ws_tf}")
                        async with self._candles_update_lock:
                            # Initialize candles if not present
                            if symbol not in BinanceConnectorFuture._candles or BinanceConnectorFuture._candles[symbol][timeframe] is None:
                                candles = await self._get_last_candles_rest_api(symbol, timeframe, self._CANDLES_DEPTH)
                                BinanceConnectorFuture._candles[symbol][timeframe] = candles
                            df = BinanceConnectorFuture._candles[symbol][timeframe]
                            if ws_date == df.index[-1]:
                                # Update existing candle
                                df.loc[ws_date, ["O", "H", "L", "C", "V"]] = [o, h, l, c, v]
                                if is_closed:
                                    is_open = True
                            else:
                                # Check for gaps or unclosed candles
                                tm_delta = timeframe_to_timedelta(timeframe)
                                if (ws_date - df.index[-1]) > tm_delta or not is_open:
                                    if (ws_date - df.index[-1]) > tm_delta:
                                        self._logger.warning(f"{symbol}_{timeframe.name}: Gap detected: ws_date({ws_date}) - "
                                                             f"last_candle({df.index[-1]}) > {tm_delta}")
                                    if not is_open:
                                        self._logger.warning(f"{symbol}_{timeframe.name}: Last candle was not closed.")
                                    candles = await self._get_last_candles_rest_api(symbol, timeframe, 0, date_start=df.index[-1])
                                    BinanceConnectorFuture._candles[symbol][timeframe] = pd.concat([df, candles]).reset_index().drop_duplicates(subset=["Date"], keep="last").set_index("Date")[-self._CANDLES_DEPTH:]
                                    is_open = False
                                    continue
                                # Add new closed candle
                                candle = pd.DataFrame({"Date": [ws_date], "O": [o], "H": [h], "L": [l], "C": [c], "V": [v]}).set_index("Date")
                                BinanceConnectorFuture._candles[symbol][timeframe] = pd.concat([df, candle])[-self._CANDLES_DEPTH:]
                                is_open = False
            except asyncio.TimeoutError:
                self._logger.error(f"BinanceFuture WebSocket timeout for {symbol}_{timeframe.name}. Reconnecting...")
                await asyncio.sleep(3)
            except Exception as e:
                self._logger.error(f"BinanceFuture WebSocket error for {symbol}_{timeframe.name}: {str(e)}\n{traceback.format_exc()}")
                await asyncio.sleep(3)

    async def get_last_candles(self, symbol: str, timeframe: Timeframe, n: int = _CANDLES_DEPTH):
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
                self._logger.warning(f"BinanceFuture connector: n={n} > MAX_CANDLES_DEPTH={self._CANDLES_DEPTH}")
            if symbol not in BinanceConnectorFuture._candles:
                BinanceConnectorFuture._candles[symbol] = {tf: None for tf in self.tf_map}
                BinanceConnectorFuture._candles_polling_tasks[symbol] = {tf: None for tf in self.tf_map}
                BinanceConnectorFuture._candles_ws_last_response_time[f"{symbol}_{tf.name}"] = None
                self._logger.info(f"BinanceFuture symbol {symbol} added to track candles.")
            if timeframe not in BinanceConnectorFuture._candles[symbol]:
                try:
                    BinanceConnectorFuture._candles[symbol][timeframe] = None
                    BinanceConnectorFuture._candles_polling_tasks[symbol][timeframe] = None
                    BinanceConnectorFuture._candles_ws_last_response_time[f"{symbol}_{timeframe.name}"] = None
                    self._logger.info(f"BinanceFuture symbol {symbol}_{timeframe.name} added to track candles.")
                    candles = await self._get_last_candles_rest_api(symbol, timeframe, self._CANDLES_DEPTH)
                    BinanceConnectorFuture._candles[symbol][timeframe] = candles
                    BinanceConnectorFuture._candles_polling_tasks[symbol][timeframe] = asyncio.create_task(
                        self._process_instrument_candles_ws(symbol, timeframe),
                        name=f"Task_BinanceConnectorFuture_candles_{sy
