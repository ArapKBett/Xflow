import asyncio
import json
import traceback
from datetime import datetime
from decimal import Decimal
from typing import Dict, Optional
from src.connectors.clients.okx_client import OKXAsyncClient
from src.entities.instrument import Instrument
from src.entities.order import Order, OrderType, OrderStatus
from src.entities.direction import Direction
from src.entities.balance import Balance
from src.logger.logger import get_logger
from ..connector_base import ConnectorBase, ConnectorType


class ConnectorOkxFuture(ConnectorBase):
    """
    OKX Futures connector implementing the ConnectorBase interface for futures trading.
    Handles instrument specifications, candles, orders, and balances using OKX's REST and WebSocket APIs.
    """

    connector_type: ConnectorType = ConnectorType.OkxFuture
    _logger = get_logger("ConnectorOkxFuture")

    def __init__(self, config: dict):
        """
        Initialize the OKX Futures connector.

        Args:
            config (dict): Configuration dictionary with API key, secret, passphrase, and testnet flag.
        """
        super().__init__(config)
        self._client = None
        self._api_key = config.get('API_KEY')
        self._api_secret = config.get('API_SEC')
        self._passphrase = config.get('PASSPHRASE')
        self._testnet = config.get('TESTNET', False)

    async def _initialize_client(self):
        """
        Initialize the OKXAsyncClient with API credentials.
        """
        if not self._client:
            self._client = await OKXAsyncClient.create(
                api_key=self._api_key,
                api_secret=self._api_secret,
                passphrase=self._passphrase,
                testnet=self._testnet
            )
            self._logger.info("OKXAsyncClient initialized.")

    async def create_order(self, symbol: str, side: Direction, order_type: OrderType,
                           price: Optional[Decimal] = None, qty: Optional[Decimal] = None,
                           stop_price: Optional[Decimal] = None) -> Order:
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
        await self._initialize_client()

        instrument = await self.get_instrument_spec(symbol)
        if not instrument:
            raise ValueError(f"Instrument {symbol} not found")

        # Validate quantity
        if qty:
            contracts = qty / instrument.contract_size
            if contracts % instrument.lot_step_market != 0:
                raise ValueError(f"Order quantity must be a multiple of lot size {instrument.lot_step_market}")
            qty_str = str(int(contracts))
        else:
            qty_str = None

        # Prepare order parameters
        params = {
            "instId": symbol,
            "tdMode": "cross",
            "side": side.value.lower(),
            "ordType": order_type.value,
            "sz": qty_str
        }

        # Handle limit orders
        if order_type == OrderType.LIMIT and price:
            params["px"] = str(price)

        # Handle stop orders
        elif order_type == OrderType.STOP and stop_price:
            params["ordType"] = "conditional"  # OKX uses "conditional" for stop orders
            params["tpTriggerPx"] = str(stop_price)
            params["tpOrdPx"] = "-1"  # Market order on trigger

        # Handle market orders
        elif order_type == OrderType.MARKET:
            params.pop("px", None)

        try:
            response = await self._client.create_order(**params)
            ord_id = response.get("data", [{}])[0].get("ordId")
            order_data = await self._client.get_order(symbol, ord_id)
            return self._parse_order_from_okx_data(order_data.get("data", [])[0])
        except Exception as e:
            self._logger.error(f"Failed to create order for {symbol}: {e}\n{traceback.format_exc()}")
            raise

    async def get_instrument_spec(self, symbol: str) -> Instrument:
        """
        Retrieve the instrument specification for a symbol.

        Args:
            symbol (str): Trading pair symbol (e.g., "BTC-USDT-SWAP").

        Returns:
            Instrument: The instrument specification.
        """
        await self._initialize_client()
        try:
            response = await self._client.get_instruments()
            for item in response.get("data", []):
                if item["instId"] == symbol:
                    return Instrument(
                        symbol=item["instId"],
                        contract_size=Decimal(item["ctVal"]),
                        tick_size=Decimal(item["tickSz"]),
                        min_qty_market=Decimal(item["minSz"]),
                        lot_step_market=Decimal(item["lotSz"])
                    )
        except Exception as e:
            self._logger.error(f"Failed to fetch instrument spec for {symbol}: {e}\n{traceback.format_exc()}")
            raise

    async def get_cur_price(self, symbol: str) -> Dict[str, Decimal]:
        """
        Fetch the current bid, ask, and last price for a symbol.

        Args:
            symbol (str): Trading pair symbol.

        Returns:
            dict: Dictionary with keys "bid", "ask", and "last" containing Decimal values.
        """
        await self._initialize_client()
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
        return Order(
            id=ord_id,
            symbol=order_data["instId"],
            price=Decimal(order_data.get("avgPx", "0")),
            theor_price=Decimal(order_data.get("px", "0")),
            volume=Decimal(order_data.get("sz", "0")),
            volume_executed=Decimal(order_data.get("accFillSz", "0")),
            commission=Decimal(order_data.get("fee", "0")),
            datetime=datetime.fromtimestamp(int(order_data.get("uTime", 0)) / 1000) if order_data.get("uTime") else None,
            side=Direction.BUY if order_data.get("side") == "buy" else Direction.SELL,
            order_type=OrderType(order_data.get("ordType")),
            status=OrderStatus(order_data.get("state"))
        )