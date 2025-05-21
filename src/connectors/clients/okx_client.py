import httpx
import websockets
import json
import asyncio
import time
import hmac
import hashlib
import base64
from typing import Optional, Dict, Any
from datetime import datetime
from websockets.legacy.client import WebSocketClientProtocol
from dataclasses import dataclass


@dataclass
class OKXClientConfig:
    client: httpx.AsyncClient
    ws_public: str
    ws_private: str
    ws_business: str
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    passphrase: Optional[str] = None
    testnet: bool = False


class OKXAsyncClient:
    """
    Asynchronous client for interacting with OKX API (REST and WebSocket).
    Supports fetching instruments, candles, balances, orders, and real-time updates.
    """

    def __init__(self, config: OKXClientConfig):
        """
        Initialize the OKX client with configuration.

        Args:
            config (OKXClientConfig): Configuration object with client, WebSocket URLs, and credentials.
        """
        self._client = config.client
        self._ws_public_url = config.ws_public
        self._ws_private_url = config.ws_private
        self._ws_business_url = config.ws_business
        self.api_key = config.api_key
        self.api_secret = config.api_secret
        self.passphrase = config.passphrase
        self.testnet = config.testnet

    @classmethod
    async def create(cls, api_key: Optional[str] = None, api_secret: Optional[str] = None,
                     passphrase: Optional[str] = None, testnet: bool = False) -> 'OKXAsyncClient':
        """
        Create an OKXAsyncClient instance with initialized HTTP client and WebSocket URLs.

        Args:
            api_key (Optional[str]): API key for authenticated requests.
            api_secret (Optional[str]): API secret for signing requests.
            passphrase (Optional[str]): Passphrase for API authentication.
            testnet (bool): Use testnet environment if True.

        Returns:
            OKXAsyncClient: Initialized client instance.
        """
        client = httpx.AsyncClient(base_url="https://www.okx.com", timeout=httpx.Timeout(10.0))
        base_ws = "wss://wspap.okx.com:8443" if testnet else "wss://ws.okx.com:8443"
        ws_public = f"{base_ws}/ws/v5/public"
        ws_private = f"{base_ws}/ws/v5/private"
        ws_business = f"{base_ws}/ws/v5/business"

        config = OKXClientConfig(
            client=client,
            ws_public=ws_public,
            ws_private=ws_private,
            ws_business=ws_business,
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase,
            testnet=testnet
        )

        return cls(config)

    def _signed_headers(self, method: str, path: str, params: Dict[str, Any] = None, body: str = "") -> Dict[str, str]:
        """
        Generate signed headers for authenticated API requests.

        Args:
            method (str): HTTP method (e.g., "GET", "POST").
            path (str): API endpoint path.
            params (Dict[str, Any], optional): Query parameters.
            body (str, optional): Request body as a string.

        Returns:
            Dict[str, str]: Headers including authentication details.
        """
        ts = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'
        request_path = path
        if method.upper() == "GET" and params:
            query_string = "&".join(f"{k}={v}" for k, v in sorted(params.items()))
            request_path += f"?{query_string}"

        prehash = f"{ts}{method.upper()}{request_path}{body}"
        signature = base64.b64encode(hmac.new(self.api_secret.encode(), prehash.encode(), hashlib.sha256).digest()).decode()

        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": ts,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }

        if self.testnet:
            headers["x-simulated-trading"] = "1"

        return headers

    async def get_instruments(self, inst_type: str = "SWAP") -> Dict[str, Any]:
        """
        Fetch instrument specifications.

        Args:
            inst_type (str): Instrument type ("SPOT", "SWAP", "FUTURES", "OPTION").

        Returns:
            Dict[str, Any]: JSON response containing instrument data.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        path = "/api/v5/public/instruments"
        params = {"instType": inst_type}
        response = await self._client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    async def get_candles(self, inst_id: str, bar: str = "1m", limit: int = 100, after: int = None) -> Dict[str, Any]:
        """
        Fetch historical candlestick data.

        Args:
            inst_id (str): Instrument ID (e.g., "BTC-USDT-SWAP").
            bar (str): Timeframe (e.g., "1m", "5m", "1H").
            limit (int): Number of candles to fetch (max 300).
            after (int, optional): Fetch candles before this timestamp (ms).

        Returns:
            Dict[str, Any]: JSON response containing candlestick data.

        Raises:
            httpx.HTTPStatusError: If the request fails.
        """
        path = "/api/v5/market/candles"
        params = {
            "instId": inst_id,
            "bar": bar,
            "limit": str(limit),
        }
        if after is not None:
            params["after"] = str(after)

        response = await self._client.get(path, params=params)
        response.raise_for_status()
        return response.json()

    async def close(self) -> None:
        """
        Close the HTTP client.
        """
        await self._client.aclose()