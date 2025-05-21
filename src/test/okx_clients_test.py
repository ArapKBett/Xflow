import asyncio
from decimal import Decimal
from src.connectors.clients.okx_client import OKXAsyncClient
from src.connectors.connector_base import Direction

async def main():
    client = await OKXAsyncClient.create(
        api_key='your_okx_testnet_api_key',
        api_secret='your_okx_testnet_api_secret',
        passphrase='your_okx_testnet_passphrase',
        testnet=True
    )

    try:
        # Test fetching instruments
        instruments = await client.get_instruments("SWAP")
        print("Instrument Example:", instruments["data"][0])

        # Test fetching candles
        candles = await client.get_candles("BTC-USDT-SWAP", bar="1m", limit=5)
        print("Candles:", candles["data"])

        # Test fetching ticker
        ticker = await client.get_ticker("BTC-USDT-SWAP")
        print("Ticker:", ticker)

        # Test fetching balance
        balance = await client.get_balance()
        print("Balance:", balance)

        # Test creating a market order
        order = await client.create_market_order("BTC-USDT-SWAP", Direction.BUY, Decimal("0.001"))
        print("Market Order:", order)

        # Test creating a limit order
        limit_order = await client.create_limit_order("BTC-USDT-SWAP", Direction.SELL, Decimal("30000"), Decimal("0.001"))
        print("Limit Order:", limit_order)

    except Exception as e:
        print(f"Error in test: {e}")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())