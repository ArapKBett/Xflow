import asyncio
from src.connectors.clients.okx_client import OKXAsyncClient


async def main():
    # Replace with your OKX testnet API credentials
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
        print("Candles:")
        for candle in candles["data"]:
            print(candle)

        # Test fetching ticker (uncomment to test)
        # ticker = await client.get_ticker("BTC-USDT-SWAP")
        # print("Ticker:", ticker)

        # Test fetching balance (requires authentication, uncomment to test)
        # balance = await client.get_balance()
        # print("Balance:", balance)

    except Exception as e:
        print(f"Error in test: {e}")
        raise
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
