import asyncio
from decimal import Decimal
from src.connectors.exchanges.connector_binance_future import ConnectorBinanceFuture
from src.entities.instrument import Instrument
from src.entities.timeframe import Timeframe
from src.entities.direction import Direction
from src.entities.order import OrderType


async def main_async():
    # Replace with your Binance testnet API credentials
    connector_binance = ConnectorBinanceFuture(config={
        'API_KEY': 'your_binance_testnet_api_key',
        'API_SECRET': 'your_binance_testnet_api_secret',
        'TESTNET': True
    })
    
    try:
        # Wait for network initialization
        await asyncio.sleep(1)

        # 1. Get instrument spec (uncomment to test)
        # instrument: Instrument = await connector_binance.get_instrument_spec(symbol='ETHUSDT')
        # print('Instrument Spec:')
        # print(instrument.__dict__)

        # 2. Get instrument candles
        candles = await connector_binance.get_last_candles(symbol='ETHUSDT',
                                                          timeframe=Timeframe.M1,
                                                          n=10)
        print('Candles:')
        print(candles)

        # 3. Test order creation (uncomment to test)
        # order = await connector_binance.create_limit_buy_order(
        #     symbol='ETHUSDT',
        #     price=Decimal('3000'),
        #     qty=Decimal('0.01')
        # )
        # print('Created Order:', order.id)

        # 4. Test balance fetching (uncomment to test)
        # balance = await connector_binance.get_last_balance()
        # print('Balance:', {k: v.__dict__ for k, v in balance.items()})

        # List background tasks
        all_tasks = asyncio.all_tasks()
        all_tasks.remove(asyncio.current_task())
        print('Background Tasks:')
        for task in all_tasks:
            print(task)

        # Wait for tasks to complete
        await asyncio.wait(all_tasks)

    except Exception as e:
        print(f"Error in main_async: {e}")
        raise
    finally:
        # Clean up network connections
        await connector_binance._stop_network()


if __name__ == "__main__":
    asyncio.run(main_async())
