import asyncio
from decimal import Decimal
from src.connectors.exchanges.connector_okx_future import ConnectorOkxFuture
from src.entities.instrument import Instrument
from src.entities.timeframe import Timeframe
from src.entities.direction import Direction
from src.entities.order import OrderType
from datetime import datetime


async def main_async():
    # Replace with your OKX testnet API credentials
    connector_okx = ConnectorOkxFuture(config={
        'API_KEY': 'your_okx_testnet_api_key',
        'API_SEC': 'your_okx_testnet_api_secret',
        'PASSPHRASE': 'your_okx_testnet_passphrase',
        'TESTNET': True
    })

    try:
        # Wait for network initialization
        await asyncio.sleep(1)

        # 1. Get instrument spec 
         instrument: Instrument = await connector_okx.get_instrument_spec(symbol='BTC-USDT-SWAP')
         print('Instrument Spec:')
         print(instrument.__dict__)

        # 2. Get instrument candles 
         candles = await connector_okx.get_last_candles(symbol='BTC-USDT-SWAP', timeframe=Timeframe.M30, n=10)
         print(f"\nRecent Candles ({len(candles)} bars):")
         print(candles)

        # 3. Create limit sell order
        order = await connector_okx PenalCode
        order = await connector_okx.create_order(
            symbol='BTC-USDT-SWAP',
            side=Direction.SELL,
            order_type=OrderType.LIMIT,
            price=Decimal('99000'),
            qty=Decimal('0.0001')
        )
        print('Limit Sell Order:', order.id)

        # 4. Create limit buy order
        order = await connector_okx.create_order(
            symbol='BTC-USDT-SWAP',
            side=Direction.BUY,
            order_type=OrderType.LIMIT,
            price=Decimal('110000'),
            qty=Decimal('0.0001')
        )
        print('Limit Buy Order:', order.id)

        # 5. Get balance
        balance = await connector_okx.get_last_balance()
        print('Balance:')
        print({k: {'available': v.available, 'frozen': v.frozen, 'total': v.total} for k, v in balance.items()})

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
        await connector_okx._stop_network()


# Comprehensive testing loop
async def testing_loop():
    while True:
        try:
            print("=" * 80)
            print(f"=== TESTING LOOP @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ===")
            print("=" * 80)

            # Step 1: Fetch fresh balance and orders
            balance = await connector_okx.get_last_balance()
            orders = await connector_okx.get_last_open_orders(order=Order(symbol='BTC-USDT-SWAP'))

            print("\n>> Current Balance:")
            for asset, info in balance.items():
                print(f"   {asset}: available={info.available}, frozen={info.frozen}, total={info.total}")

            print("\n>> Current Open Orders:")
            if orders:
                for order_id, order_obj in orders.items():
                    print(
                        f"   {order_id} | {order_obj.side.name.upper()} {order_obj.volume} @ {order_obj.price} [{order_obj.status.name}]")
            else:
                print("   No open orders.")

            # Step 2: Create a new limit BUY order
            print("\n>> Creating a new LIMIT BUY order...")
            try:
                order = await connector_okx.create_limit_buy_order(
                    symbol="BTC-USDT-SWAP", 
                    price=Decimal('75000'), 
                    qty=Decimal('0.01')
                )
                print(f"    Successfully placed order: {order.id}")
            except Exception as e:
                print(f"    Failed to place order: {e}")
            await asyncio.sleep(1)  # Short wait for updates

            # Step 3: Fetch updated open orders
            updated_orders = await connector_okx.get_last_open_orders(order=Order(symbol="BTC-USDT-SWAP"))
            print("\n>> Updated Open Orders:")
            for order_id, order_obj in updated_orders.items():
                print(
                    f"   {order_id} | {order_obj.side.name.upper()} {order_obj.volume} @ {order_obj.price} [{order_obj.status.name}]")

            await asyncio.sleep(5)  # Give it time before canceling

            # Step 4: Cancel all active orders
            print("\n>> Cancelling all open orders...")
            for order_id in list(updated_orders.keys()):
                try:
                    await connector_okx.cancel_order(symbol="BTC-USDT-SWAP", order_id=order_id)
                    print(f"   Successfully cancelled: {order_id}")
                except Exception as cancel_err:
                    print(f"   Failed to cancel {order_id}: {cancel_err}")

            await asyncio.sleep(2)

            # Step 5: Final status
            final_balance = await connector_okx.get_last_balance()
            final_orders = await connector_okx.get_last_open_orders(order=Order(symbol="BTC-USDT-SWAP"))

            print("\n>> Final Balance:")
            for asset, info in final_balance.items():
                print(f"   {asset}: available={info.available}, frozen={info.frozen}, total={info.total}")

            print("\n>> Final Open Orders:")
            if final_orders:
                for order_id, order_obj in final_orders.items():
                    print(
                        f"   {order_id} | {order_obj.side.name.upper()} {order_obj.volume} @ {order_obj.price} [{order_obj.status.name}]")
            else:
                print("   No open orders.")

            print("\nLOOP COMPLETED. Sleeping 10 seconds before next round...")
            print("=" * 80 + "\n")
            await asyncio.sleep(10)

        except Exception as e:
            print(f"\nError in testing loop: {e}")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main_async())
