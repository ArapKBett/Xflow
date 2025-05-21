# XFlow

Connector library for trading Futures via WebSocket + REST.

---

## Test
For a simple test and usage example, run:

```bash
python src/okx_connector_main.py
```

***For more detailed usuage patterns and examples, refer directly to***
```bash
src/okx_connector_main.py
```

## Sample Usage
```python
import asyncio
from src.connectors.exchanges.connector_okx_future import ConnectorOkxFuture

async def main():
    connector_okx = ConnectorOkxFuture(config={
        'API_KEY': 'your_api_key',
        'API_SEC': 'your_api_secret',
        'PASSPHRASE': 'your_passphrase',
        'TESTNET': True  # Use False for production
    })

    # Example: Create a limit buy order
    await connector_okx.create_limit_buy_order(
        symbol="BTCUSDT",
        price=30000,
        qty=0.01
    )

    # Example: Cancel an order
    await connector_okx.cancel_order(
        symbol="BTCUSDT",
        order_id="your_order_id"
    )

    # Example: Fetch latest open orders
    open_orders = await connector_okx.get_last_open_orders(symbol="BTCUSDT")
    print(open_orders)

    # Example: Fetch latest balance
    balance = await connector_okx.get_last_balance()
    print(balance)

asyncio.run(main())
```
