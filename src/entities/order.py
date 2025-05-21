from decimal import Decimal
from enum import Enum
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional

from .direction import Direction


class OrderType(Enum):
    """Enum representing the type of an order."""
    MARKET = 0
    LIMIT = 1
    STOP = 2


class OrderStatus(Enum):
    """Enum representing the status of an order."""
    PLACED = 0
    ACTIVE = 1
    EXECUTED = 2
    PARTIALLY_EXECUTED = 3
    CANCELED = 4
    EXPIRED = 5


@dataclass
class Order:
    """
    Represents a trading order.

    Attributes:
        id (Optional[str]): Order ID.
        position_id (str): Position ID.
        symbol (str): Trading pair symbol.
        order_type (Optional[OrderType]): Order type (MARKET, LIMIT, STOP).
        status (Optional[OrderStatus]): Order status.
        direction (Optional[Direction]): Order side (BUY, SELL).
        theor_price (Optional[Decimal]): Theoretical price.
        price (Optional[Decimal]): Order price.
        stop_price (Optional[Decimal]): Stop price for stop orders.
        volume (Optional[Decimal]): Order quantity.
        volume_executed (Decimal): Executed quantity.
        avg_price (Optional[Decimal]): Average execution price.
        commission (Decimal): Trading commission.
        datetime (Optional[datetime]): Order timestamp.
    """
    id: Optional[str]
    position_id: str
    symbol: str
    order_type: Optional[OrderType] = None
    status: Optional[OrderStatus] = None
    direction: Optional[Direction] = None
    theor_price: Optional[Decimal] = None
    price: Optional[Decimal] = None
    stop_price: Optional[Decimal] = None
    volume: Optional[Decimal] = None
    volume_executed: Decimal = Decimal("0")
    avg_price: Optional[Decimal] = None
    commission: Decimal = Decimal("0")
    datetime: Optional[datetime] = None

    def __post_init__(self):
        """
        Post-initialization logic to validate and enforce constraints on attributes.
        """
        self.validate_attributes()

    def validate_attributes(self):
        """
        Validate the order's attributes for logical consistency.
        """
        if self.price is not None and self.price < 0:
            raise ValueError("Price cannot be negative.")
        if self.volume is not None and self.volume < 0:
            raise ValueError("Volume cannot be negative.")
        if self.volume_executed < 0:
            raise ValueError("Executed volume cannot be negative.")
        if self.volume is not None and self.volume_executed > self.volume:
            raise ValueError("Executed volume cannot exceed total volume.")
        if self.commission < 0:
            raise ValueError("Commission cannot be negative.")

    def is_active(self) -> bool:
        """
        Check if the order is active.

        Returns:
            bool: True if the order status is ACTIVE or PARTIALLY_EXECUTED.
        """
        return self.status in {OrderStatus.ACTIVE, OrderStatus.PARTIALLY_EXECUTED}

    def is_completed(self) -> bool:
        """
        Check if the order is completed.

        Returns:
            bool: True if the order status is EXECUTED or CANCELED.
        """
        return self.status in {OrderStatus.EXECUTED, OrderStatus.CANCELED, OrderStatus.EXPIRED}

    def update_status(self, new_status: OrderStatus):
        """
        Update the order's status.

        Args:
            new_status (OrderStatus): The new status to set.
        """
        if not isinstance(new_status, OrderStatus):
            raise TypeError("new_status must be an instance of OrderStatus.")
        self.status = new_status

    def __repr__(self) -> str:
        dt_str = self.datetime.strftime('%Y-%m-%d %H:%M:%S') if self.datetime else 'None'
        return (
            f"Order(id={self.id}, symbol={self.symbol}, price={self.price}, volume={self.volume}, "
            f"executed={self.volume_executed}, status={self.status.name if self.status else None}, "
            f"type={self.order_type.name if self.order_type else None}, "
            f"direction={self.direction.name if self.direction else None}, time={dt_str})"
        )

    def get_dict_format(self) -> dict:
        """
        Convert the order to a dictionary.

        Returns:
            dict: Order data as a dictionary.
        """
        return {
            'id': self.id,
            'position_id': self.position_id,
            'symbol': self.symbol,
            'order_type': self.order_type.name if self.order_type else None,
            'status': self.status.name if self.status else None,
            'direction': self.direction.name if self.direction else None,
            'theor_price': self.theor_price,
            'price': self.price,
            'stop_price': self.stop_price,
            'volume': self.volume,
            'volume_executed': self.volume_executed,
            'avg_price': self.avg_price,
            'commission': self.commission,
            'datetime': self.datetime.isoformat() if self.datetime else None,
        }