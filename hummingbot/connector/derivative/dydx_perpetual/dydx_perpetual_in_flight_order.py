from decimal import Decimal
from typing import Dict, Any

from hummingbot.core.event.events import OrderType, TradeType
from hummingbot.connector.in_flight_order_base import InFlightOrderBase


class DydxPerpetualsInFlightOrder(InFlightOrderBase):
    def __init__(self,
                 client_order_id: str,
                 exchange_order_id: str,
                 trading_pair: str,
                 order_type: OrderType,
                 trade_type: TradeType,
                 price: Decimal,
                 amount: Decimal,
                 leverage: int,
                 position: str,
                 initial_state: str = "NEW"):
        super().__init__(
            client_order_id,
            exchange_order_id,
            trading_pair,
            order_type,
            trade_type,
            price,
            amount,
            initial_state
        )
        self.trade_id_set = set()
        self.leverage = leverage
        self.position = position


    @property
    def is_done(self):
        return self.last_state in {"FILLED", "CANCELED", "PENDING_CANCEL", "REJECTED", "EXPIRED"}

    @property
    def is_cancelled(self):
        return self.last_state == "CANCELED"

    @property
    def is_failure(self):
        return self.last_state in {"CANCELED", "PENDING_CANCEL", "REJECTED", "EXPIRED"}

    @classmethod
    def from_json(cls, data: Dict[str, Any]):
        return_val: DydxPerpetualsInFlightOrder = DydxPerpetualsInFlightOrder(
            client_order_id=data["client_order_id"],
            exchange_order_id=data["exchange_order_id"],
            trading_pair=data["trading_pair"],
            order_type=getattr(OrderType, data["order_type"]),
            trade_type=getattr(TradeType, data["trade_type"]),
            price=Decimal(data["price"]),
            amount=Decimal(data["amount"]),
            initial_state=data["last_state"]
        )
        return_val.executed_amount_base = Decimal(data["executed_amount_base"])
        return_val.executed_amount_quote = Decimal(data["executed_amount_quote"])
        return_val.fee_asset = data["fee_asset"]
        return_val.fee_paid = Decimal(data["fee_paid"])
        return return_val

    def update_with_trade_updates(self, trade_update: Dict[str, Any]):
        trade_id = trade_update.get("id")
        if str(trade_update.get("order_id")) != self.exchange_order_id or trade_id in self.trade_id_set:
            return
        self.trade_id_set.add(trade_id)
        self.executed_amount_base += Decimal(trade_update.get("qty"))
        self.executed_amount_quote += Decimal(trade_update.get("quoteQty"))
        self.fee_paid += Decimal(trade_update.get("commission"))
        if not self.fee_asset:
            self.fee_asset = trade_update.get("commissionAsset")
        return trade_update

    def update_with_execution_report(self, order: Dict[str, Any]):
        #self.trade_id_set.add(trade_id)  # trade_id not available for dydx
        executed_amount_base= Decimal(order['size']) - Decimal(order['remainingSize'])
        executed_amount_quote = executed_amount_base * Decimal(order['price'])
        self.executed_amount_base = executed_amount_base
        self.executed_amount_quote = executed_amount_quote
        self.fee_paid = executed_amount_quote * Decimal(order['trade_fee'])
        STATUS_MAP = {
            'OPEN':'OPEN',
            'ENTIRELY_FILLED':'FILLED',
            'CLOSED':'CANCELED',
            'FILLED':'FILLED',
            'CANCELED':'CANCELED',
            'UNTRIGGERED':'OPEN',
        }
        status = order['status']
        if status in STATUS_MAP:
            status = STATUS_MAP[status]
        self.last_state = status
