from collections import defaultdict
from enum import Enum
from os.path import join, realpath
import sys; sys.path.insert(0, realpath(join("/home/ubuntu/hummingbot/hummingbot/connector/derivative/dydx_perpetual/dydx_perpetual_derivative.py", "../../../")))

from hummingbot.connector.derivative.position import Position

from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.core.utils.estimate_fee import estimate_fee

from async_timeout import timeout

from hummingbot.core.clock import Clock
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.connector.derivative.dydx_perpetual.dydx_perpetual_in_flight_order import DydxPerpetualsInFlightOrder

from hummingbot.core.utils.tracking_nonce import get_tracking_nonce

import asyncio
import hashlib
import hmac
import time
import logging
from decimal import Decimal
from typing import Optional, List, Dict, Any, AsyncIterable
from urllib.parse import urlencode

import aiohttp

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.event.events import (
    OrderType,
    TradeType,
    MarketOrderFailureEvent,
    MarketEvent,
    OrderCancelledEvent,
    BuyOrderCompletedEvent,
    BuyOrderCreatedEvent,
    SellOrderCreatedEvent,
    OrderFilledEvent,
    SellOrderCompletedEvent, PositionSide, PositionMode, PositionAction)
from hummingbot.core.utils.async_utils import safe_ensure_future, safe_gather
from hummingbot.core.utils.asyncio_throttle import Throttler
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.derivative.dydx_perpetual.dydx_perpetual_order_book_tracker import DydxPerpetualOrderBookTracker
from hummingbot.connector.derivative.dydx_perpetual.dydx_perpetual_user_stream_tracker import DydxPerpetualUserStreamTracker
# from hummingbot.connector.derivative.dydx_perpetual.dydx_perpetual_utils import convert_from_exchange_trading_pair, convert_to_exchange_trading_pair
from hummingbot.connector.derivative.dydx_perpetual.constants import (
    PERPETUAL_BASE_URL,
    TESTNET_BASE_URL,
    DIFF_STREAM_URL,
    TESTNET_STREAM_URL
)
from hummingbot.connector.derivative_base import DerivativeBase, s_decimal_NaN
from hummingbot.connector.trading_rule import TradingRule


from dydx3 import Client
import configparser
from dydx3.constants import NETWORK_ID_MAINNET, NETWORK_ID_ROPSTEN
import json
from datetime import datetime
from hummingbot.core.event.events import TradeFee, TradeFeeType
from hummingbot.core.clock_mode import ClockMode


class MethodType(Enum):
    GET = "GET"
    POST = "POST"
    DELETE = "DELETE"
    PUT = "PUT"


bpm_logger = None


BROKER_ID = "x-3QreWesy"


def get_client_order_id(order_side: str, trading_pair: object):
    nonce = get_tracking_nonce()
    symbols: str = trading_pair.split("-")
    base: str = symbols[0].upper()
    quote: str = symbols[1].upper()
    return f"{BROKER_ID}-{order_side.upper()[0]}{base[0]}{base[-1]}{quote[0]}{quote[-1]}{nonce}"


class DydxPerpetualDerivative(DerivativeBase):
    MARKET_RECEIVED_ASSET_EVENT_TAG = MarketEvent.ReceivedAsset
    MARKET_BUY_ORDER_COMPLETED_EVENT_TAG = MarketEvent.BuyOrderCompleted
    MARKET_SELL_ORDER_COMPLETED_EVENT_TAG = MarketEvent.SellOrderCompleted
    MARKET_ORDER_CANCELLED_EVENT_TAG = MarketEvent.OrderCancelled
    MARKET_TRANSACTION_FAILURE_EVENT_TAG = MarketEvent.TransactionFailure
    MARKET_ORDER_FAILURE_EVENT_TAG = MarketEvent.OrderFailure
    MARKET_ORDER_FILLED_EVENT_TAG = MarketEvent.OrderFilled
    MARKET_BUY_ORDER_CREATED_EVENT_TAG = MarketEvent.BuyOrderCreated
    MARKET_SELL_ORDER_CREATED_EVENT_TAG = MarketEvent.SellOrderCreated

    API_CALL_TIMEOUT = 10.0
    SHORT_POLL_INTERVAL = 5.0
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0
    LONG_POLL_INTERVAL = 120.0
    ORDER_NOT_EXIST_CONFIRMATION_COUNT = 3

    @classmethod
    def logger(cls) -> HummingbotLogger:
        global bpm_logger
        if bpm_logger is None:
            bpm_logger = logging.getLogger(__name__)
        return bpm_logger

    def __init__(self,
                 dydx_perpetual_api_key: str = None,
                 dydx_perpetual_api_secret: str = None,
                 dydx_perpetual_api_passphrase: str = None,
                 dydx_perpetual_eth_wallet: str = None,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 **domain):
#         self._testnet = True if len(domain) > 0 else False
        self._testnet = True
        super().__init__()
        self._api_key = dydx_perpetual_api_key
        self._api_secret = dydx_perpetual_api_secret
        
        # dydx_perpetual authentication credentials are obtained from config file rather than input variables due to unconventional credentials different from centralized exchanges. 
        self._eth_address = dydx_perpetual_eth_wallet
        self._api_key_credentials = {'secret': dydx_perpetual_api_secret, 'key': dydx_perpetual_api_key, 'passphrase': dydx_perpetual_api_passphrase}
        
        self._trading_required = trading_required
        self._account_balances = {}
        self._account_available_balances = {}

        self._base_url = PERPETUAL_BASE_URL if self._testnet is False else TESTNET_BASE_URL
        self._stream_url = DIFF_STREAM_URL if self._testnet is False else TESTNET_STREAM_URL
        self._network_id = NETWORK_ID_MAINNET if self._testnet is False else NETWORK_ID_ROPSTEN
        
        self._user_stream_tracker = DydxPerpetualUserStreamTracker(
            base_url=self._base_url, 
            stream_url=self._stream_url, 
            network_id=self._network_id, 
            eth_address=self._eth_address, 
            api_key_credentials=self._api_key_credentials
        )
        print(domain)
        domain = domain['domain']
        self._order_book_tracker = DydxPerpetualOrderBookTracker(trading_pairs=trading_pairs, domain=domain)
        self._ev_loop = asyncio.get_event_loop()
        self._poll_notifier = asyncio.Event()
        self._in_flight_orders = {}
        self._order_not_found_records = {}
        self._last_timestamp = 0
        self._trading_rules = {}
        # self._trade_fees = {} # To be implemented or removed
        # self._last_update_trade_fees_timestamp = 0 # To be implemented or removed
        self._status_polling_task = None
        self._user_stream_event_listener_task = None
        self._trading_rules_polling_task = None
        self._last_poll_timestamp = 0
        self._throttler = Throttler((10.0, 1.0))
        self._funding_rate = 0
        self._account_positions = {}
        self._position_mode = None
        self._leverage = 1

    @property
    def name(self) -> str:
        return "dydx_perpetual_testnet" if self._testnet else "dydx_perpetual"

    @property
    def order_books(self) -> Dict[str, OrderBook]:
        return self._order_book_tracker.order_books

    @property
    def ready(self):
        return all(self.status_dict.values())

    @property
    def in_flight_orders(self) -> Dict[str, DydxPerpetualsInFlightOrder]:
        return self._in_flight_orders

    @property
    def status_dict(self):
        return {
            "order_books_initialized": self._order_book_tracker.ready,
            "account_balance": len(self._account_balances) > 0 if self._trading_required else True,
            "trading_rule_initialized": len(self._trading_rules) > 0,

            # TODO: Uncomment when figured out trade fees
            # "trade_fees_initialized": len(self._trade_fees) > 0
        }

    @property
    def limit_orders(self):
        return [in_flight_order.to_limit_order() for in_flight_order in self._in_flight_orders.values()]

    def start(self, clock: Clock, timestamp: float):
        super().start(clock, timestamp)

    def stop(self, clock: Clock):
        super().stop(clock)

    async def start_network(self):
        self._order_book_tracker.start()
        self._trading_rules_polling_task = safe_ensure_future(self._trading_rules_polling_loop())
        if self._trading_required:
            self._status_polling_task = safe_ensure_future(self._status_polling_loop())
            self._user_stream_tracker_task = safe_ensure_future(self._user_stream_tracker.start())
            self._user_stream_event_listener_task = safe_ensure_future(self._user_stream_event_listener())

    def _stop_network(self):
        
        self._order_book_tracker.stop()
        if self._status_polling_task is not None:
            self._status_polling_task.cancel()
        if self._user_stream_tracker_task is not None:
            self._user_stream_tracker_task.cancel()
        if self._user_stream_event_listener_task is not None:
            self._user_stream_event_listener_task.cancel()
        if self._trading_rules_polling_task is not None:
            self._trading_rules_polling_task.cancel()
        self._status_polling_task = self._user_stream_tracker_task = \
            self._user_stream_event_listener_task = None

    async def stop_network(self):
        self._stop_network()
    
    async def check_network(self) -> NetworkStatus:
        try:
            client = Client(host=self._base_url,)
            client.public.get_time()
        except asyncio.CancelledError:
            raise
        except Exception:
            return NetworkStatus.NOT_CONNECTED
        return NetworkStatus.CONNECTED

    def supported_order_types(self) -> List[OrderType]:
        """
        :return a list of OrderType supported by this connector.
        Note that Market order type is no longer required and will not be used.
        """
#         return [OrderType.LIMIT, OrderType.MARKET]
        return [OrderType.LIMIT]

    # ORDER PLACE AND CANCEL EXECUTIONS ---
    async def create_order(self,
                           trade_type: TradeType,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           order_type: OrderType,
                           position_action: PositionAction,
                           price: Optional[Decimal] = Decimal("NaN")):

        trading_rule: TradingRule = self._trading_rules[trading_pair]
        if position_action not in [PositionAction.OPEN, PositionAction.CLOSE]:
            raise ValueError("Specify either OPEN_POSITION or CLOSE_POSITION position_action.")

        amount = self.quantize_order_amount(trading_pair, amount)
        price = self.quantize_order_price(trading_pair, price)

        if amount < trading_rule.min_order_size:
            raise ValueError(f"Buy order amount {amount} is lower than the minimum order size "
                             f"{trading_rule.min_order_size}")

        order_result = None
        api_params = {"symbol": trading_pair,
                      "side": "BUY" if trade_type is TradeType.BUY else "SELL",
                      "type": "LIMIT" if order_type is OrderType.LIMIT else "MARKET",
                      "quantity": f"{amount}",
                      "newClientOrderId": order_id
                      }
        if order_type == OrderType.LIMIT:
            api_params["price"] = f"{price}"
            api_params["timeInForce"] = "GTC"

        if self._position_mode == PositionMode.HEDGE:
            if position_action == PositionAction.OPEN:
                api_params["positionSide"] = "LONG" if trade_type is TradeType.BUY else "SHORT"
            else:
                api_params["positionSide"] = "SHORT" if trade_type is TradeType.BUY else "LONG"

        self.start_tracking_order(order_id, "", trading_pair, trade_type, price, amount, order_type, self._leverage, position_action.name)
        
        # To be updated!!! (Priority)
        try:
            client = Client(
                network_id=self._network_id,
                host=self._base_url,
                default_ethereum_address = self._eth_address,
                api_key_credentials=self._api_key_credentials,
            )
            account_response = client.private.get_account()
            position_id = account_response['account']['positionId']

            # Post an bid at a price that is unlikely to match.
            order_params = {
                'position_id': position_id,
                'market': api_params['symbol'],
                'side': api_params['side'],
                'order_type': api_params['type'],
                'post_only': False,
                'size': api_params['quantity'],
                'price': api_params['price'],
                'limit_fee': '0.0015',
                'expiration_epoch_seconds': time.time() + 99999999,
            }
            result_raw = client.private.create_order(**order_params)['order']
            exchange_order_id = str(result_raw["id"])
            
            # Check if order is filled immediately
            time.sleep(0.05)
            result_fill = client.private.get_fills(**{'order_id':exchange_order_id})['fills']
            avgPrice = "0.00000"
            executedQty = "0.00000"
            if result_fill and len(result_fill) > 0:
                avgPrice = result_fill[0]['price']
                executedQty = result_fill[0]['size']
            
            # Reformat order_result
            order_result = {
                'orderId': exchange_order_id, 
                'symbol': result_raw['market'], 
                'status': 'NEW', 
                'clientOrderId': order_id, 
                'price': result_raw['price'], 
                'avgPrice': avgPrice, 
                'origQty': result_raw['size'], 
                'executedQty': executedQty,
                'cumQty': '0', 
                'cumQuote': '0', 
                'timeInForce': 'GTC', 
                'type': result_raw['type'], 
                'reduceOnly': False, 
                'closePosition': False, 
                'side': result_raw['side'], 
                'positionSide': 'BOTH', 
                'stopPrice': '0', 
                'workingType': 'CONTRACT_PRICE', 
                'priceProtect': False, 
                'origType': 'LIMIT', 
                'updateTime': time.time()
            }
            
            tracked_order = self._in_flight_orders.get(order_id)
            if tracked_order is not None:
                self.logger().info(f"Created {order_type.name.lower()} {trade_type.name.lower()} order {order_id} for "
                                   f"{amount} {trading_pair}.")
                tracked_order.exchange_order_id = exchange_order_id

            event_tag = self.MARKET_BUY_ORDER_CREATED_EVENT_TAG if trade_type is TradeType.BUY \
                else self.MARKET_SELL_ORDER_CREATED_EVENT_TAG
            event_class = BuyOrderCreatedEvent if trade_type is TradeType.BUY else SellOrderCreatedEvent
            self.trigger_event(event_tag,
                               event_class(self.current_timestamp,
                                           order_type,
                                           trading_pair,
                                           amount,
                                           price,
                                           order_id,
                                           leverage=self._leverage,
                                           position=position_action.name))
            return order_result
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.stop_tracking_order(order_id)
            self.logger().network(
                f"Error submitting order to Binance Perpetuals for {amount} {trading_pair} "
                f"{'' if order_type is OrderType.MARKET else price}.",
                exc_info=True,
                app_warning_msg=str(e)
            )
            self.trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                               MarketOrderFailureEvent(self.current_timestamp, order_id, order_type))

    async def execute_buy(self,
                          order_id: str,
                          trading_pair: str,
                          amount: Decimal,
                          order_type: OrderType,
                          position_action: PositionAction,
                          price: Optional[Decimal] = s_decimal_NaN):
        return await self.create_order(TradeType.BUY, order_id, trading_pair, amount, order_type, position_action, price)

    def buy(self, trading_pair: str, amount: object, order_type: object = OrderType.MARKET,
            price: object = s_decimal_NaN, **kwargs) -> str:

        t_pair: str = trading_pair
        order_id: str = get_client_order_id("sell", t_pair)
        safe_ensure_future(self.execute_buy(order_id, trading_pair, amount, order_type, kwargs["position_action"], price))
        return order_id

    async def execute_sell(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           order_type: OrderType,
                           position_action: PositionAction,
                           price: Optional[Decimal] = s_decimal_NaN):
        return await self.create_order(TradeType.SELL, order_id, trading_pair, amount, order_type, position_action, price)

    def sell(self, trading_pair: str, amount: object, order_type: object = OrderType.MARKET,
             price: object = s_decimal_NaN, **kwargs) -> str:

        t_pair: str = trading_pair
        order_id: str = get_client_order_id("sell", t_pair)
        safe_ensure_future(self.execute_sell(order_id, trading_pair, amount, order_type, kwargs["position_action"], price))
        return order_id

    async def cancel_all(self, timeout_seconds: float):
        incomplete_orders = [order for order in self._in_flight_orders.values() if not order.is_done]
        tasks = [self.execute_cancel(order.trading_pair, order.client_order_id) for order in incomplete_orders]
        order_id_set = set([order.client_order_id for order in incomplete_orders])
        successful_cancellations = []

        try:
            async with timeout(timeout_seconds):
                cancellation_results = await safe_gather(*tasks, return_exceptions=True)
                for cancel_result in cancellation_results:
                    # TODO: QUESTION --- SHOULD I CHECK FOR THE BinanceAPIException CONSIDERING WE ARE MOVING AWAY FROM BINANCE-CLIENT?
                    if isinstance(cancel_result, dict) and "clientOrderId" in cancel_result:
                        client_order_id = cancel_result.get("clientOrderId")
                        order_id_set.remove(client_order_id)
                        successful_cancellations.append(CancellationResult(client_order_id, True))
        except Exception:
            self.logger().network(
                "Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel order with Binance Perpetual. Check API key and network connection."
            )
        failed_cancellations = [CancellationResult(order_id, False) for order_id in order_id_set]
        return successful_cancellations + failed_cancellations

    async def cancel_all_account_orders(self, trading_pair: str):
        try:
            client = Client(
                network_id=self._network_id,
                host=self._base_url,
                default_ethereum_address = self._eth_address,
                api_key_credentials=self._api_key_credentials,
            )
            response = client.private.cancel_all_orders(market=trading_pair)
            if response['cancelOrders'] and len(response['cancelOrders']) >= 0:
                for order_id in list(self._in_flight_orders.keys()):
                    self.stop_tracking_order(order_id)
            else:
                raise IOError(f"Error cancelling all account orders. Server Response: {response}")
        except Exception as e:
            self.logger().error("Could not cancel all account orders.")
            raise e

    def cancel(self, trading_pair: str, client_order_id: str):
        safe_ensure_future(self.execute_cancel(trading_pair, client_order_id))
        return client_order_id
    
    
    async def execute_cancel(self, trading_pair: str, client_order_id: str):
        try:
            # Find exchange_order_id using client_order_id
            open_orders = [order for order in self._in_flight_orders.values() if not order.is_done]
            order_id_set = {order.client_order_id:order.exchange_order_id for order in open_orders}
            print(order_id_set)
            exchange_order_id = order_id_set[client_order_id]
            
            client = Client(
                network_id=self._network_id,
                host=self._base_url,
                default_ethereum_address = self._eth_address,
                api_key_credentials=self._api_key_credentials,
            )
            response = client.private.cancel_order(order_id)
            
            if response['cancelOrder']['status']=='OPEN':
                self.logger().info(f"Successfully canceled order {client_order_id}")
                self.stop_tracking_order(client_order_id)
                self.trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                   OrderCancelledEvent(self.current_timestamp, client_order_id))
                return response
                
        except Exception as e:
            try:
                error_response = json.loads(e.response.text)
                error_msg = error_response['errors'][0]['msg']
                if 'it is already canceled' in error_msg or 'No order exists with id' in error_msg:
                    self.logger().debug(f"The order {client_order_id} does not exist on Binance Perpetuals. "
                                        f"No cancellation needed.")
                    self.stop_tracking_order(client_order_id)
                    self.trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                       OrderCancelledEvent(self.current_timestamp, client_order_id))
                    return {"origClientOrderId": client_order_id}
            except:
                pass
            self.logger().error(f"Could not cancel order {client_order_id} (on Binance Perp. {trading_pair})")
            raise e
        return response

    # TODO: Implement
    async def close_position(self, trading_pair: str):
        pass

    def quantize_order_amount(self, trading_pair: str, amount: object, price: object = Decimal(0)):
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        # current_price: object = self.get_price(trading_pair, False)
        notional_size: object
        quantized_amount = DerivativeBase.quantize_order_amount(self, trading_pair, amount)
        if quantized_amount < trading_rule.min_order_size:
            return Decimal(0)
        """
        if price == Decimal(0):
            notional_size = current_price * quantized_amount
        else:
            notional_size = price * quantized_amount
        """

        # TODO: NOTIONAL MIN SIZE DOES NOT EXIST
        # if notional_size < trading_rule.min_notional_size * Decimal("1.01"):
        #     return Decimal(0)

        return quantized_amount

    def get_order_price_quantum(self, trading_pair: str, price: object):
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return trading_rule.min_price_increment

    def get_order_size_quantum(self, trading_pair: str, order_size: object):
        trading_rule: TradingRule = self._trading_rules[trading_pair]
        return Decimal(trading_rule.min_base_amount_increment)

    # ORDER TRACKING ---
    def start_tracking_order(self, order_id: str, exchange_order_id: str, trading_pair: str, trading_type: object,
                             price: object, amount: object, order_type: object, leverage: int, position: str):
        self._in_flight_orders[order_id] = DydxPerpetualsInFlightOrder(
            client_order_id=order_id,
            exchange_order_id=exchange_order_id,
            trading_pair=trading_pair,
            order_type=order_type,
            trade_type=trading_type,
            price=price,
            amount=amount,
            leverage=leverage,
            position=position

        )

    def stop_tracking_order(self, order_id: str):
        if order_id in self._in_flight_orders:
            del self._in_flight_orders[order_id]
        if order_id in self._order_not_found_records:
            del self._order_not_found_records[order_id]
    
    # Delete this??
    async def _iter_user_event_queue(self) -> AsyncIterable[Dict[str, any]]:
        while True:
            try:
#                 yield await self._user_stream_tracker.start()
#                 yield await self._user_stream_tracker.user_stream
                yield await self._user_stream_tracker.user_stream.get()
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network(
                    "Unknown error. Retrying after 1 seconds.",
                    exc_info=True,
                    app_warning_msg="Could not fetch user events from Binance. Check API key and network connection."
                )
                await asyncio.sleep(1.0)
    # This function: 
    # Get order trade update. if order has been filled or If order has been cancelled
    # ACCOUNT_UPDATE, # Update balance # update position 
    # MARGIN_CALL
    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                if event_message['type'] != 'subscribed':
                    continue
                # If user stream event is an order update
                if 'orders' in event_message['contents']:
                    orders = event_message['contents']['orders']
                    
                    # For each order
                    for order in orders:
                        exchange_order_id = order['id']
                        
                        # Get open orders
                        open_orders = [order for order in self._in_flight_orders.values() if not order.is_done]
                        order_id_set = {order.exchange_order_id:order.client_order_id for order in open_orders}
                        
                        # If the order has been cancelled
                        if exchange_order_id not in order_id_set:
                            continue
                        
                        # Find client_order_id using exchange_order_id
                        client_order_id = order_id_set[exchange_order_id]
                        
                        # Update _in_flight_orders
                        tracked_order = self._in_flight_orders.get(client_order_id)
                        trade_fee = self.get_fee(
                            base_currency=order['market'].split('-')[0],
                            quote_currency=order['market'].split('-')[1],
                            order_type=order['type'],
                            order_side=order['side'],
                            amount=Decimal(order['size']),
                            price=Decimal(order['price'])
                        )
                        order['trade_fee'] = trade_fee
                        tracked_order.update_with_execution_report(order)

                        # Send trigger_event for order fill event
                        trade_type = TradeType.BUY if order['side'] == "BUY" else TradeType.SELL
                        if order['status'] in ["ENTIRELY_FILLED", "FILLED", "CLOSED"]:
                            order_filled_event = OrderFilledEvent(
                                timestamp=int(time.time()),
                                order_id=client_order_id,
                                trading_pair=order['market'],
                                trade_type=trade_type,
                                order_type = OrderType.LIMIT if order['type'] == "LIMIT" else OrderType.MARKET,
                                price=Decimal(order['price']),
                                amount=Decimal(order['size']),
                                leverage=self._leverage,
                                trade_fee=trade_fee,
                                exchange_trade_id=order_message.get("t"),
                                position=tracked_order.position
                            )
                            self.trigger_event(self.MARKET_ORDER_FILLED_EVENT_TAG, order_filled_event)
                        
                        # Send trigger_event if order is done
                        if tracked_order.is_done:
                            if not tracked_order.is_failure:
                                event_tag = None
                                event_class = None
                                if trade_type is TradeType.BUY:
                                    event_tag = self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG
                                    event_class = BuyOrderCompletedEvent
                                else:
                                    event_tag = self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG
                                    event_class = SellOrderCompletedEvent
                                self.logger().info(f"The {tracked_order.order_type.name.lower()} {trade_type} order {client_order_id} has completed "
                                                   f"according to websocket delta.")
                                self.trigger_event(event_tag,
                                                   event_class(self.current_timestamp,
                                                               client_order_id,
                                                               tracked_order.base_asset,
                                                               tracked_order.quote_asset,
                                                               (tracked_order.fee_asset or tracked_order.quote_asset),
                                                               tracked_order.executed_amount_base,
                                                               tracked_order.executed_amount_quote,
                                                               tracked_order.fee_paid,
                                                               tracked_order.order_type))
                            else:
                                if tracked_order.is_cancelled:
                                    if tracked_order.client_order_id in self._in_flight_orders:
                                        self.logger().info(f"Successfully cancelled order {tracked_order.client_order_id} according to websocket delta.")
                                        self.trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                                           OrderCancelledEvent(self.current_timestamp,
                                                                               tracked_order.client_order_id))
                                    else:
                                        self.logger().info(f"The {tracked_order.order_type.name.lower()} order {tracked_order.client_order_id} has failed "
                                                           f"according to websocket delta.")
                                        self.trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                                           MarketOrderFailureEvent(self.current_timestamp,
                                                                                   tracked_order.client_order_id,
                                                                                   tracked_order.order_type))
                            self.stop_tracking_order(tracked_order.client_order_id)
                
                # if user stream event is an account update
                if 'account' in event_message['contents']:
                    update_data = event_message['contents']['account']
                    
                    # Update balances - dydx_perpetual only has USDC collateral currently
                    asset_name = "USDC"
                    self._account_balances[asset_name] = Decimal(update_data["freeCollateral"]) # Trading wallet balance 
                    self._account_available_balances[asset_name] = Decimal(update_data["freeCollateral"]) # Cross wallet balance. Not available in dydx_perpetual.

                    # update positions
                    open_positions = update_data['openPositions']
                    for market in open_positions:
                        open_position = open_positions[market]
                        position = self._account_positions.get(market, None)
                        if position is not None:
                            position.update_position(position_side=open_position['side'],
                                                     unrealized_pnl = Decimal(open_position['unrealizedPnl']),
                                                     entry_price = Decimal(open_position['entryPrice']),
                                                     amount = Decimal(open_position['size']))
                        else:
                            await self._update_positions()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error in user stream listener loop: {e}", exc_info=True)
                await asyncio.sleep(5.0)


    def tick(self, timestamp: float):
        """
        Is called automatically by the clock for each clock's tick (1 second by default).
        It checks if status polling task is due for execution.
        """
        now = time.time()
        poll_interval = (self.SHORT_POLL_INTERVAL
                         if now - self._user_stream_tracker.last_recv_time > 60.0
                         else self.LONG_POLL_INTERVAL)
        last_tick = int(self._last_timestamp / poll_interval)
        current_tick = int(timestamp / poll_interval)
        if current_tick > last_tick:
            if not self._poll_notifier.is_set():
                self._poll_notifier.set()
        self._last_timestamp = timestamp

    # MARKET AND ACCOUNT INFO ---
    def get_fee(self, base_currency: str, quote_currency: str, order_type: object, order_side: object,
                amount: object, price: object):
        is_maker = order_type is OrderType.LIMIT
        fee = estimate_fee("dydx_perpetual", is_maker)
        try:
            client = Client(
                network_id=self._network_id,
                host=self._base_url,
                default_ethereum_address = self._eth_address,
                api_key_credentials=self._api_key_credentials,
            )
            user_info = client.private.get_user()
            fee = Decimal(user_info['user']['makerFeeRate']) if is_maker else Decimal(user_info['user']['takerFeeRate'])
            fee = TradeFee(percent=fee, flat_fees=[])
        except Exception as e:
            print(e)
        return fee

    def get_order_book(self, trading_pair: str) -> OrderBook:
        order_books: dict = self._order_book_tracker.order_books
        if trading_pair not in order_books:
            raise ValueError(f"No order book exists for '{trading_pair}'.")
        return order_books[trading_pair]

    async def _update_trading_rules(self):
        last_tick = self._last_timestamp / 60.0
        current_tick = self.current_timestamp / 60.0
        if current_tick > last_tick or len(self._trading_rules) < 1:
            client = Client(host=self._base_url,)
            exchange_info = client.public.get_markets()['markets']
            trading_rules_list = self._format_trading_rules(exchange_info)
            self._trading_rules.clear()
            for trading_rule in trading_rules_list:
                self._trading_rules[trading_rule.trading_pair] = trading_rule

    def _format_trading_rules(self, exchange_info: Dict[str, Any]) -> List[TradingRule]:
        return_val = {key:{} for key in exchange_info}
        return_val = []
        for key in exchange_info:
            try:
                row = exchange_info[key]
                trading_pair = key
                min_order_size = row['minOrderSize']
                tick_size = row['tickSize']
                step_size = row['stepSize']

                return_val.append(TradingRule(
                    trading_pair,
                    min_order_size=Decimal(min_order_size),
                    min_price_increment=Decimal(tick_size),
                    min_base_amount_increment=Decimal(step_size),
                ))
            except Exception as e:
                self.logger().error(f"Error parsing the trading pair rule {rule}. Error: {e}. Skipping...",
                                    exc_info=True)
        return return_val

    async def _trading_rules_polling_loop(self):
        while True:
            try:
                await safe_gather(
                    self._update_trading_rules()

                    # TODO: Uncomment when implemented
                    # self._update_trade_fees()
                )
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching trading rules.", exc_info=True,
                                      app_warning_msg="Could not fetch new trading rules from Binance Perpetuals. "
                                                      "Check network connection.")
                await asyncio.sleep(0.5)

    async def _status_polling_loop(self):
        while True:
            try:
                self._poll_notifier = asyncio.Event()
                await self._poll_notifier.wait()
                await safe_gather(
                    self._update_balances(),
                    self._update_positions()
                )
                await self._update_order_fills_from_trades(),
                await self._update_order_status()
                self._last_poll_timestamp = self.current_timestamp
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().network("Unexpected error while fetching account updates.", exc_info=True,
                                      app_warning_msg="Could not fetch account updates from Binance Perpetuals. "
                                                      "Check API key and network connection.")
                await asyncio.sleep(0.5)

    # To be updated!!!!
    async def _update_balances(self):
        local_asset_names = {'USDC'}
        client = Client(
            network_id=self._network_id,
            host=self._base_url,
            default_ethereum_address = self._eth_address,
            api_key_credentials=self._api_key_credentials,
        )
        account = client.private.get_account()
        self._account_available_balances['USDC'] = account['account']['freeCollateral']
        self._account_balances['USDC'] = account['account']['equity']

    # TODO: Note --- Data Structure Assumes One-way Position Mode [not hedge position mode] (see Binance Futures Docs)
    # Note --- Hedge Mode allows for Both Long and Short Positions on a trading pair
    async def _update_positions(self):
        client = Client(
            network_id=self._network_id,
            host=self._base_url,
            default_ethereum_address = self._eth_address,
            api_key_credentials=self._api_key_credentials,
        )
        positions = client.private.get_positions(status='OPEN')['positions']
        for position in positions:
            trading_pair = position['market']
            position_side = PositionSide[position['side']]
            unrealized_pnl = Decimal(position['unrealizedPnl'])
            entry_price = Decimal(position['entryPrice'])
            amount = Decimal(position['size'])
            leverage = 25 # Leverage refers to the account's maximum leverage. Dydx does not have this feature yet, so set this to the maximum leverage possible.
            if amount != 0:
                self._account_positions[trading_pair + position_side.name] = Position(
                    trading_pair=trading_pair,
                    position_side=position_side,
                    unrealized_pnl=unrealized_pnl,
                    entry_price=entry_price,
                    amount=amount,
                    leverage=leverage
                )
            else:
                if (trading_pair + position_side.name) in self._account_positions:
                    del self._account_positions[trading_pair + position_side.name]
                    

    async def _update_order_fills_from_trades(self):
        last_tick = int(self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
        current_tick = int(self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)

        if current_tick > last_tick and len(self._in_flight_orders) > 0:

            # Create exchange_order_id_to_order_map
            exchange_order_id_to_order_map = defaultdict(lambda: {})
            for order in self._in_flight_orders.values():
                exchange_order_id_to_order_map[order.exchange_order_id] = order

            # Get filled orders
            client = Client(
                network_id=self._network_id,
                host=self._base_url,
                default_ethereum_address = self._eth_address,
                api_key_credentials=self._api_key_credentials,
            )
            fills = client.private.get_fills(limit=100)['fills']
            self.logger().debug(f"Polling for order fills of trading_pairs.")

            # Update order status
            for fill in fills:
                fill_exchange_order_id = fill['orderId']
                if fill_exchange_order_id in exchange_order_id_to_order_map:
                    order = exchange_order_id_to_order_map[fill_exchange_order_id]
                    trade = {
                        'id':filled_order.exchange_order_id, # exchange_order_id
                        'order_id':filled_order.client_order_id, # order_id
                        'qty':fill['size'],
                        'quoteQty':Decimal(fill['size']) * Decimal(fill['price']),
                        'commission':fill['fee'],
                        'commissionAsset': 'USDC', #optional
                    }
                    print('fill', fill)
                    print('trade', trade)
                    applied_trade = order.update_with_trade_updates(trade)
                    if applied_trade:
                        self.trigger_event(
                            self.MARKET_ORDER_FILLED_EVENT_TAG,
                            OrderFilledEvent(
                                self.current_timestamp,
                                order.client_order_id,
                                order.trading_pair,
                                order.trade_type,
                                order_type,
                                Decimal(trade.get("price")),
                                Decimal(trade.get("qty")),
                                self.get_fee(
                                    order.base_asset,
                                    order.quote_asset,
                                    order_type,
                                    order.trade_type,
                                    Decimal(trade["price"]),
                                    Decimal(trade["qty"])),
                                exchange_trade_id=trade["id"],
                                leverage=self._leverage,
                                position=order.position
                            )
                        )
                
    # To be updated!!!
    async def _update_order_status(self):
        last_tick = int(self._last_poll_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
        current_tick = int(self.current_timestamp / self.UPDATE_ORDER_STATUS_MIN_INTERVAL)
        if current_tick > last_tick and len(self._in_flight_orders) > 0:
            tracked_orders = list(self._in_flight_orders.values())
            print('tracked_orders', tracked_orders)
            
            client = Client(
                network_id=self._network_id,
                host=self._base_url,
                default_ethereum_address = self._eth_address,
                api_key_credentials=self._api_key_credentials,
            )
            results = []
            for order in tracked_orders:
                try:
                    order_update = client.private.get_order_by_id(order_id=order.exchange_order_id)['order']
                except:
                    order_update = {'error':'order not found'}
                results.append(order_update)
            self.logger().debug(f"Polling for order status updates of {len(results)} orders.")
            for order_update, tracked_order in zip(results, tracked_orders):
                print('order_update',order_update)
                print('tracked_order',tracked_order)
                client_order_id = tracked_order.client_order_id
                if client_order_id not in self._in_flight_orders:
                    continue
                if isinstance(order_update, Exception):
                    # NO_SUCH_ORDER code
                    if order_update["error"] == 'order not found':
                        self._order_not_found_records[client_order_id] = \
                            self._order_not_found_records.get(client_order_id, 0) + 1
                        if self._order_not_found_records[client_order_id] < self.ORDER_NOT_EXIST_CONFIRMATION_COUNT:
                            continue
                        self.trigger_event(
                            self.MARKET_ORDER_FAILURE_EVENT_TAG,
                            MarketOrderFailureEvent(self.current_timestamp, client_order_id, tracked_order.order_type)
                        )
                        self.stop_tracking_order(client_order_id)
                    else:
                        self.logger().network(f"Error fetching status update for the order {client_order_id}: "
                                              f"{order_update}.")
                    continue

                tracked_order.last_state = order_update.get("status")
                order_type = OrderType.LIMIT if order_update.get("type") == "LIMIT" else OrderType.MARKET
                executed_amount_base = Decimal(order_update.get("size", "0")) - Decimal(order_update.get("remainingSize", "0"))
                executed_amount_quote = executed_amount_base * Decimal(order_update.get("price", "0"))

                if tracked_order.is_done:
                    if not tracked_order.is_failure:
                        event_tag = None
                        event_class = None
                        if tracked_order.trade_type is TradeType.BUY:
                            event_tag = self.MARKET_BUY_ORDER_COMPLETED_EVENT_TAG
                            event_class = BuyOrderCompletedEvent
                        else:

                            event_tag = self.MARKET_SELL_ORDER_COMPLETED_EVENT_TAG
                            event_class = SellOrderCompletedEvent
                        self.logger().info(f"The {order_type.name.lower()} {tracked_order.trade_type.name.lower()} order {client_order_id} has "
                                           f"completed according to order status API.")
                        self.trigger_event(event_tag,
                                           event_class(self.current_timestamp,
                                                       client_order_id,
                                                       tracked_order.base_asset,
                                                       tracked_order.quote_asset,
                                                       (tracked_order.fee_asset or tracked_order.base_asset),
                                                       executed_amount_base,
                                                       executed_amount_quote,
                                                       tracked_order.fee_paid,
                                                       order_type))
                    else:
                        if tracked_order.is_cancelled:
                            self.logger().info(f"Successfully cancelled order {client_order_id} according to order status API.")
                            self.trigger_event(self.MARKET_ORDER_CANCELLED_EVENT_TAG,
                                               OrderCancelledEvent(self.current_timestamp,
                                                                   client_order_id))
                        else:
                            self.logger().info(f"The {order_type.name.lower()} order {client_order_id} has failed according to "
                                               f"order status API.")
                            self.trigger_event(self.MARKET_ORDER_FAILURE_EVENT_TAG,
                                               MarketOrderFailureEvent(self.current_timestamp,
                                                                       client_order_id,
                                                                       order_type))
                    self.stop_tracking_order(client_order_id)
                    
    # Dydx currently does not allow users to set maximum margin.
    async def _set_margin(self, trading_pair: str, leverage: int = 1):
        params = {
            "symbol": trading_pair,
            "leverage": leverage
        }
        self._leverage = leverage
        self.logger().info(f"Leverage Successfully set to {leverage}.")
#         if set_leverage["leverage"] == leverage:
#             self._leverage = leverage
#             self.logger().info(f"Leverage Successfully set to {leverage}.")
#         else:
#             self.logger().error("Unable to set leverage.")
        return leverage

    def set_margin(self, trading_pair: str, leverage: int = 1):
        safe_ensure_future(self._set_margin(trading_pair, leverage))

    """
    async def get_position_pnl(self, trading_pair: str):
        await self._update_positions()
        return self._account_positions.get(trading_pair)
    """

    async def _get_funding_rate(self, trading_pair):
        # TODO: Note --- the "premiumIndex" endpoint can get markPrice, indexPrice, and nextFundingTime as well
        client = Client(host=self._base_url,)
        market_stats = client.public.get_markets(market=trading_pair)
        self._funding_rate = Decimal(market_stats['markets'][trading_pair]['nextFundingRate'])
    
    # William's comment: currently self._funding_rate is a Decimal variable for storing funding rate for one trading_pair. Should upgrade it into a json list of funding rates for every trading_pair.
    def get_funding_rate(self, trading_pair):
        safe_ensure_future(self._get_funding_rate(trading_pair))
        return self._funding_rate

    async def _set_position_mode(self, position_mode: PositionMode):
        initial_mode = await self._get_position_mode()
        if initial_mode != position_mode:
            self.logger().error(f"Unable to set postion mode to {position_mode.name}.")
        else:
            self.logger().info(f"Using {position_mode.name} position mode.")

    async def _get_position_mode(self):
        # Set _position_mode to ONEWAY only
        if self._position_mode is None:
            self._position_mode = PositionMode.ONEWAY
        return self._position_mode

    def set_position_mode(self, position_mode: PositionMode):
        safe_ensure_future(self._set_position_mode(position_mode))
    
