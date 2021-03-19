
import asyncio
import logging
import time
from typing import Dict, List, Optional, Any, AsyncIterable
from decimal import Decimal

import aiohttp
import pandas as pd
import ujson
import requests
import cachetools.func
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage, OrderBookMessageType
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.derivative.dydx_perpetual.dydx_perpetual_order_book import DydxPerpetualOrderBook
# from hummingbot.connector.derivative.dydx_perpetual.dydx_perpetual_utils import convert_to_exchange_trading_pair

from hummingbot.connector.derivative.dydx_perpetual.constants import (
    PERPETUAL_BASE_URL,
    TESTNET_BASE_URL,
    DIFF_STREAM_URL,
    TESTNET_STREAM_URL
)

from hummingbot.core.event.events import TradeType
from dydx3 import Client
from web3 import Web3
import websocket
import ssl
import json
from datetime import datetime

class DydxPerpetualAPIOrderBookDataSource(OrderBookTrackerDataSource):
    def __init__(self, trading_pairs: List[str] = None, domain: str = "dydx_perpetual"):
        super().__init__(trading_pairs)
        self._order_book_create_function = lambda: OrderBook()
        self._base_url = TESTNET_BASE_URL if domain == "dydx_perpetual_testnet" else PERPETUAL_BASE_URL
        self._stream_url = TESTNET_STREAM_URL if domain == "dydx_perpetual_testnet" else DIFF_STREAM_URL
        self._stream_url += "/stream"
        self._domain = domain

    _bpobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bpobds_logger is None:
            cls._bpobds_logger = logging.getLogger(__name__)
        return cls._bpobds_logger

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str], domain=None) -> Dict[str, float]:
        tasks = [cls.get_last_traded_price(t_pair, domain) for t_pair in trading_pairs]
        results = await safe_gather(*tasks)
        return {t_pair: result for t_pair, result in zip(trading_pairs, results)}

    @classmethod
    async def get_last_traded_price(cls, trading_pair: str, domain=None) -> float:
        BASE_URL = TESTNET_BASE_URL if domain == "dydx_perpetual_testnet" else PERPETUAL_BASE_URL
        public_client = Client(host=BASE_URL,)
        resp = public_client.public.get_stats(market=trading_pair, days=1)
        return float(resp["markets"][trading_pair]['close'])

    """
    async def get_trading_pairs(self) -> List[str]:
        if not self._trading_pairs:
            try:
                active_markets: pd.DataFrame = await self.get_active_exchange_markets()
                self._trading_pairs = active_markets.index.tolist()
            except Exception as e:
                self._trading_pairs = []
                self.logger().network(
                    "Error getting active trading pairs.",
                    exc_info=True,
                    app_warning_msg="Error getting active trading_pairs. Check network connection."
                )
                raise e
        return self._trading_pairs
    """

    @staticmethod
    @cachetools.func.ttl_cache(ttl=10)
    def get_mid_price(trading_pair: str, domain=None) -> Optional[Decimal]:
        BASE_URL = TESTNET_BASE_URL if domain == "dydx_perpetual_testnet" else PERPETUAL_BASE_URL
        public_client = Client(host=BASE_URL,)
        resp = public_client.public.get_orderbook(market=trading_pair)
        bidPrice = resp['bids'][0]['price']
        askPrice = resp['asks'][0]['price']
        result = (Decimal(bidPrice) + Decimal(askPrice)) / Decimal(2)
        return result

    @staticmethod
    async def fetch_trading_pairs(domain=None) -> List[str]:
        try:
            BASE_URL = TESTNET_BASE_URL if domain == "dydx_perpetual_testnet" else PERPETUAL_BASE_URL
            public_client = Client(host=BASE_URL,)
            resp = public_client.public.get_markets()
            trading_pairs = [key for key in resp['markets']]
            return trading_pairs
        except Exception:
            pass
        return []

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str, limit: int = 1000, domain=None) -> Dict[str, Any]:
        try:

            BASE_URL = TESTNET_BASE_URL if domain == "dydx_perpetual_testnet" else PERPETUAL_BASE_URL
            public_client = Client(host=BASE_URL,)
            resp = public_client.public.get_orderbook(market=trading_pair,)
            snapshot = {
                'lastUpdateId':0,
                'bids':[[row['price'], row['size']] for row in resp['bids']],
                'asks':[[row['price'], row['size']] for row in resp['asks']],
            }
            return snapshot
        except Exception: pass
        return {'lastUpdateId':0,'bids':[],'asks':[]}

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        async with aiohttp.ClientSession() as client:
            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, 1000, self._base_url)
            snapshot_timestamp: float = time.time()
            snapshot_msg: OrderBookMessage = DydxPerpetualOrderBook.snapshot_message_from_exchange(
                snapshot,
                snapshot_timestamp,
                metadata={"trading_pair": trading_pair}
            )
            order_book = self.order_book_create_function()
            order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
            return order_book

    """
    async def get_tracking_pairs(self) -> Dict[str, OrderBookTrackerEntry]:
        async with aiohttp.ClientSession() as client:
            trading_pairs: List[str] = await self.get_trading_pairs()
            return_val: Dict[str, OrderBookTrackerEntry] = {}
            for trading_pair in trading_pairs:
                try:
                    snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, 1000)
                    snapshot_timestamp: float = time.time()
                    snapshot_msg: OrderBookMessage = DydxPerpetualOrderBook.snapshot_message_from_exchange(
                        snapshot,
                        snapshot_timestamp,
                        metadata={"trading_pair": trading_pair}
                    )
                    order_book: OrderBook = self.order_book_create_function()
                    order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
                    return_val[trading_pair] = OrderBookTrackerEntry(trading_pair, snapshot_timestamp, order_book)
                    self.logger().info(f"Initialized order book for {trading_pair}. ")
                    await asyncio.sleep(1)
                except Exception as e:
                    self.logger().error(f"Error getting snapshot for {trading_pair}: {e}", exc_info=True)
                    await asyncio.sleep(5)
            return return_val
    """

    async def ws_messages(self, client: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        try:
            while True:
                try:
                    raw_msg: str = await asyncio.wait_for(client.recv(), timeout=30.0)
                    yield raw_msg
                except asyncio.TimeoutError:
                    await client.pong(data=b'')
        except ConnectionClosed:
            return
        finally:
            await client.close()

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):        
        while True:
            try:
                STREAM_URL = TESTNET_STREAM_URL if self._domain == "dydx_perpetual_testnet" else DIFF_STREAM_URL
                async with websockets.connect(STREAM_URL) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for trading_pair in self._trading_pairs:
                        msg = {
                            'type':'subscribe',
                            'channel':'v3_orderbook',
                            'id':trading_pair,
                        }
                        msg = json.dumps(msg)
                        await ws.send(msg)
                    async for raw_msg in self.ws_messages(ws):
                        msg_json = ujson.loads(raw_msg)
                        try:
                            timestamp: float = time.time()
                            order_book_message = OrderBookMessage(OrderBookMessageType.DIFF, {
                                "trading_pair": msg_json["id"],
                                "update_id": int(msg_json["contents"]["offset"]),
                                "bids": msg_json["contents"]["bids"],
                                "asks": msg_json["contents"]["asks"]
                            }, timestamp=timestamp)
                            output.put_nowait(order_book_message)
                        except Exception as e: print(e)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with Websocket connection. Retrying after 30 seconds... ",
                                    exc_info=True)
                await asyncio.sleep(30.0)

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                STREAM_URL = TESTNET_STREAM_URL if self._domain == "dydx_perpetual_testnet" else DIFF_STREAM_URL
                async with websockets.connect(STREAM_URL) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    for trading_pair in self._trading_pairs:
                        msg = {
                            'type':'subscribe',
                            'channel':'v3_trades',
                            'id':trading_pair,
                        }
                        msg = json.dumps(msg)
                        await ws.send(msg)
                    async for raw_msg in self.ws_messages(ws):
                        msg_json = ujson.loads(raw_msg)
                        if 'type' in msg_json and msg_json['type']=='channel_data':
                            try:
                                trading_pair = msg_json['id']
                                for row in msg_json['contents']['trades']:
                                    millisec_timestamp = int(datetime.strptime(row["createdAt"],"%Y-%m-%dT%H:%M:%S.%fZ").timestamp() * 1000)
                                    trade_msg = OrderBookMessage(OrderBookMessageType.TRADE, {
                                        "trading_pair": trading_pair,
                                        "trade_type": float(TradeType.SELL.value) if row["side"]=='SELL' else float(TradeType.BUY.value),
                                        "trade_id": millisec_timestamp,
                                        "update_id": millisec_timestamp,
                                        "price": row["size"],
                                        "amount": row["size"]
                                    }, timestamp=millisec_timestamp * 1e-3)
                                    output.put_nowait(trade_msg)
                            except Exception as e: print(e)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                    exc_info=True)
                await asyncio.sleep(30)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                # trading_pairs: List[str] = await self.get_trading_pairs()
                async with aiohttp.ClientSession() as client:
                    for trading_pair in self._trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, domain=self._base_url)
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: OrderBookMessage = DydxPerpetualOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                snapshot_timestamp,
                                metadata={"trading_pair": trading_pair}
                            )
                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                            await asyncio.sleep(5)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.logger().error("Unexpected error.", exc_info=True)
                            await asyncio.sleep(5)
                    this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                    next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                    delta: float = next_hour.timestamp() - time.time()
                    await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)

