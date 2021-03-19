import asyncio
import logging
import time
from typing import Optional, Dict, AsyncIterable

import aiohttp
import ujson
import websockets
from websockets import ConnectionClosed

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.logger import HummingbotLogger

import json
from dydx3.helpers.request_helpers import generate_now_iso


from dydx3 import Client
from dydx3.constants import NETWORK_ID_MAINNET, NETWORK_ID_ROPSTEN
from web3 import Web3
import websocket
import ssl
import json
from datetime import datetime
import configparser

class DydxPerpetualUserStreamDataSource(UserStreamTrackerDataSource):
    _bpusds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bpusds_logger is None:
            cls._bpusds_logger = logging.getLogger(__name__)
        return cls._bpusds_logger

    @property
    def last_recv_time(self) -> float:
        return self._last_recv_time

    def __init__(self, base_url:str, stream_url:str, network_id:int, eth_address:str, api_key_credentials:dict):
        super().__init__()
        self._current_listen_key = None
        self._listen_for_user_stream_task = None
        self._last_recv_time: float = 0
        self._http_stream_url = base_url
        self._wss_stream_url = stream_url
        self._network_id = network_id
        self._eth_address = eth_address
        self._api_key_credentials = api_key_credentials

    
    # Not a standalone function; to be called by other functions.
    async def ws_messages(self, client: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        try:
            while True:
                try:
                    raw_msg: str = await asyncio.wait_for(client.recv(), timeout=50.0)
                    self._last_recv_time = time.time()
                    yield raw_msg
                except asyncio.TimeoutError:
                    try:
                        self._last_recv_time = time.time()
                        pong_waiter = await client.ping()
                        await asyncio.wait_for(pong_waiter, timeout=50.0)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("Websocket ping timed out. Going to reconnect... ")
            return
        except ConnectionClosed:
            return
        finally:
            await client.close()
    
    # Not a standalone function; to be called by other functions.
    async def log_user_stream(self, output: asyncio.Queue):
        while True:
            try:
                print(self._wss_stream_url)
                async with websockets.connect(self._wss_stream_url) as ws:
                    ws: websockets.WebSocketClientProtocol = ws
                    client = Client(
                        network_id=self._network_id,
                        host=self._http_stream_url,
                        default_ethereum_address = self._eth_address,
                        api_key_credentials=self._api_key_credentials,
                    )
                    now_iso_string = generate_now_iso()
                    signature = client.private.sign(
                        request_path='/ws/accounts',
                        method='GET',
                        iso_timestamp=now_iso_string,
                        data={},
                    )
                    req = {
                        'type': 'subscribe',
                        'channel': 'v3_accounts',
                        'accountNumber': '0',
                        'apiKey': self._api_key_credentials['key'],
                        'passphrase': self._api_key_credentials['passphrase'],
                        'timestamp': now_iso_string,
                        'signature': signature,
                    }
                    
                    # Send subscription message to websocket channel
                    try:
                        req = json.dumps(req)
                        await ws.send(req)
                    except Exception as e: print(e)
                        
                    async for raw_msg in self.ws_messages(ws):
                        msg_json: Dict[str, any] = ujson.loads(raw_msg)
                        output.put_nowait(msg_json)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error. Retrying after 5 seconds... ", exc_info=True)
                await asyncio.sleep(5)
    
    async def listen_for_user_stream(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        try:
            while True:
                try:
                    if self._listen_for_user_stream_task is not None:
                        self._listen_for_user_stream_task.cancel()
                    self._listen_for_user_stream_task = safe_ensure_future(self.log_user_stream(output))
                    await self.wait_til_next_tick(seconds=3600)
                    success: bool = await self.ping_listen_key(self._current_listen_key)
                    if not success:
                        self._current_listen_key = None
                        if self._listen_for_user_stream_task is not None:
                            self._listen_for_user_stream_task.cancel()
                            self._listen_for_user_stream_task = None
                        continue
                    self.logger().debug(f"Refreshed listen key {self._current_listen_key}.")
                    await self.wait_til_next_tick(seconds=60)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.logger().error("Unexpected error while maintaning the user event listen key. Retrying after "
                                        "5 seconds...", exc_info=True)
                    await asyncio.sleep(5)
        finally:
            if self._listen_for_user_stream_task is not None:
                self._listen_for_user_stream_task.cancel()
                self._listen_for_user_stream_task = None
            self._current_listen_key = None