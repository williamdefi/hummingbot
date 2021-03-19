import asyncio
import logging
from typing import Optional

from hummingbot.core.data_type.user_stream_tracker import UserStreamTracker
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather, safe_ensure_future
from hummingbot.logger import HummingbotLogger

from hummingbot.connector.derivative.dydx_perpetual.dydx_perpetual_user_stream_data_source import \
    DydxPerpetualUserStreamDataSource


class DydxPerpetualUserStreamTracker(UserStreamTracker):

    _bpust_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._bust_logger is None:
            cls._bust_logger = logging.getLogger(__name__)
        return cls._bust_logger

    def __init__(self, base_url:str, stream_url:str, network_id:int, eth_address:str, api_key_credentials:dict):
        super().__init__()
        self._base_url = base_url
        self._stream_url = stream_url
        self._network_id = network_id
        self._eth_address = eth_address
        self._api_key_credentials = api_key_credentials
        self._ev_loop: asyncio.events.AbstractEventLoop = asyncio.get_event_loop()
        self._data_source: Optional[UserStreamTrackerDataSource] = None
        self._user_stream_tracking_task: Optional[asyncio.Task] = None

    @property
    def exchange_name(self) -> str:
        return "dydx_perpetuals"

    @property
    def data_source(self) -> UserStreamTrackerDataSource:
        if self._data_source is None:
            self._data_source = DydxPerpetualUserStreamDataSource(
                base_url=self._base_url, 
                stream_url=self._stream_url, 
                network_id=self._network_id, 
                eth_address=self._eth_address,
                api_key_credentials=self._api_key_credentials
            )
        return self._data_source

    async def start(self):
        self._user_stream_tracking_task = safe_ensure_future(self.data_source.listen_for_user_stream(self._ev_loop, self._user_stream))
        await safe_gather(self._user_stream_tracking_task)
