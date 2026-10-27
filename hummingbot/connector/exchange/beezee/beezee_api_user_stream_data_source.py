import asyncio
import time

from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource


class BeezeeAPIUserStreamDataSource(UserStreamTrackerDataSource):
    def __init__(self):
        super().__init__()
        self._heartbeat_time = 0.0

    @property
    def last_recv_time(self) -> float:
        return self._heartbeat_time

    async def listen_for_user_stream(self, output: asyncio.Queue):
        self._heartbeat_time = time.time()
        while True:
            await self._sleep(30.0)
            self._heartbeat_time = time.time()
