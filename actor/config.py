import logging
import asyncio
from dataclasses import dataclass, field

import aiohttp
import aio_pika
from aiormq.exceptions import AMQPConnectionError


logging.basicConfig(level=logging.INFO)


@dataclass
class Connections:
    """App connections"""

    _http_session: aiohttp.ClientSession = field(default=None, init=False)
    _rmq_connection: aio_pika.RobustConnection = field(default=None, init=False)
    _loop: asyncio.AbstractEventLoop = field(default=None, init=False)

    async def close(self) -> None:
        """Close open connections."""
        tasks = []
        if isinstance(self._http_session, aiohttp.ClientSession):
            tasks.append(self._http_session.close())
        if isinstance(self._rmq_connection, aio_pika.abc.AbstractConnection):
            tasks.append(self._rmq_connection.close())
        await asyncio.gather(*tasks)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """Get the running event loop"""
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return self._loop

    @property
    def session(self) -> aiohttp.ClientSession:
        """HTTP client session."""
        if self._http_session is None:
            self._http_session = aiohttp.ClientSession()
        return self._http_session

    @property
    async def connection(self) -> aio_pika.RobustConnection:
        """Connection to the RabbitMQ broker."""
        if self._rmq_connection is None:
            retries = 5
            num = 0
            while (self._rmq_connection is None) and (num < retries):
                try:
                    self._rmq_connection = await aio_pika.connect_robust(
                        "amqp://guest:guest@rabbitmq:5672/",
                        timeout=60,
                    )
                    break
                except AMQPConnectionError:
                    await asyncio.sleep(0.1 * (2**num))
                    num += 1
            else:
                raise AMQPConnectionError(f"Connection failed after {retries} attempts")
        return self._rmq_connection


@dataclass
class Config:
    """Configuration for the app"""

    CONN: Connections = field(init=False, default_factory=Connections)


CONFIG = Config()
