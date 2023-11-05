import logging
import asyncio
import json
from typing import Any, MutableMapping, Protocol
from uuid import uuid4

from pydantic import BaseModel
import aio_pika
import aio_pika.abc

from .connector import AbstractConnector
from ..config import CONFIG


logger = logging.getLogger(__name__)


class AbstractCallbackConsumer(Protocol):
    @property
    def callback_queue(self) -> aio_pika.abc.AbstractQueue:
        ...


class RabbitMqConnector(AbstractConnector):
    """Connector class for Rabbitmq messaging."""

    def __init__(self, service_name: str):
        self.service_name = service_name
        self._channel: aio_pika.RobustChannel = None
        self._loop: asyncio.AbstractEventLoop = None

    @staticmethod
    def __parse_kwargs(kwargs) -> dict[str, Any]:
        for key, value in kwargs.items():
            if isinstance(value, BaseModel):
                kwargs[key] = value.model_dump()
        return kwargs

    @property
    async def channel(self) -> aio_pika.RobustChannel:
        if self._channel is None:
            conn = await CONFIG.CONN.connection
            self._channel = await conn.channel()
        return self._channel

    async def __call__(
        self,
        func: str,
        method: str,
        futures: MutableMapping[str, asyncio.Future],
        callback_consumer: AbstractCallbackConsumer,
        **kwargs
    ) -> Any:
        xid = str(uuid4())
        future = CONFIG.CONN.loop.create_future()
        futures[xid] = future
        channel = await self.channel
        await channel.default_exchange.publish(
            message=aio_pika.Message(
                body=json.dumps(self.__parse_kwargs(kwargs)).encode("utf-8"),
                headers={"func": func, "method": method},
                reply_to=callback_consumer.callback_queue.name,
                correlation_id=xid,
            ),
            routing_key=self.service_name,
        )
        return await future

    async def ready(self) -> None:
        conn = await CONFIG.CONN.connection
        await conn.ready()
        await self.channel
