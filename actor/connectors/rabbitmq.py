import logging
import asyncio
import json
from typing import Any, MutableMapping, Protocol
from uuid import uuid4

from pydantic import BaseModel
import aio_pika
import aio_pika.abc

from .connector import AbstractConnector


logger = logging.getLogger(__name__)

class AbstractCallbackConsumer(Protocol):

    @property
    def callback_queue(self) -> aio_pika.abc.AbstractQueue: ...


class RabbitMqConnector(AbstractConnector):
    """Connector class for Rabbitmq messaging."""

    def __init__(self, service_name: str):
        self.service_name = service_name
        self._connection: aio_pika.RobustConnection = None
        self._channel: aio_pika.RobustChannel = None
        self._loop: asyncio.AbstractEventLoop = None

    async def close(self) -> None:
        closing_tasks = []
        if self._connection is not None:
            closing_tasks.append(self._connection.close())
        if self._channel is not None:
            closing_tasks.append(self._channel.close())
        await asyncio.gather(*closing_tasks)

    @staticmethod
    def __parse_kwargs(kwargs) -> dict[str, Any]:
        resp = {"params": {}}
        for key, value in kwargs.items():
            if isinstance(value, BaseModel):
                resp["json"] = value.model_dump()
            else:
                resp["params"].update({key: value})
        return resp

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        """Get the running event loop"""
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        return self._loop

    @property
    async def channel(self) -> aio_pika.RobustChannel:
        if self._connection is None:
            self._connection = await aio_pika.connect_robust(
                "amqp://guest:guest@rabbitmq:5672/",
                timeout=60,
            )
            self._channel = await self._connection.channel()
        return self._channel

    async def __call__(
        self,
        func: str,
        method: str,
        futures: MutableMapping[str, asyncio.Future],
        callback_consumer: AbstractCallbackConsumer,
        **kwargs
    ) -> Any:
        logger.info("Replying to %s", callback_consumer.callback_queue.name)
        xid = str(uuid4())
        future = self.loop.create_future()
        futures[xid] = future
        channel = await self.channel
        await channel.default_exchange.publish(
            message=aio_pika.Message(
                body=json.dumps(kwargs).encode("utf-8"),
                headers={"func": func, "method": method},
                reply_to=callback_consumer.callback_queue.name,
                correlation_id=xid,
            ),
            routing_key=self.service_name,
        )
        return await future

    async def ready(self) -> None:
        logger.info("Health checking app")
        await self.channel
        await self._connection.ready()
