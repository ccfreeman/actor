import logging
import functools
import asyncio
import signal
import json
import inspect
from typing import (
    Self,
    TypeVar,
    Callable,
    Generator,
    Type,
    MutableMapping,
    Union,
    Any,
    get_origin,
    get_args,
)
from abc import ABC

import uvicorn
import aio_pika
import aio_pika.abc
from fastapi import FastAPI
from pydantic import BaseModel

from .config import CONFIG
from .connectors import HttpConnector, RabbitMqConnector, AbstractConnector


logger = logging.getLogger(__name__)


T = TypeVar("T")


def get_funcs(_obj: object) -> Generator[Callable, None, None]:
    for func in dir(_obj):
        if (
            callable(getattr(_obj, func))
            and not ((func.startswith("__") or func.startswith("_")))
            and hasattr(getattr(_obj, func), "methods")
        ):
            yield getattr(_obj, func)


class Server(uvicorn.Server):
    """A wrapper around a Uvicorn server. Adds some extra logging to add visibility
    to server cancellation events.
    """

    def install_signal_handlers(self) -> None:
        """We don't want Uvicorn to handle signals, we'll do it ourselves"""


class ActorServer:
    """Attach a FastAPI instance to a class. This class is used to create a FastAPI
    instance that exposes the methods of the class passed in. This is done by using the
    `get_funcs` helper function to get all the functions on the class, and then adding
    them to the FastAPI app.
    """

    def __init__(self, obj: T):
        self.obj = obj
        self.app = FastAPI(
            title=self.obj.__class__.__name__, description=self.obj.__class__.__doc__
        )
        for func in get_funcs(self.obj):
            self.app.add_api_route(
                path=f"/{func.__name__}",
                tags=["Call Methods"],
                endpoint=func,
                response_model=func.__annotations__["return"],
                methods=func.methods,
                description=func.__doc__,
            )
        self.app.add_api_route(
            path="/healthz",
            tags=["Health"],
            endpoint=self.healthz,
            methods=["GET"],
        )

    async def healthz(self) -> bool:
        """Application health endpoint."""
        return True

    async def run(self) -> None:
        """Run the FastAPI app."""
        server = Server(
            # uvicorn.Config(app=self.app, host="127.0.0.1", port=8000, reload=True)
            uvicorn.Config(app=self.app, host="0.0.0.0", port=8000)
        )
        try:
            await server.serve()
        except asyncio.CancelledError:
            logger.info("Uvicorn server was cancelled")
            await server.shutdown()


class ActorQueueConsumer:
    """A class to handle connections to the message broker."""

    def __init__(self, obj: T):
        self.obj = obj

    async def run(self) -> None:
        while True:
            try:
                await self.__consume()
            except Exception:
                logger.exception("Consume method ended unexpectedly")
            logger.info("Running again")

    async def __consume(self) -> None:
        """Run the consumer process"""
        queue_name = self.obj.__class__.__name__
        # Creating channel
        conn = await CONFIG.CONN.connection
        channel: aio_pika.abc.AbstractChannel = await conn.channel(
            on_return_raises=True
        )
        exchange = channel.default_exchange

        # Declaring queue
        queue: aio_pika.abc.AbstractQueue = await channel.declare_queue(
            queue_name, auto_delete=True
        )

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process(requeue=True):
                    body: dict[str, Any] = json.loads(message.body)
                    func = message.headers["func"]
                    methods = message.headers["method"]
                    f = getattr(self.obj, func)
                    # We need to convert any pydantic models to their type from the
                    # jsonable dictionaries we represent them as in messages.
                    # We do this by inspecting the function signature, iterating through
                    # the parameters, handling optional args, and then casting the dict
                    # object into the pydantic class
                    f_signature = inspect.signature(f)
                    for key, val in body.items():
                        arg_type_annotation = f_signature.parameters[key].annotation
                        if get_origin(arg_type_annotation) is Union:
                            for arg_type in get_args(arg_type_annotation):
                                if isinstance(arg_type, type(BaseModel)):
                                    arg_type_annotation = arg_type

                        if isinstance(arg_type_annotation, type(BaseModel)):
                            model: BaseModel = arg_type_annotation
                            body[key] = model.model_validate(val)
                    response = await f(**body)
                    await exchange.publish(
                        message=aio_pika.Message(
                            body=json.dumps(response).encode("utf-8"),
                            correlation_id=message.correlation_id,
                        ),
                        routing_key=message.reply_to,
                    )


class CallbackConsumer:
    """Consumer to receive replies"""

    def __init__(
        self, futures: MutableMapping[str, asyncio.Future], connector: RabbitMqConnector
    ):
        self.futures = futures
        self.connector = connector
        self.callback_queue: aio_pika.abc.AbstractQueue = None
        self.__proc: asyncio.Task = None
        self.__ready = asyncio.Event()

    async def ready(self) -> None:
        await self.__ready.wait()

    def run(self) -> None:
        self.__proc = asyncio.create_task(self.__consume())

    async def close(self) -> None:
        if isinstance(self.__proc, asyncio.Task):
            self.__proc.cancel()
            try:
                await self.__proc
            except:
                logger.exception("Boo boo")

    async def __process_message(
        self, message: aio_pika.abc.AbstractIncomingMessage
    ) -> None:
        xid = message.correlation_id
        body = json.loads(message.body)
        future = self.futures.pop(xid)
        future.set_result(body)

    async def __consume(self) -> None:
        """Run the consumer process"""
        try:
            channel = await self.connector.channel
            self.callback_queue = await channel.declare_queue(exclusive=True)
            self.__ready.set() # Go!
            await self.callback_queue.consume(
                self.__process_message, no_ack=True, timeout=None
            )
        except:
            logger.exception("Shit! Consume process has a problem")
        logger.warning("Damn, the reply consumer is done")


class _Proxy:
    """Proxy class for the client. This class is used to create a proxy object that
    exposes the same methods as the class passed in, but instead of calling the methods
    on the object, it calls them on the server. This is done by using the `__getattr__`
    method to return a partial function that calls the server.
    """

    def __init__(
        self, cls: type[T], connector: Type[AbstractConnector] = HttpConnector
    ):
        functools.update_wrapper(self, cls)
        self.cls = cls
        self.connector = connector(service_name=self.cls.__name__)
        self.futures: MutableMapping[str, asyncio.Future] = {}
        self.callback_consumer: CallbackConsumer = CallbackConsumer(
            futures=self.futures, connector=self.connector
        )
        # The args are used to set the method active on each server endpoint
        kwargs = {}
        if isinstance(self.connector, RabbitMqConnector):
            self.callback_consumer.run()
            kwargs.update(
                {"futures": self.futures, "callback_consumer": self.callback_consumer}
            )
        for func in get_funcs(self.cls):
            setattr(
                self,
                func.__name__,
                functools.partial(
                    self.connector, func=func.__name__, method=func.methods[0], **kwargs
                ),
            )

    async def close(self) -> None:
        tasks = []
        if isinstance(self.callback_consumer, CallbackConsumer):
            tasks.append(self.callback_consumer.close())
        await asyncio.gather(*tasks)

    async def ready(self) -> None:
        ready_tasks = [self.connector.ready()]
        if isinstance(self.connector, RabbitMqConnector):
            ready_tasks.append(self.callback_consumer.ready())
        await asyncio.gather(*ready_tasks)

    async def __aenter__(self) -> T:
        await self.ready()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()


class Actor(ABC):
    """Base class for actors. This class is used to create a proxy object that
    exposes the same methods as the class passed in, but instead of calling the methods
    on the object, it calls them on the server. This is done by using the `__getattr__`
    method to return a partial function that calls the server.
    """

    @staticmethod
    def route(methods: list[str]):
        """Mark a method on a class for exposure. This is handled by adding a `methods`
        attribute to the function, which is then used by the `app_wrapper` class to add
        the function to the FastAPI app. This way we can detect which methods a user
        wants exposed through the FastAPI app.
        """

        def wrapper(func):
            func.methods = methods
            return func

        return wrapper

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        ...

    async def ready(self) -> None:
        """Await the readiness of the actor"""

    @classmethod
    async def serve(cls: type[T], *args, **kwargs) -> None:
        """Serve the actor. This method is used to start the FastAPI server."""
        obj = cls(*args, **kwargs)
        server = ActorServer(obj)
        consumer = ActorQueueConsumer(obj)
        loop = asyncio.get_event_loop()

        async with asyncio.TaskGroup() as task_group:
            server_task = task_group.create_task(server.run())
            consumer_task = task_group.create_task(consumer.run())

            def cancel_tasks():
                server_task.cancel()
                consumer_task.cancel()

            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, cancel_tasks)
        try:
            await asyncio.gather(server_task, consumer_task)
        except asyncio.CancelledError:
            pass
        await CONFIG.CONN.close()
        logger.info("serving all done")

    @classmethod
    def proxy(
        cls: type[T],
        connector: Type[AbstractConnector] = HttpConnector,
        *args,
        **kwargs,
    ) -> T:
        """Proxy class for the client. This class is used to create a proxy object that
        exposes the same methods as the class passed in, but instead of calling the
        methods on the object, it calls them on the server. This is done by using the
        `__getattr__` method to return a partial function that calls the server.

        This is a factory method that returns an initialized class instance of the
        proxied class given as an argument.
        """
        return _Proxy(cls, connector=connector, *args, **kwargs)
