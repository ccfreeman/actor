import logging
import functools
from typing import Self, TypeVar, Callable, Generator
from abc import ABC

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

from .config import CONFIG
from .connectors import HttpConnector


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


def route(methods: list[str]):
    """Mark a method on a class for exposure. This is handled by adding a `methods`
    attribute to the function, which is then used by the `app_wrapper` class to add the
    function to the FastAPI app. This way we can detect which methods a user wants
    exposed through the FastAPI app.
    """

    def wrapper(func):
        func.methods = methods
        return func

    return wrapper


class ActorServer:
    """Attach a FastAPI instance to a class. This class is used to create a FastAPI
    instance that exposes the methods of the class passed in. This is done by using the
    `get_funcs` helper function to get all the functions on the class, and then adding
    them to the FastAPI app.
    """

    def __init__(self, cls: type[T], *args, **kwargs):
        self.cls = cls
        self.obj = cls(*args, **kwargs)
        self.app = FastAPI(title=self.cls.__name__, description=self.cls.__doc__)
        for func in get_funcs(self.obj):
            self.app.add_api_route(
                path=f"/{func.__name__}",
                tags=["Call Methods"],
                endpoint=func,
                response_model=func.__annotations__["return"],
                methods=func.methods,
                description=func.__doc__,
            )

    async def run(self) -> None:
        """Run the FastAPI app."""
        server = uvicorn.Server(
            # uvicorn.Config(app=self.app, host="127.0.0.1", port=8000, reload=True)
            uvicorn.Config(app=self.app, host="0.0.0.0", port=8000)
        )
        await server.serve()


class _Proxy:
    """Proxy class for the client. This class is used to create a proxy object that
    exposes the same methods as the class passed in, but instead of calling the methods
    on the object, it calls them on the server. This is done by using the `__getattr__`
    method to return a partial function that calls the server.
    """

    def __init__(self, cls: type[T]):
        functools.update_wrapper(self, cls)
        self.cls = cls
        self.connector = HttpConnector(service_name=self.cls.__name__)
        for func in get_funcs(self.cls):
            setattr(
                self,
                func.__name__,
                functools.partial(
                    self.connector,
                    func=func.__name__,
                    method=func.methods[0],
                ),
            )

    async def __aenter__(self) -> T:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.connector.close()


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
        await self.close()

    async def close(self) -> None:
        ...

    @classmethod
    async def serve(cls: type[T], *args, **kwargs) -> None:
        """Serve the actor. This method is used to start the FastAPI server."""
        server = ActorServer(cls, *args, **kwargs)
        await server.run()

    @classmethod
    def proxy(cls: type[T], *args, **kwargs) -> T:
        """Proxy class for the client. This class is used to create a proxy object that
        exposes the same methods as the class passed in, but instead of calling the
        methods on the object, it calls them on the server. This is done by using the
        `__getattr__` method to return a partial function that calls the server.

        This is a factory method that returns an initialized class instance of the
        proxied class given as an argument.
        """
        return _Proxy(cls)
