import logging
from typing import Any

import aiohttp
from pydantic import BaseModel

from .connector import Connector


logger = logging.getLogger(__name__)


class HttpConnector(Connector):
    """Connector class for HTTP messaging."""

    def __init__(self, service_name: str):
        self.service_name = service_name
        self.base_url = f"http://{service_name}:8000"
        self._session: aiohttp.ClientSession = None

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()

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
    def session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def __call__(self, func: str, method: str, **kwargs):
        url = f"{self.base_url}/{func}"
        logger.info("calling: %s", url)
        call_method = self.session.get if method == "GET" else self.session.post
        async with call_method(url, **self.__parse_kwargs(kwargs)) as resp:
            return await resp.json()
