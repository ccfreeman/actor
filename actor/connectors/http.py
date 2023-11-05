import logging
import asyncio
from typing import Any
from uuid import uuid4

from pydantic import BaseModel

from .connector import AbstractConnector
from ..config import CONFIG

logger = logging.getLogger(__name__)


class HttpConnector(AbstractConnector):
    """Connector class for HTTP messaging."""

    def __init__(self, service_name: str):
        self.service_name = service_name
        self.base_url = f"http://{service_name}:8000"

    @staticmethod
    def __parse_kwargs(kwargs) -> dict[str, Any]:
        resp = {"params": {}}
        for key, value in kwargs.items():
            if isinstance(value, BaseModel):
                resp["json"] = value.model_dump()
            else:
                resp["params"].update({key: value})
        return resp

    async def __call__(self, func: str, method: str, **kwargs) -> Any:
        url = f"{self.base_url}/{func}"
        logger.info("calling: %s", url)
        call_method = (
            CONFIG.CONN.session.get if method == "GET" else CONFIG.CONN.session.post
        )
        xid = str(uuid4())
        async with call_method(
            url, **self.__parse_kwargs(kwargs), headers={"x-correlation-id": xid}
        ) as resp:
            return await resp.json()

    async def __ready(self) -> bool:
        logger.info("Health checking app")
        async with CONFIG.CONN.session.get(f"{self.base_url}/healthz") as resp:
            return await resp.json()

    async def ready(self) -> None:
        while not await self.__ready():
            await asyncio.sleep(0.05)
