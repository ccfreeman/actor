from typing import Any
from abc import ABC, abstractmethod


class Connector(ABC):
    """Base for the connector classes"""

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def __call__(self, func: str, method: str, **kwargs) -> Any: ...
