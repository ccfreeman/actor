from typing import Any
from abc import ABC, abstractmethod


class AbstractConnector(ABC):
    """Base for the connector classes"""

    def __init__(self, service_name: str) -> None:
        ...

    @abstractmethod
    async def ready(self) -> None:
        ...

    @abstractmethod
    async def __call__(self, func: str, method: str, **kwargs) -> Any:
        ...
