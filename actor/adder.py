import logging
import asyncio
from typing import Optional

from pydantic import BaseModel

from .config import CONFIG
from .actor import Actor


logger = logging.getLogger(__name__)


class MultiplyOperands(BaseModel):
    """A bunch of numbers we want multiplied"""

    ops: list[int | float]


class Adder(Actor):
    """Adder class."""

    def __init__(self, special_number: int = 0):
        self.special_number = special_number

    @Actor.route(methods=["GET"])
    async def add(self, x: int, y: int) -> int:
        """Add two numbers."""
        logger.info("adding some shit")
        return x + y + self.special_number

    @Actor.route(methods=["POST"])
    async def subtract(self, x: int, y: int) -> int:
        """Subtract two numbers."""
        logger.info("subtracting some shit")
        return x - y

    @Actor.route(methods=["POST"])
    async def multiply(
        self, x: int | float, y: int | float, ops: Optional[MultiplyOperands] = None
    ) -> int | float:
        res = 1
        if ops is not None:
            for v in ops.ops:
                res *= v
        return res * x * y


async def main():
    await Adder.serve()


if __name__ == "__main__":
    asyncio.run(main())
