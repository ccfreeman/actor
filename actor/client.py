import logging
import asyncio
import random
import signal

from .config import CONFIG
from .adder import Adder, MultiplyOperands
from .connectors import HttpConnector, RabbitMqConnector


logger = logging.getLogger(__name__)


async def ops(
    adder: Adder, a: int | float, b: int | float, semaphore: asyncio.BoundedSemaphore
) -> None:
    logger.info("Calling")
    async with semaphore:
        async with asyncio.TaskGroup() as task_group:
            mult = task_group.create_task(adder.multiply(x=a, y=b))
            add = task_group.create_task(adder.add(x=a, y=b))
            sub = task_group.create_task(adder.subtract(x=a, y=b))
    logger.info("%r * %r = %r", a, b, mult.result())
    logger.info("%r + %r = %r", a, b, add.result())
    logger.info("%r - %r = %r", a, b, sub.result())


async def looper():
    semaphore = asyncio.BoundedSemaphore(100)
    async with Adder.proxy(connector=RabbitMqConnector) as adder:
        try:
            await adder.ready()
            while True:
                r = await adder.add(x=1, y=2)
                logger.info("The answer is %r", r)
                await asyncio.gather(
                    *(
                        ops(
                            adder,
                            a=random.randint(1, 100),
                            b=random.randint(1, 100),
                            semaphore=semaphore,
                        )
                        for _ in range(1)
                    )
                )
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            logger.info("looper cancelled")
    logger.info("looper exiting")


async def main():
    loop = asyncio.get_running_loop()
    looper_task = asyncio.create_task(looper())
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, looper_task.cancel)
    await looper_task
    logger.info("bye!")


if __name__ == "__main__":
    asyncio.run(main())
