import logging
import asyncio
import random
import signal

from .config import CONFIG
from .adder import Adder, MultiplyOperands


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
    try:
        semaphore = asyncio.BoundedSemaphore(100)
        async with Adder.proxy() as adder:
            while True:
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
    except:
        logger.exception("looper error")
    logger.info("looper exiting")


async def main():
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown.set)
    looper_task = asyncio.create_task(looper())
    await shutdown.wait()
    looper_task.cancel()
    await looper_task
    logger.info("bye!")


if __name__ == "__main__":
    asyncio.run(main())
