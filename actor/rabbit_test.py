import asyncio
import aio_pika
import aio_pika.abc


async def main(loop):
    # Connect with the givien parameters is also valiable.
    # aio_pika.connect_robust(host="host", login="login", password="password")
    # You can only choose one option to create a connection, url or kw-based params.
    connection = await aio_pika.connect_robust(
        "amqp://guest:guest@localhost:5672/", loop=loop
    )

    async with connection:
        queue_name = "test_queue"

        # Creating channel
        channel: aio_pika.abc.AbstractChannel = await connection.channel()

        # Declaring queue
        queue: aio_pika.abc.AbstractQueue = await channel.declare_queue(
            queue_name,
            auto_delete=True
        )

        async with queue.iterator() as queue_iter:
            # Cancel consuming after __aexit__
            async for message in queue_iter:
                async with message.process(requeue=True):
                    print(message.body)
                    if queue.name in message.body.decode():
                        break
                    


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
