"""Chapter 3 - slow consumer: losing messages without any failure.

The subscriber takes 50 ms per message, the publisher sends faster than that.
The client-side subscription buffer is made artificially small here
(`pending_msgs_limit=10`) so the effect shows up immediately - the default
holds 524288 messages, which is why in production the same thing happens
only under load, and therefore surprises people.

Without an `error_cb` passed to `nats.connect()`, dropped messages leave
no trace at all.
"""

import asyncio
import os

import nats

SUBJECT = "demo.slow"
PUBLISHED = 100

received = 0
dropped = []


async def handle(msg) -> None:
    global received
    received += 1
    await asyncio.sleep(0.05)  # pretend work: a database write, an HTTP call


async def on_error(err: Exception) -> None:
    dropped.append(err)


async def main() -> None:
    nc = await nats.connect(os.environ["NATS_URL"], error_cb=on_error)

    await nc.subscribe(SUBJECT, cb=handle, pending_msgs_limit=10)

    for i in range(PUBLISHED):
        await nc.publish(SUBJECT, f"msg-{i}".encode())
    await nc.flush()

    await asyncio.sleep(3)  # give the subscriber time to catch up

    print(f"published:            {PUBLISHED}")
    print(f"seen by our code:     {received}")
    print(f"error events:         {len(dropped)}")
    if dropped:
        print(f"first event:          {dropped[0]}")

    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
