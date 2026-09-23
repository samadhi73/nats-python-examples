"""Chapter 4 - asking for more than the server is allowed to give.

`fetch(batch=100)` against a consumer with `max_ack_pending=1`: the server
hands out one message and then stops, because nothing may be in flight
alongside it. The client keeps waiting for the remaining 99 until the fetch
times out.

The result is not "a bit slower". Every message costs a full fetch timeout.
"""

import asyncio
import os
import time

import nats
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig

SUBJECT = "part2.bench"
MESSAGES = 5
FETCH_TIMEOUT = 2


async def main() -> None:
    nc = await nats.connect(os.environ["NATS_URL"])
    js = nc.jetstream()

    sub = await js.pull_subscribe(
        SUBJECT,
        durable="mismatch",
        stream="PART2",
        config=ConsumerConfig(
            ack_policy=AckPolicy.EXPLICIT, ack_wait=60, max_ack_pending=1
        ),
    )

    processed = 0
    started = time.monotonic()
    while processed < MESSAGES:
        t0 = time.monotonic()
        try:
            msgs = await sub.fetch(batch=100, timeout=FETCH_TIMEOUT)
        except NatsTimeoutError:
            print("fetch: timed out with nothing")
            break
        print(f"fetch: asked for 100, got {len(msgs)} after {time.monotonic() - t0:.2f} s")
        for msg in msgs:
            await msg.ack()
        processed += len(msgs)

    elapsed = time.monotonic() - started
    print(f"processed={processed} in {elapsed:.2f} s "
          f"-> {processed / elapsed:.1f} msg/s")

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
