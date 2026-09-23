"""Chapter 5 - a worker that dies halfway through its work.

Fetches one message, prints it (that is our "work") and kills the process
without acknowledging. The server does not know the worker died - it only
knows that `ack_wait` has elapsed. After that it hands the same message
out again.

Run it several times in a row and compare `delivery #`.
"""

import asyncio
import os

import nats
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig

SUBJECT = "demo.orders.new"


async def main() -> None:
    nc = await nats.connect(os.environ["NATS_URL"])
    js = nc.jetstream()

    sub = await js.pull_subscribe(
        SUBJECT,
        durable="crash",
        stream="DEMO",
        # ack_wait deliberately short so the demo takes seconds, not half a minute
        config=ConsumerConfig(ack_policy=AckPolicy.EXPLICIT, ack_wait=5),
    )

    try:
        msgs = await sub.fetch(batch=1, timeout=5)
    except NatsTimeoutError:
        print("[crash] no messages", flush=True)
        await nc.drain()
        return

    msg = msgs[0]
    meta = msg.metadata
    print(f"[crash] processing: {msg.data.decode()}  "
          f"seq={meta.sequence.stream} delivery #{meta.num_delivered}", flush=True)
    print("[crash] ...and here the process dies, without ack()", flush=True)

    os._exit(1)  # hard death: no drain, no ack


if __name__ == "__main__":
    asyncio.run(main())
