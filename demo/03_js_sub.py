"""Chapter 4 - a JetStream worker: a named (durable) pull consumer.

`durable="orders"` is the name of the consumer on the server. Its state (how
far it got, what is still unacknowledged) lives on the server, so restarting
this process loses nothing.

An empty `fetch` ends with an exception, not an empty list - hence the
`try/except` around every fetch. We catch `nats.errors.TimeoutError`, because
with `batch > 1` that is the exception actually raised, not the more obvious
`FetchTimeoutError` (which is its subclass, so one `except` covers both).

Order matters: do the work first, then `ack()`.
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
        durable="orders",
        stream="DEMO",  # given explicitly: saves one round trip at startup
        config=ConsumerConfig(ack_policy=AckPolicy.EXPLICIT, ack_wait=30),
    )
    print("[worker] waiting for messages", flush=True)

    while True:
        try:
            msgs = await sub.fetch(batch=10, timeout=5)
        except NatsTimeoutError:
            continue  # no new messages is a normal state, not a failure

        for msg in msgs:
            print(f"[worker] processing: {msg.data.decode()}", flush=True)
            await msg.ack()


if __name__ == "__main__":
    asyncio.run(main())
