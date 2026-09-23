"""Chapter 4 - max_ack_pending is one pool per consumer, not a limit per worker.

One consumer with `max_ack_pending=5` and two subscriptions bound to it, as if
two worker processes shared the same `durable`. Neither acknowledges anything.
The first takes the whole allowance; the second is left with nothing, even
though the stream holds more messages.

The consumer is deleted at the end, so every run starts from the same state.
"""

import asyncio
import os

import nats
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig

SUBJECT = "part2.ceiling"
DURABLE = "ceiling"
MAX_ACK_PENDING = 5
MESSAGES = 10


async def main() -> None:
    nc = await nats.connect(os.environ["NATS_URL"])
    js = nc.jetstream()

    for i in range(MESSAGES):
        await js.publish(SUBJECT, f"task-{i}".encode())

    config = ConsumerConfig(
        ack_policy=AckPolicy.EXPLICIT, ack_wait=60, max_ack_pending=MAX_ACK_PENDING
    )
    subs = [
        await js.pull_subscribe(SUBJECT, durable=DURABLE, stream="PART2", config=config)
        for _ in range(2)
    ]

    handed_out = 0
    for n, sub in enumerate(subs, start=1):
        try:
            msgs = await sub.fetch(batch=MAX_ACK_PENDING, timeout=2)
        except NatsTimeoutError:
            msgs = []
        handed_out += len(msgs)
        print(f"subscription {n}: asked for {MAX_ACK_PENDING}, got {len(msgs)} (not acked)")

    info = await js.consumer_info("PART2", DURABLE)
    print(f"total handed out: {handed_out}   max_ack_pending={MAX_ACK_PENDING}   "
          f"server num_ack_pending={info.num_ack_pending}")

    await js.delete_consumer("PART2", DURABLE)
    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
