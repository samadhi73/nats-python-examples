"""Chapter 5 - ordering against concurrency, and partitioning as the way out.

Two customers, four events each, published strictly interleaved and in order:
alice-1, bob-1, alice-2, bob-2, ... The first event of each customer takes
50 ms, the rest take 10 ms.

  concurrent   - one consumer over `part2.orders.>`, the whole batch handed
                 to `asyncio.gather`. Every event is in flight at once, so
                 the faster ones finish first and per-customer order is lost.
  partitioned  - one consumer per customer (`filter_subject` on the customer
                 token), each with `max_ack_pending=1`, both running at the
                 same time. Within a customer the work is sequential; the two
                 customers still overlap.

NATS has no partition key like Kafka's. The partition is a token in the
subject, chosen by the publisher - or computed by the server with subject
mapping (`{{partition(n, i)}}`), which is a server configuration change.

Usage: python demo/10_ordering.py <concurrent|partitioned>
"""

import asyncio
import os
import sys
import time

import nats
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig

CUSTOMERS = ("alice", "bob")
EVENTS = 4

done: list[str] = []
started_at = 0.0


def work_seconds(event: int) -> float:
    return 0.05 if event == 1 else 0.01


async def handle(msg) -> None:
    name = msg.data.decode()
    customer, event = name.split("-")
    await asyncio.sleep(work_seconds(int(event)))
    await msg.ack()
    done.append(name)
    print(f"  {(time.monotonic() - started_at) * 1000:6.0f} ms  finished {name}")


async def drain_consumer(js, durable: str, filter_subject: str,
                         max_ack_pending: int, expected: int) -> None:
    sub = await js.pull_subscribe(
        filter_subject,
        durable=durable,
        stream="PART2",
        config=ConsumerConfig(
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=60,
            max_ack_pending=max_ack_pending,
        ),
    )
    processed = 0
    while processed < expected:
        try:
            msgs = await sub.fetch(batch=max_ack_pending, timeout=3)
        except NatsTimeoutError:
            return
        await asyncio.gather(*(handle(msg) for msg in msgs))
        processed += len(msgs)


async def main() -> None:
    global started_at
    mode = sys.argv[1]
    nc = await nats.connect(os.environ["NATS_URL"])
    js = nc.jetstream()

    for event in range(1, EVENTS + 1):
        for customer in CUSTOMERS:
            await js.publish(
                f"part2.orders.{customer}.created",
                f"{customer}-{event}".encode(),
            )

    print(f"published in order: "
          f"{', '.join(f'{c}-{e}' for e in range(1, EVENTS + 1) for c in CUSTOMERS)}")
    print(f"mode={mode}")

    started_at = time.monotonic()
    if mode == "concurrent":
        await drain_consumer(js, "order-all", "part2.orders.>", 8, len(CUSTOMERS) * EVENTS)
    else:
        await asyncio.gather(*(
            drain_consumer(js, f"order-{c}", f"part2.orders.{c}.>", 1, EVENTS)
            for c in CUSTOMERS
        ))

    for customer in CUSTOMERS:
        seen = [n for n in done if n.startswith(customer)]
        ok = seen == sorted(seen, key=lambda n: int(n.split("-")[1]))
        print(f"{customer}: {' -> '.join(seen)}   {'in order' if ok else 'OUT OF ORDER'}")

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
