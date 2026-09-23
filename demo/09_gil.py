"""Chapter 3 - asyncio tasks are not parallelism.

The same worker, the same 100 messages, the same 20 ms of work per message,
the same `asyncio.gather` over the whole batch. The only difference is what
the work is:

  io   - `await asyncio.sleep(0.02)`, standing in for a database write or an
         HTTP call: the task yields, so the other 99 run in the meantime.
  cpu  - a busy loop of roughly the same duration: nothing yields, the GIL
         lets exactly one task run at a time, and the batch is processed
         one message after another.

Usage: python demo/09_gil.py <io|cpu>
"""

import asyncio
import os
import sys
import time

import nats
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig

SUBJECT = "part2.bench"
MESSAGES = 100

# calibrated below to take roughly 20 ms
CPU_ITERATIONS = 400_000


def cpu_work() -> int:
    total = 0
    for i in range(CPU_ITERATIONS):
        total += i * i
    return total


async def handle(msg, kind: str) -> None:
    if kind == "io":
        await asyncio.sleep(0.02)
    else:
        cpu_work()
    await msg.ack()


async def main() -> None:
    kind = sys.argv[1]
    nc = await nats.connect(os.environ["NATS_URL"])
    js = nc.jetstream()

    # how long one unit of work takes on its own
    t0 = time.monotonic()
    if kind == "io":
        await asyncio.sleep(0.02)
    else:
        cpu_work()
    single = time.monotonic() - t0

    sub = await js.pull_subscribe(
        SUBJECT,
        durable=f"gil-{kind}",
        stream="PART2",
        config=ConsumerConfig(
            ack_policy=AckPolicy.EXPLICIT, ack_wait=60, max_ack_pending=1000
        ),
    )

    processed = 0
    started = time.monotonic()
    while processed < MESSAGES:
        try:
            msgs = await sub.fetch(batch=100, timeout=5)
        except NatsTimeoutError:
            break
        await asyncio.gather(*(handle(msg, kind) for msg in msgs))
        processed += len(msgs)
    elapsed = time.monotonic() - started

    print(f"work={kind}  one unit={single * 1000:5.1f} ms  "
          f"{processed} messages concurrently in {elapsed:5.2f} s  "
          f"(sequential would be {single * processed:5.2f} s)")

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
