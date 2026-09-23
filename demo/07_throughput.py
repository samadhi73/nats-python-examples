"""Chapter 4 - what ordering costs: throughput against max_ack_pending.

One variable changes between runs: `max_ack_pending`. The fetch batch follows
it (capped at 100), because a worker that asks for more messages than the
server is allowed to hand out stalls on every fetch - see 08_batch_mismatch.py.
Everything else stays fixed: 500 messages and a handler that sleeps 20 ms to
imitate I/O-bound work (a database write, an HTTP call).

Each run gets a fresh consumer name, so every run processes the same 500
messages from the beginning of the stream.

Usage: python demo/07_throughput.py <max_ack_pending>
"""

import asyncio
import os
import sys
import time

import nats
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig

SUBJECT = "part2.bench"
MESSAGES = 500
BATCH_CAP = 100
WORK_SECONDS = 0.02


async def handle(msg) -> None:
    await asyncio.sleep(WORK_SECONDS)  # I/O-bound work
    await msg.ack()


async def main() -> None:
    max_ack_pending = int(sys.argv[1])
    batch = min(max_ack_pending, BATCH_CAP)
    nc = await nats.connect(os.environ["NATS_URL"])
    js = nc.jetstream()

    sub = await js.pull_subscribe(
        SUBJECT,
        durable=f"bench-{max_ack_pending}",
        stream="PART2",
        config=ConsumerConfig(
            ack_policy=AckPolicy.EXPLICIT,
            ack_wait=60,
            max_ack_pending=max_ack_pending,
        ),
    )

    processed = 0
    biggest_batch = 0
    started = time.monotonic()

    while processed < MESSAGES:
        try:
            msgs = await sub.fetch(batch=batch, timeout=5)
        except NatsTimeoutError:
            break
        biggest_batch = max(biggest_batch, len(msgs))
        # all messages from one fetch are processed concurrently
        await asyncio.gather(*(handle(msg) for msg in msgs))
        processed += len(msgs)

    elapsed = time.monotonic() - started
    print(f"max_ack_pending={max_ack_pending:>5}  "
          f"processed={processed:>4}  "
          f"time={elapsed:6.2f} s  "
          f"throughput={processed / elapsed:7.1f} msg/s  "
          f"batch={batch}  largest fetch={biggest_batch}")

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
