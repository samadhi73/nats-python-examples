"""Chapter 2 - the test that tells fan-out from a shared pool of work.

Two workers in one process, so the whole thing runs in a single command.
The only difference between the two runs is the consumer name:

  shared:   both workers use durable="shared"       -> one pool, work is split
  fan-out:  each worker uses durable="fanout-<id>"  -> two pools, work is doubled

The server counters look similar in both cases. What distinguishes them is
how many messages each worker actually processed.
"""

import asyncio
import os
import sys

import nats
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js.api import AckPolicy, ConsumerConfig

SUBJECT = "part2.work"
MESSAGES = 20


async def worker(js, name: str, durable: str, counts: dict) -> None:
    sub = await js.pull_subscribe(
        SUBJECT,
        durable=durable,
        stream="PART2",
        config=ConsumerConfig(ack_policy=AckPolicy.EXPLICIT, ack_wait=30),
    )
    while True:
        try:
            msgs = await sub.fetch(batch=5, timeout=2)
        except NatsTimeoutError:
            return  # nothing left for us
        for msg in msgs:
            counts[name] += 1
            await asyncio.sleep(0.01)
            await msg.ack()


async def main() -> None:
    mode = sys.argv[1]  # "shared" or "fanout"
    nc = await nats.connect(os.environ["NATS_URL"])
    js = nc.jetstream()

    for i in range(MESSAGES):
        await js.publish(SUBJECT, f"task-{i}".encode())

    counts = {"worker-a": 0, "worker-b": 0}
    if mode == "shared":
        durables = {"worker-a": "shared", "worker-b": "shared"}
    else:
        durables = {"worker-a": "fanout-a", "worker-b": "fanout-b"}

    await asyncio.gather(
        *(worker(js, name, durables[name], counts) for name in counts)
    )

    total = sum(counts.values())
    print(f"mode={mode}  published={MESSAGES}  processed in total={total}")
    for name, n in counts.items():
        print(f"  {name}: {n}")

    await nc.close()


if __name__ == "__main__":
    asyncio.run(main())
