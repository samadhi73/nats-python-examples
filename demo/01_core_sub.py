"""Chapter 3 - a plain Core NATS subscriber.

There is no acknowledgement of any kind here: the subscriber only listens and
prints. It runs as the `core-sub` service in docker compose so that it can be
killed (`docker compose stop core-sub`) and brought back
(`docker compose start core-sub`).
"""

import asyncio
import os

import nats

SUBJECT = "demo.core"


async def main() -> None:
    nc = await nats.connect(os.environ["NATS_URL"])
    sub = await nc.subscribe(SUBJECT)
    print(f"[sub] listening on {SUBJECT}", flush=True)

    try:
        async for msg in sub.messages:
            print(f"[sub] received: {msg.data.decode()}", flush=True)
    finally:
        await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
