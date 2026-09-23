"""Chapter 3 - publishing to Core NATS.

Arguments: the payloads of consecutive messages. `flush()` forces a round trip
to the server, so once it returns we know the server has received the bytes -
and nothing more than that.
"""

import asyncio
import os
import sys

import nats

SUBJECT = "demo.core"


async def main() -> None:
    nc = await nats.connect(os.environ["NATS_URL"])

    for text in sys.argv[1:]:
        await nc.publish(SUBJECT, text.encode())
        print(f"[pub] sent: {text}", flush=True)

    await nc.flush()
    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
