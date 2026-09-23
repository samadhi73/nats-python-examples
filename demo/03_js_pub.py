"""Chapter 4 - publishing to JetStream.

The difference from Core NATS fits in one line: `js.publish()` returns an
acknowledgement from the server (PubAck) with the stream name and the
sequence number. Only that acknowledgement means the message was stored.
"""

import asyncio
import os
import sys

import nats

SUBJECT = "demo.orders.new"


async def main() -> None:
    nc = await nats.connect(os.environ["NATS_URL"])
    js = nc.jetstream()

    for text in sys.argv[1:]:
        ack = await js.publish(SUBJECT, text.encode())
        print(f"[pub] stored: {text}  stream={ack.stream} seq={ack.seq}", flush=True)

    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
