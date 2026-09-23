"""Chapter 5 - publisher-side deduplication (`Nats-Msg-Id`).

The DEMO stream has a two-minute deduplication window (`--dupe-window=2m`).
If a second message with the same `Nats-Msg-Id` arrives within that window,
the server does not store it and says so in the acknowledgement
(`duplicate=True`).

This protects against a repeated *publish* - not against a repeated
*delivery*. Those two sources of duplicates are independent.
"""

import asyncio
import os

import nats

SUBJECT = "demo.orders.new"
MSG_ID = "order-7777"


async def main() -> None:
    nc = await nats.connect(os.environ["NATS_URL"])
    js = nc.jetstream()

    info = await js.stream_info("DEMO")
    print(f"stream deduplication window: {info.config.duplicate_window} s")
    print(f"messages in stream before publishing: {info.state.messages}")

    for attempt in (1, 2):
        ack = await js.publish(
            SUBJECT,
            b"order 7777, amount 100",
            headers={"Nats-Msg-Id": MSG_ID},
        )
        print(f"publish #{attempt}: seq={ack.seq} duplicate={ack.duplicate}")

    info = await js.stream_info("DEMO")
    print(f"messages in stream after two publishes: {info.state.messages}")

    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
