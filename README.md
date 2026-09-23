# NATS in Python — runnable examples

Companion code for a two-part article series about [NATS](https://nats.io) from
a Python developer's point of view:

- **Part 1 — delivery guarantees:** why Core NATS loses messages silently and
  what JetStream charges for not losing them (scripts `01`–`05`).
- **Part 2 — consumer concurrency:** what happens when one worker stops keeping
  up — shared consumers vs fan-out, `max_ack_pending`, asyncio vs the GIL,
  ordering vs concurrency (scripts `06`–`10`).

Every console output quoted in the articles comes from running these scripts.

Versions: nats-server 2.11.17, `nats-py` 2.15.0, Python 3.13. The client code is
asyncio-only.

## Setup

Everything runs in Docker: a NATS server with JetStream, a container with the
`nats` CLI (`nats-box`) and a Python container with `nats-py`. The repository is
mounted into the Python containers at `/app`, so edits are picked up without a
rebuild.

```sh
docker compose up -d
```

The streams are created by an operator with the CLI, not by the application
code — the articles explain why.

```sh
# Part 1
docker compose exec nats-box nats stream add DEMO \
  --subjects="demo.orders.>" --storage=file --max-age=1h --dupe-window=2m --defaults

# Part 2
docker compose exec nats-box nats stream add PART2 \
  --subjects="part2.>" --storage=file --max-age=1h --defaults
```

Scripts `07`, `08` and `09` read from `part2.bench` and do not publish anything
themselves, so fill it once with 500 messages:

```sh
docker compose exec nats-box nats pub part2.bench --count 500 "msg {{Count}}"
```

Run any script with:

```sh
docker compose run --rm publisher python demo/<script>.py [arguments]
```

## Scripts

| Script | Part | What it shows |
|---|---|---|
| `01_core_pub.py`, `01_core_sub.py` | 1 | Core NATS: a message sent while the subscriber is down is simply gone. The subscriber runs as the `core-sub` service. |
| `02_slow_sub.py` | 1 | Slow consumer: messages dropped on the client side, visible only through `error_cb`. |
| `03_js_pub.py`, `03_js_sub.py` | 1 | JetStream: a publish that gets an acknowledgement, and a durable pull consumer (the `js-worker` service). |
| `04_crash_worker.py` | 1 | A worker dies before `ack()`; the message comes back after `ack_wait`. |
| `05_dedup_pub.py` | 1 | Publisher-side deduplication with `Nats-Msg-Id`. |
| `06_fanout_vs_shared.py shared\|fanout` | 2 | One consumer name shared by two workers splits the work; two names duplicate it. |
| `07_throughput.py <max_ack_pending>` | 2 | Throughput against `max_ack_pending` (try 1, 10, 100, 1000). |
| `08_batch_mismatch.py` | 2 | `fetch(batch=100)` against `max_ack_pending=1`: every message costs a full timeout. |
| `09_gil.py io\|cpu` | 2 | `asyncio.gather` helps with I/O-bound work and does nothing for CPU-bound work. |
| `10_ordering.py concurrent\|partitioned` | 2 | Concurrency breaks per-key ordering; one consumer per key restores it. |

Every new consumer starts from the beginning of the stream. `06` and `10`
publish fresh messages on every run, so a second run also sees the messages of
the first one — `06_fanout_vs_shared.py fanout` run after `shared` reports 40
per worker instead of 20. For the numbers quoted in the article, remove and
recreate `PART2` (and refill `part2.bench`) before each of those runs.

## Inspecting and cleaning up

```sh
docker compose exec nats-box nats stream ls
docker compose exec nats-box nats consumer report DEMO   # ack pending, redeliveries per consumer
curl -s 'http://localhost:8222/jsz?consumers=true'      # the same over HTTP

docker compose exec nats-box nats stream rm PART2 -f     # removes the stream and its consumers
docker compose down -v                                   # removes everything, including stored data
```

## License

MIT
