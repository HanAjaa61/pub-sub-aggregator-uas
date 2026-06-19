"""
Pub-Sub Log Aggregator - Publisher Service
Simulates real-world event publishers including intentional duplicates.
"""

import asyncio
import json
import logging
import math
import os
import random
import time
import uuid
from datetime import datetime, timezone

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("publisher")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "20000"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.35"))  # 35% duplicates
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
CONCURRENCY = int(os.getenv("CONCURRENCY", "8"))
DELAY_MS = int(os.getenv("DELAY_MS", "0"))  # ms between batches

TOPICS = [
    "auth.login",
    "auth.logout",
    "payment.initiated",
    "payment.completed",
    "order.created",
    "order.shipped",
    "inventory.updated",
    "user.registered",
    "error.critical",
    "metrics.cpu",
]

SOURCES = ["service-a", "service-b", "service-c", "gateway", "worker"]

# ---------------------------------------------------------------------------
# Event generation
# ---------------------------------------------------------------------------

def make_event(topic: str, event_id: str | None = None) -> dict:
    """Generate a single event dict."""
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": random.choice(SOURCES),
        "payload": {
            "value": random.randint(1, 1000),
            "region": random.choice(["asia", "eu", "us"]),
            "version": "1.0",
        },
    }


def generate_events(total: int, dup_rate: float) -> list[dict]:
    """
    Generate `total` events where ~dup_rate fraction are duplicates.
    Unique event_ids are sampled from a pool; duplicates reuse existing IDs.
    """
    unique_count = math.ceil(total * (1 - dup_rate))
    dup_count = total - unique_count

    unique_events = [make_event(random.choice(TOPICS)) for _ in range(unique_count)]
    duplicate_events = [
        make_event(e["topic"], e["event_id"])
        for e in random.choices(unique_events, k=dup_count)
    ]

    all_events = unique_events + duplicate_events
    random.shuffle(all_events)
    logger.info(
        "Generated %d events: %d unique + %d duplicates (%.1f%% dup rate)",
        total, unique_count, dup_count, dup_rate * 100,
    )
    return all_events


# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------

async def publish_batch(
    client: httpx.AsyncClient,
    batch: list[dict],
    semaphore: asyncio.Semaphore,
    stats: dict,
    retries: int = 3,
) -> None:
    """Publish a batch with retry/backoff (at-least-once delivery)."""
    async with semaphore:
        for attempt in range(1, retries + 1):
            try:
                resp = await client.post(
                    TARGET_URL,
                    json={"events": batch},
                    timeout=30.0,
                )
                if resp.status_code == 202:
                    stats["sent"] += len(batch)
                    return
                else:
                    logger.warning("HTTP %s on attempt %d", resp.status_code, attempt)
            except Exception as exc:
                logger.warning("Attempt %d failed: %s", attempt, exc)

            if attempt < retries:
                await asyncio.sleep(0.5 * attempt)  # exponential-ish backoff

        stats["failed"] += len(batch)
        logger.error("Batch of %d failed after %d attempts", len(batch), retries)


async def run_publisher():
    all_events = generate_events(TOTAL_EVENTS, DUPLICATE_RATE)
    batches = [
        all_events[i : i + BATCH_SIZE]
        for i in range(0, len(all_events), BATCH_SIZE)
    ]

    stats = {"sent": 0, "failed": 0}
    semaphore = asyncio.Semaphore(CONCURRENCY)

    logger.info(
        "Publishing %d events in %d batches (batch_size=%d, concurrency=%d)",
        TOTAL_EVENTS, len(batches), BATCH_SIZE, CONCURRENCY,
    )

    start = time.perf_counter()
    async with httpx.AsyncClient() as client:
        tasks = [
            publish_batch(client, batch, semaphore, stats)
            for batch in batches
        ]
        await asyncio.gather(*tasks)

    elapsed = time.perf_counter() - start
    throughput = stats["sent"] / elapsed if elapsed > 0 else 0

    logger.info("=" * 60)
    logger.info("Publisher finished in %.2fs", elapsed)
    logger.info("  Sent:    %d events", stats["sent"])
    logger.info("  Failed:  %d events", stats["failed"])
    logger.info("  Throughput: %.0f events/sec", throughput)
    logger.info("=" * 60)

    return stats


# ---------------------------------------------------------------------------
# Entrypoint — wait for aggregator then publish
# ---------------------------------------------------------------------------

async def wait_for_aggregator(max_wait: int = 60):
    """Poll /health until aggregator is ready."""
    async with httpx.AsyncClient() as client:
        for i in range(max_wait):
            try:
                resp = await client.get(
                    TARGET_URL.replace("/publish", "/health"),
                    timeout=3.0,
                )
                if resp.status_code == 200:
                    logger.info("Aggregator is ready.")
                    return
            except Exception:
                pass
            logger.info("Waiting for aggregator… (%d/%d)", i + 1, max_wait)
            await asyncio.sleep(1)
    raise RuntimeError("Aggregator did not become ready in time")


if __name__ == "__main__":
    asyncio.run(wait_for_aggregator())
    asyncio.run(run_publisher())
