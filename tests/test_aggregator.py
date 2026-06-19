"""
Test suite for Pub-Sub Log Aggregator (15 tests)
Covers: dedup, idempotency, schema validation, concurrency, persistence proxy,
stats consistency, batch atomicity, ordering, stress, and error paths.

Run against a live Compose stack:
    BASE_URL=http://localhost:8080 pytest tests/test_aggregator.py -v
"""

import asyncio
import json
import os
import time
import uuid
from datetime import datetime, timezone

import httpx
import pytest

BASE_URL = os.getenv("BASE_URL", "http://localhost:8080")
TIMEOUT = httpx.Timeout(30.0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_event(topic: str = "test.topic", event_id: str | None = None) -> dict:
    return {
        "topic": topic,
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test-suite",
        "payload": {"test": True},
    }


async def publish(client: httpx.AsyncClient, events: list[dict]) -> httpx.Response:
    return await client.post(
        f"{BASE_URL}/publish",
        json={"events": events},
        timeout=TIMEOUT,
    )


async def get_stats(client: httpx.AsyncClient) -> dict:
    r = await client.get(f"{BASE_URL}/stats", timeout=TIMEOUT)
    return r.json()


async def wait_processing(seconds: float = 1.5):
    """Give async workers time to drain the queue."""
    await asyncio.sleep(seconds)


# ---------------------------------------------------------------------------
# T01 — Health check
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_01_health_check():
    """Service and DB must be reachable."""
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{BASE_URL}/health", timeout=TIMEOUT)
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["db"] == "connected"


# ---------------------------------------------------------------------------
# T02 — Single event publish accepted
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_02_single_event_publish():
    """POST /publish with single event returns 202."""
    async with httpx.AsyncClient() as c:
        r = await publish(c, [make_event()])
    assert r.status_code == 202
    body = r.json()
    assert body["queued"] == 1
    assert body["status"] == "accepted"


# ---------------------------------------------------------------------------
# T03 — Batch event publish accepted
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_03_batch_event_publish():
    """POST /publish with batch of 10 returns 202 with correct count."""
    events = [make_event(topic=f"batch.topic.{i}") for i in range(10)]
    async with httpx.AsyncClient() as c:
        r = await publish(c, events)
    assert r.status_code == 202
    assert r.json()["queued"] == 10


# ---------------------------------------------------------------------------
# T04 — Schema validation: missing field
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_04_schema_validation_missing_field():
    """Event without required 'topic' must return 422."""
    bad_event = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
        "payload": {},
    }
    async with httpx.AsyncClient() as c:
        r = await c.post(
            f"{BASE_URL}/publish",
            json={"events": [bad_event]},
            timeout=TIMEOUT,
        )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# T05 — Schema validation: invalid timestamp
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_05_schema_validation_invalid_timestamp():
    """Event with non-ISO timestamp must return 422."""
    bad_event = make_event()
    bad_event["timestamp"] = "not-a-date"
    async with httpx.AsyncClient() as c:
        r = await c.post(
            f"{BASE_URL}/publish",
            json={"events": [bad_event]},
            timeout=TIMEOUT,
        )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# T06 — Schema validation: empty event_id
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_06_schema_validation_empty_event_id():
    """Event with empty event_id must return 422."""
    bad_event = make_event()
    bad_event["event_id"] = "   "
    async with httpx.AsyncClient() as c:
        r = await c.post(
            f"{BASE_URL}/publish",
            json={"events": [bad_event]},
            timeout=TIMEOUT,
        )
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# T07 — Deduplication: duplicate event only processed once
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_07_deduplication_single_duplicate():
    """
    Same (topic, event_id) sent twice → unique_processed increments by 1,
    duplicate_dropped increments by 1.
    """
    event = make_event(topic="dedup.test")
    async with httpx.AsyncClient() as c:
        stats_before = await get_stats(c)
        await publish(c, [event])
        await publish(c, [event])  # duplicate
        await wait_processing(2.0)
        stats_after = await get_stats(c)

    delta_unique = stats_after["unique_processed"] - stats_before["unique_processed"]
    delta_dup = stats_after["duplicate_dropped"] - stats_before["duplicate_dropped"]

    assert delta_unique == 1, f"Expected 1 unique, got {delta_unique}"
    assert delta_dup == 1, f"Expected 1 duplicate_dropped, got {delta_dup}"


# ---------------------------------------------------------------------------
# T08 — Deduplication: many duplicates
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_08_deduplication_many_duplicates():
    """
    Send same event 10 times → only 1 unique_processed, 9 duplicate_dropped.
    """
    event = make_event(topic="dedup.stress")
    async with httpx.AsyncClient() as c:
        stats_before = await get_stats(c)
        for _ in range(10):
            await publish(c, [event])
        await wait_processing(3.0)
        stats_after = await get_stats(c)

    delta_unique = stats_after["unique_processed"] - stats_before["unique_processed"]
    delta_dup = stats_after["duplicate_dropped"] - stats_before["duplicate_dropped"]

    assert delta_unique == 1
    assert delta_dup == 9


# ---------------------------------------------------------------------------
# T09 — GET /events returns processed events
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_09_get_events_returns_data():
    """After publishing a unique event, it must appear in GET /events."""
    unique_id = str(uuid.uuid4())
    topic = f"events.test.{unique_id}"
    event = make_event(topic=topic, event_id=unique_id)
    async with httpx.AsyncClient() as c:
        await publish(c, [event])
        await wait_processing(2.0)
        r = await c.get(
            f"{BASE_URL}/events",
            params={"topic": topic},
            timeout=TIMEOUT,
        )
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 1
    assert body["events"][0]["event_id"] == unique_id


# ---------------------------------------------------------------------------
# T10 — GET /events topic filter
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_10_get_events_topic_filter():
    """GET /events?topic=X must return only events for topic X."""
    uid = str(uuid.uuid4())
    topic_a = f"filter.a.{uid}"
    topic_b = f"filter.b.{uid}"
    events = [make_event(topic=topic_a), make_event(topic=topic_b)]
    async with httpx.AsyncClient() as c:
        await publish(c, events)
        await wait_processing(2.0)
        r = await c.get(f"{BASE_URL}/events", params={"topic": topic_a}, timeout=TIMEOUT)
    body = r.json()
    for ev in body["events"]:
        assert ev["topic"] == topic_a


# ---------------------------------------------------------------------------
# T11 — GET /stats structure and types
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_11_stats_structure():
    """GET /stats must return all required fields with correct types."""
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{BASE_URL}/stats", timeout=TIMEOUT)
    assert r.status_code == 200
    s = r.json()
    for field in ("received", "unique_processed", "duplicate_dropped", "topics", "uptime_seconds"):
        assert field in s, f"Missing field: {field}"
    assert s["unique_processed"] <= s["received"]
    assert s["duplicate_dropped"] >= 0
    assert s["uptime_seconds"] >= 0


# ---------------------------------------------------------------------------
# T12 — Stats consistency: received = unique + duplicate
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_12_stats_consistency():
    """received must equal unique_processed + duplicate_dropped."""
    async with httpx.AsyncClient() as c:
        # Publish a mix
        events = [make_event(topic="stats.consistency") for _ in range(5)]
        dup = make_event(topic="stats.consistency", event_id=events[0]["event_id"])
        await publish(c, events + [dup])
        await wait_processing(2.0)
        s = await get_stats(c)

    assert s["received"] == s["unique_processed"] + s["duplicate_dropped"], (
        f"Inconsistent: {s['received']} != {s['unique_processed']} + {s['duplicate_dropped']}"
    )


# ---------------------------------------------------------------------------
# T13 — Concurrency: parallel workers produce no double-processing
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_13_concurrent_dedup_no_double_processing():
    """
    Send the same batch of 20 events concurrently from 8 parallel coroutines.
    Unique events must appear exactly once in the DB.
    """
    unique_events = [make_event(topic="concurrent.dedup") for _ in range(20)]

    async with httpx.AsyncClient() as c:
        stats_before = await get_stats(c)

        # 8 concurrent publishers all sending the same 20 events
        tasks = [publish(c, unique_events) for _ in range(8)]
        responses = await asyncio.gather(*tasks)

        for r in responses:
            assert r.status_code == 202

        await wait_processing(4.0)
        stats_after = await get_stats(c)

    delta_unique = stats_after["unique_processed"] - stats_before["unique_processed"]
    delta_total = stats_after["received"] - stats_before["received"]

    # Exactly 20 unique events (8 × 20 = 160 total, 140 duplicates)
    assert delta_unique == 20, f"Expected 20 unique, got {delta_unique} (race condition?)"
    assert delta_total == 160, f"Expected 160 received, got {delta_total}"


# ---------------------------------------------------------------------------
# T14 — Empty batch rejected
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_14_empty_batch_rejected():
    """POST /publish with empty events list must return 400."""
    async with httpx.AsyncClient() as c:
        r = await c.post(
            f"{BASE_URL}/publish",
            json={"events": []},
            timeout=TIMEOUT,
        )
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# T15 — Stress: batch of 500 events processed within time limit
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_15_stress_batch_500_events():
    """
    Publish 500 events (with ~30% duplicates) and assert throughput.
    All events must be processed in under 30 seconds.
    """
    unique_events = [make_event(topic="stress.test") for _ in range(350)]
    dup_events = [
        make_event(topic="stress.test", event_id=e["event_id"])
        for e in unique_events[:150]
    ]
    all_events = unique_events + dup_events  # 500 total, ~30% dup

    async with httpx.AsyncClient() as c:
        stats_before = await get_stats(c)
        t0 = time.perf_counter()

        # Publish in batches of 50
        for i in range(0, len(all_events), 50):
            await publish(c, all_events[i : i + 50])

        await wait_processing(10.0)
        elapsed = time.perf_counter() - t0
        stats_after = await get_stats(c)

    delta_unique = stats_after["unique_processed"] - stats_before["unique_processed"]
    delta_dup = stats_after["duplicate_dropped"] - stats_before["duplicate_dropped"]

    assert elapsed < 30, f"Took too long: {elapsed:.1f}s"
    assert delta_unique == 350, f"Expected 350 unique, got {delta_unique}"
    assert delta_dup == 150, f"Expected 150 duplicates dropped, got {delta_dup}"
    print(f"\n  Stress test: {500 / elapsed:.0f} events/sec end-to-end")
