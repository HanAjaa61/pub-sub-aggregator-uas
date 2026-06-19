"""
Pub-Sub Log Aggregator - Aggregator Service
Handles event ingestion, deduplication, and statistics.
"""

import asyncio
import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

import asyncpg
import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("aggregator")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@storage:5432/db")
BROKER_URL = os.getenv("BROKER_URL", "redis://broker:6379")
NUM_WORKERS = int(os.getenv("NUM_WORKERS", "4"))
START_TIME = time.time()

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class EventPayload(BaseModel):
    """Minimal event schema as per spec."""
    topic: str = Field(..., min_length=1, max_length=255)
    event_id: str = Field(..., min_length=1, max_length=255)
    timestamp: str = Field(...)
    source: str = Field(..., min_length=1, max_length=255)
    payload: dict = Field(default_factory=dict)

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except ValueError:
            raise ValueError("timestamp harus berformat ISO 8601")
        return v

    @field_validator("event_id")
    @classmethod
    def validate_event_id(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("event_id tidak boleh kosong")
        return v.strip()


class PublishRequest(BaseModel):
    """Support single or batch event."""
    events: list[EventPayload]


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

async def init_db(pool: asyncpg.Pool):
    """Create tables with unique constraint for idempotent dedup."""
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                id          BIGSERIAL PRIMARY KEY,
                topic       TEXT NOT NULL,
                event_id    TEXT NOT NULL,
                source      TEXT NOT NULL,
                timestamp   TIMESTAMPTZ NOT NULL,
                payload     JSONB,
                received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT uq_topic_event UNIQUE (topic, event_id)
            );

            CREATE INDEX IF NOT EXISTS idx_pe_topic
                ON processed_events (topic);

            CREATE INDEX IF NOT EXISTS idx_pe_received_at
                ON processed_events (received_at DESC);

            -- Audit log for duplicate detections
            CREATE TABLE IF NOT EXISTS audit_log (
                id          BIGSERIAL PRIMARY KEY,
                topic       TEXT NOT NULL,
                event_id    TEXT NOT NULL,
                action      TEXT NOT NULL,  -- 'processed' | 'duplicate_dropped'
                logged_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );

            -- Atomic statistics counter (avoids lost-update under multi-worker)
            CREATE TABLE IF NOT EXISTS stats (
                key   TEXT PRIMARY KEY,
                value BIGINT NOT NULL DEFAULT 0
            );

            INSERT INTO stats (key, value)
            VALUES
                ('received', 0),
                ('unique_processed', 0),
                ('duplicate_dropped', 0)
            ON CONFLICT (key) DO NOTHING;
        """)
    logger.info("Database schema initialized")


async def process_event(conn: asyncpg.Connection, event: EventPayload) -> str:
    """
    Process a single event inside a transaction.

    Uses INSERT ... ON CONFLICT DO NOTHING (upsert-style) with a unique
    constraint on (topic, event_id) for atomic dedup — safe under concurrent
    workers.

    Isolation level: READ COMMITTED (default for PostgreSQL).
    - Sufficient here because the UNIQUE constraint + ON CONFLICT guarantees
      at-most-once insert atomically. SERIALIZABLE is unnecessary overhead.
    - Lost-update on stats is avoided by using UPDATE ... SET value = value + 1
      (atomic increment) rather than read-modify-write.

    Returns: 'processed' | 'duplicate_dropped'
    """
    async with conn.transaction():
        ts = datetime.fromisoformat(event.timestamp.replace("Z", "+00:00"))
        result = await conn.fetchrow("""
            INSERT INTO processed_events (topic, event_id, source, timestamp, payload)
            VALUES ($1, $2, $3, $4, $5::JSONB)
            ON CONFLICT (topic, event_id) DO NOTHING
            RETURNING id
        """,
            event.topic,
            event.event_id,
            event.source,
            ts,
            json.dumps(event.payload),
        )

        if result is not None:
            # New unique event — increment counters atomically
            await conn.execute("""
                UPDATE stats SET value = value + 1 WHERE key = 'received';
                UPDATE stats SET value = value + 1 WHERE key = 'unique_processed';
            """)
            await conn.execute("""
                INSERT INTO audit_log (topic, event_id, action)
                VALUES ($1, $2, 'processed')
            """, event.topic, event.event_id)
            logger.info("[PROCESSED] topic=%s event_id=%s", event.topic, event.event_id)
            return "processed"
        else:
            # Duplicate — increment only received + dropped
            await conn.execute("""
                UPDATE stats SET value = value + 1 WHERE key = 'received';
                UPDATE stats SET value = value + 1 WHERE key = 'duplicate_dropped';
            """)
            await conn.execute("""
                INSERT INTO audit_log (topic, event_id, action)
                VALUES ($1, $2, 'duplicate_dropped')
            """, event.topic, event.event_id)
            logger.info("[DUPLICATE_DROPPED] topic=%s event_id=%s", event.topic, event.event_id)
            return "duplicate_dropped"


# ---------------------------------------------------------------------------
# Redis consumer worker
# ---------------------------------------------------------------------------

async def consumer_worker(pool: asyncpg.Pool, redis: aioredis.Redis, worker_id: int):
    """
    Pull events from Redis queue and process them concurrently.
    Multiple workers run in parallel — race conditions prevented by DB
    unique constraints and transaction isolation.
    """
    logger.info("Worker %d started", worker_id)
    while True:
        try:
            item = await redis.blpop("event_queue", timeout=2)
            if item is None:
                continue
            _, raw = item
            data = json.loads(raw)
            event = EventPayload(**data)
            async with pool.acquire() as conn:
                await process_event(conn, event)
        except asyncpg.UniqueViolationError:
            # Should not happen with ON CONFLICT but guard anyway
            logger.warning("Worker %d: unique violation caught at outer level", worker_id)
        except Exception as exc:
            logger.error("Worker %d error: %s", worker_id, exc)
            await asyncio.sleep(0.1)


# ---------------------------------------------------------------------------
# App lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting aggregator…")

    # PostgreSQL pool (10 connections)
    pool = await asyncpg.create_pool(DATABASE_URL, min_size=5, max_size=10)
    await init_db(pool)
    app.state.pool = pool

    # Redis
    redis = aioredis.from_url(BROKER_URL, decode_responses=True)
    app.state.redis = redis

    # Spawn consumer workers
    workers = [
        asyncio.create_task(consumer_worker(pool, redis, i))
        for i in range(NUM_WORKERS)
    ]
    app.state.workers = workers
    logger.info("Aggregator ready. %d consumer workers active.", NUM_WORKERS)

    yield

    # Shutdown
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
    await redis.aclose()
    await pool.close()
    logger.info("Aggregator shut down cleanly.")


app = FastAPI(title="Pub-Sub Log Aggregator", lifespan=lifespan)

# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.post("/publish", status_code=202)
async def publish(request: Request, body: PublishRequest):
    """
    Accept single or batch events.
    Events are pushed to Redis queue for async processing by consumer workers.
    Batch is atomic at the queue-push level; each event is idempotent individually.
    """
    if not body.events:
        raise HTTPException(status_code=400, detail="events tidak boleh kosong")

    redis: aioredis.Redis = request.app.state.redis
    pipe = redis.pipeline()
    for event in body.events:
        pipe.rpush("event_queue", event.model_dump_json())
    await pipe.execute()

    logger.info("Queued %d events for processing", len(body.events))
    return {"queued": len(body.events), "status": "accepted"}


@app.get("/events")
async def get_events(
    request: Request,
    topic: Optional[str] = Query(None, description="Filter by topic"),
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    """Return list of uniquely processed events (optionally filtered by topic)."""
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        if topic:
            rows = await conn.fetch("""
                SELECT topic, event_id, source, timestamp, payload, received_at
                FROM processed_events
                WHERE topic = $1
                ORDER BY received_at DESC
                LIMIT $2 OFFSET $3
            """, topic, limit, offset)
        else:
            rows = await conn.fetch("""
                SELECT topic, event_id, source, timestamp, payload, received_at
                FROM processed_events
                ORDER BY received_at DESC
                LIMIT $1 OFFSET $2
            """, limit, offset)

    events = [
        {
            "topic": r["topic"],
            "event_id": r["event_id"],
            "source": r["source"],
            "timestamp": r["timestamp"].isoformat(),
            "payload": json.loads(r["payload"]) if r["payload"] else {},
            "received_at": r["received_at"].isoformat(),
        }
        for r in rows
    ]
    return {"events": events, "count": len(events)}


@app.get("/stats")
async def get_stats(request: Request):
    """Return aggregated statistics (transactionally consistent)."""
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, value FROM stats")
        stats_map = {r["key"]: r["value"] for r in rows}

        topics = await conn.fetchval(
            "SELECT COUNT(DISTINCT topic) FROM processed_events"
        )

    uptime_seconds = round(time.time() - START_TIME, 2)
    return {
        "received": stats_map.get("received", 0),
        "unique_processed": stats_map.get("unique_processed", 0),
        "duplicate_dropped": stats_map.get("duplicate_dropped", 0),
        "topics": topics or 0,
        "uptime_seconds": uptime_seconds,
        "duplicate_rate": (
            round(stats_map.get("duplicate_dropped", 0) /
                  max(stats_map.get("received", 1), 1) * 100, 2)
        ),
    }


@app.get("/health")
async def health(request: Request):
    """Health check endpoint."""
    pool: asyncpg.Pool = request.app.state.pool
    try:
        async with pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "ok", "db": "connected"}
    except Exception as e:
        return JSONResponse(status_code=503, content={"status": "error", "db": str(e)})


@app.get("/audit")
async def audit_log(
    request: Request,
    action: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=1000),
):
    """Return audit log entries for observability."""
    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        if action:
            rows = await conn.fetch("""
                SELECT topic, event_id, action, logged_at
                FROM audit_log
                WHERE action = $1
                ORDER BY logged_at DESC
                LIMIT $2
            """, action, limit)
        else:
            rows = await conn.fetch("""
                SELECT topic, event_id, action, logged_at
                FROM audit_log
                ORDER BY logged_at DESC
                LIMIT $1
            """, limit)
    return {
        "audit_log": [
            {
                "topic": r["topic"],
                "event_id": r["event_id"],
                "action": r["action"],
                "logged_at": r["logged_at"].isoformat(),
            }
            for r in rows
        ]
    }
