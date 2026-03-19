import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

import mysql.connector
import redis
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, EmailStr, Field, field_validator

from cache import CacheConfig, UserCache
from config_store import get_cache_mode, set_cache_mode
from db import fetch_user, fetch_user_freshness, insert_user, update_user
from metrics import (
    CACHE_HITS,
    CACHE_MISSES,
    CACHE_TTL_WINDOW_SECONDS,
    REQUEST_LATENCY,
    STALE_DATA_DETECTED,
    metrics_response,
)
from ui_state import (
    append_event,
    get_events,
    get_last_flow,
    get_last_state,
    mark_flow_stage,
    record_cache_activity,
    start_flow,
)


def configure_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level_name, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


configure_logging()
LOGGER = logging.getLogger("cachefront.api")
DEFAULT_CACHE_MODE = os.getenv("CACHE_MODE", "ttl").lower()
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "60"))
STALE_WINDOW_SECONDS = float(os.getenv("STALE_WINDOW_SECONDS", "2"))
STALE_EVENTS: list[float] = []


class UserUpdateRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=128)
    email: EmailStr | None = None
    status: str | None = Field(default=None, min_length=1, max_length=32)

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str | None) -> str | None:
        if value is None:
            return value
        allowed = {"active", "inactive", "suspended"}
        if value not in allowed:
            raise ValueError(f"status must be one of: {sorted(allowed)}")
        return value


class UserCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=128)
    email: EmailStr
    status: str = Field(default="active", min_length=1, max_length=32)

    @field_validator("status")
    @classmethod
    def validate_status(cls, value: str) -> str:
        allowed = {"active", "inactive", "suspended"}
        if value not in allowed:
            raise ValueError(f"status must be one of: {sorted(allowed)}")
        return value


class CacheModeUpdateRequest(BaseModel):
    mode: str = Field(min_length=3, max_length=3)

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, value: str) -> str:
        allowed = {"ttl", "cdc"}
        normalized = value.lower()
        if normalized not in allowed:
            raise ValueError(f"mode must be one of: {sorted(allowed)}")
        return normalized


redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", "6379")),
    decode_responses=True,
)
cache = UserCache(
    redis_client,
    CacheConfig(
        mode=get_cache_mode(DEFAULT_CACHE_MODE),
        ttl_seconds=CACHE_TTL_SECONDS,
    ),
)


EXPLAINERS = {
    "cdc": (
        "Change Data Capture streams committed database changes into Kafka so "
        "downstream systems can invalidate cache without guessing with TTLs."
    ),
    "redis_invalidation": (
        "Redis keys are deleted after the CDC consumer sees the database "
        "change event, so the next read repopulates the cache with fresh data."
    ),
    "cache_hit": "A cache hit means the API served data directly from Redis instead of reading MySQL.",
    "cache_miss": "A cache miss means Redis had no entry, so the API read from MySQL and repopulated the cache.",
}


def sync_cache_mode_from_store() -> str:
    configured_mode = get_cache_mode(DEFAULT_CACHE_MODE)
    if cache.config.mode != configured_mode:
        LOGGER.info("cache_mode_reloaded previous=%s current=%s", cache.config.mode, configured_mode)
        cache.config.mode = configured_mode
    CACHE_TTL_WINDOW_SECONDS.set(cache.config.ttl_seconds if cache.config.mode == "ttl" else 0)
    return cache.config.mode


def clear_stale_events() -> None:
    global STALE_EVENTS
    if STALE_EVENTS:
        STALE_EVENTS = []


def get_recent_stale_count() -> int:
    now = time.time()
    global STALE_EVENTS
    STALE_EVENTS = [ts for ts in STALE_EVENTS if now - ts < STALE_WINDOW_SECONDS]
    return len(STALE_EVENTS)


def get_observability_alert_state() -> dict[str, Any]:
    count = get_recent_stale_count()
    if count > 5:
        severity = "high"
    elif count > 2:
        severity = "medium"
    elif count > 0:
        severity = "low"
    else:
        severity = "none"

    window_label = f"{STALE_WINDOW_SECONDS:g}"
    return {
        "stale_spike": count > 0,
        "severity": severity,
        "message": f"? {count} stale events (last {window_label}s)" if count > 0 else "? Cache healthy",
    }


def normalize_updated_at(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        sanitized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(sanitized)
    raise TypeError(f"unsupported updated_at value: {type(value)!r}")


def normalize_record_timestamps(record: dict[str, Any] | None) -> dict[str, Any] | None:
    if record is None:
        return None

    normalized = dict(record)
    updated_at = normalize_updated_at(normalized.get("updated_at"))
    if updated_at is not None:
        normalized["updated_at"] = updated_at.isoformat()
    return normalized


def detect_stale_cache_hit(user_id: int, cached_user: dict[str, Any]) -> None:
    if cache.config.mode != "ttl":
        clear_stale_events()
        return

    freshness_row = fetch_user_freshness(user_id)
    if freshness_row is None:
        return

    cached_updated_at = normalize_updated_at(cached_user.get("updated_at"))
    fresh_updated_at = normalize_updated_at(freshness_row.get("updated_at"))
    if cached_updated_at is None or fresh_updated_at is None:
        return

    if cached_updated_at < fresh_updated_at:
        STALE_DATA_DETECTED.inc()
        STALE_EVENTS.append(time.time())
        LOGGER.warning(
            "STALE_DATA_DETECTED user_id=%s mode=%s cached_updated_at=%s fresh_updated_at=%s recent_stale_events=%s",
            user_id,
            cache.config.mode,
            cached_updated_at.isoformat(sep=" "),
            fresh_updated_at.isoformat(sep=" "),
            get_recent_stale_count(),
        )
        return

    clear_stale_events()


@asynccontextmanager
async def lifespan(_: FastAPI):
    sync_cache_mode_from_store()
    LOGGER.info(
        "service_start cache_mode=%s ttl_seconds=%s stale_window_seconds=%s",
        cache.config.mode,
        cache.config.ttl_seconds if cache.config.mode == "ttl" else "none",
        f"{STALE_WINDOW_SECONDS:g}",
    )
    redis_client.ping()
    yield


app = FastAPI(title="cachefront-local-simulator-api", lifespan=lifespan)


@app.middleware("http")
async def record_latency(request, call_next):
    sync_cache_mode_from_store()
    start = time.perf_counter()
    response = await call_next(request)
    REQUEST_LATENCY.observe(time.perf_counter() - start)
    return response


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok", "cache_mode": sync_cache_mode_from_store()}


@app.get("/metrics")
def metrics():
    return metrics_response()


@app.get("/observability/alerts")
def get_observability_alerts() -> dict[str, Any]:
    return get_observability_alert_state()


@app.post("/config/cache-mode")
def update_cache_mode(payload: CacheModeUpdateRequest) -> dict[str, Any]:
    previous_mode = sync_cache_mode_from_store()
    updated = set_cache_mode(payload.mode)
    cache.config.mode = updated["mode"]
    CACHE_TTL_WINDOW_SECONDS.set(cache.config.ttl_seconds if cache.config.mode == "ttl" else 0)

    invalidated_keys = 0
    changed = previous_mode != cache.config.mode
    if changed:
        clear_stale_events()
        invalidated_keys = cache.invalidate_all_users()
        append_event(
            redis_client,
            stage="SYSTEM",
            message=f"Cache mode changed from {previous_mode} to {cache.config.mode}",
            event_type="config",
            details={
                "previous_mode": previous_mode,
                "new_mode": cache.config.mode,
                "invalidated_keys": invalidated_keys,
            },
        )
        LOGGER.info(
            "cache_mode_changed previous=%s current=%s invalidated_keys=%s",
            previous_mode,
            cache.config.mode,
            invalidated_keys,
        )

    return {
        "status": "ok",
        "cache_mode": cache.config.mode,
        "previous_mode": previous_mode,
        "changed": changed,
        "invalidated_keys": invalidated_keys,
    }


def get_cache_snapshot(user_id: int, source: str, record: bool = True) -> dict[str, Any]:
    snapshot = cache.describe_user(user_id)
    if record:
        record_cache_activity(
            redis_client,
            user_id=user_id,
            cache_key=snapshot["key"],
            status=snapshot["status"],
            value=snapshot["value"],
            source=source,
        )
    return snapshot


def load_user_with_metadata(user_id: int) -> tuple[dict[str, Any], str]:
    cached_user = cache.get_user(user_id)
    if cached_user is not None:
        detect_stale_cache_hit(user_id, cached_user)
        CACHE_HITS.inc()
        get_cache_snapshot(user_id, source="read_hit")
        return cached_user, "hit"

    CACHE_MISSES.inc()
    user = fetch_user(user_id)
    if user is None:
        raise HTTPException(status_code=404, detail="user not found")

    cache.set_user(user_id, user)
    clear_stale_events()
    get_cache_snapshot(user_id, source="read_miss")
    return user, "miss"


@app.get("/user/{user_id}")
def get_user(user_id: int) -> dict[str, Any]:
    LOGGER.info("request_read user_id=%s", user_id)
    user, _ = load_user_with_metadata(user_id)
    return user


@app.post("/user/{user_id}/insert")
def create_user(user_id: int, payload: UserCreateRequest) -> dict[str, Any]:
    LOGGER.info("request_insert user_id=%s fields=%s", user_id, payload.model_dump())
    flow = start_flow(redis_client, user_id=user_id, operation="insert", details=payload.model_dump())
    try:
        created = insert_user(user_id, payload.model_dump())
    except mysql.connector.Error as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    mark_flow_stage(
        redis_client,
        user_id=user_id,
        stage="MYSQL",
        message=f"Inserted user {user_id} into MySQL",
        flow_id=flow["flow_id"],
        details={"operation": "insert"},
    )
    return created


@app.post("/user/{user_id}")
def post_user(user_id: int, payload: UserUpdateRequest) -> dict[str, Any]:
    LOGGER.info("request_write user_id=%s fields=%s", user_id, payload.model_dump(exclude_none=True))
    flow = start_flow(
        redis_client,
        user_id=user_id,
        operation="update",
        details=payload.model_dump(exclude_none=True),
    )
    updated = update_user(user_id, payload.model_dump(exclude_none=True))
    if updated is None:
        raise HTTPException(status_code=404, detail="user not found")

    mark_flow_stage(
        redis_client,
        user_id=user_id,
        stage="MYSQL",
        message=f"Updated user {user_id} in MySQL",
        flow_id=flow["flow_id"],
        details={"operation": "update"},
    )
    LOGGER.info(
        "write_complete user_id=%s cache_mode=%s direct_invalidation=false",
        user_id,
        cache.config.mode,
    )
    return updated


@app.get("/cache/{user_id}")
def get_cache_state(user_id: int) -> dict[str, Any]:
    return get_cache_snapshot(user_id, source="cache_inspection")


@app.post("/ui/cache/read/{user_id}")
def read_user_for_ui(user_id: int) -> dict[str, Any]:
    user, cache_result = load_user_with_metadata(user_id)
    return {
        "cache_result": cache_result,
        "user": user,
        "cache_state": cache.describe_user(user_id),
        "explanations": {
            "cache": EXPLAINERS["cache_hit"] if cache_result == "hit" else EXPLAINERS["cache_miss"],
            "cdc": EXPLAINERS["cdc"],
        },
    }


@app.get("/ui/state")
def get_ui_state(
    user_id: int | None = Query(default=None),
    limit: int = Query(default=15, ge=1, le=50),
) -> dict[str, Any]:
    target_user_id = user_id
    if target_user_id is None:
        last_flow = get_last_flow(redis_client)
        target_user_id = last_flow.get("user_id")

    cache_state = (
        get_cache_snapshot(
            target_user_id,
            source="ui_state",
            record=False,
        )
        if target_user_id is not None
        else None
    )
    if cache_state and isinstance(cache_state.get("value"), dict):
        cache_state = {**cache_state, "value": normalize_record_timestamps(cache_state.get("value"))}
    db_state = (
        normalize_record_timestamps(fetch_user(target_user_id, apply_replica_lag=False))
        if target_user_id is not None
        else None
    )
    return {
        "cache_mode": sync_cache_mode_from_store(),
        "explainers": EXPLAINERS,
        "last_flow": get_last_flow(redis_client),
        "state": get_last_state(redis_client),
        "cache_state": cache_state,
        "db_state": db_state,
        "events": get_events(redis_client, limit=limit),
    }
