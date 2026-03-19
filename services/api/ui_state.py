import json
import uuid
from datetime import datetime, timezone
from typing import Any

from redis import Redis


UI_EVENTS_KEY = "ui:events"
UI_LAST_FLOW_KEY = "ui:last_flow"
UI_STATE_KEY = "ui:last_state"
UI_PENDING_FLOW_KEY = "ui:pending_flow:user:{user_id}"
MAX_UI_EVENTS = 100


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_json(redis_client: Redis, key: str, default: Any) -> Any:
    raw = redis_client.get(key)
    if not raw:
        return default
    return json.loads(raw)


def _save_json(redis_client: Redis, key: str, payload: Any) -> None:
    redis_client.set(key, json.dumps(payload, default=str))


def append_event(
    redis_client: Redis,
    *,
    stage: str,
    message: str,
    user_id: int | None = None,
    flow_id: str | None = None,
    event_type: str = "system",
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "stage": stage,
        "message": message,
        "user_id": user_id,
        "flow_id": flow_id,
        "details": details or {},
        "timestamp": now_iso(),
    }
    redis_client.lpush(UI_EVENTS_KEY, json.dumps(event, default=str))
    redis_client.ltrim(UI_EVENTS_KEY, 0, MAX_UI_EVENTS - 1)
    return event


def get_events(redis_client: Redis, limit: int = 20) -> list[dict[str, Any]]:
    raw_events = redis_client.lrange(UI_EVENTS_KEY, 0, max(0, limit - 1))
    return [json.loads(item) for item in raw_events]


def get_last_flow(redis_client: Redis) -> dict[str, Any]:
    return _load_json(
        redis_client,
        UI_LAST_FLOW_KEY,
        {"flow_id": None, "user_id": None, "operation": None, "stages": {}, "status": "idle"},
    )


def get_last_state(redis_client: Redis) -> dict[str, Any]:
    return _load_json(
        redis_client,
        UI_STATE_KEY,
        {"last_db_operation": None, "last_cdc_event": None, "cache_status": None},
    )


def update_last_state(redis_client: Redis, **fields: Any) -> dict[str, Any]:
    state = get_last_state(redis_client)
    state.update(fields)
    _save_json(redis_client, UI_STATE_KEY, state)
    return state


def start_flow(
    redis_client: Redis,
    *,
    user_id: int,
    operation: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    flow = {
        "flow_id": str(uuid.uuid4()),
        "user_id": user_id,
        "operation": operation,
        "status": "in_progress",
        "details": details or {},
        "stages": {},
        "started_at": now_iso(),
        "updated_at": now_iso(),
    }
    _save_json(redis_client, UI_LAST_FLOW_KEY, flow)
    _save_json(redis_client, UI_PENDING_FLOW_KEY.format(user_id=user_id), flow)
    redis_client.expire(UI_PENDING_FLOW_KEY.format(user_id=user_id), 300)
    update_last_state(redis_client, last_db_operation=flow)
    append_event(
        redis_client,
        stage="SYSTEM",
        message=f"Started {operation} flow for user {user_id}",
        user_id=user_id,
        flow_id=flow["flow_id"],
        event_type="flow",
        details=flow["details"],
    )
    return flow


def _load_flow(redis_client: Redis, user_id: int, flow_id: str | None = None) -> dict[str, Any]:
    if flow_id:
        current = get_last_flow(redis_client)
        if current.get("flow_id") == flow_id:
            return current

    pending = _load_json(redis_client, UI_PENDING_FLOW_KEY.format(user_id=user_id), None)
    if pending:
        return pending

    last_flow = get_last_flow(redis_client)
    if last_flow.get("user_id") == user_id:
        return last_flow

    return {
        "flow_id": flow_id or str(uuid.uuid4()),
        "user_id": user_id,
        "operation": "external_change",
        "status": "in_progress",
        "details": {},
        "stages": {},
        "started_at": now_iso(),
        "updated_at": now_iso(),
    }


def mark_flow_stage(
    redis_client: Redis,
    *,
    user_id: int,
    stage: str,
    message: str,
    flow_id: str | None = None,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    flow = _load_flow(redis_client, user_id=user_id, flow_id=flow_id)
    stage_payload = {
        "stage": stage,
        "message": message,
        "timestamp": now_iso(),
        "details": details or {},
    }
    flow.setdefault("stages", {})[stage] = stage_payload
    flow["updated_at"] = stage_payload["timestamp"]
    if stage == "REDIS":
        flow["status"] = "completed"
        redis_client.delete(UI_PENDING_FLOW_KEY.format(user_id=user_id))
    _save_json(redis_client, UI_LAST_FLOW_KEY, flow)
    if flow.get("status") != "completed":
        _save_json(redis_client, UI_PENDING_FLOW_KEY.format(user_id=user_id), flow)
        redis_client.expire(UI_PENDING_FLOW_KEY.format(user_id=user_id), 300)
    append_event(
        redis_client,
        stage=stage,
        message=message,
        user_id=user_id,
        flow_id=flow["flow_id"],
        event_type="pipeline",
        details=details,
    )
    if stage in {"DEBEZIUM", "KAFKA", "CONSUMER", "REDIS"}:
        update_last_state(redis_client, last_cdc_event=stage_payload)
    return flow


def record_cache_activity(
    redis_client: Redis,
    *,
    user_id: int,
    cache_key: str,
    status: str,
    value: dict[str, Any] | None,
    source: str,
) -> dict[str, Any]:
    payload = {
        "user_id": user_id,
        "cache_key": cache_key,
        "status": status,
        "source": source,
        "value": value,
        "timestamp": now_iso(),
    }
    update_last_state(redis_client, cache_status=payload)
    append_event(
        redis_client,
        stage="REDIS",
        message=f"Cache {status} for {cache_key}",
        user_id=user_id,
        event_type="cache",
        details={"source": source},
    )
    return payload
