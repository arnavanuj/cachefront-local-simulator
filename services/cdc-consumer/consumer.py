import json
import logging
import os
import time
from typing import Any

from kafka import KafkaConsumer
from prometheus_client import Gauge, start_http_server
from redis import Redis

from config_store import get_cache_mode, default_config_path
from ui_state import mark_flow_stage


def configure_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level_name, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


configure_logging()
LOGGER = logging.getLogger("cachefront.cdc-consumer")

DEFAULT_CACHE_MODE = os.getenv("CACHE_MODE", "ttl").lower()
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "cachefront.appdb.users")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "cachefront-cdc-consumer")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
METRICS_PORT = int(os.getenv("CDC_METRICS_PORT", "9101"))
CDC_INVALIDATION_DELAY_MS = Gauge(
    "cdc_invalidation_delay_ms",
    "Observed delay between Debezium event time and Redis invalidation in milliseconds",
)


def extract_payload(event: dict[str, Any]) -> dict[str, Any]:
    payload = event.get("payload", event)
    return payload if isinstance(payload, dict) else {}


def extract_user_id(event: dict[str, Any]) -> int | None:
    payload = extract_payload(event)
    after = payload.get("after")
    before = payload.get("before")
    source_record = after or before or payload
    if isinstance(source_record, dict) and source_record.get("id") is not None:
        return int(source_record["id"])

    return None


def extract_event_time_ms(event: dict[str, Any]) -> int | None:
    payload = extract_payload(event)
    candidates = [payload.get("ts_ms")]
    source = payload.get("source")
    if isinstance(source, dict):
        candidates.append(source.get("ts_ms"))

    for candidate in candidates:
        if candidate is not None:
            return int(candidate)
    return None


def build_consumer() -> KafkaConsumer:
    last_error: Exception | None = None
    for attempt in range(1, 31):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )
            LOGGER.info("consumer_connected topic=%s bootstrap=%s", KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
            return consumer
        except Exception as exc:
            last_error = exc
            LOGGER.warning("consumer_connect_retry attempt=%s error=%s", attempt, exc)
            time.sleep(3)

    raise RuntimeError(f"failed to create Kafka consumer: {last_error}") from last_error


def main() -> None:
    start_http_server(METRICS_PORT)
    initial_mode = get_cache_mode(DEFAULT_CACHE_MODE)
    LOGGER.info(
        "consumer_start cache_mode=%s config_path=%s topic=%s bootstrap=%s",
        initial_mode,
        default_config_path(),
        KAFKA_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
    )
    LOGGER.info("metrics_server_started port=%s", METRICS_PORT)
    redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    consumer = build_consumer()
    last_reported_mode: str | None = None

    for message in consumer:
        current_mode = get_cache_mode(DEFAULT_CACHE_MODE)
        if current_mode != last_reported_mode:
            LOGGER.info("consumer_mode cache_mode=%s", current_mode)
            last_reported_mode = current_mode

        event = message.value
        user_id = extract_user_id(event)
        if user_id is None:
            LOGGER.info("event_skipped reason=no_user_id event=%s", event)
            continue

        if current_mode != "cdc":
            LOGGER.info("event_ignored cache_mode=%s user_id=%s reason=ttl_mode", current_mode, user_id)
            continue

        payload = extract_payload(event)
        operation = payload.get("op", "unknown")
        event_time_ms = extract_event_time_ms(event)
        delay_ms = None
        if event_time_ms is not None:
            delay_ms = max(0, int(time.time() * 1000) - event_time_ms)
            CDC_INVALIDATION_DELAY_MS.set(delay_ms)

        LOGGER.info(
            "cdc_event_received cache_mode=%s user_id=%s op=%s topic=%s",
            current_mode,
            user_id,
            operation,
            KAFKA_TOPIC,
        )

        mark_flow_stage(
            redis_client,
            user_id=user_id,
            stage="DEBEZIUM",
            message=f"Debezium emitted a CDC event for user {user_id}",
            details={"operation": operation},
        )
        mark_flow_stage(
            redis_client,
            user_id=user_id,
            stage="KAFKA",
            message=f"Kafka delivered the change event for user {user_id}",
            details={"operation": operation},
        )
        mark_flow_stage(
            redis_client,
            user_id=user_id,
            stage="CONSUMER",
            message=f"CDC consumer processed the change event for user {user_id}",
            details={"operation": operation, "cdc_delay_ms": delay_ms},
        )

        key = f"user:{user_id}"
        redis_client.delete(key)
        mark_flow_stage(
            redis_client,
            user_id=user_id,
            stage="REDIS",
            message=f"Redis invalidated cache key {key}",
            details={"operation": operation, "cache_key": key, "cdc_delay_ms": delay_ms},
        )
        LOGGER.info(
            "cdc_invalidation cache_mode=%s user_id=%s key=%s op=%s cdc_delay_ms=%s",
            current_mode,
            user_id,
            key,
            operation,
            delay_ms if delay_ms is not None else "unknown",
        )


if __name__ == "__main__":
    main()
