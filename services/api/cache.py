import json
import logging
from dataclasses import dataclass
from typing import Any

from redis import Redis


LOGGER = logging.getLogger("cachefront.api.cache")


@dataclass(slots=True)
class CacheConfig:
    mode: str
    ttl_seconds: int


class UserCache:
    def __init__(self, redis_client: Redis, config: CacheConfig) -> None:
        self.redis = redis_client
        self.config = config

    @staticmethod
    def key(user_id: int) -> str:
        return f"user:{user_id}"

    def get_user(self, user_id: int) -> dict[str, Any] | None:
        key = self.key(user_id)
        cached_value = self.redis.get(key)
        if cached_value is None:
            LOGGER.info("cache_miss key=%s mode=%s", key, self.config.mode)
            return None

        LOGGER.info("cache_hit key=%s mode=%s", key, self.config.mode)
        return json.loads(cached_value)

    def describe_user(self, user_id: int) -> dict[str, Any]:
        key = self.key(user_id)
        cached_value = self.redis.get(key)
        ttl_seconds = self.redis.ttl(key)
        if cached_value is None:
            return {
                "key": key,
                "exists": False,
                "status": "invalidated",
                "ttl_seconds": None,
                "value": None,
            }

        normalized_ttl = ttl_seconds if ttl_seconds >= 0 else None
        return {
            "key": key,
            "exists": True,
            "status": "fresh",
            "ttl_seconds": normalized_ttl,
            "value": json.loads(cached_value),
        }

    def set_user(self, user_id: int, payload: dict[str, Any]) -> None:
        key = self.key(user_id)
        serialized = json.dumps(payload, default=str)
        if self.config.mode == "ttl":
            self.redis.set(key, serialized, ex=self.config.ttl_seconds)
            LOGGER.info(
                "cache_set key=%s mode=%s ttl_seconds=%s",
                key,
                self.config.mode,
                self.config.ttl_seconds,
            )
            return

        self.redis.set(key, serialized)
        LOGGER.info("cache_set key=%s mode=%s ttl_seconds=none", key, self.config.mode)

    def invalidate_user(self, user_id: int) -> None:
        key = self.key(user_id)
        self.redis.delete(key)
        LOGGER.info("cache_invalidate key=%s", key)

    def invalidate_all_users(self) -> int:
        deleted = 0
        for key in self.redis.scan_iter(match="user:*"):
            deleted += int(self.redis.delete(key) or 0)
        LOGGER.info("cache_invalidate_all deleted_keys=%s", deleted)
        return deleted
