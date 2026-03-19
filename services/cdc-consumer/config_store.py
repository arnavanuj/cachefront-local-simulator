import json
import logging
import os
from pathlib import Path
from threading import Lock
from typing import Any

LOGGER = logging.getLogger("cachefront.cdc-consumer.config")
VALID_CACHE_MODES = {"ttl", "cdc"}
_CONFIG_LOCK = Lock()


def default_config_path() -> Path:
    configured = os.getenv("CACHE_CONFIG_PATH")
    if configured:
        return Path(configured)
    return Path(__file__).resolve().parents[2] / "runtime-config" / "cache_config.json"


def normalize_cache_mode(value: Any) -> str:
    mode = str(value).strip().lower()
    if mode not in VALID_CACHE_MODES:
        raise ValueError(f"mode must be one of {sorted(VALID_CACHE_MODES)}")
    return mode


def _default_payload(default_mode: str | None = None) -> dict[str, str]:
    return {"mode": normalize_cache_mode(default_mode or os.getenv("CACHE_MODE", "ttl"))}


def load_runtime_config(default_mode: str | None = None) -> dict[str, str]:
    path = default_config_path()
    fallback = _default_payload(default_mode)

    with _CONFIG_LOCK:
        if not path.exists():
            _write_runtime_config(path, fallback)
            return fallback

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as exc:
            LOGGER.warning("cache_config_read_failed path=%s error=%s", path, exc)
            _write_runtime_config(path, fallback)
            return fallback

        if not isinstance(payload, dict):
            LOGGER.warning("cache_config_invalid_shape path=%s type=%s", path, type(payload).__name__)
            _write_runtime_config(path, fallback)
            return fallback

        try:
            normalized = {"mode": normalize_cache_mode(payload.get("mode", fallback["mode"]))}
        except ValueError as exc:
            LOGGER.warning("cache_config_invalid_mode path=%s error=%s", path, exc)
            _write_runtime_config(path, fallback)
            return fallback

        if payload != normalized:
            _write_runtime_config(path, normalized)
        return normalized


def _write_runtime_config(path: Path, payload: dict[str, str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(f"{path.suffix}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    temp_path.replace(path)


def get_cache_mode(default_mode: str | None = None) -> str:
    return load_runtime_config(default_mode).get("mode", normalize_cache_mode(default_mode or "ttl"))
