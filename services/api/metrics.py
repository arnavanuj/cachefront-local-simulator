from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
from starlette.responses import Response

CACHE_HITS = Counter("cache_hits_total", "Total number of cache hits")
CACHE_MISSES = Counter("cache_misses_total", "Total number of cache misses")
DB_READS = Counter("db_reads_total", "Total number of database reads")
STALE_DATA_DETECTED = Counter("stale_data_detected_total", "Total number of explicit stale cache detections")
CACHE_TTL_WINDOW_SECONDS = Gauge("cache_ttl_window_seconds", "Configured cache TTL window in seconds")
REQUEST_LATENCY = Histogram(
    "request_latency_seconds",
    "Latency of API requests",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)


def metrics_response() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
