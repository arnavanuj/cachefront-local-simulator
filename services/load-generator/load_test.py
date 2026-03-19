import argparse
import random
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CacheFront local simulator load generator")
    parser.add_argument("--base-url", default="http://localhost:8000", help="API base URL")
    parser.add_argument("--threads", type=int, default=20, help="Concurrent worker count")
    parser.add_argument("--requests", type=int, default=500, help="Total request count")
    parser.add_argument("--user-ids", default="1,2,3,4,5", help="Comma-separated list of user ids")
    parser.add_argument(
        "--write-every",
        type=int,
        default=0,
        help="Perform one POST update every N requests; 0 disables writes",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    user_ids = [int(item.strip()) for item in args.user_ids.split(",") if item.strip()]
    latencies = []
    status_counts: dict[int, int] = {}
    lock = threading.Lock()

    def run_request(index: int) -> None:
        user_id = random.choice(user_ids)
        should_write = args.write_every > 0 and index > 0 and index % args.write_every == 0
        start = time.perf_counter()

        if should_write:
            payload = {
                "status": random.choice(["active", "inactive", "suspended"]),
                "email": f"user{user_id}@example.com",
                "name": f"User {user_id}",
            }
            response = requests.post(f"{args.base_url}/user/{user_id}", json=payload, timeout=5)
        else:
            response = requests.get(f"{args.base_url}/user/{user_id}", timeout=5)

        latency = time.perf_counter() - start
        with lock:
            latencies.append(latency)
            status_counts[response.status_code] = status_counts.get(response.status_code, 0) + 1

    print(
        f"Starting load test against {args.base_url} "
        f"with threads={args.threads} requests={args.requests} write_every={args.write_every}"
    )

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        futures = [executor.submit(run_request, index) for index in range(args.requests)]
        for _ in as_completed(futures):
            pass
    duration = time.perf_counter() - started

    throughput = args.requests / duration if duration else 0.0
    avg_latency = statistics.mean(latencies) if latencies else 0.0
    p95_latency = statistics.quantiles(latencies, n=20)[18] if len(latencies) >= 20 else avg_latency

    print("Load test completed")
    print(f"Duration: {duration:.2f}s")
    print(f"Throughput: {throughput:.2f} req/s")
    print(f"Average latency: {avg_latency * 1000:.2f} ms")
    print(f"P95 latency: {p95_latency * 1000:.2f} ms")
    print(f"Status counts: {status_counts}")


if __name__ == "__main__":
    main()
