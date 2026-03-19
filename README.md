# cachefront-local-simulator

`cachefront-local-simulator` is a production-style local simulator for one of the hardest distributed systems problems: keeping cache reads fast without serving stale data for too long. It lets you compare a classic TTL cache against a CDC-driven invalidation pipeline, observe the difference in behavior, and interact with the system through a Streamlit control panel.

## Problem Statement

Most teams start with TTL-based caching because it is simple to understand and easy to ship. The trade-off appears later:

- Short TTLs reduce stale reads but increase database pressure.
- Long TTLs reduce database load but make stale reads more likely.
- Manual invalidation logic becomes difficult to reason about as services scale.

This project demonstrates those trade-offs with a reproducible local environment. It also shows how a CDC-driven cache invalidation pipeline can reduce stale windows by reacting to committed database changes instead of waiting for entries to expire.

## Solution Overview

The simulator supports two modes:

- `ttl`: Redis entries expire after a configured time window. Stale reads are possible until the TTL expires.
- `cdc`: MySQL binlog changes flow through Debezium and Kafka to a CDC consumer, which invalidates Redis keys asynchronously.

A Streamlit UI acts as the control plane. You can:

- switch between TTL and CDC modes
- write and read test users
- inspect cache state
- observe the CDC pipeline
- verify cache correctness and consistency behavior visually

## Tech Stack

- Python 3.12
- FastAPI
- Streamlit
- Docker and Docker Compose
- Redis
- MySQL 8
- Kafka and Zookeeper
- Debezium Connect
- Prometheus
- Grafana

## Architecture Overview

```text
Streamlit UI ---> FastAPI API ---> Redis cache
                     |
                     v
                   MySQL
                     |
                     v
               Debezium Connect
                     |
                     v
                   Kafka
                     |
                     v
                CDC Consumer ---> Redis invalidation

Prometheus scrapes API metrics and Grafana visualizes system behavior.
```

### Data Flow

1. Reads go through the FastAPI API.
2. The API checks Redis first.
3. On a cache miss, the API loads from MySQL and repopulates Redis.
4. In TTL mode, Redis entries expire on a timer.
5. In CDC mode, Debezium captures MySQL changes, Kafka transports them, and the CDC consumer invalidates Redis keys.

## Key Features

- TTL-based cache invalidation simulation
- CDC-based cache invalidation simulation with Debezium and Kafka
- Streamlit control panel for writes, reads, cache inspection, and mode switching
- Real-time pipeline visualization
- Redis cache observability
- Prometheus metrics and Grafana dashboards
- Fully containerized local stack

## Challenges Faced and Learnings

This project intentionally exposes the kinds of issues teams hit in real systems.

- CDC pipeline complexity: even a local setup requires careful sequencing between MySQL binlogs, Debezium, Kafka, and the consumer.
- Kafka and Debezium integration: topic naming, connector registration, and startup ordering are common failure points.
- Docker networking: service-to-service communication differs from host-to-container access, especially for UI and observability tools.
- Streamlit and backend sync: rerun-driven UI state can easily drift from backend state without careful session management.
- Observability setup: metrics, dashboards, and alerts are most useful when they reflect current consistency, not just historical counters.

## Repository Structure

```text
.github/workflows/      GitHub Actions CI
monitoring/             Prometheus and Grafana provisioning
runtime-config/         Runtime cache mode persistence
scripts/                Utility scripts such as connector registration
services/api/           FastAPI query engine and cache logic
services/cdc-consumer/  Kafka consumer for CDC invalidation
services/load-generator/Load testing helper
ui/                     Streamlit control panel
```

## Prerequisites

Before running the project, make sure you have:

- Docker Desktop or Docker Engine with Compose support
- Python 3.12+ if you want to run any service locally outside Docker
- At least 4 GB of RAM available to Docker
- Ports `3000`, `3306`, `6379`, `8000`, `8083`, `8501`, `9090`, and `29092` free on your machine

Important:

- Docker Desktop must be running before you start the stack.
- On first startup, Debezium and Kafka can take a little time to settle.

## How to Run the Project

### 1. Clone the repository

```bash
git clone https://github.com/arnavanuj/cachefront-local-simulator.git
cd cachefront-local-simulator
```

### 2. Create your environment file

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

### 3. Start the full stack

```bash
docker compose up --build
```

### 4. Open the services

- Streamlit UI: `http://localhost:8501`
- FastAPI API: `http://localhost:8000`
- API health: `http://localhost:8000/healthz`
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000`
- Debezium Connect: `http://localhost:8083`

Grafana default credentials:

- Username: `admin`
- Password: `admin`

## Running Individual Components

### Start only the API and UI

```bash
docker compose up --build api ui
```

### Run the load generator

```bash
docker compose --profile tools run --rm load-generator
```

### Switch cache mode

You can switch modes directly from the Streamlit sidebar or by changing `.env` and restarting the stack.

## CI/CD

GitHub Actions is configured in `.github/workflows/ci.yml` to run on:

- pushes to `main`
- pull requests

The pipeline performs:

- source checkout
- Python setup
- linting with `flake8`
- Python syntax validation
- Docker Compose validation
- Docker image builds for API, UI, CDC consumer, and load generator

## Dockerization Notes

The repository includes dedicated Dockerfiles for:

- FastAPI API
- Streamlit UI
- CDC consumer
- load generator

The Compose stack orchestrates:

- MySQL
- Redis
- Zookeeper
- Kafka
- Debezium
- connector bootstrap
- FastAPI API
- Streamlit UI
- CDC consumer
- Prometheus
- Grafana
- optional load generator

## End-to-End Demo Scenarios

### TTL stale read demo

1. Ensure the app is in `ttl` mode.
2. Read a user to warm the cache.
3. Update the same user.
4. Read the cache state before TTL expiry.

Expected result:

- the UI shows cache stale while Redis still holds the old value
- the cache becomes healthy again only after expiry and repopulation

### CDC invalidation demo

1. Switch to `cdc` mode.
2. Warm the cache for a user.
3. Update the user.
4. Wait for the pipeline to complete.
5. Read again.

Expected result:

- the CDC consumer invalidates the Redis key
- the next read repopulates Redis with fresh data
- the UI shows cache healthy again after refresh

## Screenshots / Demo

Add screenshots or a short GIF here if you want to make the repository even more recruiter-friendly. Recommended captures:

- Streamlit control panel
- cache observability panel
- CDC pipeline visualization
- Grafana dashboard

## Future Enhancements

- Add automated integration tests for TTL and CDC correctness
- Publish Docker images to GHCR
- Add Terraform or deployment manifests for cloud environments
- Add role-based access control to the control panel
- Expand Grafana alerts and SLO dashboards

## Troubleshooting

- If services look stuck, run `docker compose ps` and `docker compose logs <service>`.
- If the UI cannot reach the API, confirm the `ui` service has `CACHEFRONT_API_URL=http://api:8000`.
- If CDC does not start, inspect `debezium`, `connector-bootstrap`, and `cdc-consumer` logs.
- If Grafana dashboards are empty, wait 15-30 seconds for Prometheus scrapes.

## Final Validation Checklist

Before pushing changes, validate that:

- `docker compose config` succeeds
- Docker images build successfully
- the Streamlit UI loads on port `8501`
- the API loads on port `8000`
- cache mode switching works
- TTL and CDC scenarios both behave correctly
- there are no broken imports or hardcoded local machine paths



