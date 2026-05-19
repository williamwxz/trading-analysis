# Repo Reorganization Design

**Date:** 2026-05-19

## Goal

Restructure the repository so that service code lives under `services/`, shared libraries stay in `libs/`, and infrastructure stays in `infra/`. Each service becomes a self-contained directory with its own Dockerfile and tests collocated alongside it.

---

## Target Layout

```
trading-analysis/
├── infra/
│   ├── grafana/
│   ├── schemas/              ← moved from schemas/ at root
│   └── terraform/
├── libs/
│   ├── clickhouse_client.py
│   ├── computation/
│   └── tests/                ← moved from tests/libs/
├── scripts/                  ← stays at root
├── services/
│   ├── dagster/
│   │   ├── Dockerfile        ← moved from root Dockerfile
│   │   ├── dagster.yaml      ← moved from root dagster.yaml
│   │   ├── trading_dagster/  ← Python package keeps its name
│   │   └── tests/            ← moved from tests/test_pnl_*.py, test_binance_*.py, test_exchange_*.py
│   ├── flink_pnl/
│   │   ├── Dockerfile
│   │   ├── flink_pnl/
│   │   └── tests/            ← was flink_pnl/tests/ (already collocated)
│   ├── pnl_consumer/
│   │   ├── Dockerfile
│   │   ├── pnl_consumer/
│   │   └── tests/            ← moved from tests/pnl_consumer/
│   └── streaming/
│       ├── Dockerfile
│       ├── streaming/
│       └── tests/            ← moved from tests/streaming/
├── docker-compose.yml
└── pyproject.toml
```

---

## File Moves

| From | To |
|------|----|
| `Dockerfile` | `services/dagster/Dockerfile` |
| `dagster.yaml` | `services/dagster/dagster.yaml` |
| `trading_dagster/` | `services/dagster/trading_dagster/` |
| `tests/test_pnl_compute.py` | `services/dagster/tests/test_pnl_compute.py` |
| `tests/test_pnl_recent_recompute.py` | `services/dagster/tests/test_pnl_recent_recompute.py` |
| `tests/test_binance_backfill_skip.py` | `services/dagster/tests/test_binance_backfill_skip.py` |
| `tests/test_binance_integration.py` | `services/dagster/tests/test_binance_integration.py` |
| `tests/test_exchange_price_service.py` | `services/dagster/tests/test_exchange_price_service.py` |
| `streaming/` | `services/streaming/streaming/` |
| `streaming/Dockerfile` | `services/streaming/Dockerfile` |
| `tests/streaming/` | `services/streaming/tests/` |
| `pnl_consumer/` | `services/pnl_consumer/pnl_consumer/` |
| `pnl_consumer/Dockerfile` | `services/pnl_consumer/Dockerfile` |
| `tests/pnl_consumer/` | `services/pnl_consumer/tests/` |
| `flink_pnl/flink_pnl/` | `services/flink_pnl/flink_pnl/` |
| `flink_pnl/Dockerfile` | `services/flink_pnl/Dockerfile` |
| `flink_pnl/tests/` | `services/flink_pnl/tests/` |
| `flink_pnl/requirements.txt` | `services/flink_pnl/requirements.txt` |
| `schemas/` | `infra/schemas/` |
| `tests/libs/` | `libs/tests/` |

The old `tests/` directory (root), `tests/flink_job/` stub, and top-level `flink_pnl/`, `pnl_consumer/`, `streaming/`, `trading_dagster/` dirs are removed after moves.

---

## Changes Required

### 1. Dockerfiles

All service Dockerfiles use the **repo root as build context** (so they can reach `libs/`). Only the `-f` path changes in CI; the COPY paths inside each Dockerfile update to reflect new source locations.

**services/dagster/Dockerfile** — COPY paths stay the same (`trading_dagster/`, `libs/`, `dagster.yaml`, `pyproject.toml`) since those are resolved from the build context (repo root). No content changes needed except the `dagster.yaml` COPY path.

**services/streaming/Dockerfile** — COPY `streaming/` becomes `services/streaming/streaming/`.

**services/pnl_consumer/Dockerfile** — COPY `pnl_consumer/` becomes `services/pnl_consumer/pnl_consumer/`; COPY `streaming/` becomes `services/streaming/streaming/`.

**services/flink_pnl/Dockerfile** — COPY `flink_pnl/flink_pnl/` becomes `services/flink_pnl/flink_pnl/`; COPY `streaming/` becomes `services/streaming/streaming/`; COPY `pnl_consumer/` becomes `services/pnl_consumer/pnl_consumer/`.

### 2. pyproject.toml

- `testpaths` updates to include all new test locations:
  ```toml
  testpaths = [
      "libs/tests",
      "services/dagster/tests",
      "services/streaming/tests",
      "services/pnl_consumer/tests",
      "services/flink_pnl/tests",
  ]
  ```
- `[tool.setuptools.packages.find]` `where = ["."]` already covers the new paths since setuptools will find `services/dagster/trading_dagster`, `services/streaming/streaming`, etc.
- `[tool.dagster] module_name = "trading_dagster"` stays unchanged (package name is unchanged).

### 3. CI/CD — `.github/workflows/ci-cd.yml`

Path filters update:

| Filter key | Old path | New path |
|------------|----------|----------|
| `dagster` | `trading_dagster/**`, `Dockerfile` | `services/dagster/**` |
| `streaming` | `streaming/**` | `services/streaming/**` |
| `pnl-consumer` | `pnl_consumer/**` | `services/pnl_consumer/**` |
| `flink-pnl` | `flink_pnl/**`, `streaming/models.py` | `services/flink_pnl/**`, `services/streaming/streaming/models.py` |

Docker build commands update `-f` flag:

| Service | Old `-f` | New `-f` |
|---------|----------|----------|
| dagster | *(no -f, used `.` as context+Dockerfile)* | `-f services/dagster/Dockerfile` |
| streaming | `-f streaming/Dockerfile` | `-f services/streaming/Dockerfile` |
| pnl-consumer | `-f pnl_consumer/Dockerfile` | `-f services/pnl_consumer/Dockerfile` |
| flink-pnl | `-f flink_pnl/Dockerfile` | `-f services/flink_pnl/Dockerfile` |

Build context remains `.` (repo root) for all services so `COPY libs/` continues to work.

Test commands update:
```yaml
# was:
pytest tests/ -v --tb=short -m "not integration"
pytest flink_pnl/tests/ -v --tb=short -m "not integration"

# becomes:
pytest libs/tests/ services/dagster/tests/ services/streaming/tests/ services/pnl_consumer/tests/ services/flink_pnl/tests/ -v --tb=short -m "not integration"
```

Integration test path:
```yaml
# was: pytest tests/streaming/test_streaming_integration.py
# becomes: pytest services/streaming/tests/test_streaming_integration.py
```

### 4. docker-compose.yml

```yaml
# dagster-webserver and dagster-daemon:
build:
  context: .
  dockerfile: services/dagster/Dockerfile

# dagster.local.yaml volume mount path unchanged (it's a local override, not in the image)
```

### 5. CLAUDE.md

Update all path references: `schemas/clickhouse_cloud.sql` → `infra/schemas/clickhouse_cloud.sql`, `flink_pnl/tests/` → `services/flink_pnl/tests/`, etc.

### 6. workspace.yaml

No change needed — it references the gRPC server by host/port, not file paths.

---

## What Does NOT Change

- Python package names (`trading_dagster`, `streaming`, `pnl_consumer`, `flink_pnl`, `libs`) — only directory layout changes.
- `libs/` location — stays at root so all services can reach it via build context.
- `infra/grafana/` and `infra/terraform/` — untouched.
- `scripts/` — stays at root.
- ECS task definitions, service names, ECR repo names — no infrastructure changes.
- `workspace.yaml` — gRPC config, no path references.
- `dagster.local.yaml` — local dev override, not moved.

---

## Risk Notes

- **Import paths**: All service Python modules use their package name (`from streaming.models import ...`, `from pnl_consumer.pnl_consumer import ...`) — these are resolved relative to `PYTHONPATH`/install root, not filesystem layout. The Dockerfiles set `WORKDIR` or `PYTHONPATH` appropriately. No import statement changes needed.
- **`pyproject.toml` package discovery**: `where = ["."]` with `find` will now discover packages under `services/*/`. This is correct — setuptools recurses into subdirs. The `trading_dagster` package installs as before.
- **`dagster dev` locally**: Dagster reads `pyproject.toml` for `module_name = "trading_dagster"`. Since `pip install -e .` with `where = ["."]` will find `services/dagster/trading_dagster/` and install it as the `trading_dagster` package, running `dagster dev` from repo root works normally after `pip install -e ".[dev]"`. No `PYTHONPATH` changes needed.
