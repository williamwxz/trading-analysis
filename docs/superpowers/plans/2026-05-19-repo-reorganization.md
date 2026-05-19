# Repo Reorganization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restructure the repo so all ECS services live under `services/`, shared libraries stay in `libs/`, schemas move to `infra/schemas/`, and each service has its tests collocated alongside it.

**Architecture:** Pure filesystem reorganization — no Python logic changes. All Docker build contexts remain the repo root so `libs/` is still reachable. Package names are unchanged; only directory locations shift.

**Tech Stack:** Python 3.11, Dagster, Docker, GitHub Actions, pytest

---

## File Map

**Created:**
- `services/dagster/Dockerfile` (moved from `Dockerfile`)
- `services/dagster/dagster.yaml` (moved from `dagster.yaml`)
- `services/dagster/trading_dagster/` (moved from `trading_dagster/`)
- `services/dagster/tests/` (moved from `tests/test_pnl_*.py`, `tests/test_binance_*.py`, `tests/test_exchange_price_service.py`)
- `services/streaming/Dockerfile` (moved from `streaming/Dockerfile`)
- `services/streaming/streaming/` (moved from `streaming/`)
- `services/streaming/tests/` (moved from `tests/streaming/`)
- `services/pnl_consumer/Dockerfile` (moved from `pnl_consumer/Dockerfile`)
- `services/pnl_consumer/pnl_consumer/` (moved from `pnl_consumer/`)
- `services/pnl_consumer/tests/` (moved from `tests/pnl_consumer/`)
- `services/flink_pnl/Dockerfile` (moved from `flink_pnl/Dockerfile`)
- `services/flink_pnl/requirements.txt` (moved from `flink_pnl/requirements.txt`)
- `services/flink_pnl/flink_pnl/` (moved from `flink_pnl/flink_pnl/`)
- `services/flink_pnl/tests/` (moved from `flink_pnl/tests/`)
- `infra/schemas/` (moved from `schemas/`)
- `libs/tests/` (moved from `tests/libs/`)

**Modified:**
- `pyproject.toml` — update `testpaths`, `pythonpath`
- `.github/workflows/ci-cd.yml` — update path filters, `-f` flags, test commands
- `docker-compose.yml` — update build context/dockerfile paths
- `CLAUDE.md` — update all path references
- `services/dagster/Dockerfile` — update `dagster.yaml` COPY path
- `services/streaming/Dockerfile` — update COPY paths
- `services/pnl_consumer/Dockerfile` — update COPY paths
- `services/flink_pnl/Dockerfile` — update COPY paths

**Deleted:**
- `Dockerfile` (root)
- `dagster.yaml` (root)
- `trading_dagster/` (root)
- `streaming/` (root)
- `pnl_consumer/` (root)
- `flink_pnl/` (root)
- `schemas/` (root)
- `tests/` (root — entire directory after all subdirs/files moved)

---

### Task 1: Create directory skeleton

**Files:**
- Create: `services/dagster/`, `services/streaming/`, `services/pnl_consumer/`, `services/flink_pnl/`
- Create: `infra/schemas/`
- Create: `libs/tests/`

- [ ] **Step 1: Create all new directories**

```bash
mkdir -p services/dagster/tests
mkdir -p services/streaming/tests
mkdir -p services/pnl_consumer/tests
mkdir -p services/flink_pnl/tests
mkdir -p infra/schemas
mkdir -p libs/tests
```

- [ ] **Step 2: Verify structure**

```bash
find services infra/schemas libs/tests -maxdepth 2 -type d | sort
```

Expected output includes all 8 directories above.

- [ ] **Step 3: Commit**

```bash
git add services/ infra/schemas/ libs/tests/
git commit -m "chore: scaffold services/, infra/schemas/, libs/tests/ directories"
```

---

### Task 2: Move dagster service

**Files:**
- Move: `trading_dagster/` → `services/dagster/trading_dagster/`
- Move: `Dockerfile` → `services/dagster/Dockerfile`
- Move: `dagster.yaml` → `services/dagster/dagster.yaml`

- [ ] **Step 1: Move the trading_dagster package**

```bash
git mv trading_dagster services/dagster/trading_dagster
```

- [ ] **Step 2: Move Dockerfile and dagster.yaml**

```bash
git mv Dockerfile services/dagster/Dockerfile
git mv dagster.yaml services/dagster/dagster.yaml
```

- [ ] **Step 3: Update COPY path in services/dagster/Dockerfile**

The Dockerfile copies `dagster.yaml` from the build context root. Since the file now lives at `services/dagster/dagster.yaml`, but the build context is still the repo root (`.`), the `COPY dagster.yaml .` line needs to become `COPY services/dagster/dagster.yaml .`. Open `services/dagster/Dockerfile` and change:

```dockerfile
COPY dagster.yaml .
```

to:

```dockerfile
COPY services/dagster/dagster.yaml dagster.yaml
```

Also update the `COPY trading_dagster/` lines (builder stage and runtime stage) — they stay the same because `pyproject.toml` installs the package and the runtime COPY uses the new location:

Builder stage — change:
```dockerfile
COPY trading_dagster/ trading_dagster/
```
to:
```dockerfile
COPY services/dagster/trading_dagster/ trading_dagster/
```

Runtime stage — same change:
```dockerfile
COPY trading_dagster/ trading_dagster/
```
to:
```dockerfile
COPY services/dagster/trading_dagster/ trading_dagster/
```

- [ ] **Step 4: Verify Dockerfile looks correct**

```bash
grep -n "COPY" services/dagster/Dockerfile
```

Expected — all four COPY lines:
```
COPY pyproject.toml .
COPY services/dagster/trading_dagster/ trading_dagster/
COPY libs/ libs/
COPY services/dagster/dagster.yaml dagster.yaml
```
(runtime stage has the same COPY lines minus pyproject.toml, plus `workspace.yaml`)

- [ ] **Step 5: Commit**

```bash
git add services/dagster/
git commit -m "chore: move dagster service to services/dagster/"
```

---

### Task 3: Move dagster tests

**Files:**
- Move: `tests/test_pnl_compute.py` → `services/dagster/tests/test_pnl_compute.py`
- Move: `tests/test_pnl_recent_recompute.py` → `services/dagster/tests/test_pnl_recent_recompute.py`
- Move: `tests/test_binance_backfill_skip.py` → `services/dagster/tests/test_binance_backfill_skip.py`
- Move: `tests/test_binance_integration.py` → `services/dagster/tests/test_binance_integration.py`
- Move: `tests/test_exchange_price_service.py` → `services/dagster/tests/test_exchange_price_service.py`

- [ ] **Step 1: Move test files**

```bash
git mv tests/test_pnl_compute.py          services/dagster/tests/test_pnl_compute.py
git mv tests/test_pnl_recent_recompute.py  services/dagster/tests/test_pnl_recent_recompute.py
git mv tests/test_binance_backfill_skip.py services/dagster/tests/test_binance_backfill_skip.py
git mv tests/test_binance_integration.py   services/dagster/tests/test_binance_integration.py
git mv tests/test_exchange_price_service.py services/dagster/tests/test_exchange_price_service.py
```

- [ ] **Step 2: Add `__init__.py` to the new tests dir**

```bash
touch services/dagster/tests/__init__.py
```

- [ ] **Step 3: Commit**

```bash
git add services/dagster/tests/
git commit -m "chore: move dagster tests to services/dagster/tests/"
```

---

### Task 4: Move streaming service

**Files:**
- Move: `streaming/` (directory contents) → `services/streaming/streaming/`
- Move: `tests/streaming/` → `services/streaming/tests/`

- [ ] **Step 1: Move the streaming package**

```bash
git mv streaming services/streaming/streaming
```

- [ ] **Step 2: Move streaming tests**

```bash
git mv tests/streaming services/streaming/tests
```

- [ ] **Step 3: Update COPY paths in services/streaming/Dockerfile**

Open `services/streaming/Dockerfile`. Change:
```dockerfile
COPY streaming/ ./streaming/
```
to:
```dockerfile
COPY services/streaming/streaming/ ./streaming/
```

- [ ] **Step 4: Verify**

```bash
grep "COPY" services/streaming/Dockerfile
```

Expected:
```
COPY pyproject.toml .
COPY services/streaming/streaming/ ./streaming/
```

- [ ] **Step 5: Commit**

```bash
git add services/streaming/
git commit -m "chore: move streaming service to services/streaming/"
```

---

### Task 5: Move pnl_consumer service

**Files:**
- Move: `pnl_consumer/` (directory contents minus Dockerfile) → `services/pnl_consumer/pnl_consumer/`
- Move: `pnl_consumer/Dockerfile` → `services/pnl_consumer/Dockerfile`
- Move: `pnl_consumer/requirements.txt` → `services/pnl_consumer/requirements.txt`
- Move: `tests/pnl_consumer/` → `services/pnl_consumer/tests/`

- [ ] **Step 1: Move pnl_consumer directory**

`git mv` on the whole dir first, then rearrange inside:

```bash
git mv pnl_consumer services/pnl_consumer/pnl_consumer
```

This puts the Dockerfile at `services/pnl_consumer/pnl_consumer/Dockerfile` and requirements.txt at `services/pnl_consumer/pnl_consumer/requirements.txt` — move them up one level:

```bash
git mv services/pnl_consumer/pnl_consumer/Dockerfile    services/pnl_consumer/Dockerfile
git mv services/pnl_consumer/pnl_consumer/requirements.txt services/pnl_consumer/requirements.txt
```

- [ ] **Step 2: Move pnl_consumer tests**

```bash
git mv tests/pnl_consumer services/pnl_consumer/tests
```

- [ ] **Step 3: Update COPY paths in services/pnl_consumer/Dockerfile**

Open `services/pnl_consumer/Dockerfile`. Change:
```dockerfile
COPY pnl_consumer/requirements.txt .
```
to:
```dockerfile
COPY services/pnl_consumer/requirements.txt .
```

Change:
```dockerfile
COPY streaming/ ./streaming/
```
to:
```dockerfile
COPY services/streaming/streaming/ ./streaming/
```

Change:
```dockerfile
COPY pnl_consumer/ ./pnl_consumer/
```
to:
```dockerfile
COPY services/pnl_consumer/pnl_consumer/ ./pnl_consumer/
```

- [ ] **Step 4: Verify**

```bash
grep "COPY" services/pnl_consumer/Dockerfile
```

Expected:
```
COPY services/pnl_consumer/requirements.txt .
COPY services/streaming/streaming/ ./streaming/
COPY services/pnl_consumer/pnl_consumer/ ./pnl_consumer/
COPY libs/ ./libs/
```

- [ ] **Step 5: Commit**

```bash
git add services/pnl_consumer/
git commit -m "chore: move pnl_consumer service to services/pnl_consumer/"
```

---

### Task 6: Move flink_pnl service

**Files:**
- Move: `flink_pnl/flink_pnl/` → `services/flink_pnl/flink_pnl/`
- Move: `flink_pnl/Dockerfile` → `services/flink_pnl/Dockerfile`
- Move: `flink_pnl/requirements.txt` → `services/flink_pnl/requirements.txt`
- Move: `flink_pnl/tests/` → `services/flink_pnl/tests/`

- [ ] **Step 1: Move flink_pnl contents**

```bash
git mv flink_pnl/flink_pnl        services/flink_pnl/flink_pnl
git mv flink_pnl/Dockerfile        services/flink_pnl/Dockerfile
git mv flink_pnl/requirements.txt  services/flink_pnl/requirements.txt
git mv flink_pnl/tests             services/flink_pnl/tests
```

- [ ] **Step 2: Remove now-empty flink_pnl root dir**

```bash
git rm -r flink_pnl
```

(If there are only `__pycache__` files left, `git rm -r` handles it. If flink_pnl dir is already gone after the moves, skip.)

- [ ] **Step 3: Update COPY paths in services/flink_pnl/Dockerfile**

Open `services/flink_pnl/Dockerfile`. Change:
```dockerfile
COPY flink_pnl/requirements.txt /tmp/requirements.txt
```
to:
```dockerfile
COPY services/flink_pnl/requirements.txt /tmp/requirements.txt
```

Change:
```dockerfile
COPY libs/ ./libs/
COPY streaming/ ./streaming/
COPY pnl_consumer/ ./pnl_consumer/
COPY flink_pnl/flink_pnl/ ./flink_pnl/
```
to:
```dockerfile
COPY libs/ ./libs/
COPY services/streaming/streaming/ ./streaming/
COPY services/pnl_consumer/pnl_consumer/ ./pnl_consumer/
COPY services/flink_pnl/flink_pnl/ ./flink_pnl/
```

- [ ] **Step 4: Verify**

```bash
grep "COPY" services/flink_pnl/Dockerfile
```

Expected:
```
COPY services/flink_pnl/requirements.txt /tmp/requirements.txt
COPY libs/ ./libs/
COPY services/streaming/streaming/ ./streaming/
COPY services/pnl_consumer/pnl_consumer/ ./pnl_consumer/
COPY services/flink_pnl/flink_pnl/ ./flink_pnl/
```

- [ ] **Step 5: Commit**

```bash
git add services/flink_pnl/
git rm -r --cached flink_pnl 2>/dev/null || true
git commit -m "chore: move flink_pnl service to services/flink_pnl/"
```

---

### Task 7: Move schemas and libs/tests

**Files:**
- Move: `schemas/` → `infra/schemas/`
- Move: `tests/libs/` → `libs/tests/`

- [ ] **Step 1: Move schemas**

```bash
git mv schemas/clickhouse_cloud.sql infra/schemas/clickhouse_cloud.sql
git rm schemas/__init__.py 2>/dev/null || git mv schemas/__init__.py /tmp/ && true
```

If `schemas/__init__.py` has no content worth keeping, just remove it:
```bash
git rm schemas/__init__.py
```

Then remove the now-empty `schemas/` directory (git does this automatically after `git rm`).

- [ ] **Step 2: Move libs/tests**

```bash
git mv tests/libs/test_computation.py  libs/tests/test_computation.py
git mv tests/libs/test_minute_loop.py  libs/tests/test_minute_loop.py
git mv tests/libs/test_pnl_formula.py  libs/tests/test_pnl_formula.py
git mv tests/libs/__init__.py          libs/tests/__init__.py
```

- [ ] **Step 3: Remove remaining tests/ artifacts**

After all moves, `tests/` should contain only `__init__.py`, `__pycache__/`, and the empty `flink_job/` stub (which only ever had `__pycache__`):

```bash
git rm tests/__init__.py
git rm -r tests/flink_job 2>/dev/null || true
# verify tests/ is now fully empty (only __pycache__ left, which is gitignored)
ls tests/
```

Expected: empty or only `__pycache__/` (gitignored).

- [ ] **Step 4: Commit**

```bash
git add infra/schemas/ libs/tests/
git commit -m "chore: move schemas to infra/schemas/, libs tests to libs/tests/"
```

---

### Task 8: Update pyproject.toml

**Files:**
- Modify: `pyproject.toml`

- [ ] **Step 1: Update testpaths and pythonpath**

Open `pyproject.toml`. Replace:
```toml
[tool.pytest.ini_options]
testpaths = ["tests", "flink_pnl/tests"]
pythonpath = ["."]
```
with:
```toml
[tool.pytest.ini_options]
testpaths = [
    "libs/tests",
    "services/dagster/tests",
    "services/streaming/tests",
    "services/pnl_consumer/tests",
    "services/flink_pnl/tests",
]
pythonpath = ["."]
```

- [ ] **Step 2: Run unit tests to verify discovery works**

```bash
pytest -m "not integration and not streaming_integration" --collect-only 2>&1 | head -40
```

Expected: pytest collects tests from all 5 directories with no import errors.

- [ ] **Step 3: Commit**

```bash
git add pyproject.toml
git commit -m "chore: update pytest testpaths to services/ layout"
```

---

### Task 9: Update docker-compose.yml

**Files:**
- Modify: `docker-compose.yml`

- [ ] **Step 1: Update dagster build config**

Open `docker-compose.yml`. The `dagster-webserver` and `dagster-daemon` services currently have `build: .`. Change both to:

```yaml
    build:
      context: .
      dockerfile: services/dagster/Dockerfile
```

- [ ] **Step 2: Verify the file**

```bash
grep -A3 "build:" docker-compose.yml
```

Expected: both dagster services reference `services/dagster/Dockerfile`.

- [ ] **Step 3: Commit**

```bash
git add docker-compose.yml
git commit -m "chore: update docker-compose to use services/dagster/Dockerfile"
```

---

### Task 10: Update CI/CD workflow

**Files:**
- Modify: `.github/workflows/ci-cd.yml`

- [ ] **Step 1: Update path filters**

In the `detect-changes` job, replace the `filters` block:

```yaml
          filters: |
            dagster:
              - 'trading_dagster/**'
              - 'libs/**'
              - 'Dockerfile'
              - 'pyproject.toml'
              - 'dagster.yaml'
              - '.github/workflows/ci-cd.yml'
            streaming:
              - 'streaming/**'
            pnl-consumer:
              - 'pnl_consumer/**'
              - 'libs/**'
            flink-pnl:
              - 'flink_pnl/**'
              - 'libs/**'
              - 'streaming/models.py'
            terraform:
              - 'infra/terraform/**'
            grafana:
              - 'infra/grafana/**'
```

with:

```yaml
          filters: |
            dagster:
              - 'services/dagster/**'
              - 'libs/**'
              - 'pyproject.toml'
              - '.github/workflows/ci-cd.yml'
            streaming:
              - 'services/streaming/**'
            pnl-consumer:
              - 'services/pnl_consumer/**'
              - 'libs/**'
            flink-pnl:
              - 'services/flink_pnl/**'
              - 'libs/**'
              - 'services/streaming/streaming/models.py'
            terraform:
              - 'infra/terraform/**'
            grafana:
              - 'infra/grafana/**'
```

- [ ] **Step 2: Update test commands in the `test` job**

Replace:
```yaml
      - name: Run tests
        run: |
          pytest tests/ -v --tb=short -m "not integration"
          pytest flink_pnl/tests/ -v --tb=short -m "not integration"
```

with:
```yaml
      - name: Run tests
        run: |
          pytest libs/tests/ services/dagster/tests/ services/streaming/tests/ services/pnl_consumer/tests/ services/flink_pnl/tests/ -v --tb=short -m "not integration and not streaming_integration"
```

- [ ] **Step 3: Update integration test path**

Replace:
```yaml
      - name: Run streaming integration tests
        run: pytest tests/streaming/test_streaming_integration.py -v --tb=short -m streaming_integration
```

with:
```yaml
      - name: Run streaming integration tests
        run: pytest services/streaming/tests/test_streaming_integration.py -v --tb=short -m streaming_integration
```

- [ ] **Step 4: Update dagster build command**

In `build-dagster`, the build step currently runs `docker buildx build ... .` (no `-f` flag, uses root Dockerfile). Change to:

```yaml
          docker buildx build \
            --cache-from type=registry,ref=$IMAGE_CACHE \
            --cache-to   type=registry,ref=$IMAGE_CACHE,mode=max \
            --provenance=false \
            -f services/dagster/Dockerfile \
            -t $IMAGE_URI \
            --push .
```

- [ ] **Step 5: Update streaming build command**

Change `-f streaming/Dockerfile` to `-f services/streaming/Dockerfile`.

- [ ] **Step 6: Update pnl-consumer build command**

Change `-f pnl_consumer/Dockerfile` to `-f services/pnl_consumer/Dockerfile`.

- [ ] **Step 7: Update flink-pnl build command**

Change `-f flink_pnl/Dockerfile` to `-f services/flink_pnl/Dockerfile`.

- [ ] **Step 8: Verify all Dockerfile references are updated**

```bash
grep -n "Dockerfile\|flink_pnl/\|pnl_consumer/\|streaming/\|trading_dagster/" .github/workflows/ci-cd.yml
```

Expected: zero matches (all old paths gone).

- [ ] **Step 9: Commit**

```bash
git add .github/workflows/ci-cd.yml
git commit -m "chore: update CI path filters and build commands for services/ layout"
```

---

### Task 11: Update CLAUDE.md

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update path references**

In `CLAUDE.md`, find and update all stale path references:

| Find | Replace |
|------|---------|
| `schemas/clickhouse_cloud.sql` | `infra/schemas/clickhouse_cloud.sql` |
| `` < schemas/clickhouse_cloud.sql`` | `` < infra/schemas/clickhouse_cloud.sql`` |
| `flink_pnl/tests` | `services/flink_pnl/tests` |
| `pytest tests/` | `pytest libs/tests/ services/` |
| `testpaths = ["tests", "flink_pnl/tests"]` | updated testpaths block |

Check for any remaining stale paths:
```bash
grep -n "flink_pnl/\|pnl_consumer/\|streaming/\|trading_dagster/\|schemas/" CLAUDE.md | grep -v "services/" | grep -v "infra/schemas/"
```

Fix any remaining hits.

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: update CLAUDE.md path references for services/ layout"
```

---

### Task 12: Verify everything works

- [ ] **Step 1: Run the full unit test suite**

```bash
pytest libs/tests/ services/dagster/tests/ services/streaming/tests/ services/pnl_consumer/tests/ services/flink_pnl/tests/ -v --tb=short -m "not integration and not streaming_integration"
```

Expected: all tests pass with no import errors.

- [ ] **Step 2: Verify Docker build contexts**

Do a dry-run build for each service (no push):

```bash
docker build -f services/dagster/Dockerfile     -t test-dagster     . --no-cache 2>&1 | tail -5
docker build -f services/streaming/Dockerfile   -t test-streaming   . --no-cache 2>&1 | tail -5
docker build -f services/pnl_consumer/Dockerfile -t test-pnl        . --no-cache 2>&1 | tail -5
docker build -f services/flink_pnl/Dockerfile   -t test-flink       . --no-cache 2>&1 | tail -5
```

Expected: each ends with `Successfully built ...` or `writing image sha256:...`.

- [ ] **Step 3: Verify no old top-level dirs remain**

```bash
ls trading_dagster streaming pnl_consumer flink_pnl schemas tests 2>&1
```

Expected: `No such file or directory` for all of them.

- [ ] **Step 4: Verify package is still importable**

```bash
python -c "import trading_dagster; import streaming; import pnl_consumer; import flink_pnl; import libs; print('all imports ok')"
```

Expected: `all imports ok`

- [ ] **Step 5: Final commit if any stray files remain**

```bash
git status
# stage and commit anything untracked or modified
git add -A
git commit -m "chore: cleanup stale files after repo reorganization" 2>/dev/null || echo "nothing to commit"
```
