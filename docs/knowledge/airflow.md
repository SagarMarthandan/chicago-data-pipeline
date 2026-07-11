# Airflow

### Useful Commands
```bash
# Start Airflow (via docker compose)
docker compose up airflow-webserver airflow-scheduler

# Run a DAG manually
airflow dags trigger crime_batch

# Check DAG state
airflow dags list
airflow dags state crime_batch <run_id>

# Test a single task
airflow tasks test crime_batch download_crime 2024-01-15
```

### Key Concepts
- **DAG** — Directed Acyclic Graph; defines task dependencies
- **Task** — a unit of work (operator instance)
- **Operator** — template for a task (BashOperator, DockerOperator, etc.)
- **XCom** — cross-task communication (small data only)
- **Sensor** — a special operator that waits for a condition
- **Idempotency** — re-running produces the same result; always design for this

### Executors
| Executor | What it is | When to use | Extra services |
|---|---|---|---|
| `SequentialExecutor` | One task at a time, single thread | Dev/testing only | None |
| `LocalExecutor` | Parallel tasks on one machine | Phase 1 — good fit | None (uses metadata DB) |
| `CeleryExecutor` | Distributes tasks across worker machines | Production / heavy workloads | Redis or RabbitMQ + Celery workers |

### Airflow 2.x vs 3.x — Comprehensive Comparison

Airflow 3.0 (April 2025) is a major breaking release. 2.x reached EOL in April 2026.
This table covers every difference that affects Docker setup, CLI commands, config, and daily use.

#### 1. Docker Compose — Service Commands

| Service | Airflow 2.x | Airflow 3.0 | Why it changed |
|---|---|---|---|
| Web UI | `command: webserver` | `command: api-server` | Web UI is now served by the API server. `airflow webserver` command is removed. |
| Scheduler | No command needed (image had default CMD) | `command: scheduler` (explicit) | 3.0 image has no default CMD — entrypoint runs `airflow` with no subcommand if not specified. |
| Init (migrations) | `bash -c "airflow db upgrade && airflow users create ..."` | `bash -c "airflow db migrate"` | `airflow db upgrade` → `airflow db migrate` (renamed). `airflow users create` removed (SimpleAuthManager). |

#### 2. Docker Compose — Healthcheck

| Aspect | Airflow 2.x | Airflow 3.0 |
|---|---|---|
| Health endpoint | `GET /health` | `GET /api/v2/monitor/health` |
| Response | JSON with status | JSON: `{"metadatabase": {...}, "scheduler": {...}, ...}` |
| Compose healthcheck | `curl --fail http://localhost:8080/health` | `curl --fail http://localhost:8080/api/v2/monitor/health` |

#### 3. Configuration — Environment Variables

| Config | Airflow 2.x env var | Airflow 3.0 env var | Notes |
|---|---|---|---|
| UI/API port | `AIRFLOW__WEBSERVER__WEB_SERVER_PORT` | `AIRFLOW__API__PORT` | Moved from `[webserver]` section to `[api]` section |
| DB connection | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Unchanged |
| Executor | `AIRFLOW__CORE__EXECUTOR` | `AIRFLOW__CORE__EXECUTOR` | Unchanged |
| Load examples | `AIRFLOW__CORE__LOAD_EXAMPLES` | `AIRFLOW__CORE__LOAD_EXAMPLES` | Unchanged |
| DAGs paused at creation | `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION` | Unchanged |

#### 4. Authentication

| Aspect | Airflow 2.x (FAB) | Airflow 3.0 (SimpleAuthManager) |
|---|---|---|
| Auth manager | Flask-AppBuilder (FAB) — default | SimpleAuthManager — new default |
| Create user | `airflow users create --username admin --password admin --role Admin --email ...` | **GONE** — no CLI user creation |
| Define users | Database-backed (created via CLI) | Env var: `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin,viewer:user1` |
| Passwords | Database-backed | JSON file: `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE=/path/to/passwords.json` |
| Passwords file format | N/A | `{"admin": "admin", "user1": "pass1"}` |
| Roles | Created via CLI (`Admin`, `User`, `Viewer`, `Op`) | Predefined: `viewer`, `user`, `op`, `admin` (assigned in env var) |
| File permissions | N/A | `chmod 666` — SimpleAuthManager opens with `a+` mode; airflow user (UID 50000) needs write access |
| Switch to FAB | N/A (already FAB) | Install `apache-airflow-providers-fab`, set `AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager` |

#### 5. CLI Commands

| Command | Airflow 2.x | Airflow 3.0 | Notes |
|---|---|---|---|
| Start web UI | `airflow webserver` | `airflow api-server` | `webserver` command removed |
| Start scheduler | `airflow scheduler` | `airflow scheduler` | Unchanged (but must be explicit in Docker) |
| DB migration | `airflow db upgrade` | `airflow db migrate` | Renamed; `db upgrade` shows deprecation warning |
| Create user | `airflow users create ...` | **REMOVED** | Use SimpleAuthManager env vars + passwords.json |
| List DAGs | `airflow dags list` | `airflow dags list` | Unchanged |
| Trigger DAG | `airflow dags trigger <dag_id>` | `airflow dags trigger <dag_id>` | Unchanged |
| Test task | `airflow tasks test <dag> <task> <date>` | `airflow tasks test <dag> <task> <date>` | Unchanged |
| Check version | `airflow version` | `airflow version` | Unchanged |

#### 6. Docker Image Behavior

| Aspect | Airflow 2.x image | Airflow 3.0 image |
|---|---|---|
| Default CMD | Had a default CMD (scheduler or webserver depending on image variant) | **No default CMD** — entrypoint runs `airflow` with no subcommand, crashes with "arguments required" |
| Entrypoint | `/usr/bin/dumb-init -- /entrypoint` | Same |
| User | `airflow` (UID 50000) | Same |
| Python | 3.11 available | Same |
| Image tag | `apache/airflow:2.x.x-python3.11` | `apache/airflow:3.0.0-python3.11` |

#### 7. Full Docker Compose Side-by-Side (Airflow services only)

```yaml
# ============ Airflow 2.x ============
airflow-init:
  command: >
    bash -c "
    airflow db upgrade
    airflow users create --username admin --password admin --role Admin --email admin@example.com --firstname Admin --lastname User || true
    "

airflow-webserver:
  command: webserver
  ports:
    - "8080:8080"
  healthcheck:
    test: ["CMD-SHELL", "curl --fail http://localhost:8080/health"]

airflow-scheduler:
  # No command needed — image has default CMD
  # (healthcheck optional)


# ============ Airflow 3.0 ============
airflow-init:
  command: >
    bash -c "
    airflow db migrate
    "
  # No user creation — SimpleAuthManager handles it via env vars

airflow-webserver:
  command: api-server
  ports:
    - "8080:8080"
  healthcheck:
    test: ["CMD-SHELL", "curl --fail http://localhost:8080/api/v2/monitor/health"]

airflow-scheduler:
  command: scheduler          # MUST be explicit — no default CMD in 3.0 image
```

#### 8. DAG Writing Changes

| Aspect | Airflow 2.x | Airflow 3.0 |
|---|---|---|
| DAG definition | `with DAG(...)` or `dag = DAG(...)` | Same — DAG API is backward compatible |
| `schedule` param | `schedule_interval="@daily"` | `schedule="@daily"` (`schedule_interval` deprecated, still works) |
| `start_date` | `datetime(2024, 1, 1)` | Same — always fixed past date, never `datetime.now()` |
| `catchup` | `catchup=False` | Same |
| Task dependencies | `task1 >> task2` | Same |
| `@dag` decorator | Available | Available (preferred in 3.0) |
| Asset-based scheduling | Not available | New in 3.0 — DAGs can trigger when assets (datasets) are updated |

#### 9. What Did NOT change

- `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` — same env var, same format
- `AIRFLOW__CORE__EXECUTOR` — same env var
- `AIRFLOW__CORE__LOAD_EXAMPLES` — same env var
- DAG Python API — backward compatible (old DAGs work in 3.0)
- `airflow dags list`, `airflow dags trigger`, `airflow tasks test` — same CLI
- Docker socket mount for DockerOperator — same pattern
- YAML anchors for sharing config — same pattern
- `depends_on: condition: service_completed_successfully` — same Compose feature

**Switching back to FAB auth** (if you need database-backed users):
```bash
# In airflow/requirements.txt:
apache-airflow-providers-fab

# In .env:
AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
```

### Metadata Database
Airflow needs its OWN database to track DAG runs, task states, scheduling info, and logs. This is NOT your analytics data. If you point Airflow at your warehouse DB, it creates tables like `task_instance`, `dag_run`, `xcom` and pollutes your analytics schema. Always use a separate database (can be in the same Postgres instance, just a different DB + user).

### `.env` vs `.env.example`
- `.env.example` — committed to git, documents required variables with placeholder values
- `.env` — gitignored, contains real secrets. Compose reads it automatically at `docker compose up`
- Image names (e.g., `postgres:16-alpine`) go in `docker-compose.yml`, NOT `.env`. `.env` is for secrets and environment-specific config only.

### Scheduling
- `schedule="@daily"` — runs daily
- `schedule="@manual"` or `schedule=None` — trigger by hand (use while debugging)
- `catchup=False` — don't backfill historical runs on first deploy
- `start_date` — fixed past date, NEVER `datetime.now()`

---
