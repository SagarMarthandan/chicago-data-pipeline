# How Everything Connects — Architecture Walkthrough

This section explains how every file in the repo connects to Docker, how uv links to containers, how Spark and Airflow images are built, and how docker-compose.yml ties it all together. Each subsection has its own focused diagram.

### The Big Picture (file → container mapping)

```mermaid
graph TB
    DC[docker-compose.yml]
    ENV[".env"]
    INIT[init.sql]
    PYPROJ["pyproject.toml + uv.lock"]
    VENV[".venv/"]
    ADF[airflow/Dockerfile]
    AREQ[airflow/requirements.txt]
    APW[airflow/passwords.json]
    ADAGS["airflow/dags/"]
    SDF[spark/Dockerfile]
    SJOBS["spark/jobs/"]

    DC -->|builds image from| ADF
    DC -->|builds image from| SDF
    DC -->|mounts| INIT
    DC -->|mounts| APW
    DC -->|mounts| ADAGS
    DC -->|mounts| SJOBS
    DC -->|reads vars from| ENV
    PYPROJ -->|creates| VENV
```

### 1. How uv Links to Docker

uv exists in **two places** — the host and inside the Airflow container — for different reasons:

```mermaid
graph LR
    subgraph "Host (WSL)"
        UV1[uv binary]
        PP[pyproject.toml]
        UL[uv.lock]
        VENV[".venv/"]
        UV1 -->|uv sync reads| UL
        UL -->|installs into| VENV
        PP -->|declares deps| UL
    end

    subgraph "Airflow container (build time)"
        UV2[uv binary<br/>COPY --from=uv image]
        AREQ[requirements.txt]
        SITE["/usr/local/lib/<br/>python3.11/<br/>site-packages/"]
        UV2 -->|uv pip install --system| SITE
        AREQ -->|lists packages| UV2
    end

    UV1 -.->|same tool,<br/>different purpose| UV2
```

**Host uv** manages your development Python:
- `pyproject.toml` declares what packages you need (sodapy, dbt-core, etc.)
- `uv.lock` pins exact versions for reproducibility
- `uv sync` reads the lockfile and installs into `.venv/`
- You use this for running ingestion scripts, DBT dev, ad-hoc queries

**Container uv** is used only at **build time** (in the Dockerfile), not at runtime:
- `COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv` — copies just the uv binary into the image (multi-stage copy, no install script)
- `uv pip install --system -r requirements.txt` — installs Airflow providers into the container's system Python
- `--system` means "install into the container's Python, not a venv" — containers don't need venvs because they're already isolated

**Why not `uv sync` inside Docker?** `uv sync` reads the root `uv.lock`, which has host packages (dbt-core, sodapy). The Airflow container needs different packages (airflow providers). Using `uv pip install -r requirements.txt` installs only what the container needs.

**Key point:** The two uv environments are completely independent. Host packages never enter containers, and container packages never enter the host.

### 2. How Spark Links to Docker

The Spark image is built from `spark/Dockerfile`. It starts from the official `apache/spark:3.5.1` image and adds the PostgreSQL JDBC driver:

```mermaid
graph TB
    BASE["apache/spark:3.5.1<br/>official image"]
    DOWNLOAD["Download postgresql-42.7.3.jar<br/>to /opt/spark/jars/"]
    BASE --> DOWNLOAD
    DOWNLOAD --> CUSTOM["Custom spark image<br/>chicago-data-pipeline-spark"]

    CUSTOM -->|"docker-compose.yml builds"| SM["spark-master container"]
    CUSTOM -->|"docker-compose.yml builds"| SW["spark-worker container"]

    SM -->|"command: spark-class<br/>org.apache.spark.deploy.<br/>master.Master"| MASTER["Master process<br/>UI: port 8180<br/>RPC: port 7077"]
    SW -->|"command: spark-class<br/>org.apache.spark.deploy.<br/>worker.Worker<br/>spark://spark-master:7077"| WORKER["Worker process<br/>connects to master<br/>UI: port 8081"]
```

**Why a custom image?** The official `apache/spark` image doesn't include the PostgreSQL JDBC driver. Without it, `df.write.format("jdbc")` throws `ClassNotFoundException`. Baking the JAR into the image means:
- Works offline (no Maven Central download at runtime)
- Faster startup (no download delay)
- More reliable (no network dependency)

**How docker-compose.yml uses it:**
- `build: ./spark` — tells Compose to build the image from `spark/Dockerfile`
- Both `spark-master` and `spark-worker` use the same image (same `build: ./spark`)
- The `command:` override differentiates them — same image, different process

**How spark/jobs/ is used:**
- `./spark/jobs:/opt/spark/jobs` — bind-mounted into both master and worker
- You write PySpark scripts here (e.g., `crime_batch.py`)
- Can run directly: `spark-submit --master local[*] jobs/crime_batch.py`
- Or Airflow's DockerOperator can submit them

### 3. How Airflow Links to Docker

The Airflow image is built from `airflow/Dockerfile`. It starts from `apache/airflow:3.0.0-python3.11` and adds Docker CLI + Airflow providers:

```mermaid
graph TB
    BASE["apache/airflow:3.0.0-python3.11<br/>official image"]
    DOCKER["Install docker.io CLI<br/>apt-get install"]
    UVCOPY["COPY uv binary<br/>from ghcr.io/astral-sh/uv"]
    PROVIDERS["Install providers<br/>uv pip install --system<br/>-r requirements.txt"]
    BASE --> DOCKER --> UVCOPY --> PROVIDERS
    PROVIDERS --> CUSTOM["Custom airflow image<br/>chicago-data-pipeline-airflow"]

    CUSTOM -->|"builds 3 services"| AI["airflow-init<br/>one-shot: db migrate"]
    CUSTOM -->|"builds 3 services"| AW["airflow-webserver<br/>command: api-server"]
    CUSTOM -->|"builds 3 services"| AS["airflow-scheduler<br/>command: scheduler"]
```

**Why a custom image?** The official Airflow image doesn't include:
1. **Docker CLI** — needed for DockerOperator (runs Spark jobs in isolated containers via docker.sock)
2. **Airflow providers** — the official image includes core only. We need:
   - `apache-airflow-providers-postgres` — PostgresHook, SqlSensor
   - `apache-airflow-providers-docker` — DockerOperator

**How docker-compose.yml uses it:**
- `build: ./airflow` in the YAML anchor `x-airflow-common` — all 3 Airflow services share this
- `<<: *airflow-common` merges the build config into each service
- The `command:` override differentiates the 3 services:
  - `airflow-init`: `bash -c "airflow db migrate"` (runs once, exits 0)
  - `airflow-webserver`: `command: api-server` (serves UI on port 8080)
  - `airflow-scheduler`: `command: scheduler` (runs task scheduler)

**How airflow/ files are used:**

| File | How it's used | Mount type |
|---|---|---|
| `airflow/Dockerfile` | Compose builds the image from this at `docker compose build` time | Build context |
| `airflow/requirements.txt` | Copied into image during build, installed by uv | Build context |
| `airflow/passwords.json` | Bind-mounted into container at `/opt/airflow/config/passwords.json` | Bind mount (runtime) |
| `airflow/dags/` | Bind-mounted into container at `/opt/airflow/dags/` | Bind mount (runtime) |

**Why dags/ is bind-mounted (not baked into image):** You edit DAGs frequently. A bind mount means changes on the host appear instantly in the container — no rebuild needed. If you baked DAGs into the image, you'd need to rebuild every time you change a DAG.

### 4. How docker-compose.yml Ties Everything Together

docker-compose.yml is the **orchestrator** — it defines all 6 services, their dependencies, and how files flow into containers:

```mermaid
graph TB
    ENV[.env file]
    DC[docker-compose.yml]
    ENV -->|"$$VAR interpolated<br/>at compose time"| DC

    DC -->|image: postgres:16-alpine| PG[postgres container]
    DC -->|build: ./spark| SP[spark-master + spark-worker]
    DC -->|build: ./airflow| AF[airflow-init + webserver + scheduler]

    INIT[init.sql] -->|bind mount: /docker-entrypoint-initdb.d/| PG
    SJOBS["spark/jobs/"] -->|bind mount: /opt/spark/jobs| SP
    ADAGS["airflow/dags/"] -->|bind mount: /opt/airflow/dags| AF
    APW[airflow/passwords.json] -->|bind mount: /opt/airflow/config/| AF
    DOCKSOCK["/var/run/docker.sock"] -->|bind mount| AF

    PGDATA[(postgres_data<br/>named volume)] -->|persists data| PG
    AFLOGS[(airflow_logs<br/>named volume)] -->|persists logs| AF
```

**The two types of volume mounts:**

| Type | Syntax | When to use | Example in this project |
|---|---|---|---|
| **Bind mount** | `./host/path:/container/path` | When you want host edits to appear in container immediately | `./airflow/dags:/opt/airflow/dags` |
| **Named volume** | `volume_name:/container/path` | When you want data to persist but don't need host access | `postgres_data:/var/lib/postgresql/data` |

**The startup order (enforced by `depends_on`):**

```mermaid
graph LR
    PG["postgres<br/>healthcheck: pg_isready"] -->|service_healthy| AI["airflow-init<br/>runs db migrate"]
    AI -->|"service_completed_successfully<br/>exited 0"| AW["airflow-webserver<br/>api-server"]
    AI -->|"service_completed_successfully<br/>exited 0"| AS["airflow-scheduler<br/>scheduler"]
    SM["spark-master<br/>healthcheck: port 8080"] -->|service_healthy| SW["spark-worker<br/>connects to master"]
```

This means:
1. Postgres starts first and must pass `pg_isready` healthcheck
2. `airflow-init` waits for Postgres to be healthy, then runs `airflow db migrate`, then exits 0
3. `airflow-webserver` and `airflow-scheduler` wait for `airflow-init` to exit 0, then start
4. `spark-master` starts independently (no dependency on Postgres or Airflow)
5. `spark-worker` waits for `spark-master` to be healthy, then connects

### 5. How init.sql Links to Postgres

```mermaid
graph LR
    INIT[init.sql on host] -->|"docker-compose.yml<br/>bind mount"| DIRENTRY["/docker-entrypoint-initdb.d/<br/>inside postgres container"]
    DIRENTRY -->|Postgres runs scripts<br/>in this dir alphabetically<br/>on FIRST startup only| PG[(postgres_data<br/>volume empty?)]
    PG -->|yes: empty volume| RUN[Execute init.sql<br/>creates schemas + airflow DB]
    PG -->|no: volume has data| SKIP[Skip init.sql<br/>data already exists]
```

**How it works:**
- `./init.sql:/docker-entrypoint-initdb.d/init.sql` — Compose bind-mounts the file into Postgres's init directory
- The `postgres:16-alpine` image has an entrypoint script that checks: is the data volume empty?
- If empty (first run): runs all scripts in `/docker-entrypoint-initdb.d/` alphabetically, then starts Postgres
- If not empty (subsequent runs): skips init scripts entirely, starts Postgres with existing data

**What init.sql creates:**
- 3 schemas: `raw`, `staging`, `mart` in the `chicago_analytics` database
- `airflow` user with password `airflow_pass`
- `airflow_metadata` database owned by `airflow` user
- Grants: `chicago` user gets full access to all 3 schemas

**If you change init.sql after the first run:** You must destroy the volume and recreate:
```bash
docker compose down -v    # WARNING: destroys all data
docker compose up -d      # volume is empty again, init.sql runs
```

### 6. How .env Links to docker-compose.yml

```mermaid
graph LR
    ENV[".env file<br/>POSTGRES_USER=chicago<br/>AIRFLOW__API__PORT=8080<br/>..."]
    DC["docker-compose.yml<br/>uses ${VAR} syntax"]
    ENV -->|"Compose reads .env<br/>and substitutes ${VAR}"| DC
    DC -->|passes values as<br/>environment: block| C1[postgres container]
    DC -->|passes values as<br/>environment: block| C2[airflow containers]
```

**How it works:**
- Compose automatically reads `.env` from the same directory as `docker-compose.yml`
- Any `${VAR}` in docker-compose.yml is replaced with the value from `.env`
- Example: `POSTGRES_USER: ${POSTGRES_USER}` becomes `POSTGRES_USER: chicago`
- The `.env` file is gitignored (contains secrets). `.env.example` is committed as a template

**`$$` vs `$` in Compose commands:**
- `$VAR` — Compose interpolates from `.env` at compose time (before the container starts)
- `$$VAR` — escapes to literal `$VAR`, so the container's bash shell expands it at runtime
- Use `$$` when you need bash to read an env var that was set via the `environment:` block

### 7. How docker.sock Links Airflow to Spark (DockerOperator)

When an Airflow DAG needs to run a Spark job, it uses DockerOperator to spawn a new container:

```mermaid
graph TB
    subgraph "Airflow container"
        DAG["DAG task:<br/>DockerOperator"]
        DOCKERCLI[docker CLI<br/>installed in image]
        DAG --> DOCKERCLI
    end

    DOCKSOCK["/var/run/docker.sock<br/>mounted from host"]
    DOCKERCLI -->|talks via| DOCKSOCK

    DOCKSOCK -->|creates sibling container<br/>on host Docker daemon| SPARKJOB[Spark job container<br/>runs spark-submit<br/>on spark-master network]

    SPARKJOB -->|writes results via JDBC| PG[(postgres)]
```

**How it works:**
1. The Airflow Dockerfile installs `docker.io` (Docker CLI) into the Airflow image
2. docker-compose.yml mounts `/var/run/docker.sock` from the host into the Airflow container
3. When DockerOperator runs, it uses the Docker CLI to talk to the host's Docker daemon via the socket
4. The daemon creates a **sibling container** (not inside the Airflow container — on the host alongside it)
5. That sibling container runs the Spark job and writes results to Postgres via JDBC

**Why this pattern?** It keeps Spark jobs isolated — each job runs in a fresh container with clean state. The Airflow container doesn't need Spark installed; it just needs the Docker CLI to spawn containers that do have Spark.

### 8. Complete File → Container Reference

| Host file | Used by | How | When |
|---|---|---|---|
| `docker-compose.yml` | Docker Compose | Read by `docker compose up` | Every startup |
| `.env` | docker-compose.yml | `${VAR}` interpolation | Every startup |
| `init.sql` | postgres container | Bind mount to `/docker-entrypoint-initdb.d/` | First startup only (empty volume) |
| `airflow/Dockerfile` | Compose build | `build: ./airflow` | `docker compose build` |
| `airflow/requirements.txt` | Dockerfile | `COPY` + `uv pip install` | Build time |
| `airflow/passwords.json` | airflow containers | Bind mount to `/opt/airflow/config/` | Every startup (runtime) |
| `airflow/dags/*.py` | airflow containers | Bind mount to `/opt/airflow/dags/` | Every startup (runtime, live-edited) |
| `spark/Dockerfile` | Compose build | `build: ./spark` | `docker compose build` |
| `spark/jobs/*.py` | spark + airflow containers | Bind mount to `/opt/spark/jobs/` | Every startup (runtime) |
| `pyproject.toml` | uv (host only) | `uv sync` reads it | Host dev only |
| `uv.lock` | uv (host only) | `uv sync` reads it | Host dev only |
| `.venv/` | Host Python | Created by `uv sync` | Host dev only |

---
