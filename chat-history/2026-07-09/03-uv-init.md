# uv Init (Project Mode) + uv in Docker

## Summary
Migrated from `uv venv` to `uv init` (project mode) for host Python. Added uv to Airflow Dockerfile via multi-stage COPY. Uses `uv pip install --system` in containers (not `uv sync`).

## Decisions Made
- **uv init over uv venv** — lockfile (`uv.lock`) guarantees reproducible installs. `pyproject.toml` is PEP 621 standard.
- **`uv pip install --system` in Docker (not `uv sync`)** — host and containers need different packages. `uv sync` reads root `uv.lock` (host deps) — would install dbt-core, sodapy etc. in Airflow container unnecessarily.
- **Multi-stage COPY for uv binary** — `COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv` — pulls just the binary, no install script needed

## Files Created/Modified
- `pyproject.toml` — project metadata + dependency declarations
- `uv.lock` — exact versions + hashes for reproducible installs
- `airflow/Dockerfile` — added `COPY --from=ghcr.io/astral-sh/uv:latest` + `uv pip install --system`
- Removed old `.venv/` and `requirements.txt` (replaced by uv init)

## Key Context
- Host Python: managed by uv init (project mode). Activate with `source .venv/bin/activate`.
- Container Python: managed by Dockerfiles. Each container has its own Python.
- Lockfile vs requirements.txt: lockfile pins exact versions + hashes; requirements.txt resolves at install time (can vary between machines).
- `--system` flag: installs into container's system Python, no venv needed (containers are already isolated).

## Errors Encountered
None in this chunk.
