# uv (Python Package Manager)

### What is uv?
`uv` is a fast Python package manager by Astral. It handles virtual environment creation, dependency resolution, and package installation — 10-100x faster than pip. It's written in Rust.

### Host vs Container Python
This project has TWO Python environments:

| Where | Environment | Managed by | Purpose |
|---|---|---|---|
| Host (WSL) | `.venv/` (uv) | `pyproject.toml` + `uv.lock` | Running ingestion scripts, DBT dev, ad-hoc queries |
| Containers | Container Python | `airflow/requirements.txt`, `spark/Dockerfile` | Running services inside Docker |

The host venv is for **development and testing**. Containers have their own isolated Python.

### Docker + uv Relationship
uv manages host Python. Docker images have their own Python inside the container. They're independent — but we use uv **inside** Docker too, for faster builds.

```
Host (WSL)                    Containers
┌──────────────┐              ┌──────────────────────┐
│ uv + .venv   │              │ Container Python     │
│ pyproject.toml│   independent│ + uv (for fast pip)  │
│ uv.lock      │              │ + requirements.txt   │
└──────────────┘              └──────────────────────┘
```

**How uv is used in Docker:**
- `COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv` — copies the uv binary from the official image (multi-stage copy, no install script needed)
- `uv pip install --system --no-cache-dir -r requirements.txt` — installs into the container's system Python

**Why `uv pip install --system` and NOT `uv sync`:**
- The host and containers need DIFFERENT packages. `uv sync` reads the root `uv.lock` which has host deps (dbt-core, sodapy, etc.) — not what the Airflow container needs.
- `uv pip install --system -r airflow/requirements.txt` installs only container-specific deps, using uv's fast resolver.
- `--system` installs into the container's system Python (no venv needed inside containers).

### Setup (uv init — project mode)
```bash
# One-time: initialize project (creates pyproject.toml)
uv init --bare --name chicago-data-pipeline

# Add dependencies (updates pyproject.toml + uv.lock + installs)
uv add requests sodapy dbt-core dbt-postgres python-dotenv psycopg2-binary

# Each new terminal: activate the venv
source .venv/bin/activate

# Recreate venv from lockfile (e.g., after cloning on a new machine)
uv sync
```

### Common Commands
```bash
uv add <package>              # add a dependency (updates pyproject.toml + uv.lock)
uv remove <package>           # remove a dependency
uv sync                       # install exact versions from uv.lock (reproducible)
uv pip list                   # list installed packages
uv lock --upgrade             # update all packages to latest compatible versions
deactivate                    # exit venv
```

### Key Files
| File | Committed? | Purpose |
|---|---|---|
| `pyproject.toml` | Yes | Project metadata + dependency declarations (human-edited) |
| `uv.lock` | Yes | Exact versions + hashes for reproducible installs (machine-generated) |
| `.venv/` | No (gitignored) | The actual virtual environment with installed packages |

### uv venv vs uv init
| | `uv venv` (simple) | `uv init` (project mode) |
|---|---|---|
| Config file | `requirements.txt` | `pyproject.toml` |
| Lockfile | None | `uv.lock` (exact versions pinned) |
| Add dependency | Edit `requirements.txt` manually | `uv add <package>` (auto-updates toml + lock) |
| Reproducibility | Versions resolve at install time (can vary) | Lockfile guarantees identical installs |
| Install command | `uv pip install -r requirements.txt` | `uv sync` (reads lockfile) |
| Standard | Legacy (pip-compatible) | Modern Python standard (PEP 621) |

This project uses `uv init` (project mode) for reproducibility and modern tooling.

---
