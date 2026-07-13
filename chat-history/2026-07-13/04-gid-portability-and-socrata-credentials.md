# Session 04 — GID Portability Fix + Socrata Credentials

**Date:** 2026-07-13
**Session type:** Cleanup + credential setup
**Phase:** 1.5 (post-verification)

## What Was Done

### 1. DOCKER_GID Build Arg (Portability Fix)

**Problem:** `airflow/Dockerfile` hardcoded `groupadd -g 1001 docker` — the GID 1001 is specific to this WSL2 machine. Any other developer (or CI) with a different docker.sock GID would get permission denied.

**Fix:** Made the GID a Docker build arg with a sensible default:

```dockerfile
ARG DOCKER_GID=999
RUN groupdel docker 2>/dev/null; groupadd -g ${DOCKER_GID} docker && usermod -aG docker airflow
```

**Files changed:**
- `airflow/Dockerfile` — `ARG DOCKER_GID=999` + `groupadd -g ${DOCKER_GID}`
- `docker-compose.yml` — `build.args.DOCKER_GID: ${DOCKER_GID:-999}` under `x-airflow-common`
- `.env` — `DOCKER_GID=1001` (this machine's value)
- `.env.example` — `DOCKER_GID=1001` with comment: `stat -c '%g' /var/run/docker.sock`

**Default 999** covers most native Linux installs. WSL2 typically uses 1001.

**Verified:** Rebuilt image, restarted services, confirmed `docker:x:1001:airflow` in container and docker.sock access works (`docker ps` returns container IDs).

### 2. Socrata Credentials Stored

User created a Chicago Data Portal account and obtained 4 credentials. All stored in `.env` (gitignored):

| Credential | Purpose |
|---|---|
| App Token | Increases API rate limit from 1K to 10K req/hr |
| API Key ID | Authenticated access (Phase 2+ for restricted datasets) |
| API Key Secret | Paired with API Key ID |
| Secret Token | Paired with App Token for OAuth flows |

**Files changed:**
- `.env` — all 4 credentials with comments
- `.env.example` — placeholders for all 4 with registration URL
- `docker-compose.yml` — `SOCRATA_APP_TOKEN: ${SOCRATA_APP_TOKEN}` passed to Airflow container env

**Note:** Only `SOCRATA_APP_TOKEN` is currently used (by `ingestion/download_crime.py` for rate limiting). The other 3 are stored for future use (Phase 2+ may need authenticated Socrata access for Divvy data).

### 3. Documentation Updated

- `chat-history/current-state.md`:
  - Line 98: "docker group (GID configurable via DOCKER_GID build arg)"
  - Line 120: Dockerfile description updated
  - Line 62: Socrata app token status → "configured" (was "OPTIONAL")
  - Line 179: phase-1.5-airflow-dag.md added to files tree
  - Line 220: DockerOperator risk → "Not used in Phase 1.5 — BashOperator was simpler"
  - Line 221: Socrata app token → "configured" (was "not set")
  - Chat History Chunks table: added this session entry

## Errors Hit

None — this was a cleanup/credential session with no runtime errors.

## Lessons

- **Build args for environment-specific GIDs:** Never hardcode a GID in a Dockerfile. Use `ARG` with a default and let each environment override via `.env` + docker-compose `build.args`.
- **Store all credentials even if not immediately needed:** User said "might be useful in the future" — storing all 4 Socrata credentials now saves a future session from tracking them down.
