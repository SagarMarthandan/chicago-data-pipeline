# Airflow 2.8.4 → 3.0.0 Upgrade

## Summary
Discovered Airflow 2.8.4 is EOL (end-of-life April 2026). Researched Airflow 3.x breaking changes. Upgraded to Airflow 3.0.0 (stable, 15 months of production hardening). Migrated authentication from Flask-AppBuilder (FAB) to SimpleAuthManager (3.0 default). Updated Dockerfile, docker-compose.yml, .env.example. Created passwords.json. Updated all three docs.

## Decisions Made
- **Airflow 3.0.0 (not 3.3.0)** — 3.0.0 has 15 months of production hardening. 3.3.0 released July 6, 2026 (3 days old — too new for stability).
- **SimpleAuthManager (default)** — simpler than FAB for dev. No database-backed users. If we need `airflow users create` later, can install `apache-airflow-providers-fab` and switch to FabAuthManager.
- **Passwords file mounted into container** — static, predictable password (`admin`/`admin`). SimpleAuthManager auto-generates passwords if file doesn't exist — mounting gives us control.

## Breaking Changes from 2.x → 3.0
| Change | 2.x | 3.0 | Impact |
|---|---|---|---|
| Authentication | Flask-AppBuilder (FAB) | SimpleAuthManager (new default) | `airflow users create` CLI is GONE |
| User creation | `airflow users create --username ... --password ...` | `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS=admin:admin` + passwords.json | No CLI user creation |
| Passwords | Database-backed | JSON file (`passwords.json`) | Mount file into container |
| Roles | Created via CLI | Predefined: viewer, user, op, admin | Assigned in env var |
| `airflow db migrate` | Works | Still works | No change |
| `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | Works | Still works (core components only) | No change for our setup |

## Files Created/Modified
- `airflow/Dockerfile` — image tag `apache/airflow:2.8.4-python3.11` → `apache/airflow:3.0.0-python3.11`
- `docker-compose.yml` — removed `airflow users create` from airflow-init, added SimpleAuthManager env vars + passwords.json mount, removed `_AIRFLOW_WWW_USER_*` vars
- `.env.example` — removed `AIRFLOW_WWW_USER`/`AIRFLOW_WWW_PASSWORD`, added `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS` + `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE`
- `airflow/passwords.json` (NEW) — `{"admin": "admin"}`
- `changelog.md` — new entry with breaking changes table, decisions, lessons
- `docs/operations-performed.md` — updated file descriptions + new dated upgrade section
- `docs/knowledge.md` — new "Airflow 3.0 Authentication (SimpleAuthManager)" reference section

## Key Context
- Airflow 2.x EOL: April 2026. Final 2.x release: 2.11.2 (March 2026). No more security patches.
- SimpleAuthManager is dev-oriented. For production, FabAuthManager (via `apache-airflow-providers-fab`) restores database-backed auth.
- `airflow-init` command simplified to `airflow db migrate` only (no user creation step).
- Config path changes in 3.0: e.g., `base_url` moved from `[webserver]` to `[api]` section.
- Direct database access from task code is restricted in 3.0; must use Task Execution API.

## Errors Encountered
- **Stale advisory about duplicate `command:` block** — the old `airflow users create` block was already removed in a prior edit. Advisory was based on stale file view. Verified with programmatic YAML validation: single `command:` key, no `users create`, all SimpleAuthManager vars present.
- **Edit tool left orphaned lines** — when replacing the airflow-init section, the old `command:` block wasn't fully removed in one pass. Fixed with a targeted `DEL` operation, then verified.
- **spark-worker config lost during edit** — a wide SWAP accidentally consumed spark-worker's remaining config lines. Restored with a follow-up edit. Verified with YAML validation.

## Verification
- YAML validated: `docker-compose.yml` parses cleanly
- Programmatic checks: single `command:` key (only `airflow db migrate`), SimpleAuthManager env vars present, old `_AIRFLOW_WWW_USER_*` vars removed, `passwords.json` mount present, spark-worker config complete
