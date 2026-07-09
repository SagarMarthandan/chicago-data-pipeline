# Chat History System Created

## Summary
User requested a `chat-history/` folder at repo root to serve as a "second brain" — a permanent, uncompressed reference of conversations that survives context compaction. Researched the "structured handoff" pattern (export current state into structured Markdown for fresh sessions). Created the folder structure, README, handoff document, and chunked conversation summaries.

## Why This Exists
- GLM 5.2 has a 200k context window. At ~75% usage (~150k tokens), auto-compaction compresses older messages. Compressed messages lose detail.
- This folder is the permanent, uncompressed reference. A fresh or compacted session reads these files to reconstruct what was done, why, and what's next.
- It complements the three-doc system: `changelog.md` (errors), `docs/knowledge.md` (reference), `docs/operations-performed.md` (audit trail) track the *project*. `chat-history/` tracks the *conversation*.

## Structure
```
chat-history/
├── README.md                    ← explains the system
├── current-state.md             ← HANDOFF DOC — read first in new session
├── 2026-07-08/                  ← date folder
│   └── 01-project-setup-and-migration.md
├── 2026-07-09/
│   ├── 01-docker-setup-env-and-init.md
│   ├── 02-docker-compose-and-dockerfiles.md
│   ├── 03-uv-init.md
│   ├── 04-airflow-upgrade.md
│   └── 05-chat-history-system.md  ← this file
```

## Decisions Made
- **Structured Markdown over plain text** — machine-readable, consistent format (Summary, Decisions, Files, Key Context, Errors)
- **Chunked by topic, not by message** — each file covers one logical topic, not one message. Easier to find specific context.
- **Date folders** — chronological organization, easy to navigate
- **`current-state.md` as handoff doc** — the single file a new session reads first. Contains current state, active decisions, next steps, constraints, user preferences.
- **Separation of concerns** — narrative (what happened) separated from instructions (what next session needs to do), per handoff best practices

## Files Created
- `chat-history/README.md` — explains the system, structure, how to use, chunk format
- `chat-history/current-state.md` — handoff document with current project state
- `chat-history/2026-07-08/01-project-setup-and-migration.md`
- `chat-history/2026-07-09/01-docker-setup-env-and-init.md`
- `chat-history/2026-07-09/02-docker-compose-and-dockerfiles.md`
- `chat-history/2026-07-09/03-uv-init.md`
- `chat-history/2026-07-09/04-airflow-upgrade.md`
- `chat-history/2026-07-09/05-chat-history-system.md` (this file)

## Key Context
- Research confirmed this is a recognized pattern called "structured handoffs" — exporting current state (decisions, in-progress tasks, file modifications, constraints) into structured format for fresh sessions.
- Best practices from research: preserve commitments (not just narrative), use structured formats, trigger proactively at threshold, separate narrative from instructions.
- The user referenced a "handoff skill in GitHub" — this refers to the general pattern of AI conversation handoff tools, not a specific installable skill.

## User Preferences Learned
- User wants to prevent hallucination from auto-compaction — this folder is the solution
- User wants the model to use this as a "second brain" to refer to what was done
- User wants fresh context ("fresh legs") when starting new work after compaction
