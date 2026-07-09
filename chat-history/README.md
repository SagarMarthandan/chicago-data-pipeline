# Chat History — Second Brain

This folder is a **structured record of every conversation** between the user and the AI assistant. It exists to prevent hallucination from context compaction — when the model's context window fills up and old messages get compressed/summarized, details are lost. This folder is the permanent, uncompressed reference.

## Why This Exists

- **GLM 5.2 has a 200k context window.** At ~75% usage, auto-compaction kicks in and compresses older messages. Compressed messages lose detail.
- **This folder is the "second brain."** A fresh session (or a compacted one) can read these files to reconstruct what was done, why, and what's next.
- **It is NOT a replacement** for `changelog.md` (errors/fixes), `docs/knowledge.md` (reference), or `docs/operations-performed.md` (audit trail). Those track the *project*. This tracks the *conversation*.

## Structure

```
chat-history/
├── README.md                    ← you are here (this file)
├── current-state.md             ← HANDOFF DOC — read this first in a new session
├── 2026-07-08/                  ← date folder
│   ├── 01-project-setup.md      ← chunked by topic
│   ├── 02-wsl-migration.md
│   └── 03-documentation.md
├── 2026-07-09/
│   ├── 01-docker-setup.md
│   ├── 02-init-sql.md
│   ├── 03-docker-compose.md
│   ├── 04-uv-init.md
│   ├── 05-airflow-upgrade.md
│   └── 06-chat-history-system.md
```

## How to Use

### Starting a new session
1. Read `current-state.md` first — it's the handoff document with current state, active decisions, and next steps.
2. If you need more detail on a specific topic, read the relevant chunk in the date folder.

### During a session
- When context usage approaches 75% (~150k tokens), create a new chunk for the current topic in today's date folder.
- Update `current-state.md` with the latest state.

## Chunk Format

Each chunk file follows this structure:

```markdown
# [Topic Name]

## Summary
One-paragraph summary of what was discussed.

## Decisions Made
- Decision 1 — why
- Decision 2 — why

## Files Created/Modified
- `path/to/file` — what it is

## Key Context
Anything a future session needs to know that isn't in the files themselves.

## Errors Encountered
- Error — cause — fix (if any)

## User Preferences Learned
- Preference — context
```
