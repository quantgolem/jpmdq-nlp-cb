# jpmdq-nlp-cb — agent context

The user-level rules at `~/.claude/CLAUDE.md` apply (Polars-only, medallion, vault-first, etc.).

## What this repo is

Self-contained scripts to download JPMDQ **NLP Central Bank** group data day-by-day and land it in **Databricks Unity Catalog Delta tables**. Includes a separate Japan CPI macro fetcher (`jpmdq_macro_japan_cpi.py`).

## Read first

- `README.md` — full file table, prereqs, group discovery, storage flow
- `jpmdq_nlp_cb_utils.py` — shared helpers (JPMDQ fetch, parse, retry, Databricks session/table setup) — all other scripts use this

## File map

| File | Purpose |
|---|---|
| `jpmdq_nlp_cb_utils.py` | Shared helpers (fetch/parse/retry/Databricks) |
| `jpmdq_nlp_cb_01_backfill.py` | Full historical backfill |
| `jpmdq_nlp_cb_02_incremental.py` | Daily incremental — auto-detects last ingested date |
| `jpmdq_nlp_cb_fetch_one.py` | `list_groups()` and `fetch_group(group_id, obs_date)` |
| `jpmdq_macro_japan_cpi.py` | Japan CPI macro fetcher (4 tiers: headline-core, main, sub, all-items) |

## Project rules

- **Fully synchronous** — no asyncio, no async/await. The SDK supports both; we use sync only.
- **Per-day-per-group fetch** — JPMDQ's daily-volatile universe means a multi-day range silently fixes the universe to the end-of-range day. Loop day-by-day, save per-day per-group parquet.
- **Databricks credentials** via `databricks-connect`; JPMDQ creds via `DATAQUERY_CLIENT_ID` / `DATAQUERY_CLIENT_SECRET` env vars (cluster env vars or Databricks Secrets on a cluster).
- **Diagnose-then-fix retries** — read `knowledge-base/30-Learnings/2026-05-17_diagnose_then_fix_retries.md` and `10-Skills/jpm-dataquery-learnings.md` before touching the retry/error-handling logic.
- **Japan CPI**: monthly cadence, four tiers, output to `/home/workspace/Data/jpm/downloads/jpmdq_macro/japan_cpi/<tier>__<date>.parquet`. Cap monthly runs to completed periods by default.

## Where data lives

Outputs land in Databricks (Unity Catalog) for the NLP CB pipeline, and on-Zo parquet for Japan CPI. Never commit data files.
