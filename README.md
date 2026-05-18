# JPMDQ NLP Central Bank — Databricks Ingestion

Self-contained scripts to download **JPMDQ NLP Central Bank** group data day-by-day from JPM DataQuery and land it in Databricks Unity Catalog Delta tables.

## Files

| File | Purpose |
|------|---------|
| `jpmdq_nlp_cb_utils.py` | Shared helpers: JPMDQ fetch, parse, retry logic, Databricks session/table setup |
| `jpmdq_nlp_cb_01_backfill.py` | **Full historical backfill** — downloads every business day in a date range |
| `jpmdq_nlp_cb_02_incremental.py` | **Daily incremental** — auto-detects last ingested date and fetches only missing days |
| `jpmdq_nlp_cb_fetch_one.py` | Helpers: `list_groups()` and `fetch_group(group_id, obs_date)` → polars DataFrame |
| `jpmdq_macro_japan_cpi.py` | **Japan CPI macro fetcher** (DQ_ECON_PRICES). Four tiers: `fetch_headline_core()` (~15 series), `fetch_main_categories()` (10 MIAC categories), `fetch_subcategories()` (~85 curated middle/small groups), `fetch_all_items()` (~1,636 detailed items). Monthly data → `Data/jpm/downloads/jpmdq_macro/japan_cpi/<tier>__<date>.parquet`. |

## Prerequisites

### Python environment
```bash
pip install dataquery-sdk polars pandas databricks-connect databricks-sdk pyspark
```
> `dataquery-sdk` is distributed via JPM's internal package registry.

### Databricks credentials
Configure `databricks-connect` so PySpark can reach your cluster:
```bash
databricks configure --token   # or use ~/.databrickscfg
databricks-connect configure
```

### JPMDQ credentials
```bash
export DATAQUERY_CLIENT_ID="your-client-id"
export DATAQUERY_CLIENT_SECRET="your-client-secret"
```
On a Databricks cluster, store these as cluster environment variables or Databricks Secrets.

---

## Discover your NLP group IDs

The exact group ID for "NLP Central Bank" varies by your access permissions.
The project is now fully synchronous — no `asyncio` required.

Use the built-in helper:

```bash
python jpmdq_nlp_cb_fetch_one.py
```

Or from Python:

```python
from jpmdq_nlp_cb_fetch_one import list_groups

print(list_groups(filter="nlp"))
```

---

## Storage flow (two explicit steps)

```
JPMDQ API
    │
    ▼  (1) write_day_to_volume()
/Volumes/{catalog}/{schema}/jpmdq_nlp_cb/raw/{group_id}/
    jpmdq_nlp_cb__{group_id}__{obs_date}__{ts}.parquet
    │
    ▼  (2) COPY INTO  (auto-tracks loaded files — idempotent)
{catalog}.{schema}.jpmdq_nlp_cb_bronze   ← Delta table
```

The Volume files are the raw source of truth. The Delta table is derived and can always be rebuilt with `COPY INTO`. The manifest Delta table is updated after each Volume write so the incremental script can detect the last ingested date.

### Volume: `/Volumes/{catalog}/{schema}/jpmdq_nlp_cb/raw/`
One parquet file per (group × obs_date). Naming convention:
```
jpmdq_nlp_cb__{GROUP_ID}__{YYYYMMDD}__{YYYYMMDDTHHMMSS}.parquet
```

### Bronze Delta table: `{catalog}.{schema}.jpmdq_nlp_cb_bronze`
One row per instrument × attribute × date. `value` is STRING to handle both numeric scores and NLP text outputs.

| Column | Type | Description |
|--------|------|-------------|
| `group_id` | STRING | JPMDQ group identifier |
| `instrument` | STRING | Instrument/document ID |
| `attribute` | STRING | Attribute name (e.g. SENTIMENT, HAWK_SCORE) |
| `date` | DATE | Observation date |
| `value` | STRING | Raw value (numeric or text) |
| `obs_date` | DATE | Business day fetched from JPMDQ |
| `ingested_at` | TIMESTAMP | UTC timestamp of ingestion |

Partitioned by `(group_id, obs_date)`.

### Manifest Delta table: `{catalog}.{schema}.jpmdq_nlp_cb_manifest`
One row per (group, obs_date). Drives incremental date detection.

| Column | Type | Description |
|--------|------|-------------|
| `group_id` | STRING | JPMDQ group identifier |
| `obs_date` | DATE | Business day |
| `row_count` | LONG | Rows in the parquet file |
| `instrument_count` | LONG | Distinct instruments |
| `attribute_count` | LONG | Distinct attributes |
| `status` | STRING | `ok` or `empty` |
| `volume_path` | STRING | Full path to the parquet file |
| `ingested_at` | TIMESTAMP | UTC timestamp |

---

## Usage

### Step 1 — Full backfill

Run once to download the full history. Replace group IDs with your actual NLP group names.

```bash
python jpmdq_nlp_cb_01_backfill.py \
    --groups NLP_CB_STATEMENTS NLP_CB_MINUTES \
    --start 20200101 \
    --end 20261231
```

For auto-detected GIC catalog/schema (recommended at JPM):
```bash
python jpmdq_nlp_cb_01_backfill.py \
    --groups NLP_CB_STATEMENTS \
    --start 20200101 \
    --end 20261231
# catalog/schema auto-detected from WorkspaceClient
```

For explicit catalog/schema:
```bash
python jpmdq_nlp_cb_01_backfill.py \
    --groups NLP_CB_STATEMENTS \
    --start 20200101 \
    --end 20261231 \
    --catalog users_schema_iz_prod \
    --schema lafarguette_personal_schema
```

**Dry run** (see what would be fetched without writing anything):
```bash
python jpmdq_nlp_cb_01_backfill.py \
    --groups NLP_CB_STATEMENTS \
    --start 20260101 \
    --end 20260131 \
    --dry-run
```

### Step 2 — Daily incremental (schedule this)

Run daily (e.g. as a Databricks Job with a cron trigger). Auto-detects the last ingested date.

```bash
python jpmdq_nlp_cb_02_incremental.py \
    --groups NLP_CB_STATEMENTS NLP_CB_MINUTES
```

On first run for a group (no manifest data yet), it defaults to 2 years back.
Override with `--default-start`:
```bash
python jpmdq_nlp_cb_02_incremental.py \
    --groups NLP_CB_STATEMENTS \
    --default-start 20150101
```

---

## Retry logic

Both scripts use a **diagnose-then-fix** 3-pass retry strategy per [JPMDQ learnings](https://github.com/rorozozo-ai/knowledge-base):

| Signal | Category | Action |
|--------|----------|--------|
| HTTP 429, rate limit | `rate_limit` | Sleep 180s then retry |
| HTTP 401/403, token expired | `auth_error` | Sleep 60s, re-authenticate |
| HTTP 204, empty universe | `empty_payload` | Skip (legitimate no-data day) |
| Timeout, connection reset | `timeout` | Sleep 90s then retry |
| Validation failure | `validation_failed` | Sleep 15s then retry |
| Unknown | `unknown` | Sleep 30s then retry |

---

## Running as a Databricks Job

1. Upload these three `.py` files to a Databricks workspace or DBFS path.
2. Create a Databricks Job with task type **Python Script**.
3. Set the script path to `jpmdq_nlp_cb_02_incremental.py`.
4. Add cluster environment variables:
   - `DATAQUERY_CLIENT_ID` — your JPMDQ client ID
   - `DATAQUERY_CLIENT_SECRET` — your JPMDQ client secret
5. Set the schedule (e.g. daily at 07:00 London time).
6. Parameters: `--groups NLP_CB_STATEMENTS NLP_CB_MINUTES`

---

## Querying the data

```python
# In a Databricks notebook
bronze = spark.table(f"{catalog}.{schema}.jpmdq_nlp_cb_bronze")
bronze.filter("group_id = 'NLP_CB_STATEMENTS'").display()

# Check manifest coverage
manifest = spark.table(f"{catalog}.{schema}.jpmdq_nlp_cb_manifest")
manifest.groupBy("group_id").agg({"obs_date": "max", "row_count": "sum"}).display()
```

---

## Troubleshooting

**"No groups matched"**: Check your group ID spelling using the discovery snippet above.

**"Missing DataQuery credentials"**: Set `DATAQUERY_CLIENT_ID` and `DATAQUERY_CLIENT_SECRET` env vars.

**"Could not auto-detect Databricks catalog/schema"**: Pass `--catalog` and `--schema` explicitly, or verify `databricks-sdk` is installed and `databricks configure` points to your workspace.

**0 rows returned**: The group may have no data for that date (holiday, discontinued). The script marks these as `empty_payload` and skips retries — this is correct behaviour.

**asyncio.TimeoutError**: Single fetch is taking longer than 300s. Reduce the timeout window or check JPMDQ endpoint health. Override with `export JPMDQ_FETCH_TIMEOUT_SEC=600`.
