"""
JPMDQ NLP Central Bank — Full Historical Backfill
==================================================
Edit the CONFIG section, then run:
  python jpmdq_nlp_cb_01_backfill.py

Storage flow:
  1. Fetch JPMDQ → write parquet per (group, day) to Databricks Volume
  2. COPY INTO bronze Delta table (idempotent)

Credentials from .env:
  DATAQUERY_CLIENT_ID, DATAQUERY_CLIENT_SECRET
  DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
"""
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from jpmdq_nlp_cb_utils import (
    auto_detect_catalog_schema, business_days, classify_failure,
    copy_into_bronze, ensure_tables, ensure_volume, process_group_day,
    get_ingested_dates, get_spark, upsert_manifest, write_day_to_volume,
    yesterday_ymd,
)

try:
    from dataquery import DataQuery
except ImportError as exc:
    raise SystemExit("Missing dataquery-sdk. Install: pip install dataquery-sdk") from exc


# ============================================================
# CONFIG — edit before running
# ============================================================

GROUPS        = ["NLP_CB_STATEMENTS", "NLP_CB_MINUTES"]
START_DATE    = "20200101"
END_DATE      = None          # None → yesterday
CATALOG       = ""            # "" → auto-detect GIC catalog
SCHEMA        = ""            # "" → auto-detect GIC schema
VOLUME_NAME   = "jpmdq_nlp_cb"
BRONZE_TABLE  = "jpmdq_nlp_cb_bronze"
MANIFEST_TABLE= "jpmdq_nlp_cb_manifest"
CALENDAR      = "CAL_USBANK"
FREQUENCY     = "FREQ_DAY"
CONVERSION    = "CONV_LASTBUS_ABS"
NAN_TREATMENT = "NA_NOTHING"
FORCE         = False         # True → re-download dates already in manifest
SKIP_COPY_INTO= False
DRY_RUN       = False
LOG_DIR       = "logs"


# ============================================================
# CREDENTIALS
# ============================================================

client_id = (
    os.environ.get("DATAQUERY_CLIENT_ID")
    or os.environ.get("JPM_A_CLIENT_ID")
    or os.environ.get("jpm_a_client_id")
)
client_secret = (
    os.environ.get("DATAQUERY_CLIENT_SECRET")
    or os.environ.get("JPM_A_CLIENT_SECRET")
    or os.environ.get("jpm_a_client_secret")
)
if not client_id or not client_secret:
    raise SystemExit("Missing DataQuery credentials. Set DATAQUERY_CLIENT_ID + DATAQUERY_CLIENT_SECRET in .env.")

oauth_aud = os.environ.get("DATAQUERY_OAUTH_AUD", "")
base_url  = os.environ.get("DATAQUERY_BASE_URL", "https://api-developer.jpmorgan.com")


# ============================================================
# DATABRICKS PATHS
# ============================================================

catalog = CATALOG or os.environ.get("DATABRICKS_CATALOG", "")
schema  = SCHEMA  or os.environ.get("DATABRICKS_SCHEMA",  "")
if not catalog or not schema:
    print("[catalog] auto-detecting GIC catalog/schema...", flush=True)
    catalog, schema = auto_detect_catalog_schema()
if not catalog or not schema:
    raise SystemExit("Could not detect Databricks catalog/schema. Set DATABRICKS_CATALOG / DATABRICKS_SCHEMA.")

volume_raw_path = f"/Volumes/{catalog}/{schema}/{VOLUME_NAME}/raw"
bronze_table    = f"{catalog}.{schema}.{BRONZE_TABLE}"
manifest_table  = f"{catalog}.{schema}.{MANIFEST_TABLE}"

end_date    = END_DATE or yesterday_ymd()
days        = business_days(START_DATE, end_date)
total_pairs = len(GROUPS) * len(days)

print(
    f"[backfill] groups={GROUPS}\n"
    f"[backfill] days={len(days)} ({START_DATE}→{end_date})  total_pairs={total_pairs}\n"
    f"[backfill] volume={volume_raw_path}\n"
    f"[backfill] bronze={bronze_table}  dry_run={DRY_RUN}",
    flush=True,
)


# ============================================================
# SPARK + VOLUME + TABLES
# ============================================================

if not DRY_RUN:
    spark = get_spark()
    ensure_volume(catalog, schema, VOLUME_NAME)
    ensure_tables(spark, bronze_table, manifest_table)
else:
    spark = None


# ============================================================
# FETCH LOOP  (Volume write + manifest per day)
# ============================================================

log_dir     = Path(LOG_DIR).resolve()
log_dir.mkdir(parents=True, exist_ok=True)
download_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
miss_log    = log_dir / f"backfill_misses__{download_ts}.jsonl"

dq_kwargs = {"client_id": client_id, "client_secret": client_secret, "base_url": base_url}
if oauth_aud:
    dq_kwargs["oauth_aud"] = oauth_aud

successes = []
misses    = []

with DataQuery(**dq_kwargs) as dq:

    done = 0
    for obs_date in days:
        for group_id in GROUPS:
            done += 1
            prefix = f"[{done}/{total_pairs}] {group_id} {obs_date}"

            if not DRY_RUN and not FORCE:
                if obs_date in get_ingested_dates(spark, manifest_table, group_id):
                    print(f"{prefix} → skip (in manifest)", flush=True)
                    continue

            print(prefix, end=" → ", flush=True)

            rec = process_group_day(
                dq,
                spark=spark,
                dry_run=DRY_RUN,
                volume_raw_path=volume_raw_path,
                manifest_table=manifest_table,
                group_id=group_id,
                obs_date=obs_date,
                download_ts=download_ts,
                calendar=CALENDAR,
                frequency=FREQUENCY,
                conversion=CONVERSION,
                nan_treatment=NAN_TREATMENT,
            )
            rec["pass"] = 1
            if rec["ok"]:
                print(("[dry-run] " if DRY_RUN else "") + rec["message"], flush=True)
                successes.append(rec)
            else:
                print(rec["message"], flush=True)
                misses.append(rec)
                miss_log.open("a").write(json.dumps(rec) + "\n")
                continue

    # ---- diagnose-then-fix retries ----
    actionable = [m for m in misses if classify_failure(m["message"])["strategy"] != "skip"]
    if actionable:
        print(f"\n[retry] retrying {len(actionable)} actionable misses...", flush=True)
        still_failing = []
        for m in actionable:
            group_id, obs_date = m["group_id"], m["obs_date"]
            diag     = classify_failure(m["message"])
            resolved = False
            for pass_num in (2, 3):
                if diag.get("wait_s", 0) > 0:
                    print(f"  [sleep {diag['wait_s']}s] {group_id} {obs_date} ({diag['category']})", flush=True)
                    time.sleep(diag["wait_s"])
                rec = process_group_day(
                    dq,
                    spark=spark,
                    dry_run=DRY_RUN,
                    volume_raw_path=volume_raw_path,
                    manifest_table=manifest_table,
                    group_id=group_id,
                    obs_date=obs_date,
                    download_ts=download_ts,
                    calendar=CALENDAR,
                    frequency=FREQUENCY,
                    conversion=CONVERSION,
                    nan_treatment=NAN_TREATMENT,
                )
                rec = {**m, **rec, "pass": pass_num}
                miss_log.open("a").write(json.dumps(rec) + "\n")
                if ok:
                    successes.append(rec)
                    resolved = True
                    print(f"  [recovered] {group_id} {obs_date} pass={pass_num}: {msg}", flush=True)
                    break
                diag = classify_failure(msg)
                if diag["strategy"] == "skip":
                    break
            if not resolved:
                still_failing.append({**m, "pass": 3, "ok": False})
                print(f"  [still-miss] {group_id} {obs_date}", flush=True)
        misses = still_failing


# ============================================================
# COPY INTO DELTA TABLE
# ============================================================

if not DRY_RUN and not SKIP_COPY_INTO and successes:
    print(f"\n[copy-into] loading {len(successes)} new files into {bronze_table}...", flush=True)
    copy_into_bronze(spark, bronze_table, volume_raw_path)


# ============================================================
# SUMMARY
# ============================================================

skipped_empty = sum(1 for m in misses if classify_failure(m["message"])["strategy"] == "skip")
summary = {
    "run_ts":              download_ts,
    "groups":             GROUPS,
    "date_range":         f"{START_DATE}→{end_date}",
    "total_pairs":        total_pairs,
    "successes":          len(successes),
    "misses_actionable":  len(misses) - skipped_empty,
    "misses_empty":       skipped_empty,
    "volume_raw_path":    volume_raw_path,
    "bronze_table":       bronze_table,
    "manifest_table":     manifest_table,
    "dry_run":            DRY_RUN,
}
summary_path = log_dir / f"backfill_summary__{download_ts}.json"
summary_path.write_text(json.dumps(summary, indent=2))
print(f"\n[summary]\n{json.dumps(summary, indent=2)}", flush=True)
if misses:
    print(f"[warn] {len(misses) - skipped_empty} unrecovered miss(es). See: {miss_log}", flush=True)
