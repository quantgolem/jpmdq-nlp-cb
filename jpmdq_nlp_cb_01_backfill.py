"""
JPMDQ NLP Central Bank — Full Historical Backfill
==================================================
Downloads NLP Central Bank group time-series data day-by-day from JPM DataQuery
and saves it to Databricks Delta tables.

Storage layout (Unity Catalog):
  Bronze:   {catalog}.{schema}.jpmdq_nlp_cb_bronze    — one row per instrument/attribute/date
  Manifest: {catalog}.{schema}.jpmdq_nlp_cb_manifest  — one row per (group, obs_date)

Usage (run from work laptop or as a Databricks Job):

  python jpmdq_nlp_cb_01_backfill.py \\
      --groups NLP_CB_STATEMENTS NLP_CB_MINUTES \\
      --start 20200101 \\
      --end 20261231 \\
      [--catalog my_catalog] \\
      [--schema my_schema]

Credentials:
  Set env vars DATAQUERY_CLIENT_ID + DATAQUERY_CLIENT_SECRET  (or --client-id / --client-secret).
  On a Databricks cluster you can store them as Databricks Secrets and pass via cluster env vars.

Dependencies:
  pip install dataquery-sdk polars pandas databricks-connect databricks-sdk pyspark

Discover available NLP groups:
  Use jpmdq_nlp_cb_catalog.py (see README) or run:
    from dataquery import DataQuery
    async with DataQuery(...) as dq:
        cat = await dq._client.get_groups_async()
        nlp = [g.group_id for g in cat.groups if 'NLP' in g.group_id or 'TEXT' in g.group_id]
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add parent dir to path if running from this directory (local dev)
_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from jpmdq_nlp_cb_utils import (
    FETCH_TIMEOUT_SEC,
    auto_detect_catalog_schema,
    business_days,
    classify_failure,
    ensure_tables,
    fetch_group_day,
    get_dbutils,
    get_ingested_dates,
    get_spark,
    write_day_to_delta,
    yesterday_ymd,
)

try:
    from dataquery import DataQuery
except ImportError as exc:
    raise SystemExit(
        "Missing dataquery-sdk. Install it: pip install dataquery-sdk"
    ) from exc


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="jpmdq_nlp_cb_01_backfill.py",
        description="Full historical backfill of JPMDQ NLP Central Bank groups into Databricks.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--groups", nargs="+", required=True,
        metavar="GROUP_ID",
        help="One or more JPMDQ group IDs, e.g. NLP_CB_STATEMENTS NLP_CB_MINUTES",
    )
    p.add_argument("--start", required=True, help="Start date YYYYMMDD (inclusive)")
    p.add_argument("--end", default=None,
                   help="End date YYYYMMDD (inclusive). Defaults to yesterday.")
    p.add_argument("--catalog", default="",
                   help="Databricks Unity Catalog name. Auto-detected in GIC env if omitted.")
    p.add_argument("--schema", default="",
                   help="Databricks schema name. Auto-detected in GIC env if omitted.")
    p.add_argument("--bronze-table", default="jpmdq_nlp_cb_bronze",
                   help="Unqualified bronze table name (default: jpmdq_nlp_cb_bronze).")
    p.add_argument("--manifest-table", default="jpmdq_nlp_cb_manifest",
                   help="Unqualified manifest table name (default: jpmdq_nlp_cb_manifest).")
    p.add_argument("--client-id", default="",
                   help="JPMDQ OAuth client ID (or set DATAQUERY_CLIENT_ID env var).")
    p.add_argument("--client-secret", default="",
                   help="JPMDQ OAuth client secret (or set DATAQUERY_CLIENT_SECRET env var).")
    p.add_argument("--oauth-aud", default="",
                   help="JPMDQ OAuth audience URL (or set DATAQUERY_OAUTH_AUD env var).")
    p.add_argument("--base-url", default="",
                   help="JPMDQ API base URL (default: https://api-developer.jpmorgan.com).")
    p.add_argument("--calendar", default="CAL_USBANK")
    p.add_argument("--frequency", default="FREQ_DAY")
    p.add_argument("--conversion", default="CONV_LASTBUS_ABS")
    p.add_argument("--nan-treatment", default="NA_NOTHING")
    p.add_argument("--force", action="store_true",
                   help="Re-download dates already in the manifest (overwrite).")
    p.add_argument("--dry-run", action="store_true",
                   help="Print what would be downloaded; do not write anything.")
    p.add_argument("--log-dir", default="",
                   help="Directory for JSON miss/summary logs. Defaults to ./logs.")
    return p


# ---------------------------------------------------------------------------
# Core per-(group, day) fetch-then-write
# ---------------------------------------------------------------------------

async def _try_one(
    dq,
    *,
    group_id: str,
    obs_date: str,
    spark,
    bronze_table: str,
    manifest_table: str,
    calendar: str,
    frequency: str,
    conversion: str,
    nan_treatment: str,
    dry_run: bool,
) -> tuple[bool, str]:
    """Attempt one (group, obs_date) fetch-and-write. Returns (ok, message). Never raises."""
    try:
        rows = await fetch_group_day(
            dq,
            group_id=group_id,
            obs_date=obs_date,
            calendar=calendar,
            frequency=frequency,
            conversion=conversion,
            nan_treatment=nan_treatment,
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"

    if not rows:
        return False, "empty_payload: 0 rows returned"

    if dry_run:
        instr = len({r["instrument"] for r in rows})
        attrs = len({r["attribute"] for r in rows})
        return True, f"[dry-run] rows={len(rows)} instruments={instr} attributes={attrs}"

    try:
        result = write_day_to_delta(
            spark,
            rows,
            bronze_table=bronze_table,
            manifest_table=manifest_table,
            group_id=group_id,
            obs_date=obs_date,
        )
    except Exception as exc:
        return False, f"delta_write_error: {type(exc).__name__}: {exc}"

    return True, (
        f"rows={result['row_count']} "
        f"instruments={result['instrument_count']} "
        f"attributes={result['attribute_count']}"
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main_async() -> None:
    args = build_parser().parse_args()

    # --- Credentials ---
    client_id = (
        args.client_id
        or os.environ.get("DATAQUERY_CLIENT_ID")
        or os.environ.get("JPM_A_CLIENT_ID")
        or os.environ.get("jpm_a_client_id")
    )
    client_secret = (
        args.client_secret
        or os.environ.get("DATAQUERY_CLIENT_SECRET")
        or os.environ.get("JPM_A_CLIENT_SECRET")
        or os.environ.get("jpm_a_client_secret")
    )
    if not client_id or not client_secret:
        raise SystemExit(
            "Missing DataQuery credentials.\n"
            "Set DATAQUERY_CLIENT_ID + DATAQUERY_CLIENT_SECRET env vars, "
            "or pass --client-id / --client-secret."
        )
    oauth_aud = args.oauth_aud or os.environ.get("DATAQUERY_OAUTH_AUD", "")
    base_url = (
        args.base_url
        or os.environ.get("DATAQUERY_BASE_URL", "https://api-developer.jpmorgan.com")
    )

    # --- Databricks catalog / schema ---
    catalog = args.catalog or os.environ.get("DATABRICKS_CATALOG", "")
    schema = args.schema or os.environ.get("DATABRICKS_SCHEMA", "")
    if not catalog or not schema:
        print("[catalog] auto-detecting GIC catalog/schema...", flush=True)
        catalog, schema = auto_detect_catalog_schema()
    if not catalog or not schema:
        raise SystemExit(
            "Could not auto-detect Databricks catalog/schema.\n"
            "Pass --catalog and --schema, or set DATABRICKS_CATALOG / DATABRICKS_SCHEMA."
        )

    bronze_table = f"{catalog}.{schema}.{args.bronze_table}"
    manifest_table = f"{catalog}.{schema}.{args.manifest_table}"

    # --- Date range ---
    end_date = args.end or yesterday_ymd()
    days = business_days(args.start, end_date)
    if not days:
        raise SystemExit(f"No business days between {args.start} and {end_date}.")

    # --- Logging ---
    log_dir = Path(args.log_dir or "logs").resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    miss_log = log_dir / f"backfill_misses__{run_ts}.jsonl"

    total_pairs = len(args.groups) * len(days)
    print(
        f"[backfill] groups={args.groups}  days={len(days)} ({args.start}→{end_date})\n"
        f"[backfill] total_pairs={total_pairs}  bronze={bronze_table}  dry_run={args.dry_run}",
        flush=True,
    )

    # --- Spark + tables ---
    if not args.dry_run:
        spark = get_spark()
        ensure_tables(spark, bronze_table, manifest_table)
    else:
        spark = None

    dq_kwargs: dict = {"client_id": client_id, "client_secret": client_secret, "base_url": base_url}
    if oauth_aud:
        dq_kwargs["oauth_aud"] = oauth_aud

    successes: list[dict] = []
    misses: list[dict] = []

    async with DataQuery(**dq_kwargs) as dq:
        done = 0
        for obs_date in days:
            for group_id in args.groups:
                done += 1
                prefix = f"[{done}/{total_pairs}] {group_id} {obs_date}"

                # Skip already-ingested dates (unless --force)
                if not args.dry_run and not args.force:
                    already = get_ingested_dates(spark, manifest_table, group_id)
                    if obs_date in already:
                        print(f"{prefix} → skip (already in manifest)", flush=True)
                        continue

                print(prefix, end=" → ", flush=True)
                ok, msg = await _try_one(
                    dq,
                    group_id=group_id,
                    obs_date=obs_date,
                    spark=spark,
                    bronze_table=bronze_table,
                    manifest_table=manifest_table,
                    calendar=args.calendar,
                    frequency=args.frequency,
                    conversion=args.conversion,
                    nan_treatment=args.nan_treatment,
                    dry_run=args.dry_run,
                )
                print(msg, flush=True)
                record = {"group_id": group_id, "obs_date": obs_date, "ok": ok, "message": msg, "pass": 1}
                if ok:
                    successes.append(record)
                else:
                    misses.append(record)
                    miss_log.open("a").write(json.dumps(record) + "\n")

        # --- Pass 2 + 3: diagnose-then-fix retries for misses ---
        actionable_misses = [m for m in misses if classify_failure(m["message"])["strategy"] != "skip"]
        if actionable_misses:
            print(f"\n[retry] retrying {len(actionable_misses)} actionable misses...", flush=True)
            still_failing: list[dict] = []
            for m in actionable_misses:
                group_id, obs_date = m["group_id"], m["obs_date"]
                diag = classify_failure(m["message"])
                resolved = False

                for pass_num in (2, 3):
                    wait_s = diag.get("wait_s", 30)
                    if wait_s > 0:
                        print(f"  [sleep {wait_s}s] {group_id} {obs_date} ({diag['category']})", flush=True)
                        time.sleep(wait_s)

                    ok, msg = await _try_one(
                        dq,
                        group_id=group_id,
                        obs_date=obs_date,
                        spark=spark,
                        bronze_table=bronze_table,
                        manifest_table=manifest_table,
                        calendar=args.calendar,
                        frequency=args.frequency,
                        conversion=args.conversion,
                        nan_treatment=args.nan_treatment,
                        dry_run=args.dry_run,
                    )
                    rec = {**m, "pass": pass_num, "ok": ok, "message": msg, "retry_category": diag["category"]}
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

    # --- Summary ---
    skipped_empty = sum(1 for m in misses if classify_failure(m["message"])["strategy"] == "skip")
    summary = {
        "run_ts": run_ts,
        "groups": args.groups,
        "date_range": f"{args.start}→{end_date}",
        "total_pairs": total_pairs,
        "successes": len(successes),
        "misses_actionable": len(misses) - skipped_empty,
        "misses_empty_payload": skipped_empty,
        "dry_run": args.dry_run,
        "bronze_table": bronze_table,
        "manifest_table": manifest_table,
    }
    summary_path = log_dir / f"backfill_summary__{run_ts}.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"\n[summary]\n{json.dumps(summary, indent=2)}", flush=True)
    print(f"[logs] {log_dir}", flush=True)

    if misses:
        print(
            f"\n[warn] {len(misses) - skipped_empty} unrecovered miss(es). See: {miss_log}",
            flush=True,
        )


if __name__ == "__main__":
    asyncio.run(main_async())
