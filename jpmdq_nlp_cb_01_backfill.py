"""
JPMDQ NLP Central Bank — Full Historical Backfill
==================================================
Downloads NLP Central Bank group time-series data day-by-day from JPM DataQuery
and saves it to Databricks.

Storage flow (two explicit steps):
  1. Per (group, obs_date): write a parquet file to a Databricks Volume
       /Volumes/{catalog}/{schema}/jpmdq_nlp_cb/raw/{group_id}/
         jpmdq_nlp_cb__{group_id}__{obs_date}__{ts}.parquet
  2. After all files are saved: COPY INTO the bronze Delta table
       {catalog}.{schema}.jpmdq_nlp_cb_bronze

Usage:
  python jpmdq_nlp_cb_01_backfill.py \\
      --groups NLP_CB_STATEMENTS NLP_CB_MINUTES \\
      --start 20200101 \\
      --end 20261231 \\
      [--catalog my_catalog --schema my_schema]

Credentials loaded from .env (DATAQUERY_CLIENT_ID, DATAQUERY_CLIENT_SECRET,
DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET).

Dependencies:
  pip install dataquery-sdk polars databricks-connect databricks-sdk pyspark python-dotenv requests
"""
from __future__ import annotations

import argparse
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
    auto_detect_catalog_schema,
    business_days,
    classify_failure,
    copy_into_bronze,
    ensure_tables,
    ensure_volume,
    fetch_group_day,
    get_ingested_dates,
    get_spark,
    upsert_manifest,
    write_day_to_volume,
    yesterday_ymd,
)

try:
    from dataquery import DataQuery
except ImportError as exc:
    raise SystemExit("Missing dataquery-sdk. Install: pip install dataquery-sdk") from exc


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="jpmdq_nlp_cb_01_backfill.py",
        description="Full historical backfill of JPMDQ NLP CB groups → Databricks Volume → Delta table.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--groups", nargs="+", required=True, metavar="GROUP_ID")
    p.add_argument("--start", required=True, help="Start date YYYYMMDD (inclusive)")
    p.add_argument("--end", default=None, help="End date YYYYMMDD (inclusive). Default: yesterday.")
    p.add_argument("--catalog", default="")
    p.add_argument("--schema", default="")
    p.add_argument("--volume-name", default="jpmdq_nlp_cb")
    p.add_argument("--bronze-table", default="jpmdq_nlp_cb_bronze")
    p.add_argument("--manifest-table", default="jpmdq_nlp_cb_manifest")
    p.add_argument("--client-id", default="")
    p.add_argument("--client-secret", default="")
    p.add_argument("--oauth-aud", default="")
    p.add_argument("--base-url", default="")
    p.add_argument("--calendar", default="CAL_USBANK")
    p.add_argument("--frequency", default="FREQ_DAY")
    p.add_argument("--conversion", default="CONV_LASTBUS_ABS")
    p.add_argument("--nan-treatment", default="NA_NOTHING")
    p.add_argument("--force", action="store_true", help="Re-download dates already in manifest.")
    p.add_argument("--skip-copy-into", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--log-dir", default="logs")
    return p


def fetch_and_write_day(
    dq,
    *,
    group_id: str,
    obs_date: str,
    volume_raw_path: str,
    download_ts: str,
    spark,
    manifest_table: str,
    calendar: str,
    frequency: str,
    conversion: str,
    nan_treatment: str,
    dry_run: bool,
) -> tuple[bool, str, str | None]:
    try:
        rows = fetch_group_day(
            dq,
            group_id=group_id,
            obs_date=obs_date,
            calendar=calendar,
            frequency=frequency,
            conversion=conversion,
            nan_treatment=nan_treatment,
        )
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}", None

    if not rows:
        return False, "empty_payload: 0 rows returned", None

    row_count = len(rows)
    instrument_count = len({r["instrument"] for r in rows})
    attribute_count = len({r["attribute"] for r in rows})
    msg = f"rows={row_count} instruments={instrument_count} attributes={attribute_count}"

    if dry_run:
        return True, f"[dry-run] {msg}", None

    try:
        vpath = write_day_to_volume(
            rows,
            volume_raw_path=volume_raw_path,
            group_id=group_id,
            obs_date=obs_date,
            download_ts=download_ts,
        )
    except Exception as exc:
        return False, f"volume_write_error: {type(exc).__name__}: {exc}", None

    if spark is not None:
        try:
            upsert_manifest(
                spark,
                manifest_table=manifest_table,
                group_id=group_id,
                obs_date=obs_date,
                row_count=row_count,
                instrument_count=instrument_count,
                attribute_count=attribute_count,
                status="ok",
                volume_path=vpath,
            )
        except Exception as exc:
            print(f"  [warn] manifest update failed: {exc}", flush=True)

    return True, msg, vpath


def run_backfill() -> None:
    args = build_parser().parse_args()

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
            "Set DATAQUERY_CLIENT_ID + DATAQUERY_CLIENT_SECRET in .env or environment."
        )

    oauth_aud = args.oauth_aud or os.environ.get("DATAQUERY_OAUTH_AUD", "")
    base_url = args.base_url or os.environ.get("DATAQUERY_BASE_URL", "https://api-developer.jpmorgan.com")

    catalog = args.catalog or os.environ.get("DATABRICKS_CATALOG", "")
    schema = args.schema or os.environ.get("DATABRICKS_SCHEMA", "")
    if not catalog or not schema:
        print("[catalog] auto-detecting GIC catalog/schema...", flush=True)
        catalog, schema = auto_detect_catalog_schema()
    if not catalog or not schema:
        raise SystemExit(
            "Could not detect Databricks catalog/schema.\n"
            "Pass --catalog and --schema, or set DATABRICKS_CATALOG / DATABRICKS_SCHEMA."
        )

    volume_raw_path = f"/Volumes/{catalog}/{schema}/{args.volume_name}/raw"
    bronze_table = f"{catalog}.{schema}.{args.bronze_table}"
    manifest_table = f"{catalog}.{schema}.{args.manifest_table}"

    end_date = args.end or yesterday_ymd()
    days = business_days(args.start, end_date)
    if not days:
        raise SystemExit(f"No business days between {args.start} and {end_date}.")

    total_pairs = len(args.groups) * len(days)
    print(
        f"[backfill] groups={args.groups}\n"
        f"[backfill] days={len(days)} ({args.start}→{end_date})  total_pairs={total_pairs}\n"
        f"[backfill] volume={volume_raw_path}\n"
        f"[backfill] bronze={bronze_table}  dry_run={args.dry_run}",
        flush=True,
    )

    if not args.dry_run:
        spark = get_spark()
        ensure_volume(catalog, schema, args.volume_name)
        ensure_tables(spark, bronze_table, manifest_table)
    else:
        spark = None

    log_dir = Path(args.log_dir).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    download_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    miss_log = log_dir / f"backfill_misses__{download_ts}.jsonl"

    dq_kwargs: dict = {"client_id": client_id, "client_secret": client_secret, "base_url": base_url}
    if oauth_aud:
        dq_kwargs["oauth_aud"] = oauth_aud

    successes: list[dict] = []
    misses: list[dict] = []

    with DataQuery(**dq_kwargs) as dq:
        done = 0
        for obs_date in days:
            for group_id in args.groups:
                done += 1
                prefix = f"[{done}/{total_pairs}] {group_id} {obs_date}"

                if not args.dry_run and not args.force:
                    already = get_ingested_dates(spark, manifest_table, group_id)
                    if obs_date in already:
                        print(f"{prefix} → skip (in manifest)", flush=True)
                        continue

                print(prefix, end=" → ", flush=True)
                ok, msg, vpath = fetch_and_write_day(
                    dq,
                    group_id=group_id,
                    obs_date=obs_date,
                    volume_raw_path=volume_raw_path,
                    download_ts=download_ts,
                    spark=spark,
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

        # Diagnose-then-fix retries
        actionable = [m for m in misses if classify_failure(m["message"])["strategy"] != "skip"]
        if actionable:
            print(f"\n[retry] retrying {len(actionable)} actionable misses...", flush=True)
            still_failing: list[dict] = []
            for m in actionable:
                group_id, obs_date = m["group_id"], m["obs_date"]
                diag = classify_failure(m["message"])
                resolved = False
                for pass_num in (2, 3):
                    wait_s = diag.get("wait_s", 30)
                    if wait_s > 0:
                        print(f"  [sleep {wait_s}s] {group_id} {obs_date} ({diag['category']})", flush=True)
                        time.sleep(wait_s)
                    ok, msg, vpath = fetch_and_write_day(
                        dq,
                        group_id=group_id,
                        obs_date=obs_date,
                        volume_raw_path=volume_raw_path,
                        download_ts=download_ts,
                        spark=spark,
                        manifest_table=manifest_table,
                        calendar=args.calendar,
                        frequency=args.frequency,
                        conversion=args.conversion,
                        nan_treatment=args.nan_treatment,
                        dry_run=args.dry_run,
                    )
                    rec = {**m, "pass": pass_num, "ok": ok, "message": msg}
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

    if not args.dry_run and not args.skip_copy_into and successes:
        print(f"\n[copy-into] loading {len(successes)} new files into {bronze_table}...", flush=True)
        copy_into_bronze(spark, bronze_table, volume_raw_path)

    skipped_empty = sum(1 for m in misses if classify_failure(m["message"])["strategy"] == "skip")
    summary = {
        "run_ts": download_ts,
        "groups": args.groups,
        "date_range": f"{args.start}→{end_date}",
        "total_pairs": total_pairs,
        "successes": len(successes),
        "misses_actionable": len(misses) - skipped_empty,
        "misses_empty_payload": skipped_empty,
        "volume_raw_path": volume_raw_path,
        "bronze_table": bronze_table,
        "manifest_table": manifest_table,
        "dry_run": args.dry_run,
    }
    summary_path = log_dir / f"backfill_summary__{download_ts}.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"\n[summary]\n{json.dumps(summary, indent=2)}", flush=True)
    if misses:
        print(f"[warn] {len(misses) - skipped_empty} unrecovered miss(es). See: {miss_log}", flush=True)


if __name__ == "__main__":
    run_backfill()
