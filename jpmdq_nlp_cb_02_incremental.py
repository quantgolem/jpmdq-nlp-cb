"""
JPMDQ NLP Central Bank — Daily Incremental Load
================================================
Continues from the last ingested date in the manifest Delta table.
Safe to run daily as a Databricks Job or scheduled script.

For each group:
  1. Read max(obs_date) from manifest (status='ok').
  2. Fetch every business day from (last_date + 1) to --end (default: yesterday).
  3. Write each parquet file to the Databricks Volume:
       /Volumes/{catalog}/{schema}/jpmdq_nlp_cb/raw/{group_id}/
  4. Update the manifest Delta table after each successful file write.
  5. Run COPY INTO at the end — picks up only the new files (idempotent).

Usage:

  python jpmdq_nlp_cb_02_incremental.py \\
      --groups NLP_CB_STATEMENTS NLP_CB_MINUTES \\
      [--end 20261231]           \\
      [--default-start 20230101] \\
      [--catalog my_catalog]     \\
      [--schema my_schema]

  If a group has no data in the manifest yet, --default-start is used
  (default: 2 years before --end).

Credentials:
  Set DATAQUERY_CLIENT_ID + DATAQUERY_CLIENT_SECRET env vars,
  or use Databricks cluster environment variables.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

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
    get_last_ingested_date,
    get_spark,
    upsert_manifest,
    write_day_to_volume,
    yesterday_ymd,
)

try:
    from dataquery import DataQuery
except ImportError as exc:
    raise SystemExit("Missing dataquery-sdk. Install: pip install dataquery-sdk") from exc


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="jpmdq_nlp_cb_02_incremental.py",
        description="Daily incremental load: Volume write + COPY INTO for missing dates.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--groups", nargs="+", required=True, metavar="GROUP_ID")
    p.add_argument("--end", default=None,
                   help="End date YYYYMMDD (inclusive). Default: yesterday.")
    p.add_argument("--default-start", default=None,
                   help="Start date for groups with no manifest data yet. Default: 2 years ago.")
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
    p.add_argument("--skip-copy-into", action="store_true",
                   help="Write files to Volume only; skip the COPY INTO step.")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--log-dir", default="logs")
    return p


# ---------------------------------------------------------------------------
# Per-(group, day) fetch → Volume write
# ---------------------------------------------------------------------------

async def _try_one(
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
) -> tuple[bool, str]:
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

    row_count = len(rows)
    instrument_count = len({r["instrument"] for r in rows})
    attribute_count = len({r["attribute"] for r in rows})
    msg = f"rows={row_count} instruments={instrument_count} attributes={attribute_count}"

    if dry_run:
        return True, f"[dry-run] {msg}"

    # Write to Volume
    try:
        vpath = write_day_to_volume(
            rows,
            volume_raw_path=volume_raw_path,
            group_id=group_id,
            obs_date=obs_date,
            download_ts=download_ts,
        )
    except Exception as exc:
        return False, f"volume_write_error: {type(exc).__name__}: {exc}"

    # Update manifest
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

    return True, msg


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main_async() -> None:
    args = build_parser().parse_args()

    # Credentials
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
            "Set DATAQUERY_CLIENT_ID + DATAQUERY_CLIENT_SECRET env vars."
        )
    oauth_aud = args.oauth_aud or os.environ.get("DATAQUERY_OAUTH_AUD", "")
    base_url = args.base_url or os.environ.get("DATAQUERY_BASE_URL", "https://api-developer.jpmorgan.com")

    # Catalog / schema
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
    two_years_ago = (datetime.utcnow() - timedelta(days=730)).strftime("%Y%m%d")
    default_start = args.default_start or two_years_ago

    # Spark + tables + volume
    if not args.dry_run:
        spark = get_spark()
        ensure_volume(catalog, schema, args.volume_name)
        ensure_tables(spark, bronze_table, manifest_table)
    else:
        spark = None

    # Per-group date ranges
    group_ranges: dict[str, list[str]] = {}
    for group_id in args.groups:
        last = None if args.dry_run else get_last_ingested_date(spark, manifest_table, group_id)
        if last:
            next_day = (datetime.strptime(last, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
            start = next_day
        else:
            start = default_start

        if start > end_date:
            print(f"[{group_id}] up to date (last={last}, end={end_date}). Nothing to do.", flush=True)
            group_ranges[group_id] = []
        else:
            days = business_days(start, end_date)
            group_ranges[group_id] = days
            print(f"[{group_id}] last={last or 'none'} → {len(days)} days ({start}→{end_date})", flush=True)

    total_pairs = sum(len(v) for v in group_ranges.values())
    if total_pairs == 0:
        print("[done] All groups up to date. Nothing to fetch.", flush=True)
        return

    log_dir = Path(args.log_dir).resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    download_ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    miss_log = log_dir / f"incremental_misses__{download_ts}.jsonl"

    print(
        f"\n[incremental] total_pairs={total_pairs}  volume={volume_raw_path}"
        f"  bronze={bronze_table}  dry_run={args.dry_run}",
        flush=True,
    )

    dq_kwargs: dict = {"client_id": client_id, "client_secret": client_secret, "base_url": base_url}
    if oauth_aud:
        dq_kwargs["oauth_aud"] = oauth_aud

    successes: list[dict] = []
    misses: list[dict] = []

    async with DataQuery(**dq_kwargs) as dq:
        done = 0
        for group_id, days in group_ranges.items():
            for obs_date in days:
                done += 1
                prefix = f"[{done}/{total_pairs}] {group_id} {obs_date}"
                print(prefix, end=" → ", flush=True)

                ok, msg = await _try_one(
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
                        time.sleep(wait_s)
                    ok, msg = await _try_one(
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
                        print(f"  [recovered] {group_id} {obs_date}: {msg}", flush=True)
                        break
                    diag = classify_failure(msg)
                    if diag["strategy"] == "skip":
                        break
                if not resolved:
                    still_failing.append({**m, "pass": 3, "ok": False})
                    print(f"  [still-miss] {group_id} {obs_date}", flush=True)
            misses = still_failing

    # COPY INTO — picks up only new files (idempotent)
    if not args.dry_run and not args.skip_copy_into and successes:
        print(f"\n[copy-into] loading new files into {bronze_table}...", flush=True)
        copy_into_bronze(spark, bronze_table, volume_raw_path)

    # Summary
    skipped_empty = sum(1 for m in misses if classify_failure(m["message"])["strategy"] == "skip")
    summary = {
        "run_ts": download_ts,
        "groups": args.groups,
        "end_date": end_date,
        "total_pairs": total_pairs,
        "successes": len(successes),
        "misses_actionable": len(misses) - skipped_empty,
        "misses_empty_payload": skipped_empty,
        "volume_raw_path": volume_raw_path,
        "bronze_table": bronze_table,
        "manifest_table": manifest_table,
        "dry_run": args.dry_run,
        "group_ranges": {
            g: {"count": len(d), "start": d[0] if d else None, "end": d[-1] if d else None}
            for g, d in group_ranges.items()
        },
    }
    summary_path = log_dir / f"incremental_summary__{download_ts}.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(f"\n[summary]\n{json.dumps(summary, indent=2)}", flush=True)


if __name__ == "__main__":
    asyncio.run(main_async())
