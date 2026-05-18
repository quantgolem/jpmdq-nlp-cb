"""
Shared utilities for JPMDQ NLP Central Bank group ingestion.

Embedded self-contained helpers — no dependency on local project packages.
Derived from jpmorgan-dataquery-assets/utils/group_returns.py patterns.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import date, datetime, timedelta
from typing import Any, Sequence


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def business_days(start: str, end: str) -> list[str]:
    """Return Mon–Fri dates in [start, end] as YYYYMMDD strings."""
    s = datetime.strptime(start, "%Y%m%d").date()
    e = datetime.strptime(end, "%Y%m%d").date()
    out: list[str] = []
    d = s
    while d <= e:
        if d.weekday() < 5:
            out.append(d.strftime("%Y%m%d"))
        d += timedelta(days=1)
    return out


def yesterday_ymd() -> str:
    return (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")


# ---------------------------------------------------------------------------
# JPMDQ response → row list
# ---------------------------------------------------------------------------

def _first_present(*values: Any) -> Any:
    for v in values:
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        return v
    return None


def _norm_date(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return str(value)[:10]
    text = str(value)
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date().isoformat()
    if len(text) >= 10:
        return text[:10]
    return text


def ts_to_rows(
    ts: Any,
    *,
    group_id: str | None = None,
    obs_date: str | None = None,
    ingested_at: str | None = None,
) -> list[dict]:
    """
    Convert a DataQuery TimeSeriesResponse to a flat list of dicts.

    value is kept as str (handles both numeric and NLP text attributes).
    """
    now = ingested_at or datetime.utcnow().isoformat()
    rows: list[dict] = []
    instruments = getattr(ts, "instruments", []) if ts is not None else []
    for inst in instruments or []:
        instrument = _first_present(
            getattr(inst, "instrument_name", None),
            getattr(inst, "instrument_id", None),
            getattr(inst, "name", None),
            "UNKNOWN",
        )
        for attr in (getattr(inst, "attributes", []) or []):
            attribute = _first_present(
                getattr(attr, "attribute_id", None),
                getattr(attr, "attribute", None),
                getattr(attr, "name", None),
                "UNKNOWN",
            )
            series = _first_present(
                getattr(attr, "time_series", None),
                getattr(attr, "series", None),
                [],
            )
            for pt in series or []:
                if isinstance(pt, dict):
                    raw_date = _first_present(pt.get("date"), pt.get("obs_date"), pt.get("time"))
                    raw_val = pt.get("value")
                elif isinstance(pt, (list, tuple)) and len(pt) >= 2:
                    raw_date = pt[0]
                    raw_val = pt[1]
                else:
                    continue
                d = _norm_date(raw_date)
                if d is None:
                    continue
                rows.append({
                    "group_id": group_id,
                    "instrument": str(instrument),
                    "attribute": str(attribute),
                    "date": d,
                    "value": str(raw_val) if raw_val is not None else None,
                    "obs_date": obs_date or d,
                    "ingested_at": now,
                })
    return rows


# ---------------------------------------------------------------------------
# Attribute auto-discovery
# ---------------------------------------------------------------------------

FETCH_TIMEOUT_SEC = int(os.environ.get("JPMDQ_FETCH_TIMEOUT_SEC", "300"))


async def discover_group_attributes(dq: Any, group_id: str) -> list[str]:
    """Return deduplicated attribute IDs for a group (follows pagination)."""
    client = getattr(dq, "_client", dq)
    seen: list[str] = []

    def _collect(resp_obj: Any) -> None:
        for inst in (getattr(resp_obj, "instruments", []) or []):
            for attr in (getattr(inst, "attributes", []) or []):
                aid = _first_present(
                    getattr(attr, "attribute_id", None),
                    getattr(attr, "attribute", None),
                    getattr(attr, "name", None),
                )
                if not aid:
                    continue
                aid = str(aid)
                if "," in aid:
                    continue  # qualifier-style ids rejected by time-series API
                if aid not in seen:
                    seen.append(aid)

    try:
        first = await asyncio.wait_for(
            client.get_group_attributes_async(group_id=group_id),
            timeout=FETCH_TIMEOUT_SEC,
        )
    except Exception:
        return []

    _collect(first)
    next_url = first.get_next_link() if first else None
    page_count, visited = 1, set()
    while next_url and page_count < 200:
        if next_url in visited:
            break
        visited.add(next_url)
        page_count += 1
        if not next_url.startswith(("http://", "https://")):
            next_url = client._build_api_url(next_url.lstrip("/"))
        try:
            async with await client._enter_request_cm("GET", next_url) as resp:
                await client._handle_response(resp)
                payload = await resp.json()
            if "instruments" not in payload:
                break
            from dataquery.types.models import AttributesResponse
            page = AttributesResponse(**payload)
            _collect(page)
            next_url = page.get_next_link() if page else None
        except Exception:
            break
    return seen


# ---------------------------------------------------------------------------
# Group time-series fetch (paginated + chunked fallback + timeout)
# ---------------------------------------------------------------------------

def _is_no_content(exc: Exception) -> bool:
    s = str(exc).lower()
    return "no content" in s or "'204'" in s or "code': '204'" in s or (
        "field required" in s and "instruments" in s
    )


async def _fetch_one_call(
    dq: Any,
    *,
    group_id: str,
    attributes: Sequence[str],
    start_date: str,
    end_date: str,
    calendar: str,
    frequency: str,
    conversion: str,
    nan_treatment: str,
    obs_date: str,
    ingested_at: str,
) -> list[dict]:
    """Paginated fetch for a single (group, day) call."""
    client = getattr(dq, "_client", dq)
    try:
        first = await asyncio.wait_for(
            client.get_group_time_series_async(
                group_id=group_id,
                attributes=list(attributes),
                start_date=start_date,
                end_date=end_date,
                calendar=calendar,
                frequency=frequency,
                conversion=conversion,
                nan_treatment=nan_treatment,
            ),
            timeout=FETCH_TIMEOUT_SEC,
        )
    except Exception as exc:
        if _is_no_content(exc):
            return []
        raise

    rows = ts_to_rows(first, group_id=group_id, obs_date=obs_date, ingested_at=ingested_at)
    next_url = first.get_next_link() if first else None
    page_count, visited = 1, set()
    while next_url and page_count < 200:
        if next_url in visited:
            break
        visited.add(next_url)
        page_count += 1
        if not next_url.startswith(("http://", "https://")):
            next_url = client._build_api_url(next_url.lstrip("/"))
        try:
            async with await client._enter_request_cm("GET", next_url) as resp:
                await client._handle_response(resp)
                payload = await resp.json()
            if "instruments" not in payload:
                break
            from dataquery.types.models import TimeSeriesResponse
            page = TimeSeriesResponse(**payload)
            rows += ts_to_rows(page, group_id=group_id, obs_date=obs_date, ingested_at=ingested_at)
            next_url = page.get_next_link() if page else None
        except Exception as exc:
            if _is_no_content(exc):
                break
            raise
    return rows


async def fetch_group_day(
    dq: Any,
    *,
    group_id: str,
    obs_date: str,
    calendar: str = "CAL_USBANK",
    frequency: str = "FREQ_DAY",
    conversion: str = "CONV_LASTBUS_ABS",
    nan_treatment: str = "NA_NOTHING",
    chunk_size: int = 10,
) -> list[dict]:
    """
    Fetch one (group, day) pair. Returns flat list of row dicts.

    Strategy:
    1. Discover attributes.
    2. Try full-attribute call.
    3. If 0 rows, fall back to per-attribute-chunk calls.
    """
    ingested_at = datetime.utcnow().isoformat()
    attrs = await discover_group_attributes(dq, group_id)
    if not attrs:
        print(f"  [warn] no attributes discovered for {group_id}; trying empty attribute list", flush=True)

    common_kwargs = dict(
        group_id=group_id,
        start_date=obs_date,
        end_date=obs_date,
        calendar=calendar,
        frequency=frequency,
        conversion=conversion,
        nan_treatment=nan_treatment,
        obs_date=obs_date,
        ingested_at=ingested_at,
    )

    rows = await _fetch_one_call(dq, attributes=attrs, **common_kwargs)
    if rows:
        return rows

    if not attrs:
        return []

    print(f"  [empty-full] {group_id} {obs_date}: full-attrs returned 0 rows, chunking", flush=True)
    all_rows: list[dict] = []
    for i in range(0, len(attrs), chunk_size):
        chunk = attrs[i:i + chunk_size]
        try:
            chunk_rows = await _fetch_one_call(dq, attributes=chunk, **common_kwargs)
            all_rows.extend(chunk_rows)
        except Exception as exc:
            print(f"  [chunk-fail] {group_id} {obs_date} attrs[{i}:{i+chunk_size}]: {exc}", flush=True)
    return all_rows


# ---------------------------------------------------------------------------
# Retry classifier
# ---------------------------------------------------------------------------

def classify_failure(message: str) -> dict:
    """Classify a failure message and return retry strategy."""
    s = (message or "").lower()
    if "429" in s or "rate limit" in s or "too many requests" in s or "quota" in s:
        return {"category": "rate_limit", "strategy": "long_backoff", "wait_s": 180}
    if any(k in s for k in ["401", "403", "unauthor", "invalid_token", "token expired"]):
        return {"category": "auth_error", "strategy": "sleep_and_retry", "wait_s": 60}
    if "204" in s or "no content" in s or "empty" in s or "0 rows" in s:
        return {"category": "empty_payload", "strategy": "skip", "wait_s": 0}
    if "timeouterror" in s or "timed out" in s or "timeout" in s or "connection reset" in s or "ssl" in s:
        return {"category": "timeout", "strategy": "long_backoff", "wait_s": 90}
    if "validation" in s:
        return {"category": "validation_failed", "strategy": "retry", "wait_s": 15}
    return {"category": "unknown", "strategy": "std_backoff", "wait_s": 30}


# ---------------------------------------------------------------------------
# Databricks session + table helpers
# ---------------------------------------------------------------------------

def get_spark():
    """Get or create a Spark session (Databricks cluster or databricks-connect)."""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark is not None:
            return spark
    except Exception:
        pass
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except Exception:
        pass
    from pyspark.sql import SparkSession
    return SparkSession.builder.appName("jpmdq_nlp_cb").getOrCreate()


def get_dbutils(spark):
    """Return dbutils if on a Databricks notebook/job; else None."""
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except Exception:
        return None


def auto_detect_catalog_schema() -> tuple[str, str]:
    """
    Auto-detect GIC Unity Catalog / personal schema via WorkspaceClient.
    Returns ("", "") if not in GIC environment or sdk not installed.
    """
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        user = w.current_user.me().user_name.split("@")[0]
        matches = [x for x in w.catalogs.list() if "users_schema_iz" in x.name]
        if matches:
            catalog = matches[0].name
            schema = f"{user}_personal_schema"
            return catalog, schema
    except Exception:
        pass
    return "", ""


BRONZE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  group_id      STRING,
  instrument    STRING,
  attribute     STRING,
  date          DATE,
  value         STRING,
  obs_date      DATE,
  ingested_at   TIMESTAMP
)
USING DELTA
PARTITIONED BY (group_id, obs_date)
TBLPROPERTIES ('delta.autoOptimize.autoCompact' = 'true')
"""

MANIFEST_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  group_id         STRING,
  obs_date         DATE,
  row_count        LONG,
  instrument_count LONG,
  attribute_count  LONG,
  status           STRING,
  ingested_at      TIMESTAMP
)
USING DELTA
"""


def ensure_tables(spark, bronze_table: str, manifest_table: str) -> None:
    """CREATE TABLE IF NOT EXISTS for bronze and manifest tables."""
    spark.sql(BRONZE_DDL.format(table=bronze_table))
    spark.sql(MANIFEST_DDL.format(table=manifest_table))
    print(f"[tables] bronze={bronze_table}  manifest={manifest_table}", flush=True)


def write_day_to_delta(
    spark,
    rows: list[dict],
    *,
    bronze_table: str,
    manifest_table: str,
    group_id: str,
    obs_date: str,
) -> dict:
    """
    Idempotently write one (group, obs_date) batch to the bronze Delta table
    and update the manifest.

    Pattern: DELETE existing rows for (group_id, obs_date), then INSERT.
    """
    from pyspark.sql import Row
    import pyspark.sql.functions as F

    # DELETE existing rows for idempotency
    spark.sql(
        f"DELETE FROM {bronze_table} "
        f"WHERE group_id = '{group_id}' AND obs_date = CAST('{obs_date[:4]}-{obs_date[4:6]}-{obs_date[6:]}' AS DATE)"
    )

    result = {"group_id": group_id, "obs_date": obs_date, "row_count": 0,
              "instrument_count": 0, "attribute_count": 0, "status": "empty"}

    if rows:
        df_pd = _rows_to_pandas(rows)
        sdf = spark.createDataFrame(df_pd)
        sdf = (
            sdf
            .withColumn("date", F.to_date("date"))
            .withColumn("obs_date", F.to_date("obs_date"))
            .withColumn("ingested_at", F.to_timestamp("ingested_at"))
        )
        sdf.write.mode("append").saveAsTable(bronze_table)

        row_count = len(rows)
        instrument_count = len({r["instrument"] for r in rows})
        attribute_count = len({r["attribute"] for r in rows})
        result.update({
            "row_count": row_count,
            "instrument_count": instrument_count,
            "attribute_count": attribute_count,
            "status": "ok",
        })

    # Upsert manifest
    ingested_at = datetime.utcnow().isoformat()
    obs_date_fmt = f"{obs_date[:4]}-{obs_date[4:6]}-{obs_date[6:]}"
    spark.sql(
        f"DELETE FROM {manifest_table} "
        f"WHERE group_id = '{group_id}' AND obs_date = CAST('{obs_date_fmt}' AS DATE)"
    )
    manifest_row = spark.createDataFrame([{
        "group_id": group_id,
        "obs_date": obs_date_fmt,
        "row_count": result["row_count"],
        "instrument_count": result["instrument_count"],
        "attribute_count": result["attribute_count"],
        "status": result["status"],
        "ingested_at": ingested_at,
    }])
    import pyspark.sql.functions as F
    manifest_row = (
        manifest_row
        .withColumn("obs_date", F.to_date("obs_date"))
        .withColumn("ingested_at", F.to_timestamp("ingested_at"))
    )
    manifest_row.write.mode("append").saveAsTable(manifest_table)
    return result


def get_ingested_dates(spark, manifest_table: str, group_id: str) -> set[str]:
    """Return set of YYYYMMDD dates already ingested (status=ok) for a group."""
    try:
        rows = spark.sql(
            f"SELECT obs_date FROM {manifest_table} "
            f"WHERE group_id = '{group_id}' AND status = 'ok'"
        ).collect()
        return {str(r["obs_date"]).replace("-", "") for r in rows}
    except Exception:
        return set()


def get_last_ingested_date(spark, manifest_table: str, group_id: str) -> str | None:
    """Return max(obs_date) as YYYYMMDD for a group, or None if no data."""
    try:
        row = spark.sql(
            f"SELECT MAX(obs_date) AS max_date FROM {manifest_table} "
            f"WHERE group_id = '{group_id}' AND status = 'ok'"
        ).first()
        if row and row["max_date"]:
            return str(row["max_date"]).replace("-", "")
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Minimal pandas bridge (avoids polars dependency)
# ---------------------------------------------------------------------------

def _rows_to_pandas(rows: list[dict]):
    """Convert list of row dicts to a pandas DataFrame."""
    try:
        import pandas as pd
        return pd.DataFrame(rows)
    except ImportError:
        raise ImportError("pandas is required: pip install pandas")
