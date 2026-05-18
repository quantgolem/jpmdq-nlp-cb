"""
Shared utilities for JPMDQ NLP Central Bank group ingestion.

Embedded self-contained helpers — no dependency on local project packages.

Storage flow:
  1. Fetch from JPMDQ  →  rows (list of dicts)
  2. Write parquet to Databricks Volume  (raw bronze, one file per group × day)
  3. COPY INTO Delta table  (idempotent; auto-tracks loaded files)
"""
from __future__ import annotations

import io
import os
from datetime import date, datetime, timedelta
from typing import Any, Sequence

from dotenv import load_dotenv

load_dotenv()


FETCH_TIMEOUT_SEC = int(os.environ.get("JPMDQ_FETCH_TIMEOUT_SEC", "300"))


def resolve_jpmdq_credentials():
    if "DATAQUERY_CLIENT_ID" in os.environ and "DATAQUERY_CLIENT_SECRET" in os.environ:
        return os.environ["DATAQUERY_CLIENT_ID"], os.environ["DATAQUERY_CLIENT_SECRET"]
    if "JPMDQ_CLIENT_ID" in os.environ and "JPMDQ_CLIENT_SECRET" in os.environ:
        return os.environ["JPMDQ_CLIENT_ID"], os.environ["JPMDQ_CLIENT_SECRET"]
    return None, None


# ---------------------------------------------------------------------------
# Date helpers
# ---------------------------------------------------------------------------

def business_days(start: str, end: str) -> list[str]:
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


def ymd_to_iso(ymd: str) -> str:
    return f"{ymd[:4]}-{ymd[4:6]}-{ymd[6:]}"


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
            series = _first_present(getattr(attr, "time_series", None), getattr(attr, "series", None), [])
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
# Sync pagination helper
# ---------------------------------------------------------------------------

def _sync_get_page(client: Any, url: str) -> dict:
    """Follow a pagination link using requests with the client's bearer token."""
    import requests

    token = None
    for attr in ("access_token", "_access_token", "token", "_token"):
        t = getattr(client, attr, None)
        if t:
            token = t
            break

    if not url.startswith(("http://", "https://")):
        base = os.environ.get("DATAQUERY_BASE_URL", "https://api-developer.jpmorgan.com")
        url = f"{base.rstrip('/')}/{url.lstrip('/')}"

    headers = {"Authorization": f"Bearer {token}"} if token else {}
    r = requests.get(url, headers=headers, timeout=FETCH_TIMEOUT_SEC)
    r.raise_for_status()
    return r.json()


# ---------------------------------------------------------------------------
# Attribute auto-discovery
# ---------------------------------------------------------------------------

def discover_group_attributes(dq: Any, group_id: str) -> list[str]:
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
                    continue
                if aid not in seen:
                    seen.append(aid)

    try:
        first = client.get_group_attributes(group_id=group_id)
    except Exception:
        return []

    _collect(first)

    next_url = first.get_next_link() if first else None
    visited: set[str] = set()
    count = 0
    while next_url and count < 200 and next_url not in visited:
        visited.add(next_url)
        count += 1
        try:
            payload = _sync_get_page(client, next_url)
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
# Group time-series fetch (sync, paginated + chunked fallback)
# ---------------------------------------------------------------------------

def _is_no_content(exc: Exception) -> bool:
    s = str(exc).lower()
    return (
        "no content" in s or "'204'" in s or "code': '204'" in s
        or ("field required" in s and "instruments" in s)
    )


def _call_time_series(
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
    client = getattr(dq, "_client", dq)

    try:
        first = client.get_group_time_series(
            group_id=group_id,
            attributes=list(attributes),
            start_date=start_date,
            end_date=end_date,
            calendar=calendar,
            frequency=frequency,
            conversion=conversion,
            nan_treatment=nan_treatment,
        )
    except Exception as exc:
        if _is_no_content(exc):
            return []
        raise

    rows = ts_to_rows(first, group_id=group_id, obs_date=obs_date, ingested_at=ingested_at)

    next_url = first.get_next_link() if first else None
    visited: set[str] = set()
    count = 0
    while next_url and count < 200 and next_url not in visited:
        visited.add(next_url)
        count += 1
        try:
            payload = _sync_get_page(client, next_url)
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


def fetch_group_day(
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
    ingested_at = datetime.utcnow().isoformat()
    attrs = discover_group_attributes(dq, group_id)
    if not attrs:
        print(f"  [warn] no attributes discovered for {group_id}; trying empty list", flush=True)

    common = dict(
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

    rows = _call_time_series(dq, attributes=attrs or [], **common)
    if rows:
        return rows

    if not attrs:
        return []

    print(f"  [empty-full] {group_id} {obs_date}: chunking {len(attrs)} attrs", flush=True)
    all_rows: list[dict] = []
    for i in range(0, len(attrs), chunk_size):
        chunk = attrs[i : i + chunk_size]
        try:
            all_rows.extend(_call_time_series(dq, attributes=chunk, **common))
        except Exception as exc:
            print(f"  [chunk-fail] attrs[{i}:{i+chunk_size}]: {exc}", flush=True)
    return all_rows


# ---------------------------------------------------------------------------
# Retry classifier
# ---------------------------------------------------------------------------

def classify_failure(message: str) -> dict:
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


def process_group_day(
    dq: Any,
    *,
    spark: Any,
    dry_run: bool,
    volume_raw_path: str,
    manifest_table: str,
    group_id: str,
    obs_date: str,
    download_ts: str,
    calendar: str = "CAL_USBANK",
    frequency: str = "FREQ_DAY",
    conversion: str = "CONV_LASTBUS_ABS",
    nan_treatment: str = "NA_NOTHING",
) -> dict:
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
        return {"group_id": group_id, "obs_date": obs_date, "ok": False, "message": f"{type(exc).__name__}: {exc}"}

    if not rows:
        return {"group_id": group_id, "obs_date": obs_date, "ok": False, "message": "empty_payload: 0 rows returned"}

    row_count = len(rows)
    instrument_count = len({r["instrument"] for r in rows})
    attribute_count = len({r["attribute"] for r in rows})
    message = f"rows={row_count} instruments={instrument_count} attributes={attribute_count}"

    record = {
        "group_id": group_id,
        "obs_date": obs_date,
        "ok": True,
        "message": message,
        "row_count": row_count,
        "instrument_count": instrument_count,
        "attribute_count": attribute_count,
    }

    if dry_run:
        return record

    try:
        vpath = write_day_to_volume(
            rows,
            volume_raw_path=volume_raw_path,
            group_id=group_id,
            obs_date=obs_date,
            download_ts=download_ts,
        )
        record["volume_path"] = vpath
    except Exception as exc:
        return {"group_id": group_id, "obs_date": obs_date, "ok": False, "message": f"volume_write_error: {type(exc).__name__}: {exc}"}

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
        record["manifest_warning"] = str(exc)

    return record


# ---------------------------------------------------------------------------
# Databricks helpers
# ---------------------------------------------------------------------------

def get_spark():
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


def auto_detect_catalog_schema() -> tuple[str, str]:
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


# ---------------------------------------------------------------------------
# Volume write  (step 1 of 2)
# ---------------------------------------------------------------------------

FILE_PREFIX = "jpmdq_nlp_cb"


def volume_file_path(volume_raw_path: str, group_id: str, obs_date: str, download_ts: str) -> str:
    filename = f"{FILE_PREFIX}__{group_id}__{obs_date}__{download_ts}.parquet"
    return f"{volume_raw_path.rstrip('/')}/{group_id}/{filename}"


def write_day_to_volume(
    rows: list[dict],
    *,
    volume_raw_path: str,
    group_id: str,
    obs_date: str,
    download_ts: str,
) -> str:
    import polars as pl
    from databricks.sdk import WorkspaceClient

    dest_path = volume_file_path(volume_raw_path, group_id, obs_date, download_ts)

    df = (
        pl.DataFrame(rows)
        .with_columns([
            pl.col("date").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            pl.col("obs_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
            pl.col("ingested_at").str.to_datetime(strict=False),
        ])
    )

    buf = io.BytesIO()
    df.write_parquet(buf)
    buf.seek(0)

    w = WorkspaceClient()
    w.files.upload(dest_path, buf, overwrite=True)
    return dest_path


# ---------------------------------------------------------------------------
# Delta table setup + COPY INTO  (step 2 of 2)
# ---------------------------------------------------------------------------

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
  volume_path      STRING,
  ingested_at      TIMESTAMP
)
USING DELTA
"""


def ensure_tables(spark, bronze_table: str, manifest_table: str) -> None:
    spark.sql(BRONZE_DDL.format(table=bronze_table))
    spark.sql(MANIFEST_DDL.format(table=manifest_table))
    print(f"[tables] bronze={bronze_table}  manifest={manifest_table}", flush=True)


def ensure_volume(catalog: str, schema: str, volume_name: str) -> None:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.catalog import VolumeType

    w = WorkspaceClient()
    try:
        w.volumes.create(
            catalog_name=catalog,
            schema_name=schema,
            name=volume_name,
            volume_type=VolumeType.MANAGED,
        )
        print(f"[volume] created /Volumes/{catalog}/{schema}/{volume_name}", flush=True)
    except Exception as exc:
        if "already exists" in str(exc).lower():
            pass
        else:
            print(f"[volume] could not create (may already exist): {exc}", flush=True)


def copy_into_bronze(spark, bronze_table: str, volume_raw_path: str) -> None:
    spark.sql(f"""
        COPY INTO {bronze_table}
        FROM (
          SELECT
            CAST(group_id    AS STRING)    AS group_id,
            CAST(instrument  AS STRING)    AS instrument,
            CAST(attribute   AS STRING)    AS attribute,
            CAST(date        AS DATE)      AS date,
            CAST(value       AS STRING)    AS value,
            CAST(obs_date    AS DATE)      AS obs_date,
            CAST(ingested_at AS TIMESTAMP) AS ingested_at
          FROM '{volume_raw_path}'
        )
        FILEFORMAT = PARQUET
        COPY_OPTIONS ('recursiveFileLookup' = 'true', 'mergeSchema' = 'true')
    """)
    print(f"[copy-into] loaded new files from {volume_raw_path} → {bronze_table}", flush=True)


# ---------------------------------------------------------------------------
# Manifest helpers
# ---------------------------------------------------------------------------

def upsert_manifest(
    spark,
    *,
    manifest_table: str,
    group_id: str,
    obs_date: str,
    row_count: int,
    instrument_count: int,
    attribute_count: int,
    status: str,
    volume_path: str,
) -> None:
    iso_date = ymd_to_iso(obs_date)
    ingested_at = datetime.utcnow().isoformat()

    spark.sql(
        f"DELETE FROM {manifest_table} "
        f"WHERE group_id = '{group_id}' AND obs_date = CAST('{iso_date}' AS DATE)"
    )
    from pyspark.sql import functions as F
    spark.createDataFrame([{
        "group_id": group_id,
        "obs_date": iso_date,
        "row_count": row_count,
        "instrument_count": instrument_count,
        "attribute_count": attribute_count,
        "status": status,
        "volume_path": volume_path,
        "ingested_at": ingested_at,
    }]).withColumn("obs_date", F.to_date("obs_date")
    ).withColumn("ingested_at", F.to_timestamp("ingested_at")
    ).write.mode("append").saveAsTable(manifest_table)


def get_ingested_dates(spark, manifest_table: str, group_id: str) -> set[str]:
    try:
        rows = spark.sql(
            f"SELECT obs_date FROM {manifest_table} "
            f"WHERE group_id = '{group_id}' AND status = 'ok'"
        ).collect()
        return {str(r["obs_date"]).replace("-", "") for r in rows}
    except Exception:
        return set()


def get_last_ingested_date(spark, manifest_table: str, group_id: str) -> str | None:
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


if __name__ == "__main__":
    # ── 1. business_days ────────────────────────────────────────────────────
    days = business_days("20240101", "20240110")
    print("business_days:", days)

    # ── 2. classify_failure ─────────────────────────────────────────────────
    for msg in ["429 too many requests", "401 unauthorized", "204 no content",
                "connection timed out", "unknown error xyz"]:
        print(f"  classify({msg!r}): {classify_failure(msg)}")

    # ── 3. ts_to_rows — no live connection needed ───────────────────────────
    class _Pt:
        def __init__(self, d, v): self.date = d; self.value = v
    class _Attr:
        def __init__(self): self.attribute_id = "SENTIMENT"; self.time_series = [{"date": "2024-01-02", "value": "0.45"}]
    class _Inst:
        def __init__(self): self.instrument_name = "ECB"; self.attributes = [_Attr()]
    class _TS:
        def __init__(self): self.instruments = [_Inst()]

    rows = ts_to_rows(_TS(), group_id="NLP_CB_STATEMENTS", obs_date="20240102")
    print("ts_to_rows sample:", rows)

    # ── 4. Live fetch_group_day (needs .env with DATAQUERY_* vars) ───────────
    try:
        from dataquery import DataQuery
        client_id     = os.environ["DATAQUERY_CLIENT_ID"]
        client_secret = os.environ["DATAQUERY_CLIENT_SECRET"]
        base_url      = os.environ.get("DATAQUERY_BASE_URL", "https://api-developer.jpmorgan.com")
        with DataQuery(client_id=client_id, client_secret=client_secret, base_url=base_url) as dq:
            rows = fetch_group_day(dq, group_id="NLP_CB_STATEMENTS", obs_date="20240102")
            print(f"fetch_group_day: {len(rows)} rows")
            if rows:
                import polars as pl
                print(pl.DataFrame(rows).head(5))
    except Exception as exc:
        print(f"[live test skipped] {exc}")