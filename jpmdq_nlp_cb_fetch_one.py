"""
Fetch helpers: list available groups, fetch one group/day → polars DataFrame.

Public API is 100% synchronous. Uses nest_asyncio to patch the event loop so
asyncio.run() works safely inside interactive Python, Jupyter, and scripts alike.
Single aiohttp session is kept alive across all calls.
"""
import asyncio
import os
from pathlib import Path

import polars as pl
import nest_asyncio
from dotenv import load_dotenv

load_dotenv()

from dataquery import DataQuery
from dataquery.types.models import AttributesResponse, TimeSeriesResponse

def _resolve_creds():
    """Resolve JPMDQ credentials from environment, trying multiple naming conventions.

    Returns (client_id, client_secret). Sets DATAQUERY_CLIENT_ID / DATAQUERY_CLIENT_SECRET
    in os.environ so the underlying DataQuery SDK can pick them up too.
    """
    pairs = [
        ('JPM_A_CLIENT_ID', 'JPM_A_CLIENT_SECRET'),
        ('JPM_G_CLIENT_ID', 'JPM_G_CLIENT_SECRET'),
        ('DATAQUERY_CLIENT_ID', 'DATAQUERY_CLIENT_SECRET'),
        ('jpm_a_client_id', 'jpm_a_client_secret'),
        ('jpm_g_client_id', 'jpm_g_client_secret'),
    ]
    for cid_key, sec_key in pairs:
        cid_val = os.environ.get(cid_key)
        sec_val = os.environ.get(sec_key)
        if cid_val and sec_val:
            os.environ.setdefault('DATAQUERY_CLIENT_ID', cid_val)
            os.environ.setdefault('DATAQUERY_CLIENT_SECRET', sec_val)
            return cid_val, sec_val
    print("No JPMDQ credentials found: set JPM_A_CLIENT_ID/SECRET, DATAQUERY_CLIENT_ID/SECRET, etc.", flush=True)
    raise SystemExit(1)

CLIENT_ID, CLIENT_SECRET = _resolve_creds()
nest_asyncio.apply()
BASE_URL = os.environ.get("DATAQUERY_BASE_URL", "https://api-developer.jpmorgan.com")


def _parse_attrs(resp_obj) -> list[str]:
    seen: list[str] = []
    for inst in getattr(resp_obj, "instruments", []) or []:
        for attr in getattr(inst, "attributes", []) or []:
            aid = (
                getattr(attr, "attribute_id", None)
                or getattr(attr, "attribute", None)
                or getattr(attr, "name", None)
            )
            if not aid:
                continue
            aid = str(aid)
            if "," in aid:
                continue
            if aid not in seen:
                seen.append(aid)
    return seen


def _parse_ts(resp_obj) -> list[dict]:
    rows = []
    for inst in getattr(resp_obj, "instruments", []) or []:
        instrument = str(
            getattr(inst, "instrument_name", None)
            or getattr(inst, "instrument_id", None)
            or "UNKNOWN"
        )
        for attr in getattr(inst, "attributes", []) or []:
            attribute = str(
                getattr(attr, "attribute_id", None)
                or getattr(attr, "attribute", None)
                or "UNKNOWN"
            )
            for pt in getattr(attr, "time_series", None) or getattr(attr, "series", None) or []:
                if isinstance(pt, dict):
                    date_val = pt.get("date") or pt.get("obs_date") or pt.get("time")
                    value = pt.get("value")
                elif isinstance(pt, (list, tuple)) and len(pt) >= 2:
                    date_val, value = pt[0], pt[1]
                else:
                    continue
                rows.append({
                    "instrument": instrument,
                    "attribute": attribute,
                    "date": str(date_val)[:10],
                    "value": str(value) if value is not None else None,
                })
    return rows


def list_groups(filter: str = "") -> pl.DataFrame:
    """Return all 575 JPMDQ groups (e.g. FI_GO_BO_AA_AUD_GOV, FI_SW_*, DQ_ECON_*) as a polars DataFrame.

    Reads from the catalog snapshot on disk when available (fast, no API call).
    Falls back to the live API otherwise.

    Columns: group_id, name, description

    Args:
        filter: optional case-insensitive substring to match against group_id or name.

    Example:
        all_groups  = list_groups()
        fi_go_only  = list_groups(filter="FI_GO")
        swaps_only  = list_groups(filter="FI_SW")
    """
    CATALOG_PATH = Path("/home/workspace/Data/jpm/downloads/jpmdq/catalog/jpmdq_01a_catalog__latest.parquet")

    if CATALOG_PATH.exists():
        df = pl.read_parquet(CATALOG_PATH).select(
            pl.col("group_id"),
            pl.col("group_name").alias("name"),
            pl.col("description"),
        )
    else:
        async def _inner() -> list[dict]:
            async with DataQuery(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, base_url=BASE_URL) as dq:
                try:
                    groups = await dq.list_groups_async(limit=None)
                except Exception as exc:
                    print(f"[error] list_groups_async failed: {exc}")
                    return []
                rows = []
                seen: set[str] = set()
                for g in groups or []:
                    gid = str(getattr(g, "group_id", None) or "")
                    name = str(getattr(g, "group_name", None) or "")
                    desc = str(getattr(g, "description", None) or "")
                    if gid and gid not in seen:
                        seen.add(gid)
                        rows.append({"group_id": gid, "name": name, "description": desc})
                return rows

        rows = asyncio.run(_inner())
        df = pl.DataFrame(rows) if rows else pl.DataFrame({"group_id": [], "name": [], "description": []})

    if filter:
        f = filter.lower()
        df = df.filter(
            pl.col("group_id").str.to_lowercase().str.contains(f)
            | pl.col("name").str.to_lowercase().str.contains(f)
        )

    print(f"[list_groups] {len(df)} groups" + (f" matching '{filter}'" if filter else ""))
    return df


def fetch_group(
    group_id: str,
    obs_date: str,
    calendar: str = "CAL_USBANK",
    frequency: str = "FREQ_DAY",
    conversion: str = "CONV_LASTBUS_ABS",
    nan_treatment: str = "NA_NOTHING",
    search_text: str | None = None,
) -> pl.DataFrame:
    """Fetch one group for one obs_date → polars DataFrame with columns instrument/attribute/date/value."""
    async def _inner():
        async with DataQuery(client_id=CLIENT_ID, client_secret=CLIENT_SECRET, base_url=BASE_URL) as dq:
            client = dq._client

            # --- search for matching instruments (optional) ---
            search_ids: list[str] | None = None
            if search_text:
                try:
                    search_resp = await client.search_instruments_async(
                        group_id=group_id,
                        keywords=search_text,
                    )
                    search_ids = []
                    for inst in getattr(search_resp, "instruments", []) or []:
                        iid = str(getattr(inst, "instrument_id", None) or getattr(inst, "instrument_name", None) or "")
                        if iid:
                            search_ids.append(iid)
                    print(f"[search] {group_id} text='{search_text}' → {len(search_ids)} instrument(s)")
                except Exception as exc:
                    print(f"[warn] search_text failed: {exc}")

            # --- attribute discovery (with pagination) ---
            attrs: list[str] = []
            try:
                first_attr = await client.get_group_attributes_async(group_id=group_id)
                attrs = _parse_attrs(first_attr)

                next_url = first_attr.get_next_link() if hasattr(first_attr, "get_next_link") else None
                visited: set[str] = set()
                while next_url and len(visited) < 200:
                    if next_url in visited:
                        break
                    visited.add(next_url)
                    absolute = next_url if next_url.startswith("http") else client._build_api_url(next_url.lstrip("/"))
                    try:
                        async with await client._enter_request_cm("GET", absolute) as r:
                            await client._handle_response(r)
                            payload = await r.json()
                        page = AttributesResponse(**payload)
                        attrs.extend(a for a in _parse_attrs(page) if a not in attrs)
                        next_url = page.get_next_link() if hasattr(page, "get_next_link") else None
                    except Exception:
                        break
            except Exception as exc:
                print(f"[warn] attribute discovery failed: {exc}")

            print(f"[fetch_group] {group_id} {obs_date}  attrs={len(attrs)}")
            if not attrs:
                print("[error] no attributes discovered — cannot fetch time series")
                return []

            # --- time-series fetch in chunks of 10 (API rejects >10 attrs) ---
            CHUNK = 10
            rows: list[dict] = []
            attr_chunks = [attrs[i:i + CHUNK] for i in range(0, len(attrs), CHUNK)]

            for chunk_idx, chunk_attrs in enumerate(attr_chunks):
                try:
                    first_ts = await client.get_group_time_series_async(
                        group_id=group_id,
                        attributes=chunk_attrs,
                        start_date=obs_date,
                        end_date=obs_date,
                        calendar=calendar,
                        frequency=frequency,
                        conversion=conversion,
                        nan_treatment=nan_treatment,
                        **({"filter": search_ids} if search_ids else {}),
                    )
                except Exception as exc:
                    print(f"[warn] chunk {chunk_idx + 1}/{len(attr_chunks)} failed: {exc}")
                    continue

                rows.extend(_parse_ts(first_ts))

                next_url = first_ts.get_next_link() if hasattr(first_ts, "get_next_link") else None
                visited: set[str] = set()
                while next_url and len(visited) < 200:
                    if next_url in visited:
                        break
                    visited.add(next_url)
                    absolute = next_url if next_url.startswith("http") else client._build_api_url(next_url.lstrip("/"))
                    try:
                        async with await client._enter_request_cm("GET", absolute) as r:
                            await client._handle_response(r)
                            payload = await r.json()
                        page = TimeSeriesResponse(**payload)
                        rows.extend(_parse_ts(page))
                        next_url = page.get_next_link() if hasattr(page, "get_next_link") else None
                    except Exception:
                        break

            return rows

    rows = asyncio.run(_inner())
    if not rows:
        print("[fetch_group] no data returned")
        return pl.DataFrame()

    print(f"[fetch_group] {len(rows)} rows")
    return pl.DataFrame(rows)


if __name__ == "__main__":
    # ── 1. All accessible groups ────────────────────────────────────────────
    all_groups = list_groups()
    print(all_groups)

    # ── 2. Filter to government-bond groups ────────────────────────────────
    fi_go_groups = list_groups(filter="FI_GO")
    print(fi_go_groups)

    # ── 3. Fetch one govie group for one day ───────────────────────────────
    df = fetch_group("FI_GO_BO_AA_AUD_GOV", "20260515")
    print(df)
    print(f"\nshape: {df.shape}  —  {df['instrument'].n_unique()} instruments × {df['attribute'].n_unique()} attributes")
