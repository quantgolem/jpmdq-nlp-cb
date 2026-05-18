"""
Fetch helpers: list available groups, fetch one group/day → polars DataFrame.

Public API is 100% synchronous. Internally uses asyncio.run() to keep the
aiohttp session alive inside a single event loop (the SDK's _run_sync pattern
creates a new loop per call, which closes the session between calls).

Usage:
    from jpmdq_nlp_cb_fetch_one import list_groups, fetch_one

    groups = list_groups()                         # all groups visible to credentials
    nlp    = list_groups(filter="nlp")             # filter by substring
    df     = fetch_one("NLP_CB_STATEMENTS", "20240101")
"""
import asyncio
import os

import polars as pl
from dotenv import load_dotenv

load_dotenv()

from dataquery import DataQuery
from dataquery.types.models import AttributesResponse, TimeSeriesResponse


def _credentials() -> tuple[str, str, str]:
    """Resolve JPMDQ credentials from environment."""
    for id_key, sec_key in [
        ("DATAQUERY_CLIENT_ID", "DATAQUERY_CLIENT_SECRET"),
        ("JPM_A_CLIENT_ID", "JPM_A_CLIENT_SECRET"),
        ("jpm_a_client_id", "jpm_a_client_secret"),
    ]:
        cid = os.environ.get(id_key)
        csec = os.environ.get(sec_key)
        if cid and csec:
            return cid, csec, os.environ.get("DATAQUERY_BASE_URL", "https://api-developer.jpmorgan.com")
    raise SystemExit("No JPMDQ credentials found. Set JPM_A_CLIENT_ID / JPM_A_CLIENT_SECRET.")


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
    """Return all JPMDQ groups accessible to the credentials as a polars DataFrame.

    Columns: group_id, name, description

    Args:
        filter: optional case-insensitive substring to match against group_id or name.

    Example:
        all_groups = list_groups()
        fi_groups  = list_groups(filter="FI_GO")
    """
    client_id, client_secret, base_url = _credentials()

    async def _inner() -> list[dict]:
        async with DataQuery(client_id=client_id, client_secret=client_secret, base_url=base_url) as dq:
            client = dq._client
            try:
                groups = await client.list_groups_async()
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


def fetch_one(
    group_id: str,
    obs_date: str,
    calendar: str = "CAL_USBANK",
    frequency: str = "FREQ_DAY",
    conversion: str = "CONV_LASTBUS_ABS",
    nan_treatment: str = "NA_NOTHING",
) -> pl.DataFrame:
    """Fetch one group for one obs_date → polars DataFrame with columns instrument/attribute/date/value."""
    client_id, client_secret, base_url = _credentials()

    async def _inner():
        async with DataQuery(client_id=client_id, client_secret=client_secret, base_url=base_url) as dq:
            client = dq._client

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
                    if not next_url.startswith("http"):
                        next_url = f"{base_url.rstrip('/')}/{next_url.lstrip('/')}"
                    try:
                        async with await client._enter_request_cm("GET", next_url) as r:
                            await client._handle_response(r)
                            payload = await r.json()
                        page = AttributesResponse(**payload)
                        attrs.extend(a for a in _parse_attrs(page) if a not in attrs)
                        next_url = page.get_next_link() if hasattr(page, "get_next_link") else None
                    except Exception:
                        break
            except Exception as exc:
                print(f"[warn] attribute discovery failed: {exc}")

            print(f"[fetch_one] {group_id} {obs_date}  attrs={len(attrs)}")
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
                    if not next_url.startswith("http"):
                        next_url = f"{base_url.rstrip('/')}/{next_url.lstrip('/')}"
                    try:
                        async with await client._enter_request_cm("GET", next_url) as r:
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
        print("[fetch_one] no data returned")
        return pl.DataFrame()

    print(f"[fetch_one] {len(rows)} rows")
    return pl.DataFrame(rows)


if __name__ == "__main__":
    # ── 1. All accessible groups ────────────────────────────────────────────
    all_groups = list_groups()
    print(all_groups)

    # ── 2. Filter to economics groups ──────────────────────────────────────
    econ_groups = list_groups(filter="DQ_ECON")
    print(econ_groups)

    # ── 3. Fetch one govie group for one day ───────────────────────────────
    df = fetch_one("FI_GO_BO_AA_AUD_GOV", "20260515")
    print(df)
    print(f"\nshape: {df.shape}  —  {df['instrument'].n_unique()} instruments × {df['attribute'].n_unique()} attributes")
