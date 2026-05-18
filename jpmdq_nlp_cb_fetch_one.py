"""
Debug helpers: list available groups, fetch one group/day → polars DataFrame.

Usage:
    from jpmdq_nlp_cb_fetch_one import list_groups, fetch_one

    groups = list_groups()          # polars DataFrame: group_id, name, description
    df     = fetch_one("NLP_CB_STATEMENTS", "20240101")
"""
import os
import polars as pl
from dotenv import load_dotenv
load_dotenv()

from dataquery import DataQuery


def resolve_jpmdq_credentials():
    """Check for credentials in order: DATAQUERY_CLIENT_ID/SECRET, then jpm_a_client_id/secret."""
    if "DATAQUERY_CLIENT_ID" in os.environ and "DATAQUERY_CLIENT_SECRET" in os.environ:
        return os.environ["DATAQUERY_CLIENT_ID"], os.environ["DATAQUERY_CLIENT_SECRET"]
    if "jpm_a_client_id" in os.environ and "jpm_a_client_secret" in os.environ:
        return os.environ["jpm_a_client_id"], os.environ["jpm_a_client_secret"]
    raise SystemExit("No JPMDQ credentials found: DATAQUERY_CLIENT_ID/SECRET or jpm_a_client_id/secret")


def list_groups(filter: str = "") -> pl.DataFrame:
    """
    Return all JPMDQ groups your credentials can see as a polars DataFrame.
    Optionally filter by substring (case-insensitive) on group_id or name.

    Columns: group_id, name, description
    """
    client_id, client_secret = resolve_jpmdq_credentials()
    base_url      = os.environ.get("DATAQUERY_BASE_URL", "https://api-developer.jpmorgan.com")

    rows  = []
    seen  = set()

    with DataQuery(client_id=client_id, client_secret=client_secret, base_url=base_url) as dq:
        client = dq._client

        try:
            resp = client.get_groups()
        except Exception as exc:
            print(f"[error] get_groups failed: {exc}")
            return pl.DataFrame()

        def _harvest(resp_obj):
            groups = (
                getattr(resp_obj, "groups", None)
                or getattr(resp_obj, "data",   None)
                or (resp_obj if isinstance(resp_obj, list) else [])
            )
            for g in groups or []:
                gid  = str(getattr(g, "group_id",    None) or getattr(g, "id",   "") or "")
                name = str(getattr(g, "name",        None) or getattr(g, "label","") or "")
                desc = str(getattr(g, "description", None) or "")
                if gid and gid not in seen:
                    seen.add(gid)
                    rows.append({"group_id": gid, "name": name, "description": desc})

        _harvest(resp)

        # follow pagination
        import requests
        next_url = resp.get_next_link() if hasattr(resp, "get_next_link") else None
        visited, count = set(), 0
        while next_url and count < 200 and next_url not in visited:
            visited.add(next_url)
            count += 1
            token = None
            for attr in ("access_token", "_access_token", "token", "_token"):
                token = getattr(client, attr, None)
                if token:
                    break
            if not next_url.startswith("http"):
                next_url = f"{base_url.rstrip('/')}/{next_url.lstrip('/')}"
            try:
                r = requests.get(next_url, headers={"Authorization": f"Bearer {token}"}, timeout=120)
                r.raise_for_status()
                payload = r.json()
                _harvest(payload)
                links    = payload.get("_links", {}) if isinstance(payload, dict) else {}
                next_url = (links.get("next") or {}).get("href")
            except Exception:
                break

    df = pl.DataFrame(rows) if rows else pl.DataFrame({"group_id": [], "name": [], "description": []})

    if filter:
        f = filter.lower()
        df = df.filter(
            pl.col("group_id").str.to_lowercase().str.contains(f)
            | pl.col("name").str.to_lowercase().str.contains(f)
        )

    print(f"[list_groups] {len(df)} groups found" + (f" matching '{filter}'" if filter else ""))
    return df


def fetch_one(group_id: str, obs_date: str) -> pl.DataFrame:
    client_id, client_secret = resolve_jpmdq_credentials()
    base_url      = os.environ.get("DATAQUERY_BASE_URL", "https://api-developer.jpmorgan.com")

    with DataQuery(client_id=client_id, client_secret=client_secret, base_url=base_url) as dq:
        client = dq._client

        # Discover attributes
        try:
            attr_resp = client.get_group_attributes(group_id=group_id)
            attrs = [
                str(getattr(a, "attribute_id", None) or getattr(a, "attribute", None))
                for inst in (getattr(attr_resp, "instruments", []) or [])
                for a in (getattr(inst, "attributes", []) or [])
            ]
            attrs = [a for a in attrs if a and "," not in a]
        except Exception as exc:
            print(f"[warn] attribute discovery failed: {exc}")
            attrs = []

        print(f"[fetch_one] {group_id} {obs_date}  attrs={len(attrs)}")

        # Fetch time series
        try:
            resp = client.get_group_time_series(
                group_id=group_id,
                attributes=attrs,
                start_date=obs_date,
                end_date=obs_date,
                calendar="CAL_USBANK",
                frequency="FREQ_DAY",
                conversion="CONV_LASTBUS_ABS",
                nan_treatment="NA_NOTHING",
            )
        except Exception as exc:
            print(f"[error] {exc}")
            return pl.DataFrame()

    # Parse response into rows
    rows = []
    for inst in (getattr(resp, "instruments", []) or []):
        instrument = str(
            getattr(inst, "instrument_name", None)
            or getattr(inst, "instrument_id", None)
            or "UNKNOWN"
        )
        for attr in (getattr(inst, "attributes", []) or []):
            attribute = str(
                getattr(attr, "attribute_id", None)
                or getattr(attr, "attribute", None)
                or "UNKNOWN"
            )
            for pt in (getattr(attr, "time_series", None) or getattr(attr, "series", None) or []):
                if isinstance(pt, dict):
                    date_val = pt.get("date") or pt.get("obs_date") or pt.get("time")
                    value    = pt.get("value")
                elif isinstance(pt, (list, tuple)) and len(pt) >= 2:
                    date_val, value = pt[0], pt[1]
                else:
                    continue
                rows.append({"instrument": instrument, "attribute": attribute,
                              "date": str(date_val)[:10], "value": str(value) if value is not None else None})

    if not rows:
        print("[fetch_one] no data returned")
        return pl.DataFrame()

    print(f"[fetch_one] {len(rows)} rows")
    return pl.DataFrame(rows)


if __name__ == "__main__":
    # ── 1. List all groups your credentials can see ─────────────────────────
    all_groups = list_groups()
    print(all_groups)

    # ── 2. Filter to NLP-related groups only ────────────────────────────────
    nlp_groups = list_groups(filter="nlp")
    print(nlp_groups)

    # ── 3. Fetch one group for one day → inspect the DataFrame ──────────────
    # Replace with the exact group_id you found in step 2
    df = fetch_one("NLP_CB_STATEMENTS", "20240101")
    print(df)
    print(df.schema)
