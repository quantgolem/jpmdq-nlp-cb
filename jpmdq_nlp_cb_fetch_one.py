"""
Quick debug fetch: one group, one day → polars DataFrame.

Usage:
    from jpmdq_nlp_cb_fetch_one import fetch_one
    df = fetch_one("NLP_CB_STATEMENTS", "20240101")
    print(df)
"""
import os
import polars as pl
from dotenv import load_dotenv
load_dotenv()

from dataquery import DataQuery


def fetch_one(group_id: str, obs_date: str) -> pl.DataFrame:
    client_id     = os.environ["DATAQUERY_CLIENT_ID"]
    client_secret = os.environ["DATAQUERY_CLIENT_SECRET"]
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
