"""
Japan CPI macro fetcher (DQ_ECON_PRICES).

Different shape from govies / NLP-CB scripts:
  * monthly data, not daily
  * source = Ministry of Internal Affairs and Communications, Japan
  * 16,774 Japan price series in DQ_ECON_PRICES; this script targets the
    Consumer Price Index subset (~3,230 series) and exposes four tiers:

      1. fetch_headline_core()  -> ~15 series  (All items, ex-fresh-food, ex-energy,
                                                Tokyo early indicator, etc.)
      2. fetch_main_categories()-> ~10 series  (the 10 MIAC top-level categories)
      3. fetch_subcategories()  -> ~85 series  (curated MIAC middle/small groups)
      4. fetch_all_items()      -> ~1,636 series (every detailed item, slow)

Public API is 100% synchronous. asyncio is hidden inside asyncio.run().

Usage:
    from jpmdq_macro_japan_cpi import (
        fetch_headline_core, fetch_main_categories,
        fetch_subcategories, fetch_all_items, list_japan_cpi_instruments,
    )

    df = fetch_headline_core()                # latest available month
    df = fetch_main_categories("20260331")    # specific month-end
    df = fetch_subcategories(unit="Percentage")   # YoY % instead of Index
"""
from __future__ import annotations

import asyncio
import os
from datetime import date, timedelta
from pathlib import Path

import polars as pl
from dotenv import load_dotenv

load_dotenv()

from dataquery import DataQuery
from dataquery.types.models import InstrumentsResponse

GROUP_ID = "DQ_ECON_PRICES"
DEFAULT_OUT = Path("/home/workspace/Data/jpm/downloads/jpmdq_macro/japan_cpi")
INST_CACHE = DEFAULT_OUT / "_instruments.parquet"

# ───────────────────────── curated tier definitions ─────────────────────────

# Headline + core (Japan national + Tokyo, NSA + SA, monthly + quarterly).
HEADLINE_CORE = [
    "CPI - All items",
    "CPI - All items, seasonally adjusted",
    "CPI - All items, less fresh food",
    "CPI - All items, less fresh food, seasonally adjusted",
    "CPI - All items, less fresh food and energy",
    "CPI - All items, less fresh food and energy, seasonally adjusted",
    "CPI - All items, less food (less alcoholic beverages) and energy",
    "CPI - All items, less food (less alcoholic beverages) and energy, seasonally adjusted",
    "CPI - All items, less imputed rent",
    "CPI - All items, less imputed rent & fresh food",
    "CPI - Core - all items, less food (less alcoholic beverages) and energy, sa by JPM",
    "CPI Tokyo - All items",
    "CPI Tokyo - All items, less fresh food",
    "CPI Tokyo - All items, less fresh food and energy",
    "CPI Tokyo - All items, seasonally adjusted",
]

# 10 MIAC top-level categories.
MAIN_CATEGORIES = [
    "CPI - Food",
    "CPI - Housing",
    "CPI - Fuel, light & water charges",
    "CPI - Furniture & household utensils",
    "CPI - Clothes & footwear",
    "CPI - Medical care",
    "CPI - Transportation & communication",
    "CPI - Education",
    "CPI - Culture & recreation",
    "CPI - Miscellaneous",
]

# Curated ~85 middle/small groups (every entry has been validated to exist in
# the DQ_ECON_PRICES Japan-national catalog snapshot).
SUBCATEGORIES = [
    # ── Food
    "CPI - Cereals", "CPI - Fresh fish & seafood", "CPI - Fish & seafood",
    "CPI - Salted & dried fish", "CPI - Fish-paste products",
    "CPI - Other processed fish & seafood",
    "CPI - Raw meats", "CPI - Seasoned meat", "CPI - Meats",
    "CPI - Fresh milk & dairy products", "CPI - Dairy products & eggs",
    "CPI - Vegetables & seaweeds", "CPI - Fresh vegetables",
    "CPI - Processed vegetables & seaweeds",
    "CPI - Fruits", "CPI - Fresh fruits", "CPI - Processed fruits",
    "CPI - Oils, fats & seasonings", "CPI - Cakes & candies", "CPI - Cooked food",
    "CPI - Beverages", "CPI - Alcoholic beverages",
    "CPI - Meals outside the home", "CPI - Eating out", "CPI - School lunch",
    # ── Housing
    "CPI - Rent", "CPI - Imputed rent",
    "CPI - Repairs & maintenance",
    "CPI - Service charges for repairs & maintenance",
    "CPI - Tools & materials for repairs & maintenance",
    # ── Fuel, light & water charges
    "CPI - Electricity", "CPI - Gas",
    "CPI - Gas, manufactured & piped", "CPI - Liquefied propane",
    "CPI - Other fuel & light",
    "CPI - Water & sewerage charges", "CPI - Water charges",
    # ── Furniture & household utensils
    "CPI - Durable goods", "CPI - Domestic non-durable goods",
    "CPI - Other domestic non-durable goods",
    "CPI - Interior furnishings", "CPI - Bedding", "CPI - Domestic utensils",
    "CPI - Domestic services", "CPI - Durable goods assisting housework",
    # ── Clothes & footwear
    "CPI - Clothing", "CPI - Children's clothing", "CPI - Women's clothing",
    "CPI - Japanese clothing", "CPI - Shirts, sweaters & underwear",
    "CPI - Footwear", "CPI - Other clothing",
    "CPI - Services related to clothing",
    # ── Medical care
    "CPI - Medicines & health fortification",
    "CPI - Medical supplies & appliances", "CPI - Medical services",
    # ── Transportation & communication
    "CPI - Automobiles", "CPI - Automotive maintenance",
    "CPI - Public transportation", "CPI - Private transportation",
    "CPI - Communication", "CPI - Gasoline",
    # ── Education
    "CPI - School fees", "CPI - School textbooks",
    "CPI - School textbooks & reference books for study",
    "CPI - Tutorial fees", "CPI - After-school childcare fees",
    # ── Culture & recreation
    "CPI - Recreational goods", "CPI - Recreational durable goods",
    "CPI - Books & other reading materials", "CPI - Recreational services",
    # ── Miscellaneous
    "CPI - Personal care services", "CPI - Personal effects",
    "CPI - Tobacco", "CPI - Funeral fees",
    # ── Useful special aggregates / market-watched composites
    "CPI - Energy", "CPI - Goods", "CPI - Services",
    "CPI - Food, less fresh food", "CPI - Goods, less fresh food",
    "CPI - Housing, less imputed rent", "CPI - Services, less imputed rent",
    "CPI - Agricultural, aquatic & livestock products",
    "CPI - Industrial products",
    "CPI - Public services", "CPI - General services",
    "CPI - Semi-durable goods", "CPI - Fresh food",
]


# ─────────────────────────────── plumbing ──────────────────────────────────

def _credentials() -> tuple[str, str, str]:
    for id_key, sec_key in [
        ("DATAQUERY_CLIENT_ID", "DATAQUERY_CLIENT_SECRET"),
        ("JPM_A_CLIENT_ID", "JPM_A_CLIENT_SECRET"),
    ]:
        cid = os.environ.get(id_key)
        csec = os.environ.get(sec_key)
        if cid and csec:
            return cid, csec, os.environ.get(
                "DATAQUERY_BASE_URL", "https://api-developer.jpmorgan.com"
            )
    raise SystemExit("Set JPM_A_CLIENT_ID and JPM_A_CLIENT_SECRET (or DATAQUERY_*)")


def _today_str() -> str:
    return date.today().strftime("%Y%m%d")


def _months_back(end_date: str, months: int) -> str:
    y, m, d = int(end_date[:4]), int(end_date[4:6]), int(end_date[6:8])
    # Subtract `months` months naïvely.
    total = y * 12 + (m - 1) - months
    ny, nm = total // 12, (total % 12) + 1
    return f"{ny:04d}{nm:02d}01"


def _flat_name(name_pipe: str, position: int) -> str:
    """Pull a positional segment out of a pipe-delimited instrument name."""
    parts = [p.strip() for p in name_pipe.split("|")]
    return parts[position] if 0 <= position < len(parts) else ""


# ─────────────────────────── public functions ──────────────────────────────

def list_japan_cpi_instruments(filter: str = "", refresh: bool = False) -> pl.DataFrame:
    """All Japan-CPI instruments visible to the credentials, as a polars DataFrame.

    Returns columns: instrument_id, name, series (CPI - <thing>), dataset, frequency,
    adjustment, unit.

    Cached to disk; use refresh=True to force a re-fetch.
    """
    if refresh or not INST_CACHE.exists():
        client_id, client_secret, base_url = _credentials()

        async def _inner() -> list[dict]:
            async with DataQuery(
                client_id=client_id, client_secret=client_secret, base_url=base_url
            ) as dq:
                client = dq._client
                rows: list[dict] = []

                async def _consume(resp):
                    for inst in resp.instruments or []:
                        rows.append({
                            "instrument_id": str(inst.instrument_id),
                            "name": str(inst.instrument_name),
                        })

                # Search for both Japan-national CPI and Tokyo CPI.
                for keywords in ["Japan CPI", "Japan CPI Tokyo"]:
                    resp = await client.search_instruments_async(GROUP_ID, keywords)
                    await _consume(resp)
                    visited: set[str] = set()
                    next_url = (
                        resp.get_next_link() if hasattr(resp, "get_next_link") else None
                    )
                    while next_url and len(visited) < 500:
                        if next_url in visited:
                            break
                        visited.add(next_url)
                        absolute = (
                            next_url
                            if next_url.startswith("http")
                            else client._build_api_url(next_url.lstrip("/"))
                        )
                        async with await client._enter_request_cm("GET", absolute) as r:
                            await client._handle_response(r)
                            payload = await r.json()
                        page = InstrumentsResponse(**payload)
                        await _consume(page)
                        next_url = (
                            page.get_next_link()
                            if hasattr(page, "get_next_link")
                            else None
                        )
                return rows

        rows = asyncio.run(_inner())
        df = pl.DataFrame(rows).unique(subset=["instrument_id"])
        # Decompose pipe-delimited name.
        df = df.with_columns([
            pl.col("name").map_elements(lambda n: _flat_name(n, 2), return_dtype=pl.String).alias("series"),
            pl.col("name").map_elements(lambda n: _flat_name(n, 4), return_dtype=pl.String).alias("dataset"),
            pl.col("name").map_elements(lambda n: _flat_name(n, 5), return_dtype=pl.String).alias("frequency"),
            pl.col("name").map_elements(lambda n: _flat_name(n, 6), return_dtype=pl.String).alias("adjustment"),
            pl.col("name").map_elements(lambda n: _flat_name(n, 7), return_dtype=pl.String).alias("unit"),
        ])
        INST_CACHE.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(INST_CACHE)
    else:
        df = pl.read_parquet(INST_CACHE)

    if filter:
        df = df.filter(pl.col("name").str.contains(f"(?i){filter}"))
    print(f"[list_japan_cpi_instruments] {len(df)} instruments" + (f" matching '{filter}'" if filter else ""))
    return df


def _fetch_series(
    series_names: list[str],
    end_date: str | None = None,
    months_back: int = 6,
    unit: str = "Index",
    dataset_contains: str | None = None,
    chunk_size: int = 20,
    save_label: str | None = None,
) -> pl.DataFrame:
    """Fetch a set of Japan-CPI series for a recent monthly window.

    Args:
        series_names: list like ['CPI - Food', 'CPI - Energy', ...].
        end_date:     YYYYMMDD; defaults to today.
        months_back:  window size — we need >1 month to actually get a print.
        unit:         'Index' or 'Percentage' (YoY %).
        dataset_contains: substring filter on dataset, e.g. 'Tokyo' or 'Japan'.
            Default None keeps Japan-national + Tokyo if both match.
        chunk_size:   instruments per API call (JPMDQ rejects very large lists).
        save_label:   if set, parquet is written to
                      Data/jpm/downloads/jpmdq_macro/japan_cpi/<label>__<end>.parquet
    """
    end_date = end_date or _today_str()
    start_date = _months_back(end_date, months_back)

    inst = list_japan_cpi_instruments()

    # Build per-series patterns that match only the third pipe segment.
    series_patterns = [f"| {s} |" for s in series_names]

    selected = inst.filter(
        pl.col("name").str.contains_any(series_patterns)
        & pl.col("unit").eq(unit)
    )
    if dataset_contains:
        selected = selected.filter(pl.col("dataset").str.contains(dataset_contains))

    if selected.height == 0:
        print(f"[fetch] 0 instruments matched {len(series_names)} requested series — empty df")
        return pl.DataFrame()

    matched_series = set(selected["series"].to_list())
    missing = [s for s in series_names if s not in matched_series]
    if missing:
        print(f"[fetch] {len(missing)} requested series not in catalog: {missing[:5]}{'…' if len(missing) > 5 else ''}")

    ids = selected["instrument_id"].to_list()
    name_by_id = dict(zip(selected["instrument_id"], selected["name"]))
    series_by_id = dict(zip(selected["instrument_id"], selected["series"]))
    dataset_by_id = dict(zip(selected["instrument_id"], selected["dataset"]))

    print(f"[fetch] {len(ids)} instruments × {months_back} months window (unit={unit}, end={end_date})")

    client_id, client_secret, base_url = _credentials()

    async def _inner() -> list[dict]:
        async with DataQuery(
            client_id=client_id, client_secret=client_secret, base_url=base_url
        ) as dq:
            client = dq._client
            out: list[dict] = []
            for i in range(0, len(ids), chunk_size):
                chunk = ids[i : i + chunk_size]
                try:
                    resp = await client.get_instrument_time_series_async(
                        instruments=chunk,
                        attributes=["NO_ATTRIBUTE"],
                        start_date=start_date,
                        end_date=end_date,
                        calendar="CAL_USBANK",
                        frequency="FREQ_MONTH",
                        conversion="CONV_LASTBUS_ABS",
                        nan_treatment="NA_NOTHING",
                    )
                except Exception as exc:
                    print(f"  [warn] chunk {i // chunk_size + 1}: {exc}")
                    continue

                for inst_obj in resp.instruments or []:
                    iid = str(inst_obj.instrument_id)
                    for attr in inst_obj.attributes or []:
                        ts = getattr(attr, "time_series", None) or []
                        for pt in ts:
                            if isinstance(pt, (list, tuple)) and len(pt) >= 2:
                                d_, v_ = pt[0], pt[1]
                            elif isinstance(pt, dict):
                                d_, v_ = pt.get("date") or pt.get("time"), pt.get("value")
                            else:
                                continue
                            if v_ is None:
                                continue
                            out.append({
                                "date": str(d_)[:10],
                                "instrument_id": iid,
                                "series": series_by_id.get(iid, ""),
                                "dataset": dataset_by_id.get(iid, ""),
                                "unit": unit,
                                "value": float(v_),
                                "instrument_name": name_by_id.get(iid, ""),
                            })
            return out

    rows = asyncio.run(_inner())
    if not rows:
        print("[fetch] no observations returned")
        return pl.DataFrame()

    df = pl.DataFrame(rows).sort(["series", "dataset", "date"])
    print(f"[fetch] {df.height} rows ({df['series'].n_unique()} unique series)")

    if save_label:
        DEFAULT_OUT.mkdir(parents=True, exist_ok=True)
        out_path = DEFAULT_OUT / f"{save_label}__{end_date}.parquet"
        df.write_parquet(out_path)
        print(f"[fetch] wrote {out_path}")

    return df


def fetch_headline_core(end_date: str | None = None, unit: str = "Index", save: bool = True) -> pl.DataFrame:
    """Tier 1 (~15 series): headline + core variants (Japan national + Tokyo)."""
    return _fetch_series(
        HEADLINE_CORE,
        end_date=end_date,
        unit=unit,
        save_label="headline_core" if save else None,
    )


def fetch_main_categories(end_date: str | None = None, unit: str = "Index", save: bool = True) -> pl.DataFrame:
    """Tier 2 (~10 series): 10 MIAC top-level CPI categories (Japan national)."""
    return _fetch_series(
        MAIN_CATEGORIES,
        end_date=end_date,
        unit=unit,
        dataset_contains="Japan",
        save_label="main_categories" if save else None,
    )


def fetch_subcategories(end_date: str | None = None, unit: str = "Index", save: bool = True) -> pl.DataFrame:
    """Tier 3 (~85 series): curated MIAC middle/small groups (Japan national)."""
    return _fetch_series(
        SUBCATEGORIES,
        end_date=end_date,
        unit=unit,
        dataset_contains="Japan",
        save_label="subcategories" if save else None,
    )


def fetch_all_items(end_date: str | None = None, unit: str = "Index", save: bool = True) -> pl.DataFrame:
    """Tier 4 (~1,636 series): every detailed Japan-national CPI item. Slow (~100 API calls)."""
    inst = list_japan_cpi_instruments()
    all_japan_cpi = (
        inst.filter(pl.col("dataset").str.contains("Consumer Price Index - Japan"))
        .filter(pl.col("unit").eq(unit))
        .filter(pl.col("series").str.starts_with("CPI - "))
    )
    series_names = all_japan_cpi["series"].unique().to_list()
    print(f"[fetch_all_items] expanding to {len(series_names)} unique series")
    return _fetch_series(
        series_names,
        end_date=end_date,
        unit=unit,
        dataset_contains="Japan",
        save_label="all_items" if save else None,
    )


# ───────────────────────────── working example ─────────────────────────────

if __name__ == "__main__":
    print("\n=== TIER 1: headline + core (~15 series) ===")
    df1 = fetch_headline_core()
    print(df1.head(10))
    print(f"shape: {df1.shape}")

    print("\n=== TIER 2: 10 main MIAC categories ===")
    df2 = fetch_main_categories()
    print(df2.head(10))
    print(f"shape: {df2.shape}")

    print("\n=== TIER 3: ~85 curated subcategories ===")
    df3 = fetch_subcategories()
    print(df3.head(10))
    print(f"shape: {df3.shape}")
    if df3.height:
        print(f"unique series matched: {df3['series'].n_unique()}")

    print("\nDone. Parquet files in", DEFAULT_OUT)
