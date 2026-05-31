"""Step 4 — Geocode POIs: BrasilAPI CEP lookup + TomTom structured fallback.

Strategy
--------
Phase A — CEP batch geocoding (BrasilAPI)
  • Extract all unique CEPs from step_02 output.
  • Query https://brasilapi.com.br/api/cep/v2/{cep} asynchronously.
  • Results cached to data/cache/cep_brasilapi.parquet (persistent across runs).
  • Join lat/lon back to POI table → geo_precision = "cep".

Phase B — City-centre detection (geobr)
  • Load Brazilian municipality centroids via geobr (cached after first download).
  • For every geocoded POI, compute Haversine distance to its municipality centroid.
  • If distance ≤ city_center_threshold_m → flag geo_precision = "municipio_centroid".
    (BrasilAPI sometimes returns the municipality centroid when it has no finer data.)
  • POIs where BrasilAPI returned no coordinates → geo_precision = "none".

Phase C — TomTom structured re-geocoding
  • Re-geocode records flagged "municipio_centroid" or "none" using the TomTom
    Structured Geocode API (requires TOMTOM_API_KEY in .env).
  • Results cached to data/cache/tomtom_{dump_date}.parquet.
  • geo_precision updated to: tomtom_point_address, tomtom_address_range,
    tomtom_street, tomtom_cross_street, tomtom_geography.
  • If TOMTOM_API_KEY is absent the phase is skipped with a warning.

Reads
-----
  data/intermediate/step_02_poi_joined_<dump_date>.parquet

Writes
------
  data/intermediate/step_04_geocoded_<dump_date>.parquet          (full run)
  data/intermediate/step_04_geocoded_<dump_date>__<slug>.parquet  (filtered run)
    All step_02 columns + lat (DOUBLE) + lon (DOUBLE) + geo_precision (VARCHAR)

Filtering
---------
  Pass geocode_categories=["amenity=hospital"] to restrict geocoding to a
  subset of POIs for testing.  The output is written to a separate file so
  it never overwrites the full-run result.
"""

import asyncio
import logging
import math
import os
import re
import signal
import time
from pathlib import Path

import aiohttp
import duckdb
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm as atqdm

from app.config_loader import get_config

log = logging.getLogger(__name__)

GEOCODED_OUT      = "step_04_geocoded_{date}.parquet"
GEOCODED_OUT_FILT = "step_04_geocoded_{date}__{slug}.parquet"
CEP_CACHE_FILE = "cep_brasilapi.parquet"
TT_CACHE_FILE  = "tomtom_{date}.parquet"
MUNI_CACHE_FILE = "municipio_centroids.parquet"

BRASILAPI_URL = "https://brasilapi.com.br/api/cep/v2/{cep}"
TOMTOM_URL    = "https://api.tomtom.com/search/2/structuredGeocode.json"

# TomTom result type → internal precision label
_TT_PRECISION: dict[str, str] = {
    "Point Address":   "tomtom_point_address",
    "Address Range":   "tomtom_address_range",
    "Street":          "tomtom_street",
    "Cross Street":    "tomtom_cross_street",
    "Geography":       "tomtom_geography",
}


# ── Geometry helpers ─────────────────────────────────────────────────────────

def _haversine_m(lat1: np.ndarray, lon1: np.ndarray,
                 lat2: np.ndarray, lon2: np.ndarray) -> np.ndarray:
    """Vectorised Haversine distance in metres (WGS-84)."""
    R = 6_371_000.0
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlam = np.radians(lon2 - lon1)
    a = np.sin(dphi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlam / 2) ** 2
    return 2.0 * R * np.arcsin(np.sqrt(np.clip(a, 0, 1)))


# ── Phase B: municipality centroids (geobr) ──────────────────────────────────

def _load_municipio_centroids(cache_dir: Path) -> pd.DataFrame:
    """Return DataFrame {municipio (str, 7-digit), lat_centro, lon_centro}.

    Uses the Python geobr package to download Brazilian municipality polygons
    on the first call; subsequent calls read from the local cache.
    """
    cache_path = cache_dir / MUNI_CACHE_FILE
    if cache_path.exists():
        log.info("    Municipality centroids: loading from cache…")
        return _parquet_read(cache_path)

    log.info("    Municipality centroids: downloading via geobr (one-time)…")
    import geobr  # heavy import — only on first run
    gdf = geobr.read_municipality(code_muni="all", year=2020)
    gdf = gdf.to_crs("EPSG:4326")
    centroids = gdf.geometry.centroid
    df = pd.DataFrame({
        "municipio":  gdf["code_muni"].astype(str).str.zfill(7),
        "lat_centro": centroids.y.values,
        "lon_centro": centroids.x.values,
    })
    _parquet_write(df, cache_path)
    log.info("    Cached %d municipality centroids → %s", len(df), MUNI_CACHE_FILE)
    return df


# ── Phase A: BrasilAPI async CEP lookup ─────────────────────────────────────

async def _fetch_cep(
    session: aiohttp.ClientSession,
    cep: str,
    sem: asyncio.Semaphore | None = None,
    retries: int = 3,
) -> dict:
    """Fetch a single CEP from BrasilAPI.

    Returns dict with keys: cep, lat, lon, transient_error.
    transient_error=True means the result must NOT be cached (network/quota
    failure) so the CEP is retried on the next run.
    """
    url = BRASILAPI_URL.format(cep=cep)
    try:
        for attempt in range(retries):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            data = await resp.json(content_type=None)
                            coords = (data.get("location") or {}).get("coordinates") or {}
                            try:
                                lat = float(coords["latitude"])  if coords.get("latitude")  else None
                                lon = float(coords["longitude"]) if coords.get("longitude") else None
                            except (TypeError, ValueError):
                                lat, lon = None, None
                            # Valid response (even if no coords) → cache it
                            return {"cep": cep, "lat": lat, "lon": lon, "transient_error": False}
                        elif resp.status == 404:
                            # CEP does not exist → cache as no-coords
                            return {"cep": cep, "lat": None, "lon": None, "transient_error": False}
                        elif resp.status == 429:
                            await asyncio.sleep(2.0 ** attempt)
                        else:
                            await asyncio.sleep(1.0)
            except asyncio.TimeoutError:
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                raise   # propagate immediately — don't retry, don't cache
            except Exception as exc:
                log.debug("BrasilAPI error for CEP %s (attempt %d): %s", cep, attempt, exc)
                await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        # Task was cancelled (e.g. Ctrl+C) — return as transient so it's not cached
        return {"cep": cep, "lat": None, "lon": None, "transient_error": True}
    # All retries exhausted due to network/server error → do NOT cache
    log.debug("BrasilAPI: transient error for CEP %s — will retry next run", cep)
    return {"cep": cep, "lat": None, "lon": None, "transient_error": True}


def _run_async(coro) -> list:
    """Run an async coroutine with a graceful SIGINT handler.

    Python 3.12's asyncio.Runner._on_sigint raises KeyboardInterrupt directly
    into the event loop, bypassing all finally blocks.  We replace the SIGINT
    handler with one that cancels the main task cleanly, giving coroutines a
    chance to flush caches before exiting.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    main_task: asyncio.Task | None = None

    def _sigint_handler(sig, frame):
        log.warning("    Interrupted — flushing cache and exiting…")
        if main_task and not main_task.done():
            loop.call_soon_threadsafe(main_task.cancel)

    old_handler = signal.signal(signal.SIGINT, _sigint_handler)
    try:
        main_task = loop.create_task(coro)
        return loop.run_until_complete(main_task)
    except asyncio.CancelledError:
        return []
    finally:
        signal.signal(signal.SIGINT, old_handler)
        # Cancel and await any remaining tasks so the loop closes cleanly
        pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
        for t in pending:
            t.cancel()
        if pending:
            loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
        loop.close()


def _parquet_write(df: pd.DataFrame, path: Path) -> None:
    """Write DataFrame to Parquet using DuckDB (no pyarrow dependency)."""
    con = duckdb.connect()
    con.register("_df", df)
    con.execute(f"COPY _df TO '{path}' (FORMAT PARQUET, COMPRESSION 'zstd')")
    con.close()


def _parquet_read(path: Path) -> pd.DataFrame:
    """Read Parquet using DuckDB (no pyarrow dependency)."""
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{path}')").df()
    con.close()
    return df


def _write_partial_output(
    cache_df: pd.DataFrame,
    joined_path: Path,
    geocoded_out: Path,
    compression: str,
    row_group: int,
) -> None:
    """Join current CEP cache with full POI table and write partial geocoded output.

    Records not yet in cache get lat=NULL, lon=NULL, geo_precision='pending'.
    This makes the output file usable at any point during the fetch phase.
    """
    con = duckdb.connect()
    con.register("cep_cache", cache_df)
    con.execute(f"""
        COPY (
            SELECT
                p.*,
                g.lat,
                g.lon,
                CASE
                    WHEN g.lat IS NOT NULL THEN 'cep'
                    WHEN g.cep IS NOT NULL THEN 'none'
                    ELSE 'pending'
                END AS geo_precision
            FROM read_parquet('{joined_path}') p
            LEFT JOIN (
                SELECT
                    lpad(regexp_replace(cep, '[^0-9]', '', 'g'), 8, '0') AS cep_clean,
                    lat, lon, cep
                FROM cep_cache
            ) g ON lpad(regexp_replace(p.cep, '[^0-9]', '', 'g'), 8, '0') = g.cep_clean
        ) TO '{geocoded_out}'
        (FORMAT PARQUET, COMPRESSION '{compression}', ROW_GROUP_SIZE {row_group})
    """)
    con.close()


async def _fetch_all_ceps(
    ceps: list[str],
    workers: int,
    cache_path: Path,
    joined_path: Path,
    geocoded_out: Path,
    compression: str,
    row_group: int,
    cache_checkpoint: int = 100,
    output_checkpoint: int = 5_000,
) -> list[dict]:
    """Fetch CEPs with a fixed worker-pool (not one task per CEP).

    Using asyncio.Queue + N worker coroutines means only N tasks exist at
    any time.  Ctrl+C cancels N tasks (fast) instead of 400K+ tasks (slow).

    Checkpoints:
      - CEP cache saved every `cache_checkpoint` new results.
      - Final geocoded parquet rebuilt every `output_checkpoint` new results.
    """
    # Resume from existing cache
    if cache_path.exists():
        existing     = _parquet_read(cache_path)
        already_done = {r["cep"] for r in existing.to_dict("records")}
        results: list[dict] = existing.to_dict("records")
    else:
        already_done: set = set()
        results = []

    remaining = [c for c in ceps if c not in already_done]
    if not remaining:
        return results

    queue: asyncio.Queue = asyncio.Queue()
    for cep in remaining:
        queue.put_nowait(cep)

    since_cache_save   = 0
    since_output_write = 0
    pbar = atqdm(total=len(remaining), desc="  BrasilAPI CEP",
                 initial=len(already_done))

    connector = aiohttp.TCPConnector(limit=workers, ssl=False)

    async def _worker(session: aiohttp.ClientSession) -> None:
        nonlocal since_cache_save, since_output_write
        while True:
            try:
                cep = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                row = await _fetch_cep(session, cep)
            except asyncio.CancelledError:
                queue.put_nowait(cep)   # put back so it's retried next run
                raise
            finally:
                pbar.update(1)

            if not row.get("transient_error"):
                results.append({"cep": row["cep"], "lat": row["lat"], "lon": row["lon"]})
                since_cache_save   += 1
                since_output_write += 1

            # ── Cache checkpoint ──────────────────────────────────────────
            if since_cache_save >= cache_checkpoint:
                df = pd.DataFrame(results, columns=["cep", "lat", "lon"])
                _parquet_write(df, cache_path)
                since_cache_save = 0

            # ── Output checkpoint ─────────────────────────────────────────
            if since_output_write >= output_checkpoint:
                df = pd.DataFrame(results, columns=["cep", "lat", "lon"])
                if since_cache_save > 0:
                    _parquet_write(df, cache_path)
                    since_cache_save = 0
                log.info("    Checkpoint: rebuilding output (%d/%d CEPs)…",
                         len(results), len(ceps))
                _write_partial_output(df, joined_path, geocoded_out,
                                      compression, row_group)
                since_output_write = 0

    worker_tasks: list[asyncio.Task] = []
    try:
        async with aiohttp.ClientSession(connector=connector) as session:
            worker_tasks = [
                asyncio.create_task(_worker(session)) for _ in range(workers)
            ]
            await asyncio.gather(*worker_tasks, return_exceptions=True)

    except (asyncio.CancelledError, KeyboardInterrupt):
        for t in worker_tasks:
            t.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)

    finally:
        pbar.close()
        # Always flush on any exit path (clean, Ctrl+C, exception)
        if results:
            df = pd.DataFrame(results, columns=["cep", "lat", "lon"])
            _parquet_write(df, cache_path)
            log.info("    Cache saved: %d entries", len(df))

    return results


def _geocode_ceps(
    unique_ceps: list[str],
    cache_dir: Path,
    workers: int,
    joined_path: Path,
    geocoded_out: Path,
    compression: str,
    row_group: int,
    cache_checkpoint: int = 100,
    output_checkpoint: int = 5_000,
) -> pd.DataFrame:
    """Return DataFrame {cep, lat, lon} for all unique_ceps. Uses/updates local cache."""
    cache_path = cache_dir / CEP_CACHE_FILE

    if cache_path.exists():
        cached = _parquet_read(cache_path)
        known  = set(cached["cep"].tolist())
    else:
        cached = pd.DataFrame(columns=["cep", "lat", "lon"])
        known  = set()

    to_fetch = [c for c in unique_ceps if c not in known]
    if to_fetch:
        log.info(
            "    Fetching %d new CEPs (workers=%d, cache every %d, output every %d)…",
            len(to_fetch), workers, cache_checkpoint, output_checkpoint,
        )
        all_rows = _run_async(_fetch_all_ceps(
            to_fetch, workers, cache_path,
            joined_path, geocoded_out, compression, row_group,
            cache_checkpoint, output_checkpoint,
        ))
        combined = pd.DataFrame(all_rows, columns=["cep", "lat", "lon"])
        log.info("    CEP cache: %d total entries", len(combined))
    else:
        combined = cached
        log.info("    All %d CEPs already in cache", len(unique_ceps))

    return combined[combined["cep"].isin(set(unique_ceps))].copy()


# ── Phase C: TomTom structured geocoding ─────────────────────────────────────

_TT_TRANSIENT = {"failed", "error"}   # geo_precision values that must NOT be cached

async def _fetch_tomtom(
    session: aiohttp.ClientSession,
    row: dict,
    api_key: str,
    sem: asyncio.Semaphore,
    retries: int = 3,
) -> dict:
    """Fetch structured geocode from TomTom.

    geo_precision values:
      tomtom_*           → valid result, cache it
      none               → API returned 200 but no results, cache it (won't improve)
      failed / error     → transient failure, do NOT cache (retry next run)
    """
    cnpj = row["cnpj"]
    params: dict = {
        "key":         api_key,
        "countryCode": "BRA",
        "language":    "pt-BR",
        "limit":       1,
    }
    # Build structured address fields from what we have
    logradouro = " ".join(filter(None, [
        str(row.get("tipo_logradouro") or "").strip(),
        str(row.get("logradouro") or "").strip(),
    ])).strip()
    if logradouro:
        params["streetName"] = logradouro
    if row.get("numero"):
        params["streetNumber"] = str(row["numero"]).strip()
    if row.get("cep_clean"):
        params["postalCode"] = row["cep_clean"]
    if row.get("municipio_descricao"):
        params["municipality"] = str(row["municipio_descricao"]).strip()
    if row.get("uf"):
        params["countrySubdivision"] = str(row["uf"]).strip()

    for attempt in range(retries):
        try:
            async with sem:
                async with session.get(
                    TOMTOM_URL, params=params,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        results = data.get("results") or []
                        if results:
                            best = results[0]
                            pos  = best.get("position", {})
                            mtype = best.get("type", "")
                            prec = _TT_PRECISION.get(mtype, "tomtom_unknown")
                            return {
                                "cnpj": cnpj,
                                "lat":  pos.get("lat"),
                                "lon":  pos.get("lon"),
                                "geo_precision": prec,
                            }
                        return {"cnpj": cnpj, "lat": None, "lon": None, "geo_precision": "none"}
                    elif resp.status == 403:
                        log.error("TomTom: HTTP 403 — check TOMTOM_API_KEY and quota")
                        return {"cnpj": cnpj, "lat": None, "lon": None, "geo_precision": "none"}
                    elif resp.status == 429:
                        await asyncio.sleep(2.0 ** attempt)
                    else:
                        await asyncio.sleep(1.0)
        except asyncio.TimeoutError:
            await asyncio.sleep(1.0)
        except Exception as exc:
            log.debug("TomTom error for CNPJ %s (attempt %d): %s", cnpj, attempt, exc)
            await asyncio.sleep(1.0)
    return {"cnpj": cnpj, "lat": None, "lon": None, "geo_precision": "failed"}


async def _fetch_all_tomtom(rows: list[dict], api_key: str, workers: int) -> list[dict]:
    sem = asyncio.Semaphore(workers)
    connector = aiohttp.TCPConnector(limit=workers, ssl=False)
    results = []
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [_fetch_tomtom(session, r, api_key, sem) for r in rows]
        for coro in atqdm(asyncio.as_completed(tasks), total=len(tasks), desc="  TomTom geocode"):
            results.append(await coro)
    return results


def _geocode_tomtom(
    df_fallback: pd.DataFrame,
    api_key: str,
    cache_dir: Path,
    dump_date: str,
    workers: int,
) -> pd.DataFrame:
    """Return DataFrame {cnpj, lat, lon, geo_precision} for fallback records."""
    cache_path = cache_dir / TT_CACHE_FILE.format(date=dump_date)
    if cache_path.exists():
        cached     = _parquet_read(cache_path)
        known_cnpj = set(cached["cnpj"].tolist())
    else:
        cached     = pd.DataFrame(columns=["cnpj", "lat", "lon", "geo_precision"])
        known_cnpj = set()

    to_fetch = df_fallback[~df_fallback["cnpj"].isin(known_cnpj)]
    if len(to_fetch) == 0:
        log.info("    TomTom: all %d fallback records already cached", len(df_fallback))
        return cached[cached["cnpj"].isin(set(df_fallback["cnpj"].tolist()))].copy()

    needed_cols = ["cnpj", "tipo_logradouro", "logradouro", "numero",
                   "municipio_descricao", "uf", "cep_clean"]
    rows = to_fetch[[c for c in needed_cols if c in to_fetch.columns]].to_dict("records")
    log.info("    TomTom: fetching %d records (workers=%d)…", len(rows), workers)

    new_results = _run_async(_fetch_all_tomtom(rows, api_key, workers))
    # Exclude transient errors from cache so they are retried next run
    cacheable   = [r for r in new_results if r.get("geo_precision") not in _TT_TRANSIENT]
    transient_n = len(new_results) - len(cacheable)
    if transient_n:
        log.warning("    TomTom: %d transient errors excluded from cache — will retry next run",
                    transient_n)
    new_df   = pd.DataFrame(cacheable) if cacheable else pd.DataFrame(
        columns=["cnpj", "lat", "lon", "geo_precision"])
    combined = pd.concat([cached, new_df], ignore_index=True)
    _parquet_write(combined, cache_path)
    log.info("    TomTom cache: %d total entries", len(combined))
    return combined[combined["cnpj"].isin(set(df_fallback["cnpj"].tolist()))].copy()


# ── Step entry point ─────────────────────────────────────────────────────────

def _category_slug(categories: list[str]) -> str:
    """Turn ['amenity=hospital', 'amenity=clinic'] into 'amenity=hospital_amenity=clinic'."""
    return "_".join(re.sub(r"[^\w=]", "-", c) for c in sorted(categories))


def output_exists(intermediate_dir: Path, dump_date: str,
                  geocode_categories: list[str] | None = None) -> bool:
    fname = (
        GEOCODED_OUT_FILT.format(date=dump_date, slug=_category_slug(geocode_categories))
        if geocode_categories
        else GEOCODED_OUT.format(date=dump_date)
    )
    return (intermediate_dir / fname).exists()


def run(
    intermediate_dir: Path,
    cache_dir: Path,
    dump_date: str,
    force: bool = False,
    geocode_categories: list[str] | None = None,
) -> Path:
    """Geocode POIs from step_02 → enriched Parquet with lat/lon.  Returns output path."""
    if geocode_categories:
        slug = _category_slug(geocode_categories)
        geocoded_out = intermediate_dir / GEOCODED_OUT_FILT.format(date=dump_date, slug=slug)
        log.info("  Filtered geocoding — categories: %s", geocode_categories)
    else:
        geocoded_out = intermediate_dir / GEOCODED_OUT.format(date=dump_date)

    if not force and geocoded_out.exists():
        log.info("Step 4 already done — skipping (use --force to reprocess)")
        log.info("  %s", geocoded_out.name)
        return geocoded_out

    # Load secrets from .env (does nothing if already in environment)
    load_dotenv()
    tomtom_key = os.getenv("TOMTOM_API_KEY", "").strip()

    cfg       = get_config()
    geo_cfg   = cfg.get("geocoding", {})
    cep_workers       = int(geo_cfg.get("cep_workers", 20))
    tt_workers        = int(geo_cfg.get("tomtom_workers", 10))
    city_thresh       = float(geo_cfg.get("city_center_threshold_m", 500))
    tt_enabled        = bool(geo_cfg.get("tomtom_enabled", True))
    cache_checkpoint  = int(geo_cfg.get("cep_cache_checkpoint", 100))
    output_checkpoint = int(geo_cfg.get("cep_output_checkpoint", 5_000))
    compression  = cfg["output"]["parquet_compression"]
    row_group    = cfg["output"]["parquet_row_group_size"]

    joined_path = intermediate_dir / f"step_02_poi_joined_{dump_date}.parquet"
    if not joined_path.exists():
        raise FileNotFoundError(
            f"Missing Step 2 output: {joined_path}. Run step 2 first."
        )

    t0 = time.perf_counter()
    log.info("Step 4 — Geocoding POIs…")

    # ── Load address fields (optionally filtered by osm_category) ────────────
    log.info("  Loading address fields from step_02 output…")
    con = duckdb.connect(":memory:")
    if geocode_categories:
        cat_list = ", ".join(f"'{c}'" for c in geocode_categories)
        where    = f"WHERE osm_category IN ({cat_list})"
    else:
        where = ""
    df = con.execute(f"""
        SELECT cnpj, tipo_logradouro, logradouro, numero,
               bairro, cep, municipio, municipio_descricao, uf
        FROM '{joined_path}'
        {where}
    """).df()
    con.close()
    n_total = len(df)
    log.info("  %s POIs loaded%s", f"{n_total:,}",
             f" (category filter: {geocode_categories})" if geocode_categories else "")

    # Normalise CEP: digits only, zero-padded to 8
    df["cep_clean"] = (
        df["cep"].astype(str)
        .str.replace(r"\D", "", regex=True)
        .str.zfill(8)
    )
    df.loc[df["cep_clean"].str.len() != 8, "cep_clean"] = None

    unique_ceps = df["cep_clean"].dropna().unique().tolist()
    log.info("  %d unique CEPs found", len(unique_ceps))

    # ── Phase A: BrasilAPI CEP lookup ────────────────────────────────────────
    log.info("Step 4 — Phase A: BrasilAPI CEP geocoding…")
    cep_df = _geocode_ceps(
        unique_ceps, cache_dir, cep_workers,
        joined_path=joined_path,
        geocoded_out=geocoded_out,
        compression=compression,
        row_group=row_group,
        cache_checkpoint=cache_checkpoint,
        output_checkpoint=output_checkpoint,
    )

    df = df.merge(
        cep_df[["cep", "lat", "lon"]].rename(columns={"cep": "cep_clean"}),
        on="cep_clean", how="left",
    )

    # Ensure lat/lon are float64 regardless of whether the merge produced object dtype
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

    cep_found = df["lat"].notna().sum()
    log.info(
        "  BrasilAPI: %s/%s POIs have coordinates (%.1f%%)",
        f"{cep_found:,}", f"{n_total:,}", 100.0 * cep_found / n_total,
    )

    df["geo_precision"] = np.where(df["lat"].notna(), "cep", "none")

    # ── Phase B: city-centre detection ───────────────────────────────────────
    log.info("Step 4 — Phase B: City-centre detection (threshold=%.0f m)…", city_thresh)
    muni_centroids = _load_municipio_centroids(cache_dir)

    df = df.merge(muni_centroids, on="municipio", how="left")

    has_both = (
        (df["geo_precision"] == "cep") &
        df["lat_centro"].notna()
    )
    if has_both.any():
        sub = df.loc[has_both]
        distances = _haversine_m(
            sub["lat"].values.astype(float),
            sub["lon"].values.astype(float),
            sub["lat_centro"].values.astype(float),
            sub["lon_centro"].values.astype(float),
        )
        city_center_idx = sub.index[distances <= city_thresh]
        df.loc[city_center_idx, "geo_precision"] = "municipio_centroid"
        log.info(
            "  Flagged %s city-centre records (≤ %.0f m from municipality centroid)",
            f"{len(city_center_idx):,}", city_thresh,
        )

    df.drop(columns=["lat_centro", "lon_centro"], errors="ignore", inplace=True)

    # ── Phase C: TomTom fallback ─────────────────────────────────────────────
    fallback_mask  = df["geo_precision"].isin(["municipio_centroid", "none"])
    fallback_count = fallback_mask.sum()
    log.info(
        "Step 4 — Phase C: TomTom fallback needed for %s records…",
        f"{fallback_count:,}",
    )

    if fallback_count > 0 and tt_enabled:
        if not tomtom_key:
            log.warning(
                "  TOMTOM_API_KEY not set in .env — skipping TomTom phase."
                " Set the key and re-run with --from-step geocode --force to fill in."
            )
        else:
            tt_df = _geocode_tomtom(
                df[fallback_mask][["cnpj", "tipo_logradouro", "logradouro", "numero",
                                   "municipio_descricao", "uf", "cep_clean"]],
                tomtom_key, cache_dir, dump_date, tt_workers,
            )

            # Merge TomTom results back (vectorised)
            tt_valid = tt_df[tt_df["lat"].notna()][["cnpj", "lat", "lon", "geo_precision"]]
            if len(tt_valid) > 0:
                df = df.merge(
                    tt_valid.rename(columns={
                        "lat": "lat_tt", "lon": "lon_tt", "geo_precision": "prec_tt",
                    }),
                    on="cnpj", how="left",
                )
                update_mask = fallback_mask & df["lat_tt"].notna()
                df.loc[update_mask, "lat"]           = pd.to_numeric(df.loc[update_mask, "lat_tt"], errors="coerce")
                df.loc[update_mask, "lon"]           = pd.to_numeric(df.loc[update_mask, "lon_tt"], errors="coerce")
                df.loc[update_mask, "geo_precision"] = df.loc[update_mask, "prec_tt"]
                df.drop(columns=["lat_tt", "lon_tt", "prec_tt"], errors="ignore", inplace=True)

                tt_success = update_mask.sum()
                log.info(
                    "  TomTom: improved %s/%s fallback records",
                    f"{tt_success:,}", f"{fallback_count:,}",
                )
    elif not tt_enabled:
        log.info("  TomTom phase disabled (geocoding.tomtom_enabled=false in config.yaml)")

    # ── Final summary ─────────────────────────────────────────────────────────
    log.info("  Geocoding summary:")
    for prec, cnt in df["geo_precision"].value_counts().items():
        log.info("    %-32s %s  (%.1f%%)", prec, f"{cnt:,}", 100.0 * cnt / n_total)

    # ── Write output: merge geo columns into POI table ───────────────────────
    log.info("Step 4 — Writing output…")
    geo_cols = df[["cnpj", "lat", "lon", "geo_precision"]].copy()

    if geocode_categories:
        cat_list  = ", ".join(f"'{c}'" for c in geocode_categories)
        poi_where = f"WHERE p.osm_category IN ({cat_list})"
    else:
        poi_where = ""

    con2 = duckdb.connect(":memory:")
    con2.register("geo_cols", geo_cols)
    con2.execute(f"""
        COPY (
            SELECT p.*, g.lat, g.lon, g.geo_precision
            FROM '{joined_path}' p
            LEFT JOIN geo_cols g USING (cnpj)
            {poi_where}
        ) TO '{geocoded_out}'
        (FORMAT PARQUET, COMPRESSION '{compression}', ROW_GROUP_SIZE {row_group})
    """)
    with_coords = con2.execute(
        f"SELECT count(*) FROM '{geocoded_out}' WHERE lat IS NOT NULL"
    ).fetchone()[0]
    con2.close()

    size_mb = geocoded_out.stat().st_size / 1_048_576
    log.info(
        "  Wrote %s rows → %s (%.1f MB)",
        f"{n_total:,}", geocoded_out.name, size_mb,
    )
    log.info(
        "  Total with coordinates: %s/%s (%.1f%%)",
        f"{with_coords:,}", f"{n_total:,}", 100.0 * with_coords / n_total,
    )
    log.info("Step 4 complete in %.1f s", time.perf_counter() - t0)
    return geocoded_out
