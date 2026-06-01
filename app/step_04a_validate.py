"""Step 04a Validation — Compare APT geocoding against TomTom Search API.

Loads a geocoded output parquet produced by step_04a_apt_geocode.py,
draws a stratified random sample of 300 matched POIs (spread across all
precision layers), re-geocodes each with the TomTom Structured Geocode API,
then computes the haversine distance between the APT coordinate and the
TomTom coordinate.

Output:
  data/output/apt_validation_300_{date}.csv   — full row-level results
  Logs mean ± stdev distance (metres) overall and per precision layer.

Usage:
    python -m app.step_04a_validate \\
        --dump-date 2026-05-10 \\
        --uf SP \\
        --sample 300

    # Explicit parquet:
    python -m app.step_04a_validate \\
        --parquet data/intermediate/step_04_geocoded_2026-05-10__apt_SP.parquet \\
        --sample 300

Requires TOMTOM_API_KEY in .env (or environment).
"""

import argparse
import asyncio
import csv
import logging
import math
import os
import time
from pathlib import Path

import aiohttp
import duckdb
from dotenv import load_dotenv

from app.config_loader import resolve_path
from app.utils.logging_utils import setup_logging

load_dotenv()
log = logging.getLogger(__name__)

# ── TomTom API ───────────────────────────────────────────────────────────────
_TT_STRUCTURED_URL = "https://api.tomtom.com/search/2/structuredGeocode.json"
_TT_WORKERS        = 5   # concurrent requests — safe for most TomTom plans
_TT_TIMEOUT_S      = 15


# ── Haversine distance ────────────────────────────────────────────────────────
def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return distance in metres between two WGS84 points."""
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi  = math.radians(lat2 - lat1)
    dlam  = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


# ── TomTom query ─────────────────────────────────────────────────────────────
async def _geocode_tt(
    session: aiohttp.ClientSession,
    api_key: str,
    row: dict,
) -> dict:
    """Call TomTom Structured Geocode and return enriched row."""
    params = {
        "key":                 api_key,
        "countryCode":         "BR",
        "countrySubdivision":  row["uf"],
        "municipality":        row["municipio_descricao"] or "",
        "streetName":          (row["tipo_logradouro"] or "") + " " + (row["logradouro"] or ""),
        "streetNumber":        row["numero"] or "",
        "postalCode":          row["cep"] or "",
        "limit":               1,
    }

    row = dict(row)  # copy so we can mutate
    row["tt_lat"]    = None
    row["tt_lon"]    = None
    row["tt_score"]  = None
    row["tt_type"]   = None
    row["tt_error"]  = None
    row["distance_m"] = None

    try:
        async with session.get(
            _TT_STRUCTURED_URL,
            params=params,
            timeout=aiohttp.ClientTimeout(total=_TT_TIMEOUT_S),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                results = data.get("results", [])
                if results:
                    r  = results[0]
                    pos = r.get("position", {})
                    row["tt_lat"]   = pos.get("lat")
                    row["tt_lon"]   = pos.get("lon")
                    row["tt_score"] = r.get("score")
                    row["tt_type"]  = r.get("type")
                    if row["tt_lat"] is not None:
                        row["distance_m"] = _haversine_m(
                            row["apt_lat"], row["apt_lon"],
                            row["tt_lat"],  row["tt_lon"],
                        )
                else:
                    row["tt_error"] = "no_results"
            else:
                row["tt_error"] = f"http_{resp.status}"
    except asyncio.TimeoutError:
        row["tt_error"] = "timeout"
    except Exception as exc:
        row["tt_error"] = f"error:{exc}"

    return row


# ── Async worker pool ─────────────────────────────────────────────────────────
async def _run_validation(
    sample_rows: list[dict],
    api_key: str,
) -> list[dict]:
    queue: asyncio.Queue = asyncio.Queue()
    for r in sample_rows:
        await queue.put(r)

    results: list[dict] = []
    done = 0

    async def worker():
        nonlocal done
        connector = aiohttp.TCPConnector(limit=_TT_WORKERS)
        async with aiohttp.ClientSession(connector=connector) as session:
            while True:
                try:
                    row = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                enriched = await _geocode_tt(session, api_key, row)
                results.append(enriched)
                done += 1
                if done % 50 == 0 or done == len(sample_rows):
                    pct = 100 * done / len(sample_rows)
                    log.info("  TomTom validation: %d/%d (%.0f%%)", done, len(sample_rows), pct)

    await asyncio.gather(*[worker() for _ in range(_TT_WORKERS)])
    return results


# ── Stratified sample ─────────────────────────────────────────────────────────
_APT_LAYERS = [
    "apt_cep_num_street",
    "apt_cep_num",
    "apt_street_exact",
    "apt_fuzzy",
]


def _draw_sample(parquet_path: Path, n: int) -> list[dict]:
    """Return up to n rows, stratified across precision layers."""
    con = duckdb.connect()

    # Count per layer
    counts = con.execute(f"""
        SELECT geo_precision, count(*) AS cnt
        FROM read_parquet('{parquet_path}')
        WHERE geo_precision IN ('apt_cep_num_street','apt_cep_num','apt_street_exact','apt_fuzzy')
          AND lat IS NOT NULL AND lon IS NOT NULL
        GROUP BY geo_precision
        ORDER BY cnt DESC
    """).fetchall()
    log.info("  Available matched POIs per layer:")
    total_matched = 0
    for prec, cnt in counts:
        log.info("    %-28s %s", prec, f"{cnt:,}")
        total_matched += cnt
    log.info("    %-28s %s", "TOTAL", f"{total_matched:,}")

    # Proportional allocation (at least 1 per layer if present)
    alloc: dict[str, int] = {}
    for prec, cnt in counts:
        share = max(1, round(n * cnt / total_matched)) if total_matched else 0
        alloc[prec] = min(share, cnt)

    # Trim to exactly n
    allocated_total = sum(alloc.values())
    if allocated_total > n:
        # Reduce the largest layer
        for prec in sorted(alloc, key=alloc.get, reverse=True):
            excess = allocated_total - n
            reduce = min(excess, alloc[prec] - 1)
            alloc[prec] -= reduce
            allocated_total -= reduce
            if allocated_total <= n:
                break
    elif allocated_total < n and total_matched >= n:
        # Top up the largest layer
        for prec, cnt in counts:
            shortage = n - allocated_total
            extra = min(shortage, cnt - alloc[prec])
            alloc[prec] += extra
            allocated_total += extra
            if allocated_total >= n:
                break

    log.info("  Sample allocation:")
    rows_all: list[dict] = []
    for prec, k in alloc.items():
        if k <= 0:
            continue
        log.info("    %-28s %d", prec, k)
        rows = con.execute(f"""
            SELECT
                cnpj,
                razao_social,
                tipo_logradouro,
                logradouro,
                numero,
                complemento,
                bairro,
                cep,
                municipio,
                municipio_descricao,
                uf,
                lat   AS apt_lat,
                lon   AS apt_lon,
                geo_precision
            FROM read_parquet('{parquet_path}')
            WHERE geo_precision = '{prec}'
              AND lat IS NOT NULL AND lon IS NOT NULL
            USING SAMPLE {k} ROWS
        """).fetchall()
        cols = [
            "cnpj","razao_social","tipo_logradouro","logradouro","numero",
            "complemento","bairro","cep","municipio","municipio_descricao",
            "uf","apt_lat","apt_lon","geo_precision",
        ]
        rows_all.extend(dict(zip(cols, r)) for r in rows)

    con.close()
    return rows_all


# ── Statistics ────────────────────────────────────────────────────────────────
def _print_stats(results: list[dict]) -> None:
    dists = [r["distance_m"] for r in results if r["distance_m"] is not None]
    no_result = sum(1 for r in results if r["tt_error"] == "no_results" or r["tt_lat"] is None)
    errors    = sum(1 for r in results if r["tt_error"] and r["tt_error"] != "no_results")

    log.info("")
    log.info("═" * 56)
    log.info("  VALIDATION RESULTS  (n=%d)", len(results))
    log.info("═" * 56)
    log.info("  TomTom matched:       %d  (%.1f%%)", len(dists), 100*len(dists)/len(results))
    log.info("  TomTom no result:     %d", no_result)
    log.info("  TomTom errors:        %d", errors)

    if dists:
        mean = sum(dists) / len(dists)
        var  = sum((d - mean) ** 2 for d in dists) / len(dists)
        std  = math.sqrt(var)
        sorted_d = sorted(dists)
        p50  = sorted_d[len(sorted_d) // 2]
        p90  = sorted_d[int(0.90 * len(sorted_d))]
        p95  = sorted_d[int(0.95 * len(sorted_d))]
        p99  = sorted_d[int(0.99 * len(sorted_d))]
        log.info("")
        log.info("  Distance APT ↔ TomTom (metres):")
        log.info("    Mean:   %8.1f m", mean)
        log.info("    StdDev: %8.1f m", std)
        log.info("    Median: %8.1f m", p50)
        log.info("    P90:    %8.1f m", p90)
        log.info("    P95:    %8.1f m", p95)
        log.info("    P99:    %8.1f m", p99)
        log.info("    Min:    %8.1f m", sorted_d[0])
        log.info("    Max:    %8.1f m", sorted_d[-1])

        # Buckets
        buckets = [
            (   0,   50, "<  50 m  (same block)"),
            (  50,  200, "50–200 m (same street)"),
            ( 200,  500, "200–500 m (nearby)"),
            ( 500, 1000, "500 m–1 km"),
            (1000, 5000, "1–5 km"),
            (5000, float("inf"), "> 5 km  (suspect)"),
        ]
        log.info("")
        log.info("  Distance distribution:")
        for lo, hi, label in buckets:
            cnt = sum(1 for d in dists if lo <= d < hi)
            bar = "█" * int(30 * cnt / len(dists))
            log.info("    %-28s %4d  %5.1f%%  %s", label, cnt, 100*cnt/len(dists), bar)

        # Per-layer breakdown
        log.info("")
        log.info("  Mean distance by precision layer:")
        for layer in _APT_LAYERS:
            ld = [r["distance_m"] for r in results
                  if r["geo_precision"] == layer and r["distance_m"] is not None]
            if ld:
                lm = sum(ld) / len(ld)
                ls = math.sqrt(sum((d - lm) ** 2 for d in ld) / len(ld))
                log.info("    %-28s n=%3d  mean=%7.1f m  std=%7.1f m",
                         layer, len(ld), lm, ls)
    log.info("═" * 56)


# ── CSV writer ────────────────────────────────────────────────────────────────
_CSV_FIELDS = [
    "cnpj", "razao_social", "uf", "municipio_descricao",
    "bairro", "tipo_logradouro", "logradouro", "numero", "complemento", "cep",
    "geo_precision",
    "apt_lat", "apt_lon",
    "tt_lat",  "tt_lon", "tt_score", "tt_type",
    "distance_m", "tt_error",
]


def _write_csv(results: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=_CSV_FIELDS, extrasaction="ignore")
        w.writeheader()
        for row in results:
            w.writerow({k: ("" if row.get(k) is None else row[k]) for k in _CSV_FIELDS})
    log.info("  CSV written: %s  (%d rows)", out_path.name, len(results))


# ── Main ──────────────────────────────────────────────────────────────────────
def run(
    parquet_path: Path,
    output_dir: Path,
    sample: int = 300,
) -> Path:
    api_key = os.environ.get("TOMTOM_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("TOMTOM_API_KEY not set in .env or environment")

    t_total = time.perf_counter()
    log.info("Step 04a Validate — APT vs TomTom API")
    log.info("  Parquet : %s", parquet_path.name)
    log.info("  Sample  : %d POIs", sample)
    log.info("  Workers : %d", _TT_WORKERS)

    # Draw sample
    log.info("Drawing stratified sample…")
    sample_rows = _draw_sample(parquet_path, sample)
    log.info("  Sampled %d POIs", len(sample_rows))

    # Re-geocode with TomTom
    log.info("Re-geocoding with TomTom API…")
    results = asyncio.run(_run_validation(sample_rows, api_key))

    # Output CSV
    date_tag = parquet_path.stem.split("_")[3] if "_" in parquet_path.stem else "unknown"
    out_path = output_dir / f"apt_validation_{sample}_{date_tag}.csv"
    _write_csv(results, out_path)

    # Statistics
    _print_stats(results)

    log.info("Validation complete in %.1f s", time.perf_counter() - t_total)
    return out_path


# ── CLI ───────────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate APT geocoding against TomTom API")
    grp = p.add_mutually_exclusive_group(required=True)
    grp.add_argument("--dump-date", metavar="YYYY-MM-DD",
                     help="Dump date — used to locate parquet automatically")
    grp.add_argument("--parquet", metavar="PATH",
                     help="Explicit path to geocoded parquet file")
    p.add_argument("--uf", nargs="+", metavar="UF",
                   help="State filter used to build parquet filename (with --dump-date)")
    p.add_argument("--sample", type=int, default=300,
                   help="Number of POIs to validate (default 300)")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    log_dir = resolve_path("log_dir")
    setup_logging(log_dir)

    if args.parquet:
        parquet_path = Path(args.parquet)
    else:
        intermediate_dir = resolve_path("intermediate_dir")
        if args.uf:
            slug = "_".join(sorted(args.uf))
            parquet_path = intermediate_dir / f"step_04_geocoded_{args.dump_date}__apt_{slug}.parquet"
        else:
            # Find any apt geocoded file for this dump date
            candidates = sorted(
                intermediate_dir.glob(f"step_04_geocoded_{args.dump_date}__apt_*.parquet")
            )
            if not candidates:
                raise FileNotFoundError(
                    f"No apt geocoded parquet found for {args.dump_date} in {intermediate_dir}"
                )
            parquet_path = candidates[0]
            if len(candidates) > 1:
                log.warning("Multiple parquet files found; using %s", parquet_path.name)

    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet not found: {parquet_path}")

    output_dir = resolve_path("output_dir")

    run(
        parquet_path=parquet_path,
        output_dir=output_dir,
        sample=args.sample,
    )
