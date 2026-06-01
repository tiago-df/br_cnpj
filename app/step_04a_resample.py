"""Step 04a Resample — re-geocode the validation sample with updated join logic.

Reads the existing validation CSV (which already contains TomTom API coords),
extracts the 300 CNPJs, re-runs them through the updated step_04a join logic
(in-process, no server call), and computes the new haversine distances vs the
same TomTom reference coords.

This lets you compare before/after improvements without spending any API quota.

Output:
  data/output/apt_resample_{date}.csv   — full row-level results (new APT coords)
  Console: side-by-side distance comparison old vs new, per layer.

Usage:
    python -m app.step_04a_resample \\
        --dump-date 2026-05-10 \\
        --uf SP \\
        --validation-csv data/output/apt_validation_300_2026-05-10.csv
"""

import argparse
import csv
import logging
import math
import time
from pathlib import Path

import duckdb
import pandas as pd

from app.config_loader import resolve_path
from app.utils.logging_utils import setup_logging

# Re-use normalisers and join logic from the geocoder
from app.step_04a_apt_geocode import (
    _norm_street, _norm_num, _norm_city,
    _build_apt_index,
)

log = logging.getLogger(__name__)

_APT_LAYERS = ["apt_cep_num_street", "apt_street_exact", "apt_fuzzy", "apt_cep_num"]


# ── Haversine ─────────────────────────────────────────────────────────────────
def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


# ── Re-geocode the sample CNPJs ───────────────────────────────────────────────
def _run_join(
    con: duckdb.DuckDBPyConnection,
    sim_threshold: float,
    fuzzy_threshold: float,
) -> dict[str, tuple]:
    """Run all layers against the sample_poi table. Returns {cnpj: (lat, lon, prec)}."""

    results: dict[str, tuple] = {}

    def run_layer(sql: str, label: str) -> None:
        rows = con.execute(sql).fetchall()
        for cnpj, lat, lon in rows:
            if cnpj not in results:
                results[cnpj] = (lat, lon, label)

    # Pre-filters
    con.execute("""
        CREATE OR REPLACE TEMP TABLE apt_cep_filter AS
        SELECT a.* FROM apt a
        JOIN (SELECT DISTINCT cep_norm FROM sample_poi WHERE cep_norm IS NOT NULL) f
          ON a.cep_norm = f.cep_norm
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE apt_city_filter AS
        SELECT a.* FROM apt a
        JOIN (SELECT DISTINCT city_norm FROM sample_poi WHERE city_norm IS NOT NULL) f
          ON a.city_norm = f.city_norm
    """)

    # Layer 1: CEP + num + street (sim >= 0.90)
    run_layer(f"""
        SELECT DISTINCT ON (p.cnpj) p.cnpj, a.lat, a.lon
        FROM sample_poi p
        JOIN apt_cep_filter a
          ON  p.cep_norm = a.cep_norm
          AND p.num_norm = a.num_norm
          AND p.cep_norm IS NOT NULL AND p.num_norm IS NOT NULL
          AND p.street_norm IS NOT NULL AND a.street_norm IS NOT NULL
        WHERE jaro_winkler_similarity(p.street_norm, a.street_norm) >= {sim_threshold}
        ORDER BY p.cnpj, jaro_winkler_similarity(p.street_norm, a.street_norm) DESC
    """, "apt_cep_num_street")

    # Layer 2 removed (CEP+num only — too many false positives)

    # Layer 3: street exact + num + city, bairro-disambiguated
    already = f"AND p.cnpj NOT IN ({', '.join(repr(c) for c in results)})" if results else ""
    run_layer(f"""
        WITH cands AS (
            SELECT
                p.cnpj, a.lat, a.lon,
                CASE WHEN p.bairro_norm IS NOT NULL AND a.suburb_norm IS NOT NULL
                          AND p.bairro_norm = a.suburb_norm THEN 1 ELSE 0 END AS bairro_match,
                count(*) OVER (PARTITION BY p.cnpj) AS n_total,
                sum(CASE WHEN p.bairro_norm IS NOT NULL AND a.suburb_norm IS NOT NULL
                              AND p.bairro_norm = a.suburb_norm THEN 1 ELSE 0 END)
                    OVER (PARTITION BY p.cnpj) AS n_bairro
            FROM sample_poi p
            JOIN apt_city_filter a
              ON  p.street_norm = a.street_norm AND p.num_norm = a.num_norm
              AND p.city_norm = a.city_norm
              AND p.street_norm IS NOT NULL AND p.num_norm IS NOT NULL
            WHERE 1=1 {already}
        )
        SELECT DISTINCT ON (cnpj) cnpj, lat, lon
        FROM cands
        WHERE n_total = 1 OR n_bairro = 1
        ORDER BY cnpj, bairro_match DESC
    """, "apt_street_exact")

    # Layer 4: fuzzy + bairro blocking
    already = f"AND p.cnpj NOT IN ({', '.join(repr(c) for c in results)})" if results else ""
    run_layer(f"""
        SELECT DISTINCT ON (p.cnpj) p.cnpj, a.lat, a.lon
        FROM sample_poi p
        JOIN apt_city_filter a
          ON  p.city_norm            = a.city_norm
          AND left(p.street_norm, 5) = left(a.street_norm, 5)
          AND p.num_norm             = a.num_norm
          AND p.street_norm IS NOT NULL AND p.num_norm IS NOT NULL
          AND (p.bairro_norm IS NULL OR a.suburb_norm IS NULL
               OR left(p.bairro_norm, 4) = left(a.suburb_norm, 4))
        WHERE jaro_winkler_similarity(p.street_norm, a.street_norm) >= {fuzzy_threshold}
          {already}
        ORDER BY p.cnpj, jaro_winkler_similarity(p.street_norm, a.street_norm) DESC
    """, "apt_fuzzy")

    return results


# ── Statistics helpers ────────────────────────────────────────────────────────
def _stats(dists: list[float]) -> dict:
    if not dists:
        return {}
    s = sorted(dists)
    n = len(s)
    mean = sum(s) / n
    std  = math.sqrt(sum((d - mean) ** 2 for d in s) / n)
    return {
        "n":      n,
        "mean":   mean,
        "std":    std,
        "median": s[n // 2],
        "p90":    s[int(0.90 * n)],
        "p95":    s[int(0.95 * n)],
        "lt50":   sum(1 for d in s if d < 50),
        "gt5k":   sum(1 for d in s if d >= 5000),
    }


def _print_comparison(
    old_rows: list[dict],
    new_matched: dict[str, tuple],
    tt_coords: dict[str, tuple],
) -> None:
    """Print side-by-side before/after distance stats."""

    layers = ["apt_cep_num_street", "apt_street_exact", "apt_fuzzy", "apt_cep_num", "ALL"]

    # Old distances (from the validation CSV)
    old_by_layer: dict[str, list[float]] = {l: [] for l in layers}
    for r in old_rows:
        d = float(r["distance_m"]) if r["distance_m"] else None
        if d is not None:
            old_by_layer[r["old_precision"]].append(d)
            old_by_layer["ALL"].append(d)

    # New distances
    new_by_layer: dict[str, list[float]] = {l: [] for l in layers}
    for cnpj, (new_lat, new_lon, prec) in new_matched.items():
        if cnpj not in tt_coords:
            continue
        tt_lat, tt_lon = tt_coords[cnpj]
        if tt_lat is None:
            continue
        d = _haversine_m(new_lat, new_lon, tt_lat, tt_lon)
        new_by_layer[prec].append(d)
        new_by_layer["ALL"].append(d)

    # CNPJs that changed layer
    layer_changes: dict[str, dict[str, int]] = {}
    for r in old_rows:
        cnpj = r["cnpj"]
        old_p = r["old_precision"]
        new_p = new_matched[cnpj][2] if cnpj in new_matched else "none"
        if old_p != new_p:
            layer_changes.setdefault(old_p, {}).setdefault(new_p, 0)
            layer_changes[old_p][new_p] += 1

    print()
    print("═" * 72)
    print("  COMPARISON: OLD join  vs  NEW join (same 300 POIs, same TomTom ref)")
    print("═" * 72)

    fmt = "  {:<26}  {:>5}  {:>8}  {:>8}  {:>6}  {:>6}  {:>6}"
    print(fmt.format("Layer", "n", "mean(m)", "std(m)", "med(m)", "<50m%", ">5km%"))
    print("  " + "-" * 68)

    for layer in layers:
        print(f"  {'── ' + layer + ' ──':─<68}")
        for tag, by_layer in [("OLD", old_by_layer), ("NEW", new_by_layer)]:
            s = _stats(by_layer[layer])
            if not s:
                print(fmt.format(f"    {tag}", 0, "—", "—", "—", "—", "—"))
                continue
            lt50_pct = 100 * s["lt50"] / s["n"]
            gt5k_pct = 100 * s["gt5k"] / s["n"]
            mean_s = f"{s['mean']:.0f}"
            std_s  = f"{s['std']:.0f}"
            med_s  = f"{s['median']:.0f}"
            print(fmt.format(
                f"    {tag}", s["n"], mean_s, std_s, med_s,
                f"{lt50_pct:.0f}%", f"{gt5k_pct:.0f}%",
            ))

    if layer_changes:
        print()
        print("  Layer reassignments (OLD → NEW):")
        for old_p, moves in sorted(layer_changes.items()):
            for new_p, cnt in sorted(moves.items()):
                print(f"    {old_p:<28} → {new_p:<28} : {cnt}")

    # CNPJs matched in old but not in new (dropped)
    old_cnpjs = {r["cnpj"] for r in old_rows}
    dropped   = old_cnpjs - set(new_matched)
    gained    = set(new_matched) - old_cnpjs
    print()
    print(f"  Previously matched, now unmatched: {len(dropped)}")
    print(f"  Previously unmatched, now matched: {len(gained)}")
    print("═" * 72)


# ── Abbrev SQL (copy from apt_geocode to avoid importing the full run()) ──────
_ABBREV_SQL = """
    CASE
        WHEN regexp_matches(upper(trim(tipo_logradouro)), '^R$|^RUA$')             THEN 'RUA '
        WHEN regexp_matches(upper(trim(tipo_logradouro)), '^AV$|^AVENIDA$')        THEN 'AVENIDA '
        WHEN regexp_matches(upper(trim(tipo_logradouro)), '^TV$|^TRAV$|^TRAVESSA$') THEN 'TRAVESSA '
        WHEN regexp_matches(upper(trim(tipo_logradouro)), '^AL$|^ALM$|^ALAMEDA$')  THEN 'ALAMEDA '
        WHEN regexp_matches(upper(trim(tipo_logradouro)), '^PC$|^PCA$|^PRACA$')    THEN 'PRACA '
        WHEN regexp_matches(upper(trim(tipo_logradouro)), '^EST$|^ESTRADA$')        THEN 'ESTRADA '
        WHEN regexp_matches(upper(trim(tipo_logradouro)), '^ROD$|^RODOVIA$')        THEN 'RODOVIA '
        WHEN regexp_matches(upper(trim(tipo_logradouro)), '^LG$|^LARGO$')           THEN 'LARGO '
        ELSE upper(trim(tipo_logradouro)) || ' '
    END || upper(trim(logradouro))
"""


# ── Main ──────────────────────────────────────────────────────────────────────
def run(
    validation_csv: Path,
    intermediate_dir: Path,
    cache_dir: Path,
    dump_date: str,
    gpkg_paths: list[Path],
    uf_filter: list[str] | None,
    output_dir: Path,
    sim_threshold: float = 0.90,
    fuzzy_threshold: float = 0.85,
) -> None:
    t0 = time.perf_counter()
    log.info("Step 04a Resample — re-geocode validation sample with updated join")
    log.info("  Validation CSV : %s", validation_csv.name)
    log.info("  sim_threshold  : %.2f", sim_threshold)
    log.info("  fuzzy_threshold: %.2f", fuzzy_threshold)

    # ── Load validation CSV ───────────────────────────────────────────────────
    val_rows = list(csv.DictReader(validation_csv.open(encoding="utf-8")))
    cnpj_list = [r["cnpj"] for r in val_rows]
    log.info("  Loaded %d rows from validation CSV", len(val_rows))

    # TomTom reference coords
    tt_coords: dict[str, tuple] = {}
    for r in val_rows:
        if r["tt_lat"] and r["tt_lon"]:
            tt_coords[r["cnpj"]] = (float(r["tt_lat"]), float(r["tt_lon"]))

    # Annotate old precision
    for r in val_rows:
        r["old_precision"] = r["geo_precision"]

    # ── Build / load APT index ────────────────────────────────────────────────
    log.info("Building APT index (v2 — includes suburb_norm)…")
    apt_index_paths = [_build_apt_index(g, cache_dir) for g in gpkg_paths]
    apt_glob = (
        str(apt_index_paths[0])
        if len(apt_index_paths) == 1
        else "[" + ", ".join(f"'{p}'" for p in apt_index_paths) + "]"
    )

    # ── DuckDB session ────────────────────────────────────────────────────────
    con = duckdb.connect()
    con.execute("SET memory_limit='8GB'; SET threads=4;")

    log.info("  Loading APT index…")
    con.execute(f"CREATE TABLE apt AS SELECT * FROM read_parquet('{apt_glob}')")
    n_apt = con.execute("SELECT count(*) FROM apt").fetchone()[0]
    log.info("  APT: %s rows", f"{n_apt:,}")

    # ── Load sample POIs from step_02 ─────────────────────────────────────────
    joined_path = intermediate_dir / f"step_02_poi_joined_{dump_date}.parquet"
    uf_where = ""
    if uf_filter:
        ufs = ", ".join(f"'{u}'" for u in uf_filter)
        uf_where = f"AND uf IN ({ufs})"

    cnpj_in = ", ".join(f"'{c}'" for c in cnpj_list)

    log.info("  Loading %d sample POIs from step_02…", len(cnpj_list))
    con.execute(f"""
        CREATE TABLE sample_poi AS
        SELECT
            cnpj, tipo_logradouro, logradouro, numero,
            bairro, cep, municipio, municipio_descricao, uf,
            regexp_replace(cep, '[^0-9]', '', 'g') AS cep_norm,
            CASE WHEN upper(trim(numero)) IN ('S/N','SN','S N','0','') THEN NULL
                 ELSE regexp_extract(trim(numero), '^\\d+')
            END AS num_norm,
            regexp_replace(
                regexp_replace({_ABBREV_SQL}, '\\s+(DE|DA|DO|DAS|DOS|E)\\s+', ' ', 'g'),
            '\\s+', ' ', 'g') AS street_norm,
            upper(trim(municipio_descricao)) AS city_norm,
            regexp_replace(
                regexp_replace(upper(trim(bairro)), '\\s+(DE|DA|DO|DAS|DOS|E)\\s+', ' ', 'g'),
            '\\s+', ' ', 'g') AS bairro_norm
        FROM read_parquet('{joined_path}')
        WHERE cnpj IN ({cnpj_in}) {uf_where}
    """)
    n_sample = con.execute("SELECT count(*) FROM sample_poi").fetchone()[0]
    log.info("  Sample POIs loaded: %d", n_sample)

    # ── Run updated join ──────────────────────────────────────────────────────
    log.info("Running updated 4-layer join…")
    new_matched = _run_join(con, sim_threshold, fuzzy_threshold)
    log.info("  Matched: %d / %d", len(new_matched), n_sample)

    con.close()

    # ── Build output CSV ──────────────────────────────────────────────────────
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"apt_resample_{dump_date}.csv"

    fieldnames = [
        "cnpj", "razao_social", "uf", "municipio_descricao",
        "bairro", "tipo_logradouro", "logradouro", "numero", "cep",
        "old_precision",
        "old_apt_lat", "old_apt_lon", "old_distance_m",
        "new_precision",
        "new_apt_lat", "new_apt_lon", "new_distance_m",
        "tt_lat", "tt_lon",
        "delta_m",    # new_distance - old_distance (negative = improvement)
    ]

    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in val_rows:
            cnpj = r["cnpj"]
            old_d = float(r["distance_m"]) if r["distance_m"] else None
            new_lat, new_lon, new_prec = new_matched.get(cnpj, (None, None, "none"))
            tt = tt_coords.get(cnpj)
            new_d = None
            if new_lat is not None and tt:
                new_d = _haversine_m(new_lat, new_lon, tt[0], tt[1])
            delta = (new_d - old_d) if (new_d is not None and old_d is not None) else None
            w.writerow({
                "cnpj":           cnpj,
                "razao_social":   r.get("razao_social", ""),
                "uf":             r.get("uf", ""),
                "municipio_descricao": r.get("municipio_descricao", ""),
                "bairro":         r.get("bairro", ""),
                "tipo_logradouro": r.get("tipo_logradouro", ""),
                "logradouro":     r.get("logradouro", ""),
                "numero":         r.get("numero", ""),
                "cep":            r.get("cep", ""),
                "old_precision":  r["old_precision"],
                "old_apt_lat":    r.get("apt_lat", ""),
                "old_apt_lon":    r.get("apt_lon", ""),
                "old_distance_m": f"{old_d:.1f}" if old_d is not None else "",
                "new_precision":  new_prec,
                "new_apt_lat":    f"{new_lat:.7f}" if new_lat is not None else "",
                "new_apt_lon":    f"{new_lon:.7f}" if new_lon is not None else "",
                "new_distance_m": f"{new_d:.1f}" if new_d is not None else "",
                "tt_lat":         r.get("tt_lat", ""),
                "tt_lon":         r.get("tt_lon", ""),
                "delta_m":        f"{delta:.1f}" if delta is not None else "",
            })

    log.info("  Output CSV: %s (%d rows)", out_path.name, len(val_rows))

    # ── Comparison report ─────────────────────────────────────────────────────
    _print_comparison(val_rows, new_matched, tt_coords)
    log.info("Resample complete in %.1f s", time.perf_counter() - t0)


# ── CLI ───────────────────────────────────────────────────────────────────────
def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Re-geocode validation sample with updated join logic")
    p.add_argument("--dump-date",       required=True, metavar="YYYY-MM-DD")
    p.add_argument("--gpkg",            nargs="+", required=True)
    p.add_argument("--uf",              nargs="+", metavar="UF")
    p.add_argument("--validation-csv",  required=True, metavar="PATH",
                   help="Path to the existing apt_validation_*.csv")
    p.add_argument("--sim-threshold",   type=float, default=0.90)
    p.add_argument("--fuzzy-threshold", type=float, default=0.85)
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    log_dir = resolve_path("log_dir")
    setup_logging(log_dir)

    gpkg_paths = [Path(g) for g in args.gpkg]
    for g in gpkg_paths:
        if not g.exists():
            raise FileNotFoundError(f"GeoPackage not found: {g}")

    run(
        validation_csv   = Path(args.validation_csv),
        intermediate_dir = resolve_path("intermediate_dir"),
        cache_dir        = resolve_path("cache_dir"),
        dump_date        = args.dump_date,
        gpkg_paths       = gpkg_paths,
        uf_filter        = args.uf,
        output_dir       = resolve_path("output_dir"),
        sim_threshold    = args.sim_threshold,
        fuzzy_threshold  = args.fuzzy_threshold,
    )
