"""Step 04a — Geocode POIs using TomTom Orbis Address Points (local join).

Reads TomTom Orbis Address Points GeoPackage(s), builds a normalised
Parquet index (cached), then joins against the step_02 POI table in
four layers of decreasing confidence:

  Layer 1 — CEP + housenumber + street similarity ≥ 0.80
             geo_precision = "apt_cep_num_street"   (highest confidence)

  Layer 2 — CEP + housenumber, unique match only (no ambiguity)
             geo_precision = "apt_cep_num"

  Layer 3 — Normalised street name + housenumber + city (exact)
             geo_precision = "apt_street_exact"

  Layer 4 — Fuzzy street name (jaro_winkler ≥ 0.85) + housenumber + city
             Blocked by city + first 5 chars of street to limit candidates
             geo_precision = "apt_fuzzy"

Unmatched records are left for the BrasilAPI / TomTom API fallback.

Usage (standalone):
    python -m app.step_04a_apt_geocode \\
        --dump-date 2026-05-10 \\
        --gpkg data/input/Orbis_Address_Points_BRA_SP.gpkg \\
        --uf SP

    # Multiple GeoPackages (full Brazil):
    python -m app.step_04a_apt_geocode \\
        --dump-date 2026-05-10 \\
        --gpkg data/input/Orbis_Address_Points_BRA_*.gpkg

Reads:
    data/intermediate/step_02_poi_joined_<date>.parquet

Writes:
    data/cache/apt_index_<gpkg_stem>.parquet   (reused across runs)
    data/intermediate/step_04_geocoded_<date>__apt[_<uf>].parquet
"""

import argparse
import logging
import re
import time
import unicodedata
from pathlib import Path

import duckdb
import pandas as pd

from app.config_loader import get_config, resolve_path
from app.utils.logging_utils import setup_logging

log = logging.getLogger(__name__)

# ── Street name normalisation ────────────────────────────────────────────────

# Common abbreviations used in RF CNPJ logradouro field
_ABBREV = [
    (r"\bR\b",      "RUA"),
    (r"\bAV\b",     "AVENIDA"),
    (r"\bAVDA\b",   "AVENIDA"),
    (r"\bAL\b",     "ALAMEDA"),
    (r"\bALM\b",    "ALAMEDA"),
    (r"\bTV\b",     "TRAVESSA"),
    (r"\bTRAV\b",   "TRAVESSA"),
    (r"\bPC\b",     "PRACA"),
    (r"\bPCA\b",    "PRACA"),
    (r"\bPRA\b",    "PRACA"),
    (r"\bLG\b",     "LARGO"),
    (r"\bROD\b",    "RODOVIA"),
    (r"\bEST\b",    "ESTRADA"),
    (r"\bBC\b",     "BECO"),
    (r"\bLD\b",     "LADEIRA"),
    (r"\bVL\b",     "VILA"),
    (r"\bJD\b",     "JARDIM"),
    (r"\bCOND\b",   "CONDOMINIO"),
    (r"\bSER\b",    "SERVIDAO"),
]
_ABBREV_RE = [(re.compile(p, re.IGNORECASE), r) for p, r in _ABBREV]
_PREP_RE   = re.compile(r"\b(DE|DA|DO|DAS|DOS|E)\b")
_SPACE_RE  = re.compile(r"\s+")

_SN_VALUES = {"S/N", "SN", "S N", "SEM NUMERO", "SEM NÚMERO", "0", "", "N/A", "S"}


def _norm_street(s: str | None) -> str | None:
    """Normalise a street name: strip accents, uppercase, expand abbrevs."""
    if not s:
        return None
    # Strip accents (handles APT's UTF-8 accented names)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.upper().strip()
    # Expand abbreviations
    for pattern, replacement in _ABBREV_RE:
        s = pattern.sub(replacement, s)
    # Remove common prepositions
    s = _PREP_RE.sub("", s)
    s = _SPACE_RE.sub(" ", s).strip()
    return s or None


def _norm_num(s: str | None) -> str | None:
    """Return the leading numeric part of a house number, or None for S/N."""
    if not s:
        return None
    s = str(s).strip().upper()
    if s in _SN_VALUES:
        return None
    m = re.match(r"^(\d+)", s)
    return m.group(1) if m else None


def _norm_city(s: str | None) -> str | None:
    """Normalise city name: strip accents, uppercase."""
    if not s:
        return None
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return _SPACE_RE.sub(" ", s.upper().strip()) or None


# ── APT pre-processing ───────────────────────────────────────────────────────

def _build_apt_index(gpkg_path: Path, cache_dir: Path) -> Path:
    """Read a GeoPackage and write a normalised Parquet index.

    Returns the path to the cached Parquet file.
    The index contains only the fields needed for joining + lat/lon.
    """
    index_path = cache_dir / f"apt_index_{gpkg_path.stem}.parquet"
    if index_path.exists():
        log.info("  APT index already cached: %s", index_path.name)
        return index_path

    log.info("  Pre-processing APT GeoPackage: %s (this runs once)…", gpkg_path.name)
    t0 = time.perf_counter()

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")

    # Register Python UDFs for normalisation
    con.create_function("norm_street", _norm_street, ["VARCHAR"], "VARCHAR")
    con.create_function("norm_num",    _norm_num,    ["VARCHAR"], "VARCHAR")
    con.create_function("norm_city",   _norm_city,   ["VARCHAR"], "VARCHAR")

    gpkg = str(gpkg_path)
    con.execute(f"""
        COPY (
            SELECT
                regexp_replace("postcode:pt-Latn", '[^0-9]', '', 'g')  AS cep_norm,
                norm_num(housenumber)                                    AS num_norm,
                housenumber                                              AS housenumber_raw,
                norm_street("street:pt-Latn")                           AS street_norm,
                "street:pt-Latn"                                        AS street_raw,
                norm_city("city:pt-Latn")                               AS city_norm,
                "suburb:pt-Latn"                                        AS suburb,
                ST_Y(geom)                                              AS lat,
                ST_X(geom)                                              AS lon
            FROM ST_Read('{gpkg}')
            WHERE geom IS NOT NULL
        ) TO '{index_path}'
        (FORMAT PARQUET, COMPRESSION 'zstd', ROW_GROUP_SIZE 122880)
    """)
    con.close()

    n = duckdb.execute(f"SELECT count(*) FROM read_parquet('{index_path}')").fetchone()[0]
    size_mb = index_path.stat().st_size / 1_048_576
    log.info("  APT index: %s rows → %s (%.1f MB) in %.1f s",
             f"{n:,}", index_path.name, size_mb, time.perf_counter() - t0)
    return index_path


# ── Join layers ──────────────────────────────────────────────────────────────

def _run_join(con: duckdb.DuckDBPyConnection, label: str, sql: str) -> int:
    """Execute a join SQL that inserts into 'matched', return row count."""
    t0 = time.perf_counter()
    con.execute(sql)
    n = con.execute("SELECT count(*) FROM matched WHERE geo_precision = ?",
                    [label]).fetchone()[0]
    log.info("    %-25s %s matches (%.1f s)", label, f"{n:,}", time.perf_counter() - t0)
    return n


def _geocode_layers(
    con: duckdb.DuckDBPyConnection,
    poi_parquet: str,
    apt_parquet: str,
    uf_filter: list[str] | None,
    sim_threshold: float = 0.80,
    fuzzy_threshold: float = 0.85,
) -> None:
    """Run all 4 join layers. Results accumulate in the 'matched' temp table."""
    uf_where = ""
    if uf_filter:
        ufs = ", ".join(f"'{u}'" for u in uf_filter)
        uf_where = f"AND p.uf IN ({ufs})"

    # ── Load POI and APT into memory ─────────────────────────────────────────
    log.info("  Loading POI table…")
    con.execute(f"""
        CREATE OR REPLACE TABLE poi AS
        SELECT
            cnpj,
            tipo_logradouro,
            logradouro,
            numero,
            bairro,
            cep,
            municipio,
            municipio_descricao,
            uf,
            -- normalised fields for joining
            regexp_replace(cep, '[^0-9]', '', 'g')                  AS cep_norm,
            norm_num(numero)                                          AS num_norm,
            norm_street(tipo_logradouro || ' ' || logradouro)        AS street_norm,
            norm_city(municipio_descricao)                            AS city_norm
        FROM read_parquet('{poi_parquet}')
        WHERE 1=1 {uf_where}
    """)
    n_poi = con.execute("SELECT count(*) FROM poi").fetchone()[0]
    log.info("  POIs loaded: %s", f"{n_poi:,}")

    log.info("  Loading APT index…")
    con.execute(f"CREATE OR REPLACE TABLE apt AS SELECT * FROM read_parquet('{apt_parquet}')")
    n_apt = con.execute("SELECT count(*) FROM apt").fetchone()[0]
    log.info("  APT points loaded: %s", f"{n_apt:,}")

    # ── Result table ─────────────────────────────────────────────────────────
    con.execute("""
        CREATE OR REPLACE TABLE matched (
            cnpj          VARCHAR,
            lat           DOUBLE,
            lon           DOUBLE,
            geo_precision VARCHAR
        )
    """)

    # ── Layer 1: CEP + num + street similarity ≥ threshold ───────────────────
    log.info("  Layer 1: CEP + num + street similarity ≥ %.2f…", sim_threshold)
    _run_join(con, "apt_cep_num_street", f"""
        INSERT INTO matched
        SELECT DISTINCT ON (p.cnpj)
            p.cnpj,
            a.lat,
            a.lon,
            'apt_cep_num_street' AS geo_precision
        FROM poi p
        JOIN apt a
          ON  p.cep_norm  = a.cep_norm
          AND p.num_norm  = a.num_norm
          AND p.cep_norm IS NOT NULL
          AND p.num_norm IS NOT NULL
        WHERE jaro_winkler_similarity(p.street_norm, a.street_norm) >= {sim_threshold}
        ORDER BY p.cnpj, jaro_winkler_similarity(p.street_norm, a.street_norm) DESC
    """)

    # ── Layer 2: CEP + num, unique match only ────────────────────────────────
    log.info("  Layer 2: CEP + num (unique match, no street validation)…")
    _run_join(con, "apt_cep_num", """
        INSERT INTO matched
        WITH candidates AS (
            SELECT
                p.cnpj,
                a.lat,
                a.lon,
                count(*) OVER (PARTITION BY p.cnpj) AS n_matches
            FROM poi p
            JOIN apt a
              ON  p.cep_norm = a.cep_norm
              AND p.num_norm = a.num_norm
              AND p.cep_norm IS NOT NULL
              AND p.num_norm IS NOT NULL
            WHERE p.cnpj NOT IN (SELECT cnpj FROM matched)
        )
        SELECT cnpj, lat, lon, 'apt_cep_num' AS geo_precision
        FROM candidates
        WHERE n_matches = 1
    """)

    # ── Layer 3: normalised street + num + city (exact) ──────────────────────
    log.info("  Layer 3: street_norm + num + city (exact)…")
    _run_join(con, "apt_street_exact", """
        INSERT INTO matched
        SELECT DISTINCT ON (p.cnpj)
            p.cnpj,
            a.lat,
            a.lon,
            'apt_street_exact' AS geo_precision
        FROM poi p
        JOIN apt a
          ON  p.street_norm = a.street_norm
          AND p.num_norm    = a.num_norm
          AND p.city_norm   = a.city_norm
          AND p.street_norm IS NOT NULL
          AND p.num_norm    IS NOT NULL
          AND p.city_norm   IS NOT NULL
        WHERE p.cnpj NOT IN (SELECT cnpj FROM matched)
        ORDER BY p.cnpj
    """)

    # ── Layer 4: fuzzy street (jaro_winkler ≥ threshold) + num + city ────────
    log.info("  Layer 4: fuzzy street (jaro_winkler ≥ %.2f) + num + city…", fuzzy_threshold)
    _run_join(con, "apt_fuzzy", f"""
        INSERT INTO matched
        SELECT DISTINCT ON (p.cnpj)
            p.cnpj,
            a.lat,
            a.lon,
            'apt_fuzzy' AS geo_precision
        FROM poi p
        JOIN apt a
          ON  p.city_norm              = a.city_norm
          AND left(p.street_norm, 5)   = left(a.street_norm, 5)
          AND p.num_norm               = a.num_norm
          AND p.street_norm IS NOT NULL
          AND p.num_norm    IS NOT NULL
          AND p.city_norm   IS NOT NULL
        WHERE p.cnpj NOT IN (SELECT cnpj FROM matched)
          AND jaro_winkler_similarity(p.street_norm, a.street_norm) >= {fuzzy_threshold}
        ORDER BY p.cnpj, jaro_winkler_similarity(p.street_norm, a.street_norm) DESC
    """)

    total_matched = con.execute("SELECT count(*) FROM matched").fetchone()[0]
    pct = 100 * total_matched / n_poi if n_poi else 0
    log.info("  Total matched: %s / %s (%.1f%%)", f"{total_matched:,}", f"{n_poi:,}", pct)


# ── Main run ─────────────────────────────────────────────────────────────────

def run(
    intermediate_dir: Path,
    cache_dir: Path,
    dump_date: str,
    gpkg_paths: list[Path],
    uf_filter: list[str] | None = None,
    sim_threshold: float = 0.80,
    fuzzy_threshold: float = 0.85,
    force: bool = False,
) -> Path:
    """Geocode POIs via APT local join.  Returns output Parquet path."""
    slug = "_".join(sorted(uf_filter)) if uf_filter else "all"
    out_name = f"step_04_geocoded_{dump_date}__apt_{slug}.parquet"
    out_path = intermediate_dir / out_name

    if not force and out_path.exists():
        log.info("Step 04a already done — skipping (use --force to reprocess)")
        log.info("  %s", out_path.name)
        return out_path

    cfg         = get_config()
    compression = cfg["output"]["parquet_compression"]
    row_group   = cfg["output"]["parquet_row_group_size"]
    mem         = cfg["duckdb"]["memory_limit"]

    joined_path = intermediate_dir / f"step_02_poi_joined_{dump_date}.parquet"
    if not joined_path.exists():
        raise FileNotFoundError(f"Missing step_02 output: {joined_path}")

    t_total = time.perf_counter()
    log.info("Step 04a — APT Geocoding  (UF filter: %s)", uf_filter or "all")

    # ── Pre-process each GeoPackage ───────────────────────────────────────────
    apt_index_paths = []
    for gpkg in gpkg_paths:
        apt_index_paths.append(_build_apt_index(gpkg, cache_dir))

    # Merge multiple state indexes if needed
    if len(apt_index_paths) == 1:
        apt_parquet = str(apt_index_paths[0])
    else:
        apt_parquet = "[" + ", ".join(f"'{p}'" for p in apt_index_paths) + "]"

    # ── DuckDB session ────────────────────────────────────────────────────────
    con = duckdb.connect(":memory:", config={"memory_limit": mem})
    con.create_function("norm_street", _norm_street, ["VARCHAR"], "VARCHAR")
    con.create_function("norm_num",    _norm_num,    ["VARCHAR"], "VARCHAR")
    con.create_function("norm_city",   _norm_city,   ["VARCHAR"], "VARCHAR")

    # ── Run join layers ───────────────────────────────────────────────────────
    _geocode_layers(
        con,
        poi_parquet=str(joined_path),
        apt_parquet=apt_parquet,
        uf_filter=uf_filter,
        sim_threshold=sim_threshold,
        fuzzy_threshold=fuzzy_threshold,
    )

    # ── Write output: merge geo columns into full POI table ───────────────────
    log.info("  Writing output…")
    uf_where = ""
    if uf_filter:
        ufs = ", ".join(f"'{u}'" for u in uf_filter)
        uf_where = f"WHERE p.uf IN ({ufs})"

    con.execute(f"""
        COPY (
            SELECT
                p.*,
                m.lat,
                m.lon,
                COALESCE(m.geo_precision, 'none') AS geo_precision
            FROM read_parquet('{joined_path}') p
            LEFT JOIN matched m USING (cnpj)
            {uf_where}
        ) TO '{out_path}'
        (FORMAT PARQUET, COMPRESSION '{compression}', ROW_GROUP_SIZE {row_group})
    """)

    # ── Summary ───────────────────────────────────────────────────────────────
    summary = con.execute(f"""
        SELECT geo_precision, count(*) n
        FROM read_parquet('{out_path}')
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    con.close()

    total = sum(n for _, n in summary)
    log.info("  Output: %s rows → %s (%.1f MB)",
             f"{total:,}", out_path.name, out_path.stat().st_size / 1_048_576)
    log.info("  Breakdown:")
    for prec, n in summary:
        log.info("    %-28s %s  (%.1f%%)", prec, f"{n:,}", 100 * n / total)

    log.info("Step 04a complete in %.1f s", time.perf_counter() - t_total)
    return out_path


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="APT local geocoding step")
    p.add_argument("--dump-date", required=True)
    p.add_argument("--gpkg", nargs="+", required=True,
                   help="Path(s) to Orbis Address Points GeoPackage(s)")
    p.add_argument("--uf", nargs="+", metavar="UF",
                   help="Restrict to these state codes (e.g. SP RJ)")
    p.add_argument("--sim-threshold", type=float, default=0.80,
                   help="Jaro-Winkler threshold for Layer 1 (default 0.80)")
    p.add_argument("--fuzzy-threshold", type=float, default=0.85,
                   help="Jaro-Winkler threshold for Layer 4 (default 0.85)")
    p.add_argument("--force", action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    log_dir = resolve_path("log_dir")
    setup_logging(log_dir)

    intermediate_dir = resolve_path("intermediate_dir")
    cache_dir        = resolve_path("cache_dir")

    gpkg_paths = [Path(g) for g in args.gpkg]
    for g in gpkg_paths:
        if not g.exists():
            raise FileNotFoundError(f"GeoPackage not found: {g}")

    run(
        intermediate_dir = intermediate_dir,
        cache_dir        = cache_dir,
        dump_date        = args.dump_date,
        gpkg_paths       = gpkg_paths,
        uf_filter        = args.uf,
        sim_threshold    = args.sim_threshold,
        fuzzy_threshold  = args.fuzzy_threshold,
        force            = args.force,
    )
