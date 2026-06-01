"""Step 04a — Geocode POIs using TomTom Orbis Address Points (local join).

Processes POIs in chunks, saving progress to a checkpoint parquet after
every chunk so the run can be interrupted and resumed at any point.

Join strategy (4 layers, applied in order within each chunk):

  Layer 1 — CEP + housenumber + street similarity ≥ sim_threshold
             geo_precision = "apt_cep_num_street"   (highest confidence)

  Layer 2 — CEP + housenumber, unique match only (no street ambiguity)
             geo_precision = "apt_cep_num"

  Layer 3 — Normalised street + housenumber + city (exact match)
             geo_precision = "apt_street_exact"

  Layer 4 — Fuzzy street (jaro_winkler ≥ fuzzy_threshold) + housenumber + city
             Blocked by city + first 5 chars of street
             geo_precision = "apt_fuzzy"

Checkpoint / resume:
  Progress is appended to data/cache/apt_progress_{date}_{slug}.parquet
  after every chunk.  On restart, already-matched CNPJs are skipped.
  The final geocoded parquet is rebuilt from the checkpoint at the end
  (or at any time with --from-checkpoint).

Usage:
    python -m app.step_04a_apt_geocode \\
        --dump-date 2026-05-10 \\
        --gpkg data/input/Orbis_Address_Points_BRA_SP.gpkg \\
        --uf SP

    # Full Brazil (multiple GeoPackages):
    python -m app.step_04a_apt_geocode \\
        --dump-date 2026-05-10 \\
        --gpkg data/input/Orbis_Address_Points_BRA_*.gpkg
"""

import argparse
import logging
import re
import time
import unicodedata
from pathlib import Path

import duckdb
import pandas as pd
from tqdm import tqdm

from app.config_loader import get_config, resolve_path
from app.utils.logging_utils import setup_logging

log = logging.getLogger(__name__)

# ── Street name normalisation ────────────────────────────────────────────────

_ABBREV = [
    (r"\bR\b",    "RUA"),   (r"\bAV\b",   "AVENIDA"),
    (r"\bAVDA\b", "AVENIDA"), (r"\bAL\b",  "ALAMEDA"),
    (r"\bALM\b",  "ALAMEDA"), (r"\bTV\b",  "TRAVESSA"),
    (r"\bTRAV\b", "TRAVESSA"),(r"\bPC\b",  "PRACA"),
    (r"\bPCA\b",  "PRACA"),  (r"\bPRA\b",  "PRACA"),
    (r"\bLG\b",   "LARGO"),  (r"\bROD\b",  "RODOVIA"),
    (r"\bEST\b",  "ESTRADA"),(r"\bBC\b",   "BECO"),
    (r"\bLD\b",   "LADEIRA"),(r"\bVL\b",   "VILA"),
    (r"\bJD\b",   "JARDIM"), (r"\bCOND\b", "CONDOMINIO"),
]
_ABBREV_RE = [(re.compile(p, re.IGNORECASE), r) for p, r in _ABBREV]
_PREP_RE   = re.compile(r"\b(DE|DA|DO|DAS|DOS|E)\b")
_SPACE_RE  = re.compile(r"\s+")
_SN_VALUES = {"S/N", "SN", "S N", "SEM NUMERO", "SEM NÚMERO", "0", "", "N/A", "S"}


def _norm_street(s: str | None) -> str | None:
    if not s:
        return None
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.upper().strip()
    for pattern, replacement in _ABBREV_RE:
        s = pattern.sub(replacement, s)
    s = _PREP_RE.sub("", s)
    return _SPACE_RE.sub(" ", s).strip() or None


def _norm_num(s: str | None) -> str | None:
    if not s:
        return None
    s = str(s).strip().upper()
    if s in _SN_VALUES:
        return None
    m = re.match(r"^(\d+)", s)
    return m.group(1) if m else None


def _norm_city(s: str | None) -> str | None:
    if not s:
        return None
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return _SPACE_RE.sub(" ", s.upper().strip()) or None


# ── APT pre-processing ───────────────────────────────────────────────────────

def _build_apt_index(gpkg_path: Path, cache_dir: Path) -> Path:
    """Convert GeoPackage → normalised Parquet index (cached, runs once).

    v2: adds suburb_norm (normalised suburb for bairro matching in layers 3+4).
    Old _v1 cache files (no suffix) are superseded — the new name forces a
    one-time rebuild automatically.
    """
    index_path = cache_dir / f"apt_index_{gpkg_path.stem}_v2.parquet"
    if index_path.exists():
        log.info("  APT index cached: %s", index_path.name)
        return index_path

    log.info("  Pre-processing %s → Parquet (runs once)…", gpkg_path.name)
    t0 = time.perf_counter()

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.create_function("norm_street", _norm_street, ["VARCHAR"], "VARCHAR", null_handling="special")
    con.create_function("norm_num",    _norm_num,    ["VARCHAR"], "VARCHAR", null_handling="special")
    con.create_function("norm_city",   _norm_city,   ["VARCHAR"], "VARCHAR", null_handling="special")

    con.execute(f"""
        COPY (
            SELECT
                regexp_replace("postcode:pt-Latn", '[^0-9]', '', 'g') AS cep_norm,
                norm_num(housenumber)                                   AS num_norm,
                housenumber                                             AS housenumber_raw,
                norm_street("street:pt-Latn")                          AS street_norm,
                "street:pt-Latn"                                       AS street_raw,
                norm_city("city:pt-Latn")                              AS city_norm,
                norm_city("suburb:pt-Latn")                            AS suburb_norm,
                "suburb:pt-Latn"                                       AS suburb_raw,
                ST_Y(geom)                                             AS lat,
                ST_X(geom)                                             AS lon
            FROM ST_Read('{gpkg_path}')
            WHERE geom IS NOT NULL
        ) TO '{index_path}'
        (FORMAT PARQUET, COMPRESSION 'zstd', ROW_GROUP_SIZE 122880)
    """)
    con.close()

    n    = duckdb.execute(f"SELECT count(*) FROM read_parquet('{index_path}')").fetchone()[0]
    size = index_path.stat().st_size / 1_048_576
    log.info("  APT index: %s rows → %.1f MB in %.1f s",
             f"{n:,}", size, time.perf_counter() - t0)
    return index_path


# ── Checkpoint helpers ───────────────────────────────────────────────────────

def _load_checkpoint(path: Path) -> dict[str, tuple]:
    """Return {cnpj: (lat, lon, geo_precision)} for already-matched records."""
    if not path.exists():
        return {}
    rows = duckdb.execute(
        f"SELECT cnpj, lat, lon, geo_precision FROM read_parquet('{path}')"
    ).fetchall()
    return {r[0]: (r[1], r[2], r[3]) for r in rows}


def _save_checkpoint(matched: dict[str, tuple], path: Path) -> None:
    """Write the full matched dict to the checkpoint parquet."""
    if not matched:
        return
    df = pd.DataFrame(
        [(cnpj, lat, lon, prec) for cnpj, (lat, lon, prec) in matched.items()],
        columns=["cnpj", "lat", "lon", "geo_precision"],
    )
    con = duckdb.connect()
    con.register("df", df)
    con.execute(
        f"COPY df TO '{path}' (FORMAT PARQUET, COMPRESSION 'zstd')"
    )
    con.close()


# ── Per-chunk join ───────────────────────────────────────────────────────────

def _process_chunk(
    con: duckdb.DuckDBPyConnection,
    chunk_cnpjs: list[str],
    already_matched: set[str],
    sim_threshold: float,
    fuzzy_threshold: float,
) -> list[tuple]:
    """Run 4-layer join for a chunk of CNPJs.  Returns [(cnpj, lat, lon, prec)]."""

    # Register this chunk as a temp table
    cnpj_vals = ", ".join(f"('{c}')" for c in chunk_cnpjs)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE chunk_cnpjs (cnpj VARCHAR);
        INSERT INTO chunk_cnpjs VALUES {cnpj_vals};
    """)

    # POI subset for this chunk (join against full poi_full)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE chunk AS
        SELECT p.*
        FROM poi_full p
        JOIN chunk_cnpjs c USING (cnpj)
    """)

    results: list[tuple] = []
    matched_in_chunk: set[str] = set()

    # ── Helper: run one layer SQL, collect results ────────────────────────────
    def run_layer(sql: str, label: str) -> None:
        rows = con.execute(sql).fetchall()
        for cnpj, lat, lon in rows:
            if cnpj not in matched_in_chunk and cnpj not in already_matched:
                results.append((cnpj, lat, lon, label))
                matched_in_chunk.add(cnpj)

    # Pre-filter APT to relevant CEPs and cities (huge performance gain).
    # suburb_clean: strip parenthetical suffixes from APT suburb_norm inline
    # so L3/L4 bairro comparisons use the same normalisation as poi bairro_norm.
    con.execute("""
        CREATE OR REPLACE TEMP TABLE apt_cep_filter AS
        SELECT a.*,
               regexp_replace(
                   regexp_replace(coalesce(a.suburb_norm,''), '\\s*\\([^)]*\\)\\s*', '', 'g'),
               '\\s+', ' ', 'g') AS suburb_clean
        FROM apt a
        JOIN (SELECT DISTINCT cep_norm FROM chunk WHERE cep_norm IS NOT NULL) f
          ON a.cep_norm = f.cep_norm
    """)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE apt_city_filter AS
        SELECT a.*,
               regexp_replace(
                   regexp_replace(coalesce(a.suburb_norm,''), '\\s*\\([^)]*\\)\\s*', '', 'g'),
               '\\s+', ' ', 'g') AS suburb_clean
        FROM apt a
        JOIN (SELECT DISTINCT city_norm FROM chunk WHERE city_norm IS NOT NULL) f
          ON a.city_norm = f.city_norm
    """)

    # ── Layer 1: CEP + num + street similarity (threshold 0.90) ─────────────────
    # Higher threshold reduces false-positives on large rural CEPs.
    run_layer(f"""
        SELECT DISTINCT ON (p.cnpj)
            p.cnpj, a.lat, a.lon
        FROM chunk p
        JOIN apt_cep_filter a
          ON  p.cep_norm = a.cep_norm
          AND p.num_norm = a.num_norm
          AND p.cep_norm IS NOT NULL
          AND p.num_norm IS NOT NULL
          AND p.street_norm IS NOT NULL
          AND a.street_norm IS NOT NULL
        WHERE jaro_winkler_similarity(p.street_norm, a.street_norm) >= {sim_threshold}
        ORDER BY p.cnpj, jaro_winkler_similarity(p.street_norm, a.street_norm) DESC
    """, "apt_cep_num_street")

    # Layer 2 (CEP + num only) removed — same CEP can cover many streets,
    # producing false positives especially in small towns with a single CEP.

    # ── Layer 3: street_norm + num + city + bairro disambiguation ────────────
    # Bairro exact match (after aggressive normalisation: parentheses stripped,
    # prepositions removed) disambiguates identical street+num+city combinations
    # in different neighbourhoods (e.g. "RUA SANTO ANTONIO" in multiple bairros).
    # Accept if: exactly 1 total candidate (bairro unavailable / not useful),
    #         OR bairro uniquely narrows candidates to exactly 1 match.
    # suburb_clean already has parenthetical suffixes stripped (computed in pre-filter).
    already_sql = (
        f"AND p.cnpj NOT IN ({', '.join(repr(c) for c in matched_in_chunk)})"
        if matched_in_chunk else ""
    )
    run_layer(f"""
        WITH cands AS (
            SELECT
                p.cnpj, a.lat, a.lon,
                CASE
                    WHEN p.bairro_norm IS NOT NULL
                     AND a.suburb_clean IS NOT NULL
                     AND p.bairro_norm = a.suburb_clean
                     AND length(p.bairro_norm) > 0
                     AND length(a.suburb_clean) > 0 THEN 1
                    ELSE 0
                END AS bairro_match,
                count(*) OVER (PARTITION BY p.cnpj)         AS n_total,
                sum(CASE
                        WHEN p.bairro_norm IS NOT NULL
                         AND a.suburb_clean IS NOT NULL
                         AND p.bairro_norm = a.suburb_clean
                         AND length(p.bairro_norm) > 0
                         AND length(a.suburb_clean) > 0 THEN 1
                        ELSE 0
                    END) OVER (PARTITION BY p.cnpj)          AS n_bairro
            FROM chunk p
            JOIN apt_city_filter a
              ON  p.street_norm = a.street_norm
              AND p.num_norm    = a.num_norm
              AND p.city_norm   = a.city_norm
              AND p.street_norm IS NOT NULL
              AND p.num_norm    IS NOT NULL
            WHERE 1=1 {already_sql}
        )
        SELECT DISTINCT ON (cnpj) cnpj, lat, lon
        FROM cands
        WHERE n_total = 1          -- unique city+street+num match (no bairro needed)
           OR n_bairro = 1         -- bairro uniquely identifies one candidate
        ORDER BY cnpj, bairro_match DESC
    """, "apt_street_exact")

    # ── Layer 4: fuzzy street + num + city + bairro JW blocking ─────────────
    # Bairro similarity (JW >= 0.80) used as soft blocking:
    #   - Skipped when either side has no bairro/suburb data (preserves recall)
    #   - JW 0.80 tolerates abbreviation differences ("VL ANTONIO" ≈ "VILA ANTONIO")
    #     and minor spelling variants, while rejecting cross-neighbourhood matches
    #     ("VILA ANTONIO" ≠ "JARDIM AMERICA", JW ~0.60)
    # suburb_clean has parenthetical suffixes stripped (computed in pre-filter).
    already_sql = (
        f"AND p.cnpj NOT IN ({', '.join(repr(c) for c in matched_in_chunk)})"
        if matched_in_chunk else ""
    )
    run_layer(f"""
        SELECT DISTINCT ON (p.cnpj)
            p.cnpj, a.lat, a.lon
        FROM chunk p
        JOIN apt_city_filter a
          ON  p.city_norm            = a.city_norm
          AND left(p.street_norm, 5) = left(a.street_norm, 5)
          AND p.num_norm             = a.num_norm
          AND p.street_norm IS NOT NULL
          AND p.num_norm    IS NOT NULL
          -- Bairro blocking: JW >= 0.80 when both sides have data
          AND (p.bairro_norm IS NULL
               OR length(a.suburb_clean) = 0
               OR jaro_winkler_similarity(p.bairro_norm, a.suburb_clean) >= 0.80)
        WHERE jaro_winkler_similarity(p.street_norm, a.street_norm) >= {fuzzy_threshold}
          {already_sql}
        ORDER BY p.cnpj, jaro_winkler_similarity(p.street_norm, a.street_norm) DESC
    """, "apt_fuzzy")

    return results


# ── Main run ─────────────────────────────────────────────────────────────────

def run(
    intermediate_dir: Path,
    cache_dir: Path,
    dump_date: str,
    gpkg_paths: list[Path],
    uf_filter: list[str] | None = None,
    chunk_size: int = 10_000,
    sim_threshold: float = 0.90,
    fuzzy_threshold: float = 0.85,
    force: bool = False,
) -> Path:
    """Geocode POIs via APT local join with incremental checkpointing."""
    slug     = "_".join(sorted(uf_filter)) if uf_filter else "all"
    out_name = f"step_04_geocoded_{dump_date}__apt_{slug}.parquet"
    out_path = intermediate_dir / out_name
    chk_path = cache_dir / f"apt_progress_{dump_date}_{slug}.parquet"

    cfg         = get_config()
    compression = cfg["output"]["parquet_compression"]
    row_group   = cfg["output"]["parquet_row_group_size"]
    mem         = cfg["duckdb"]["memory_limit"]

    joined_path = intermediate_dir / f"step_02_poi_joined_{dump_date}.parquet"
    if not joined_path.exists():
        raise FileNotFoundError(f"Missing step_02: {joined_path}")

    t_total = time.perf_counter()
    log.info("Step 04a — APT Geocoding  (UF: %s, chunk: %s)", uf_filter or "all", f"{chunk_size:,}")

    # ── Pre-process GeoPackages ───────────────────────────────────────────────
    apt_index_paths = [_build_apt_index(g, cache_dir) for g in gpkg_paths]
    apt_glob = (
        str(apt_index_paths[0])
        if len(apt_index_paths) == 1
        else "[" + ", ".join(f"'{p}'" for p in apt_index_paths) + "]"
    )

    # ── Load checkpoint (resume support) ─────────────────────────────────────
    matched: dict[str, tuple] = {} if force else _load_checkpoint(chk_path)
    log.info("  Checkpoint: %s CNPJs already matched", f"{len(matched):,}")

    # ── DuckDB session ────────────────────────────────────────────────────────
    con = duckdb.connect(":memory:", config={"memory_limit": mem})
    con.create_function("norm_street", _norm_street, ["VARCHAR"], "VARCHAR", null_handling="special")
    con.create_function("norm_num",    _norm_num,    ["VARCHAR"], "VARCHAR", null_handling="special")
    con.create_function("norm_city",   _norm_city,   ["VARCHAR"], "VARCHAR", null_handling="special")

    # Load APT into memory (stays for the session)
    log.info("  Loading APT index into memory…")
    t0 = time.perf_counter()
    con.execute(f"CREATE OR REPLACE TABLE apt AS SELECT * FROM read_parquet('{apt_glob}')")
    n_apt = con.execute("SELECT count(*) FROM apt").fetchone()[0]
    log.info("  APT: %s rows loaded in %.1f s", f"{n_apt:,}", time.perf_counter() - t0)

    # Load POI addresses with normalised fields.
    # CNPJ data is already uppercase ASCII — no Python UDFs needed on this side.
    # norm_num and norm_street only needed for APT (UTF-8 with accents).
    uf_where = ""
    if uf_filter:
        ufs = ", ".join(f"'{u}'" for u in uf_filter)
        uf_where = f"AND uf IN ({ufs})"

    # Abbreviation expansion via SQL CASE (top 8 most common in CNPJ data)
    abbrev_sql = """
        CASE
            WHEN regexp_matches(upper(trim(tipo_logradouro)), '^R$|^RUA$')         THEN 'RUA '
            WHEN regexp_matches(upper(trim(tipo_logradouro)), '^AV$|^AVENIDA$')    THEN 'AVENIDA '
            WHEN regexp_matches(upper(trim(tipo_logradouro)), '^TV$|^TRAV$|^TRAVESSA$') THEN 'TRAVESSA '
            WHEN regexp_matches(upper(trim(tipo_logradouro)), '^AL$|^ALM$|^ALAMEDA$')  THEN 'ALAMEDA '
            WHEN regexp_matches(upper(trim(tipo_logradouro)), '^PC$|^PCA$|^PRACA$')    THEN 'PRACA '
            WHEN regexp_matches(upper(trim(tipo_logradouro)), '^EST$|^ESTRADA$')        THEN 'ESTRADA '
            WHEN regexp_matches(upper(trim(tipo_logradouro)), '^ROD$|^RODOVIA$')        THEN 'RODOVIA '
            WHEN regexp_matches(upper(trim(tipo_logradouro)), '^LG$|^LARGO$')           THEN 'LARGO '
            ELSE upper(trim(tipo_logradouro)) || ' '
        END || upper(trim(logradouro))
    """
    con.execute(f"""
        CREATE OR REPLACE TABLE poi_full AS
        SELECT
            cnpj, tipo_logradouro, logradouro, numero,
            bairro, cep, municipio, municipio_descricao, uf,
            -- CEP: digits only
            regexp_replace(cep, '[^0-9]', '', 'g')                            AS cep_norm,
            -- Numero: leading digits, NULL for S/N
            CASE
                WHEN upper(trim(numero)) IN ('S/N','SN','S N','0','')         THEN NULL
                ELSE regexp_extract(trim(numero), '^\d+')
            END                                                                AS num_norm,
            -- Street: expand abbreviation + upper (CNPJ already ASCII)
            regexp_replace(
                regexp_replace({abbrev_sql}, '\s+(DE|DA|DO|DAS|DOS|E)\s+', ' ', 'g'),
            '\s+', ' ', 'g')                                                   AS street_norm,
            -- City: upper (CNPJ already ASCII)
            upper(trim(municipio_descricao))                                   AS city_norm,
            -- Bairro: strip parenthetical suffixes (e.g. "(ZONA NORTE)"), remove prepositions,
            --         collapse spaces. CNPJ data is already ASCII uppercase.
            regexp_replace(
                regexp_replace(
                    regexp_replace(upper(trim(bairro)), '\\s*\\([^)]*\\)\\s*', '', 'g'),
                '\\s+(DE|DA|DO|DAS|DOS|E)\\s+', ' ', 'g'),
            '\\s+', ' ', 'g')                                                  AS bairro_norm
        FROM read_parquet('{joined_path}')
        WHERE 1=1 {uf_where}
    """)
    n_poi = con.execute("SELECT count(*) FROM poi_full").fetchone()[0]
    log.info("  POIs loaded: %s", f"{n_poi:,}")

    # Get remaining CNPJs (skip already matched)
    if matched:
        matched_list = ", ".join(f"'{c}'" for c in matched)
        remaining = [r[0] for r in con.execute(
            f"SELECT cnpj FROM poi_full WHERE cnpj NOT IN ({matched_list})"
        ).fetchall()]
    else:
        remaining = [r[0] for r in con.execute(
            "SELECT cnpj FROM poi_full"
        ).fetchall()]

    log.info("  Remaining to process: %s / %s", f"{len(remaining):,}", f"{n_poi:,}")

    # ── Chunked processing ────────────────────────────────────────────────────
    chunks = [remaining[i:i + chunk_size] for i in range(0, len(remaining), chunk_size)]
    n_chunks = len(chunks)
    new_matches = 0

    pbar = tqdm(total=len(remaining), desc="APT geocoding", unit="POI")

    for chunk_idx, chunk in enumerate(chunks):
        results = _process_chunk(
            con, chunk, set(matched.keys()),
            sim_threshold, fuzzy_threshold,
        )

        for cnpj, lat, lon, prec in results:
            matched[cnpj] = (lat, lon, prec)
            new_matches += 1

        pbar.update(len(chunk))

        # Save checkpoint after every chunk
        _save_checkpoint(matched, chk_path)

        if (chunk_idx + 1) % 10 == 0 or chunk_idx == n_chunks - 1:
            pct = 100 * len(matched) / n_poi if n_poi else 0
            log.info("  Chunk %d/%d | matched: %s (%.1f%%)",
                     chunk_idx + 1, n_chunks, f"{len(matched):,}", pct)

    pbar.close()
    con.close()

    # ── Write final output ────────────────────────────────────────────────────
    log.info("  Writing final output…")
    _write_output(matched, str(joined_path), out_path, uf_filter, compression, row_group)

    # Summary
    summary = duckdb.execute(f"""
        SELECT geo_precision, count(*) n
        FROM read_parquet('{out_path}')
        GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    total = sum(n for _, n in summary)
    log.info("  Output: %s rows → %s (%.1f MB)",
             f"{total:,}", out_path.name, out_path.stat().st_size / 1_048_576)
    for prec, n in summary:
        log.info("    %-28s %s  (%.1f%%)", prec, f"{n:,}", 100 * n / total)

    log.info("Step 04a complete in %.1f s  (new matches: %s)",
             time.perf_counter() - t_total, f"{new_matches:,}")
    return out_path


def _write_output(
    matched: dict[str, tuple],
    joined_parquet: str,
    out_path: Path,
    uf_filter: list[str] | None,
    compression: str,
    row_group: int,
) -> None:
    """Join matched coordinates back into the full POI table and write parquet."""
    df_geo = pd.DataFrame(
        [(cnpj, lat, lon, prec) for cnpj, (lat, lon, prec) in matched.items()],
        columns=["cnpj", "lat", "lon", "geo_precision"],
    )
    uf_where = ""
    if uf_filter:
        ufs = ", ".join(f"'{u}'" for u in uf_filter)
        uf_where = f"WHERE p.uf IN ({ufs})"

    con = duckdb.connect()
    con.register("geo_df", df_geo)
    con.execute(f"""
        COPY (
            SELECT p.*, g.lat, g.lon,
                   COALESCE(g.geo_precision, 'none') AS geo_precision
            FROM read_parquet('{joined_parquet}') p
            LEFT JOIN geo_df g USING (cnpj)
            {uf_where}
        ) TO '{out_path}'
        (FORMAT PARQUET, COMPRESSION '{compression}', ROW_GROUP_SIZE {row_group})
    """)
    con.close()


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="APT local geocoding — chunked with checkpoint")
    p.add_argument("--dump-date", required=True)
    p.add_argument("--gpkg", nargs="+", required=True)
    p.add_argument("--uf", nargs="+", metavar="UF")
    p.add_argument("--chunk-size", type=int, default=10_000,
                   help="POIs per processing chunk (default 10000)")
    p.add_argument("--sim-threshold",   type=float, default=0.90,
                   help="Jaro-Winkler threshold for Layer 1 CEP+num+street (default 0.90)")
    p.add_argument("--fuzzy-threshold", type=float, default=0.85,
                   help="Jaro-Winkler threshold for Layer 4 fuzzy street (default 0.85)")
    p.add_argument("--force", action="store_true",
                   help="Ignore existing checkpoint and restart from scratch")
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
        intermediate_dir = resolve_path("intermediate_dir"),
        cache_dir        = resolve_path("cache_dir"),
        dump_date        = args.dump_date,
        gpkg_paths       = gpkg_paths,
        uf_filter        = args.uf,
        chunk_size       = args.chunk_size,
        sim_threshold    = args.sim_threshold,
        fuzzy_threshold  = args.fuzzy_threshold,
        force            = args.force,
    )
