"""Step 2 — Join filtered intermediates with lookup tables and persist.

Reads:
  data/intermediate/step_01_estab_<dump_date>.parquet
  data/intermediate/step_01_empresas_<dump_date>.parquet
  data/input/<dump_date>/csv/*.csv  (lookup zips extracted by zip_utils)
  app/cnae_osm_map.yaml             (CNAE → OSM tag mapping)

Writes:
  data/intermediate/step_02_poi_joined_<dump_date>.parquet

Applies optional UF / CNAE sub-filters if provided.
If the output file already exists and --force is not set, this step is skipped.
"""

import logging
import time
from pathlib import Path

import duckdb
import yaml

from app.config_loader import get_config
from app.schema import (
    CNAE_COLUMNS,
    MOTIVO_COLUMNS,
    MUNICIPIO_COLUMNS,
    NATUREZA_COLUMNS,
    PAIS_COLUMNS,
    QUALIFICACAO_COLUMNS,
)
from app.utils.zip_utils import extract_zips

log = logging.getLogger(__name__)

JOINED_OUT = "step_02_poi_joined_{date}.parquet"


def _read_csv_expr(paths: list[str], columns: list[str], encoding: str, delim: str) -> str:
    """Return a SQL expression that reads the CSVs with proper column names.

    DuckDB 1.5.x: passing encoding= breaks the delimiter sniffer, causing only
    1 column (column0) to be visible.  Without encoding, DuckDB correctly detects
    all columns with zero-padded names (column00, column01, …).

    Workaround: omit encoding so the sniffer works, then alias column00..columnN
    to the real names in a subquery.  Key filter/join fields (situacao_cadastral,
    uf, cnae, cnpj_basico) are pure ASCII so encoding is irrelevant for them.
    Text fields (names, addresses) retain their raw Windows-1252 bytes in Parquet.
    """
    path_list = ", ".join(f"'{p}'" for p in paths)
    # DuckDB zero-pads auto-generated column names: column00, column01, …
    width = len(str(len(columns) - 1))
    aliases = ", ".join(
        f"column{str(i).zfill(width)} AS {c}" for i, c in enumerate(columns)
    )
    raw_read = (
        f"read_csv([{path_list}], "
        f"header=false, sep='{delim}', quote='\"', "
        f"all_varchar=true, ignore_errors=true)"
    )
    return f"(SELECT {aliases} FROM {raw_read})"


def _load_lookup(con: duckdb.DuckDBPyConnection, versioned_dir: Path,
                 pattern: str, table: str, columns: list[str],
                 encoding: str, delim: str, required: bool = True) -> bool:
    zip_paths = sorted(versioned_dir.glob(pattern))
    if not zip_paths:
        if required:
            raise FileNotFoundError(f"Required lookup not found: {pattern} in {versioned_dir}")
        log.warning("Optional lookup not found, skipping: %s", pattern)
        return False
    csv_paths = extract_zips(zip_paths, versioned_dir)
    sql = _read_csv_expr(csv_paths, columns, encoding, delim)
    con.execute(f"CREATE OR REPLACE VIEW {table} AS SELECT * FROM {sql}")
    n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    log.debug("Loaded lookup %s: %s rows", table, f"{n:,}")
    return True


def output_exists(intermediate_dir: Path, dump_date: str) -> bool:
    return (intermediate_dir / JOINED_OUT.format(date=dump_date)).exists()


def run(
    raw_dir: Path,
    intermediate_dir: Path,
    dump_date: str,
    uf_filter: list[str] | None = None,
    cnae_filter: list[str] | None = None,
    force: bool = False,
) -> Path:
    """Join filtered intermediates → enriched POI Parquet. Returns output path."""
    joined_out = intermediate_dir / JOINED_OUT.format(date=dump_date)

    if not force and joined_out.exists():
        log.info("Step 2 already done — skipping (use --force to reprocess)")
        log.info("  %s", joined_out.name)
        return joined_out

    cfg = get_config()
    enc   = cfg["file"]["encoding"]
    delim = cfg["file"]["delimiter"]
    mem   = cfg["duckdb"]["memory_limit"]
    threads     = cfg["duckdb"]["threads"]
    compression = cfg["output"]["parquet_compression"]
    row_group   = cfg["output"]["parquet_row_group_size"]

    estab_path    = intermediate_dir / f"step_01_estab_{dump_date}.parquet"
    empresas_path = intermediate_dir / f"step_01_empresas_{dump_date}.parquet"

    for p in (estab_path, empresas_path):
        if not p.exists():
            raise FileNotFoundError(
                f"Missing Step 1 output: {p}. Run step 1 first (or use --from-step filter)."
            )

    db_cfg = {"memory_limit": mem}
    if threads:
        db_cfg["threads"] = threads
    con = duckdb.connect(":memory:", config=db_cfg)

    # ── Load OSM category map ─────────────────────────────────────────────
    osm_map_path = Path(__file__).parent / "cnae_osm_map.yaml"
    with open(osm_map_path, "r", encoding="utf-8") as fh:
        osm_map: dict = yaml.safe_load(fh) or {}

    if osm_map:
        values_sql = ", ".join(f"('{k}', '{v}')" for k, v in osm_map.items())
        con.execute(f"""
            CREATE TEMP TABLE osm_map (cnae_code VARCHAR, osm_category VARCHAR);
            INSERT INTO osm_map VALUES {values_sql};
        """)
        log.debug("Loaded %d CNAE → OSM mappings", len(osm_map))
    else:
        con.execute("CREATE TEMP TABLE osm_map (cnae_code VARCHAR, osm_category VARCHAR)")
        log.warning("OSM map is empty — osm_category will be null for all records")

    versioned_dir = raw_dir / dump_date

    t0 = time.perf_counter()
    log.info("Step 2 — Loading intermediates…")

    con.execute(f"CREATE VIEW estab    AS SELECT * FROM '{estab_path}'")
    con.execute(f"CREATE VIEW empresas AS SELECT * FROM '{empresas_path}'")

    # Lookups: extract zip → read CSV
    _load_lookup(con, versioned_dir, "Cnaes.zip",     "lu_cnae",      CNAE_COLUMNS,      enc, delim)
    _load_lookup(con, versioned_dir, "Naturezas.zip",  "lu_natureza",  NATUREZA_COLUMNS,  enc, delim)
    _load_lookup(con, versioned_dir, "Municipios.zip", "lu_municipio", MUNICIPIO_COLUMNS, enc, delim)

    for pat, tbl, cols in [
        ("Motivos.zip",       "lu_motivo",      MOTIVO_COLUMNS),
        ("Paises.zip",        "lu_pais",        PAIS_COLUMNS),
        ("Qualificacoes.zip", "lu_qualificacao", QUALIFICACAO_COLUMNS),
    ]:
        _load_lookup(con, versioned_dir, pat, tbl, cols, enc, delim, required=False)

    extra = []
    if uf_filter:
        uf_list = ", ".join(f"'{u.upper()}'" for u in uf_filter)
        extra.append(f"e.uf IN ({uf_list})")
    if cnae_filter:
        cnae_list = ", ".join(f"'{c}'" for c in cnae_filter)
        extra.append(f"e.cnae_fiscal_principal IN ({cnae_list})")

    extra_sql = ("AND " + " AND ".join(extra)) if extra else ""

    log.info("Step 2 — Joining and enriching…")
    con.execute(f"""
        COPY (
            SELECT
                printf('%08s%04s%02s', e.cnpj_basico, e.cnpj_ordem, e.cnpj_dv) AS cnpj,
                e.cnpj_basico,
                emp.razao_social,
                e.nome_fantasia,
                emp.natureza_juridica,
                COALESCE(n.descricao, emp.natureza_juridica)   AS natureza_juridica_descricao,
                emp.porte,
                emp.capital_social,
                e.cnae_fiscal_principal,
                COALESCE(c.descricao, e.cnae_fiscal_principal) AS cnae_fiscal_principal_descricao,
                e.cnae_fiscal_secundaria,
                om.osm_category,
                e.identificador_matriz_filial,
                e.situacao_cadastral,
                e.data_situacao_cadastral,
                e.data_inicio_atividade,
                e.tipo_logradouro,
                e.logradouro,
                e.numero,
                e.complemento,
                e.bairro,
                e.cep,
                e.municipio,
                COALESCE(m.descricao, e.municipio)             AS municipio_descricao,
                e.uf,
                e.ddd1,
                e.telefone1,
                e.ddd2,
                e.telefone2,
                e.correio_eletronico
            FROM estab e
            JOIN empresas emp USING (cnpj_basico)
            LEFT JOIN lu_cnae      c  ON c.codigo  = e.cnae_fiscal_principal
            LEFT JOIN lu_natureza  n  ON n.codigo  = emp.natureza_juridica
            LEFT JOIN lu_municipio m  ON m.codigo  = e.municipio
            LEFT JOIN osm_map      om ON om.cnae_code = e.cnae_fiscal_principal
            {extra_sql}
        ) TO '{joined_out}'
        (FORMAT PARQUET, COMPRESSION '{compression}', ROW_GROUP_SIZE {row_group})
    """)

    row_count = con.execute(f"SELECT count(*) FROM '{joined_out}'").fetchone()[0]
    con.close()

    log.info("  Wrote %s POI rows → %s (%.1f MB)",
             f"{row_count:,}", joined_out.name, joined_out.stat().st_size / 1_048_576)
    log.info("Step 2 complete in %.1f s", time.perf_counter() - t0)
    return joined_out
