"""Step 1 — Filter raw dump files and persist intermediate Parquet snapshots.

Reads zipped CSVs directly from the versioned input directory.
Applies first-pass filters independently on each entity type:
  - Estabelecimentos: keep situacao_cadastral = '02' (active only)
  - Empresas: drop MEI by porte and natureza_juridica

Writes two intermediate Parquet files:
  data/intermediate/step_01_estab_<dump_date>.parquet
  data/intermediate/step_01_empresas_<dump_date>.parquet

If both files already exist and --force is not set, this step is skipped.
"""

import logging
import time
from pathlib import Path

import duckdb

from app.config_loader import get_config
from app.schema import EMPRESAS_COLUMNS, ESTAB_COLUMNS

log = logging.getLogger(__name__)

ESTAB_OUT    = "step_01_estab_{date}.parquet"
EMPRESAS_OUT = "step_01_empresas_{date}.parquet"


def _glob_zips(raw_dir: Path, pattern: str) -> list[str]:
    paths = sorted(raw_dir.glob(pattern))
    if not paths:
        raise FileNotFoundError(f"No files matching '{pattern}' in {raw_dir}. Run download first.")
    return [str(p) for p in paths]


def _read_csv_expr(paths: list[str], columns: list[str], encoding: str, delim: str) -> str:
    path_list = ", ".join(f"'{p}'" for p in paths)
    col_names = ", ".join(f"'{c}'" for c in columns)
    return (
        f"read_csv([{path_list}], "
        f"header=false, sep='{delim}', encoding='{encoding}', "
        f"column_names=[{col_names}], all_varchar=true, ignore_errors=true)"
    )


def outputs_exist(intermediate_dir: Path, dump_date: str) -> bool:
    estab    = intermediate_dir / ESTAB_OUT.format(date=dump_date)
    empresas = intermediate_dir / EMPRESAS_OUT.format(date=dump_date)
    return estab.exists() and empresas.exists()


def run(
    raw_dir: Path,
    intermediate_dir: Path,
    dump_date: str,
    force: bool = False,
) -> tuple[Path, Path]:
    """Filter raw zips → intermediate Parquet. Returns (estab_path, empresas_path)."""
    estab_out    = intermediate_dir / ESTAB_OUT.format(date=dump_date)
    empresas_out = intermediate_dir / EMPRESAS_OUT.format(date=dump_date)

    if not force and estab_out.exists() and empresas_out.exists():
        log.info("Step 1 already done — skipping (use --force to reprocess)")
        log.info("  %s", estab_out.name)
        log.info("  %s", empresas_out.name)
        return estab_out, empresas_out

    cfg = get_config()
    enc  = cfg["file"]["encoding"]
    delim = cfg["file"]["delimiter"]
    mem  = cfg["duckdb"]["memory_limit"]
    threads = cfg["duckdb"]["threads"]  # 0 = DuckDB default (all cores)
    compression = cfg["output"]["parquet_compression"]
    row_group   = cfg["output"]["parquet_row_group_size"]

    filters = cfg["filters"]
    situacao_ativa       = filters["situacao_ativa"]
    excluir_porte_mei    = filters["excluir_porte_mei"]
    excluir_natureza_mei = filters["excluir_natureza_mei"]

    db_cfg = {"memory_limit": mem}
    if threads:
        db_cfg["threads"] = threads
    con = duckdb.connect(":memory:", config=db_cfg)

    versioned_dir = raw_dir / dump_date

    t0 = time.perf_counter()
    log.info("Step 1 — Filtering Estabelecimentos (situacao_cadastral = %s)…", situacao_ativa)

    estab_paths = _glob_zips(versioned_dir, "Estabelecimentos*.zip")
    estab_sql   = _read_csv_expr(estab_paths, ESTAB_COLUMNS, enc, delim)

    con.execute(f"""
        COPY (
            SELECT * FROM {estab_sql}
            WHERE situacao_cadastral = '{situacao_ativa}'
        ) TO '{estab_out}'
        (FORMAT PARQUET, COMPRESSION '{compression}', ROW_GROUP_SIZE {row_group})
    """)
    estab_count = con.execute(f"SELECT count(*) FROM '{estab_out}'").fetchone()[0]
    log.info("  Wrote %s rows → %s (%.1f MB)",
             f"{estab_count:,}", estab_out.name, estab_out.stat().st_size / 1_048_576)

    log.info("Step 1 — Filtering Empresas (excluding MEI)…")
    emp_paths = _glob_zips(versioned_dir, "Empresas*.zip")
    emp_sql   = _read_csv_expr(emp_paths, EMPRESAS_COLUMNS, enc, delim)

    con.execute(f"""
        COPY (
            SELECT * FROM {emp_sql}
            WHERE porte != '{excluir_porte_mei}'
              AND natureza_juridica != '{excluir_natureza_mei}'
        ) TO '{empresas_out}'
        (FORMAT PARQUET, COMPRESSION '{compression}', ROW_GROUP_SIZE {row_group})
    """)
    emp_count = con.execute(f"SELECT count(*) FROM '{empresas_out}'").fetchone()[0]
    log.info("  Wrote %s rows → %s (%.1f MB)",
             f"{emp_count:,}", empresas_out.name, empresas_out.stat().st_size / 1_048_576)

    con.close()
    log.info("Step 1 complete in %.1f s", time.perf_counter() - t0)
    return estab_out, empresas_out
