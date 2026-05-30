"""Step 3 — Export the joined POI Parquet to the requested output formats.

Reads:
  data/intermediate/step_02_poi_joined_<dump_date>.parquet

Writes (to data/output/<dump_date>/):
  poi.parquet
  poi.csv.gz
  poi.jsonl
  poi.geojson   (null geometry — coordinates added in the geocoding phase)

Each format is written only if the output file does not already exist,
unless --force is set.
"""

import json
import logging
import time
from pathlib import Path

import duckdb

from app.config_loader import get_config

log = logging.getLogger(__name__)


def _versioned_out_dir(output_dir: Path, dump_date: str) -> Path:
    d = output_dir / dump_date
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_parquet(con: duckdb.DuckDBPyConnection, out: Path) -> Path:
    cfg = get_config()
    comp = cfg["output"]["parquet_compression"]
    rgs  = cfg["output"]["parquet_row_group_size"]
    con.execute(f"COPY poi TO '{out}' (FORMAT PARQUET, COMPRESSION '{comp}', ROW_GROUP_SIZE {rgs})")
    return out


def write_csv(con: duckdb.DuckDBPyConnection, out: Path) -> Path:
    con.execute(f"COPY poi TO '{out}' (FORMAT CSV, HEADER true, COMPRESSION 'gzip', DELIMITER ',')")
    return out


def write_jsonl(con: duckdb.DuckDBPyConnection, out: Path) -> Path:
    con.execute(f"COPY poi TO '{out}' (FORMAT JSON, ARRAY false)")
    return out


def write_geojson(con: duckdb.DuckDBPyConnection, out: Path) -> Path:
    """GeoJSON with null geometry — placeholder until geocoding phase."""
    rows = con.execute("SELECT * FROM poi").fetchdf()
    features = [
        {
            "type": "Feature",
            "geometry": None,
            "properties": rec,
        }
        for rec in rows.to_dict(orient="records")
    ]
    fc = {"type": "FeatureCollection", "features": features}
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(fc, fh, ensure_ascii=False, default=str)
    return out


WRITERS: dict[str, tuple[str, callable]] = {
    "parquet": ("poi.parquet",  write_parquet),
    "csv":     ("poi.csv.gz",   write_csv),
    "jsonl":   ("poi.jsonl",    write_jsonl),
    "geojson": ("poi.geojson",  write_geojson),
}


def run(
    intermediate_dir: Path,
    output_dir: Path,
    dump_date: str,
    formats: list[str],
    force: bool = False,
) -> list[Path]:
    """Export joined POI to requested formats. Returns list of written paths."""
    joined_path = intermediate_dir / f"step_02_poi_joined_{dump_date}.parquet"
    if not joined_path.exists():
        raise FileNotFoundError(
            f"Missing Step 2 output: {joined_path}. Run step 2 first (or use --from-step join)."
        )

    cfg = get_config()
    mem     = cfg["duckdb"]["memory_limit"]
    threads = cfg["duckdb"]["threads"] or None

    out_dir = _versioned_out_dir(output_dir, dump_date)
    con = duckdb.connect(":memory:", config={"threads": threads, "memory_limit": mem})
    con.execute(f"CREATE VIEW poi AS SELECT * FROM '{joined_path}'")

    t0 = time.perf_counter()
    log.info("Step 3 — Exporting %s format(s) to %s…", formats, out_dir)

    written = []
    for fmt in formats:
        filename, writer_fn = WRITERS[fmt]
        dest = out_dir / filename
        if not force and dest.exists():
            log.info("  %-8s %s already exists, skipping (use --force)", fmt, dest.name)
            written.append(dest)
            continue
        log.info("  Writing %s…", dest.name)
        writer_fn(con, dest)
        size_mb = dest.stat().st_size / 1_048_576
        log.info("  ✓ %-8s %s  (%.1f MB)", fmt, dest.name, size_mb)
        written.append(dest)

    con.close()
    log.info("Step 3 complete in %.1f s", time.perf_counter() - t0)
    return written
