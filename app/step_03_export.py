"""Step 3 — Export the joined POI Parquet to the requested output formats.

Reads (in priority order):
  data/intermediate/step_04_geocoded_<dump_date>.parquet   (with lat/lon, preferred)
  data/intermediate/step_02_poi_joined_<dump_date>.parquet (fallback, no coordinates)

Writes (to data/output/<dump_date>/):
  poi.parquet
  poi.csv.gz
  poi.jsonl
  poi.geojson   (Point geometry when lat/lon available; null geometry otherwise)

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
    """GeoJSON — Point geometry when lat/lon are available, null otherwise."""
    rows = con.execute("SELECT * FROM poi").fetchdf()
    has_coords = "lat" in rows.columns and "lon" in rows.columns

    features = []
    for rec in rows.to_dict(orient="records"):
        lat = rec.pop("lat", None) if has_coords else None
        lon = rec.pop("lon", None) if has_coords else None
        try:
            geometry = (
                {"type": "Point", "coordinates": [float(lon), float(lat)]}
                if (lat is not None and lon is not None)
                else None
            )
        except (TypeError, ValueError):
            geometry = None
        features.append({"type": "Feature", "geometry": geometry, "properties": rec})

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
    source_parquet: Path | None = None,
) -> list[Path]:
    """Export POI Parquet to requested formats. Returns list of written paths.

    source_parquet: explicit path override (e.g. step_04_geocoded_*.parquet).
                    Defaults to step_02_poi_joined_*.parquet if not provided.
    """
    if source_parquet is not None:
        joined_path = source_parquet
        log.info("  Source: %s", joined_path.name)
    else:
        joined_path = intermediate_dir / f"step_02_poi_joined_{dump_date}.parquet"

    if not joined_path.exists():
        raise FileNotFoundError(
            f"Missing source Parquet: {joined_path}. Run upstream steps first."
        )

    cfg = get_config()
    mem     = cfg["duckdb"]["memory_limit"]
    threads = cfg["duckdb"]["threads"]  # 0 = DuckDB default (all cores)

    out_dir = _versioned_out_dir(output_dir, dump_date)
    db_cfg = {"memory_limit": mem}
    if threads:
        db_cfg["threads"] = threads
    con = duckdb.connect(":memory:", config=db_cfg)
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
