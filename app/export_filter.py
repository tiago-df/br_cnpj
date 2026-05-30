"""Filtered export from the joined POI dataset.

Reads data/intermediate/step_02_poi_joined_<dump_date>.parquet and writes
a filtered subset to a user-specified output directory.

Filters (all optional, combinable):
  --uf         One or more state codes (case-insensitive)
  --category   OSM tag, e.g. "amenity=fuel" or just "amenity"
  --cnae       One or more raw CNAE codes, e.g. 4731800

Output formats: parquet (default), csv, jsonl, geojson

Usage examples:
  python -m app.export_filter --uf RR --category amenity=fuel /mnt/disk1/export/
  python -m app.export_filter --uf SP RJ --category shop=supermarket /tmp/out/
  python -m app.export_filter --category tourism=hotel --format csv /mnt/disk1/hotels/
  python -m app.export_filter --uf AM --dump-date 2026-04 /mnt/disk1/am/
  python -m app.export_filter --count --category amenity=fuel
"""

import argparse
import json
import logging
import sys
import time
from datetime import date
from pathlib import Path

import duckdb

from app.config_loader import get_config, resolve_path
from app.utils.logging_utils import setup_logging

log = logging.getLogger(__name__)


def _build_where_clauses(
    uf: list[str] | None,
    category: str | None,
    cnae: list[str] | None,
) -> str:
    clauses = []

    if uf:
        uf_list = ", ".join(f"'{u.upper()}'" for u in uf)
        clauses.append(f"uf IN ({uf_list})")

    if category:
        if "=" in category:
            # Exact match: amenity=fuel
            clauses.append(f"osm_category = '{category}'")
        else:
            # Key-only match: amenity → matches amenity=fuel, amenity=restaurant …
            clauses.append(f"osm_category LIKE '{category}=%'")

    if cnae:
        cnae_list = ", ".join(f"'{c}'" for c in cnae)
        clauses.append(f"cnae_fiscal_principal IN ({cnae_list})")

    return ("WHERE " + " AND ".join(clauses)) if clauses else ""


def _safe_filename(category: str | None, uf: list[str] | None) -> str:
    parts = []
    if category:
        parts.append(category.replace("=", "_").replace("/", "-"))
    if uf:
        parts.append("_".join(u.upper() for u in sorted(uf)))
    return "_".join(parts) if parts else "poi_export"


def run(
    joined_path: Path,
    out_dir: Path,
    uf: list[str] | None = None,
    category: str | None = None,
    cnae: list[str] | None = None,
    formats: list[str] | None = None,
    count_only: bool = False,
) -> list[Path]:
    """Filter and export. Returns list of written file paths."""
    if not joined_path.exists():
        raise FileNotFoundError(
            f"Joined dataset not found: {joined_path}. "
            "Run the full pipeline first (python -m app.main)."
        )

    cfg = get_config()
    mem     = cfg["duckdb"]["memory_limit"]
    threads = cfg["duckdb"]["threads"] or None

    con = duckdb.connect(":memory:", config={"threads": threads, "memory_limit": mem})
    con.execute(f"CREATE VIEW poi AS SELECT * FROM '{joined_path}'")

    where = _build_where_clauses(uf, category, cnae)

    if count_only:
        n = con.execute(f"SELECT count(*) FROM poi {where}").fetchone()[0]
        log.info("Matching records: %s", f"{n:,}")
        con.close()
        return []

    out_dir.mkdir(parents=True, exist_ok=True)
    base_name = _safe_filename(category, uf)
    formats = formats or ["parquet"]

    written = []
    t0 = time.perf_counter()

    for fmt in formats:
        if fmt == "parquet":
            comp = cfg["output"]["parquet_compression"]
            rgs  = cfg["output"]["parquet_row_group_size"]
            dest = out_dir / f"{base_name}.parquet"
            con.execute(f"""
                COPY (SELECT * FROM poi {where})
                TO '{dest}'
                (FORMAT PARQUET, COMPRESSION '{comp}', ROW_GROUP_SIZE {rgs})
            """)

        elif fmt == "csv":
            dest = out_dir / f"{base_name}.csv.gz"
            con.execute(f"""
                COPY (SELECT * FROM poi {where})
                TO '{dest}'
                (FORMAT CSV, HEADER true, COMPRESSION 'gzip', DELIMITER ',')
            """)

        elif fmt == "jsonl":
            dest = out_dir / f"{base_name}.jsonl"
            con.execute(f"""
                COPY (SELECT * FROM poi {where})
                TO '{dest}'
                (FORMAT JSON, ARRAY false)
            """)

        elif fmt == "geojson":
            dest = out_dir / f"{base_name}.geojson"
            rows = con.execute(f"SELECT * FROM poi {where}").fetchdf()
            features = [
                {"type": "Feature", "geometry": None, "properties": rec}
                for rec in rows.to_dict(orient="records")
            ]
            with open(dest, "w", encoding="utf-8") as fh:
                json.dump({"type": "FeatureCollection", "features": features},
                          fh, ensure_ascii=False, default=str)

        else:
            log.error("Unknown format: %s", fmt)
            continue

        size_mb = dest.stat().st_size / 1_048_576
        log.info("  ✓ %-8s %s  (%.1f MB)", fmt, dest, size_mb)
        written.append(dest)

    row_count = con.execute(f"SELECT count(*) FROM poi {where}").fetchone()[0]
    con.close()

    log.info("Exported %s records in %.1f s", f"{row_count:,}", time.perf_counter() - t0)
    return written


def main():
    setup_logging(resolve_path("log_dir"))

    parser = argparse.ArgumentParser(
        description="Filtered POI export",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "out_dir", nargs="?", default=None,
        help="Output directory path (required unless --count is set)",
    )
    parser.add_argument(
        "--dump-date", default=date.today().strftime("%Y-%m"),
        help="Dump month used in pipeline run, e.g. 2026-05 (default: current month)",
    )
    parser.add_argument("--uf",       nargs="+", metavar="UF",   help="State code(s), e.g. RR SP RJ")
    parser.add_argument("--category", metavar="TAG",
                        help='OSM tag to filter. Exact: "amenity=fuel". Key-only: "amenity"')
    parser.add_argument("--cnae",     nargs="+", metavar="CODE", help="Raw CNAE code(s), e.g. 4731800")
    parser.add_argument(
        "--format", nargs="+", choices=["parquet", "csv", "jsonl", "geojson"],
        default=["parquet"], metavar="FMT",
    )
    parser.add_argument("--count", action="store_true",
                        help="Print matching row count and exit (no file written)")
    args = parser.parse_args()

    if not args.count and not args.out_dir:
        parser.error("out_dir is required unless --count is set")

    if not args.uf and not args.category and not args.cnae and not args.count:
        log.warning("No filters specified — exporting the entire dataset")

    intermediate_dir = resolve_path("intermediate_dir")
    joined_path = intermediate_dir / f"step_02_poi_joined_{args.dump_date}.parquet"
    out_dir = Path(args.out_dir) if args.out_dir else None

    log.info("Export filter")
    log.info("  Source  : %s", joined_path)
    log.info("  UF      : %s", args.uf or "all")
    log.info("  Category: %s", args.category or "all")
    log.info("  CNAE    : %s", args.cnae or "all")
    log.info("  Formats : %s", args.format)
    if out_dir:
        log.info("  Out dir : %s", out_dir)

    try:
        run(
            joined_path=joined_path,
            out_dir=out_dir,
            uf=args.uf,
            category=args.category,
            cnae=args.cnae,
            formats=args.format,
            count_only=args.count,
        )
    except FileNotFoundError as exc:
        log.error("%s", exc)
        sys.exit(1)
    except Exception as exc:
        log.exception("Export failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
