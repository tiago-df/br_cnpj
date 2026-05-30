"""BR CNPJ → POI Pipeline — main orchestrator.

Runs the full pipeline or resumes from a specific step.

Steps:
  0  download   Download raw dump zips from Receita Federal (auto-detects latest)
  1  filter     Filter raw zips → intermediate Parquet (step_01_*)
  2  join       Join + enrich intermediates → joined Parquet (step_02_*)
  3  export     Export joined Parquet → final output formats (step_03_*)

Usage examples:
  python -m app.main                             # full run, auto-detect latest dump
  python -m app.main --dump-date 2026-04-12      # specific dump date
  python -m app.main --from-step join --dump-date 2026-04-12
  python -m app.main --skip-download --dump-date 2026-04-12
  python -m app.main --format parquet csv
  python -m app.main --uf SP RJ
  python -m app.main --cnae 4731800
  python -m app.main --force
  python -m app.main --count
"""

import argparse
import logging
import sys
import time

from app import download, step_01_filter, step_02_join, step_03_export
from app.config_loader import resolve_path
from app.utils.logging_utils import setup_logging

STEP_ORDER = ["download", "filter", "join", "export"]
FORMATS    = list(step_03_export.WRITERS.keys())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="BR CNPJ → POI pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dump-date", default=None,
        help="Dump date YYYY-MM-DD (default: auto-detect latest from source)",
    )
    parser.add_argument(
        "--from-step", choices=STEP_ORDER, default="download",
        metavar="STEP",
        help=f"Resume from step. Choices: {STEP_ORDER} (default: download)",
    )
    parser.add_argument(
        "--skip-download", action="store_true",
        help="Alias for --from-step filter (requires --dump-date)",
    )
    parser.add_argument(
        "--format", nargs="+", choices=FORMATS, default=["parquet"],
        metavar="FMT",
        help=f"Output format(s). Choices: {FORMATS} (default: parquet)",
    )
    parser.add_argument("--uf",   nargs="+", metavar="UF")
    parser.add_argument("--cnae", nargs="+", metavar="CODE")
    parser.add_argument("--force", action="store_true",
                        help="Reprocess all steps even if intermediates exist")
    parser.add_argument("--count", action="store_true",
                        help="Print POI row count from step 2 output then exit")
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()

    log_dir = resolve_path("log_dir")
    log = setup_logging(log_dir, level=logging.DEBUG if args.verbose else logging.INFO)

    if args.skip_download:
        args.from_step = "filter"
        if not args.dump_date:
            log.error("--skip-download requires --dump-date YYYY-MM-DD")
            sys.exit(1)

    start_idx = STEP_ORDER.index(args.from_step)

    input_dir        = resolve_path("input_dir")
    intermediate_dir = resolve_path("intermediate_dir")
    output_dir       = resolve_path("output_dir")

    dump_date = args.dump_date  # may be None — download step will resolve it

    log.info("=" * 60)
    log.info("BR CNPJ → POI Pipeline")
    log.info("Dump date : %s", dump_date or "auto-detect latest")
    log.info("From step : %s", args.from_step)
    log.info("Formats   : %s", args.format)
    if args.uf:
        log.info("UF filter : %s", args.uf)
    if args.cnae:
        log.info("CNAE filter: %s", args.cnae)
    log.info("Force     : %s", args.force)
    log.info("=" * 60)

    t_total = time.perf_counter()

    try:
        # ── Step 0: Download ──────────────────────────────────────────────
        if start_idx <= STEP_ORDER.index("download"):
            dump_date, _ = download.run(
                raw_dir=input_dir,
                dump_date=dump_date,
                types=download.ALL_TYPES,
                resume=True,
                force=args.force,
            )
            log.info("Dump date resolved: %s", dump_date)
        else:
            if not dump_date:
                log.error("--dump-date is required when skipping download")
                sys.exit(1)

        # ── Step 1: Filter ────────────────────────────────────────────────
        if start_idx <= STEP_ORDER.index("filter"):
            step_01_filter.run(
                raw_dir=input_dir,
                intermediate_dir=intermediate_dir,
                dump_date=dump_date,
                force=args.force,
            )

        # ── Step 2: Join ──────────────────────────────────────────────────
        if start_idx <= STEP_ORDER.index("join"):
            step_02_join.run(
                raw_dir=input_dir,
                intermediate_dir=intermediate_dir,
                dump_date=dump_date,
                uf_filter=args.uf,
                cnae_filter=args.cnae,
                force=args.force,
            )

        # ── Count mode ────────────────────────────────────────────────────
        if args.count:
            import duckdb
            joined = intermediate_dir / f"step_02_poi_joined_{dump_date}.parquet"
            n = duckdb.execute(f"SELECT count(*) FROM '{joined}'").fetchone()[0]
            log.info("POI count: %s", f"{n:,}")
            return

        # ── Step 3: Export ────────────────────────────────────────────────
        if start_idx <= STEP_ORDER.index("export"):
            step_03_export.run(
                intermediate_dir=intermediate_dir,
                output_dir=output_dir,
                dump_date=dump_date,
                formats=args.format,
                force=args.force,
            )

    except FileNotFoundError as exc:
        log.error("Missing input file: %s", exc)
        sys.exit(1)
    except Exception as exc:
        log.exception("Pipeline failed: %s", exc)
        sys.exit(1)

    log.info("=" * 60)
    log.info("Pipeline finished in %.1f s", time.perf_counter() - t_total)
    log.info("=" * 60)


if __name__ == "__main__":
    main()
