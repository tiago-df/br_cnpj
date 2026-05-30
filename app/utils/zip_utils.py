"""Utilities for extracting RF CNPJ zip files before DuckDB reads them.

The inner files inside the RF zips have no .csv extension (e.g.
'K3241.K03200Y0.D60509.ESTABELE'), which prevents DuckDB's auto-detection.
This module extracts zips to a 'csv/' subdirectory alongside the originals,
renaming each inner file to <zip_stem>.csv so DuckDB can read them reliably.

Extraction is idempotent: if the .csv file already exists and is non-empty
it is not re-extracted.
"""

import logging
import zipfile
from pathlib import Path

log = logging.getLogger(__name__)


def extract_zip(zip_path: Path, out_dir: Path) -> Path:
    """Extract the single CSV from zip_path into out_dir/<zip_stem>.csv.

    Returns the path to the extracted CSV file.
    """
    dest = out_dir / f"{zip_path.stem}.csv"
    if dest.exists() and dest.stat().st_size > 0:
        return dest

    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.namelist()
        if not members:
            raise ValueError(f"Empty zip: {zip_path}")
        # RF zips always contain exactly one file
        inner = members[0]
        log.debug("Extracting %s → %s", zip_path.name, dest.name)
        with zf.open(inner) as src, open(dest, "wb") as fh:
            while chunk := src.read(8 * 1024 * 1024):
                fh.write(chunk)

    return dest


def extract_zips(zip_paths: list[Path], versioned_dir: Path) -> list[str]:
    """Extract a list of zip files to versioned_dir/csv/ and return CSV paths as strings."""
    out_dir = versioned_dir / "csv"
    csv_paths = []
    for zp in sorted(zip_paths):
        csv_path = extract_zip(zp, out_dir)
        csv_paths.append(str(csv_path))
        log.info("  ✓ extracted %s (%.0f MB)", csv_path.name,
                 csv_path.stat().st_size / 1_048_576)
    return csv_paths
