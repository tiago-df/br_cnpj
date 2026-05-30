"""Step 0 — Download Receita Federal CNPJ dump files.

The source site organises dumps as dated directories:
    https://dados-abertos-rf-cnpj.casadosdados.com.br/arquivos/YYYY-MM-DD/

Downloads are stored locally in versioned subdirectories:
    data/input/<YYYY-MM-DD>/Estabelecimentos0.zip
    data/input/<YYYY-MM-DD>/Empresas0.zip
    ...

Usage (called from main.py or directly):
    python -m app.download                        # latest available dump
    python -m app.download --dump-date 2026-04-12 # specific dump date
    python -m app.download --dry-run              # list files without downloading
    python -m app.download --types emp estab      # download only specific types
    python -m app.download --resume               # resume partial downloads
    python -m app.download --force                # re-download even if complete
"""

import argparse
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests
from tqdm import tqdm

from app.config_loader import get_config, resolve_path
from app.utils.logging_utils import setup_logging

log = logging.getLogger(__name__)

ARQUIVOS_PATH = "/arquivos/"

TYPE_PATTERNS: dict[str, str] = {
    "emp":       r"Empresas\d+\.zip",
    "estab":     r"Estabelecimentos\d+\.zip",
    "cnae":      r"Cnaes\.zip",
    "natureza":  r"Naturezas\.zip",
    "municipio": r"Municipios\.zip",
    "pais":      r"Paises\.zip",
    "quals":     r"Qualificacoes\.zip",
    "motivos":   r"Motivos\.zip",
    "simples":   r"Simples\.zip",
    "socios":    r"Socios\d+\.zip",
}

ALL_TYPES = list(TYPE_PATTERNS.keys())


def fetch_latest_dump_date(session: requests.Session, base_url: str) -> str:
    """Return the most recent dump date (YYYY-MM-DD) from the /arquivos/ index."""
    resp = session.get(f"{base_url}{ARQUIVOS_PATH}", timeout=30)
    resp.raise_for_status()
    # Dates appear as href="YYYY-MM-DD/"
    dates = re.findall(r'href="(\d{4}-\d{2}-\d{2})/"', resp.text)
    if not dates:
        raise RuntimeError("Could not find any dated dump directories in /arquivos/")
    return sorted(dates)[-1]


def fetch_file_list(session: requests.Session, base_url: str, dump_date: str) -> list[dict]:
    """Return [{name, url}] for all zip files in the given dump directory."""
    url = f"{base_url}{ARQUIVOS_PATH}{dump_date}/"
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    # Links look like: href="Empresas0.zip"
    names = re.findall(r'href="([^"?/][^"]*\.zip)"', resp.text)
    return [{"name": n, "url": f"{url}{n}"} for n in names]


def filter_by_type(files: list[dict], types: list[str]) -> list[dict]:
    patterns = [TYPE_PATTERNS[t] for t in types]
    return [f for f in files if any(re.search(p, f["name"], re.IGNORECASE) for p in patterns)]


def download_file(
    session: requests.Session,
    url: str,
    dest: Path,
    chunk_size: int,
    resume: bool = False,
) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    existing_bytes = dest.stat().st_size if (resume and dest.exists()) else 0
    headers = {"Range": f"bytes={existing_bytes}-"} if existing_bytes else {}

    cfg = get_config()
    attempts = cfg["download"]["retry_attempts"]
    backoff  = cfg["download"]["retry_backoff_seconds"]

    for attempt in range(1, attempts + 1):
        try:
            resp = session.get(url, headers=headers, stream=True, timeout=120)
            if resume and existing_bytes and resp.status_code == 416:
                return dest  # already complete
            resp.raise_for_status()

            total = int(resp.headers.get("content-length", 0)) + existing_bytes
            mode  = "ab" if existing_bytes else "wb"

            with (
                open(dest, mode) as fh,
                tqdm(
                    total=total or None,
                    initial=existing_bytes,
                    unit="B", unit_scale=True, unit_divisor=1024,
                    desc=dest.name, leave=False,
                ) as bar,
            ):
                for chunk in resp.iter_content(chunk_size):
                    fh.write(chunk)
                    bar.update(len(chunk))
            return dest

        except (requests.RequestException, OSError) as exc:
            if attempt == attempts:
                raise
            log.warning("Attempt %d failed (%s), retrying in %ds…", attempt, exc, backoff)
            time.sleep(backoff)

    raise RuntimeError("unreachable")


def is_valid_zip(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 22:
        return False
    with open(path, "rb") as fh:
        fh.seek(-22, 2)
        return fh.read(4) == b"PK\x05\x06"


def run(
    raw_dir: Path,
    dump_date: str | None = None,
    types: list[str] | None = None,
    resume: bool = False,
    force: bool = False,
    dry_run: bool = False,
) -> tuple[str, Path]:
    """Download dump files. Returns (dump_date, versioned_input_dir)."""
    cfg      = get_config()
    base_url = cfg["source"]["base_url"]
    chunk    = cfg["download"]["chunk_size_mb"] * 1024 * 1024
    workers  = cfg["download"]["workers"]
    types    = types or ALL_TYPES

    session = requests.Session()
    session.headers["User-Agent"] = "cnpj-poi-pipeline/2.0"

    if not dump_date:
        log.info("Fetching dump index to find latest available date…")
        dump_date = fetch_latest_dump_date(session, base_url)
        log.info("Latest dump: %s", dump_date)

    all_files = fetch_file_list(session, base_url, dump_date)
    files     = filter_by_type(all_files, types)

    if not files:
        raise ValueError(f"No files matched types {types} for dump {dump_date}")

    log.info("Dump date  : %s", dump_date)
    log.info("Files found: %d", len(files))
    for f in files:
        log.info("  %s", f["name"])

    versioned_dir = raw_dir / dump_date
    versioned_dir.mkdir(parents=True, exist_ok=True)

    if dry_run:
        return dump_date, versioned_dir

    def _download(f: dict) -> tuple[str, bool]:
        dest = versioned_dir / f["name"]
        if not force and is_valid_zip(dest):
            log.info("  ✓ %s already complete, skipping", f["name"])
            return f["name"], True
        download_file(session, f["url"], dest, chunk, resume=resume and not force)
        ok = is_valid_zip(dest)
        if ok:
            log.info("  ✓ %s", f["name"])
        else:
            log.error("  ✗ %s — ZIP integrity check failed", f["name"])
        return f["name"], ok

    failed = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_download, f): f for f in files}
        for future in as_completed(futures):
            name, ok = future.result()
            if not ok:
                failed.append(name)

    if failed:
        raise RuntimeError(f"Download failed for: {failed}")

    log.info("Download complete → %s", versioned_dir)
    return dump_date, versioned_dir


def main():
    setup_logging(resolve_path("log_dir"))

    parser = argparse.ArgumentParser(description="Download RF CNPJ dump files")
    parser.add_argument("--dump-date", default=None,
                        help="Dump date YYYY-MM-DD (default: latest available)")
    parser.add_argument("--types", nargs="+", choices=ALL_TYPES, default=ALL_TYPES)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--force",  action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    input_dir = resolve_path("input_dir")
    try:
        dump_date, dest = run(input_dir, args.dump_date, args.types,
                              args.resume, args.force, args.dry_run)
        log.info("Dump date used: %s", dump_date)
        log.info("Input dir     : %s", dest)
    except Exception as exc:
        log.error("Download failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
