"""Step 0 — Download Receita Federal CNPJ dump files.

Downloads are stored in versioned subdirectories:
    data/input/<YYYY-MM>/Estabelecimentos0.zip
    data/input/<YYYY-MM>/Empresas0.zip
    ...

Usage (called from main.py or directly):
    python -m app.download --dump-date 2026-05
    python -m app.download --dump-date 2026-05 --dry-run
    python -m app.download --dump-date 2026-05 --types emp estab
    python -m app.download --dump-date 2026-05 --resume
    python -m app.download --dump-date 2026-05 --force
"""

import argparse
import logging
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

import requests
from tqdm import tqdm

from app.config_loader import get_config, resolve_path
from app.utils.logging_utils import setup_logging

log = logging.getLogger(__name__)

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
}

ALL_TYPES = list(TYPE_PATTERNS.keys())


def fetch_file_list(session: requests.Session, base_url: str) -> list[dict]:
    resp = session.get(f"{base_url}/", timeout=30)
    resp.raise_for_status()

    files = []
    for match in re.finditer(r'href="([^"]+\.zip)"[^>]*>([^<]*)</a>', resp.text, re.IGNORECASE):
        href, label = match.group(1), match.group(2).strip()
        url = href if href.startswith("http") else f"{base_url}/{href.lstrip('/')}"
        name = url.split("/")[-1]
        files.append({"name": name, "url": url, "label": label or name})

    if not files:
        for match in re.finditer(r'href="([^"]+\.zip)"', resp.text, re.IGNORECASE):
            href = match.group(1)
            url = href if href.startswith("http") else f"{base_url}/{href.lstrip('/')}"
            name = url.split("/")[-1]
            if not any(f["name"] == name for f in files):
                files.append({"name": name, "url": url, "label": name})

    return files


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
    backoff = cfg["download"]["retry_backoff_seconds"]

    for attempt in range(1, attempts + 1):
        try:
            resp = session.get(url, headers=headers, stream=True, timeout=60)
            if resume and existing_bytes and resp.status_code == 416:
                return dest
            resp.raise_for_status()

            total = int(resp.headers.get("content-length", 0)) + existing_bytes
            mode = "ab" if existing_bytes else "wb"

            with (
                open(dest, mode) as fh,
                tqdm(
                    total=total or None,
                    initial=existing_bytes,
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=dest.name,
                    leave=False,
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
    dump_date: str,
    types: list[str],
    raw_dir: Path,
    resume: bool = False,
    force: bool = False,
    dry_run: bool = False,
) -> Path:
    """Download dump files for the given month. Returns the versioned input directory."""
    cfg = get_config()
    base_url = cfg["source"]["base_url"]
    chunk_size = cfg["download"]["chunk_size_mb"] * 1024 * 1024
    workers = cfg["download"]["workers"]

    versioned_dir = raw_dir / dump_date
    versioned_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers["User-Agent"] = "cnpj-poi-pipeline/2.0"

    log.info("Fetching file index from %s…", base_url)
    all_files = fetch_file_list(session, base_url)
    files = filter_by_type(all_files, types)

    if not files:
        raise ValueError(f"No files matched types: {types}")

    log.info("Found %d file(s) for types %s", len(files), types)
    for f in files:
        log.info("  %s", f["name"])

    if dry_run:
        return versioned_dir

    def _download(f: dict) -> tuple[str, bool]:
        dest = versioned_dir / f["name"]
        if not force and is_valid_zip(dest):
            log.info("  ✓ %s already complete, skipping (use --force to re-download)", f["name"])
            return f["name"], True
        download_file(session, f["url"], dest, chunk_size, resume=resume and not force)
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
    return versioned_dir


def main():
    setup_logging(resolve_path("log_dir"))

    parser = argparse.ArgumentParser(description="Download RF CNPJ dump files")
    parser.add_argument("--dump-date", default=date.today().strftime("%Y-%m"),
                        help="Dump month, e.g. 2026-05 (default: current month)")
    parser.add_argument("--types", nargs="+", choices=ALL_TYPES, default=ALL_TYPES)
    parser.add_argument("--resume", action="store_true", help="Resume partial downloads")
    parser.add_argument("--force", action="store_true", help="Re-download even if file exists")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    input_dir = resolve_path("input_dir")
    try:
        run(args.dump_date, args.types, input_dir, args.resume, args.force, args.dry_run)
    except Exception as exc:
        log.error("Download failed: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
