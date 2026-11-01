#!/usr/bin/env python3
"""Download the public NASA HTTP log dataset through the Kaggle CLI.

The Kaggle CLI needs a configured Kaggle API token. If you do not want to set up
credentials, download the dataset in your browser and put the extracted log file
in data/raw/nasa_access.log instead.
"""
from __future__ import annotations

import argparse
import gzip
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATASET = "adchatakora/nasa-http-access-logs"
RAW_DIR = ROOT / "data" / "raw"
DEST = RAW_DIR / "nasa_access.log"
DATA_SUFFIXES = {".log", ".txt", ".gz"}


def find_candidates(folder: Path) -> list[Path]:
    files = [p for p in folder.rglob("*") if p.is_file() and p.suffix.lower() in DATA_SUFFIXES]
    # Ignore the final destination if a repeated run finds it.
    return [p for p in files if p.resolve() != DEST.resolve()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Download NASA HTTP access logs from Kaggle.")
    parser.parse_args()

    if shutil.which("kaggle") is None:
        print("Kaggle CLI was not found. Install it with: python -m pip install kaggle")
        print("Then configure your Kaggle API token, or download the dataset manually:")
        print(f"https://www.kaggle.com/datasets/{DATASET}")
        print(f"Place the extracted .log/.txt file at: {DEST}")
        return 2

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Downloading Kaggle dataset: {DATASET}")
    try:
        subprocess.run(
            ["kaggle", "datasets", "download", "-d", DATASET, "-p", str(RAW_DIR), "--unzip"],
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        print("Kaggle download failed. Check your internet connection and Kaggle API token.", file=sys.stderr)
        print(f"Dataset page: https://www.kaggle.com/datasets/{DATASET}", file=sys.stderr)
        return exc.returncode or 1

    candidates = find_candidates(RAW_DIR)
    # Prefer an already-extracted log over a compressed source; if several are
    # present, pick the largest data file and print what was selected.
    plain = [p for p in candidates if p.suffix.lower() in {".log", ".txt"}]
    chosen_pool = plain if plain else candidates
    if not chosen_pool:
        print(f"Download succeeded, but no .log, .txt, or .gz file was found in {RAW_DIR}.", file=sys.stderr)
        print("Inspect the extracted files and put the access-log text file at data/raw/nasa_access.log.")
        return 3

    source = max(chosen_pool, key=lambda p: p.stat().st_size)
    print(f"Selected dataset file: {source} ({source.stat().st_size:,} bytes)")
    if source.suffix.lower() == ".gz":
        with gzip.open(source, "rb") as src, DEST.open("wb") as dst:
            shutil.copyfileobj(src, dst)
    else:
        if source.resolve() != DEST.resolve():
            shutil.copy2(source, DEST)

    # Retain downloaded source files in data/raw for traceability and if the archive contains multiple logs.
    print(f"Ready: {DEST}")
    print("Next: python -m loglens.etl --input data/raw/nasa_access.log --source-id nasa_aug95 --dry-run --limit 100000")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
