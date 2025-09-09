"""
Minimal ETL: kaggle dataset -> Pandas -> print sample

Steps:
1) Download a dataset from Kaggle
2) Unzip it and find CSV files
3) Load into pandas Datafram
4) Print first rows (instead of loading to SQL)
"""

import subprocess
import zipfile
from pathlib import Path
import pandas as pd
import os

# Config
KAGGLE_DATASET = os.getenv(
    "KAGGLE_DATASET",
    "giovamata/airlinedelaycauses" # default dataset
)

DOWNLOAD_DIR = Path("data")
DOWNLOAD_DIR.mkdir(exist_ok=True)

def kaggle_download_and_unzip(dataset: str, out_dir: Path):
    """Download dataset ZIP via Kaggle CLI and extract CSV's"""
    print(f"[INFO] Downloading dataset: {dataset}")
    subprocess.run(
        ["kaggle", "datasets", "download", "-d", dataset, "-p", str(out_dir)],
        check=True
    )

    csvs = []
    for z in out_dir.glob("*.zip"):
        print(f"[INFO] Unzipping: {z.name}")
        with zipfile.ZipFile(z, "r") as zip_ref:
            zip_ref.extractall(out_dir)
        z.unlink()

    for p in out_dir.rglob("*.csv"):
        csvs.append(p)

    if not csvs:
        raise RuntimeError("No CSV files found after download/unzip")
    return csvs

def main():
    # E: Extract
    csvs = kaggle_download_and_unzip(KAGGLE_DATASET, DOWNLOAD_DIR)

    # Pick first CSV
    target = csvs[0]
    print(f"[INFO] Using {target.name}")

    # T: Transform (normalize columns a bit)
    df = pd.read_csv(target)
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(r"[^a-z0-9_]+", "_", regex=True)
        .str.strip("_")
    )

    # L: Load (for now, just show a preview)
    print(f"[INFO] Loaded DataFrame shape: {df.shape}")
    print(df.head())

if __name__ == "__main__":
    main()