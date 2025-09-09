from __future__ import annotations
import os, sys, subprocess, zipfile, urllib.parse
from pathlib import Path
from typing import List
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

KAGGLE_DATASET = os.getenv("KAGGLE_DATASET", "giovamata/airlinedelaycauses")
DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "data"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
TABLE_NAME = os.getenv("TABLE_NAME", "dbo.airline_delay_causes")

AZURE_SQL_CONN_STR = os.getenv("AZURE_SQL_CONN_STR")
ODBC_CONN_STR = os.getenv("ODBC_CONN_STR")

def run(cmd: list[str]) -> None:
    print(f"[CMD] {' '.join(cmd)}")
    res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    print(res.stdout)
    res.check_returncode()

def kaggle_download_and_unzip(dataset: str, out_dir: Path) -> list[Path]:
    run(["kaggle", "datasets", "download", "-d", dataset, "-p", str(out_dir), "--unzip", "-q"])
    csvs = list(out_dir.rglob("*.csv"))
    if not csvs:
        raise RuntimeError("No CSVs found after download/unzip.")
    print("[INFO] CSVs found:")
    for p in csvs:
        print("   -", p)
    return csvs

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
                  .str.lower()
                  .str.replace(r"[^a-z0-9_]+", "_", regex=True)
                  .str.strip("_")
    )
    return df

def smart_cast(df: pd.DataFrame) -> pd.DataFrame:
    """Cast common airline delay fields to numeric where possible."""
    df = df.copy()
    # try ints for common keys
    for col in df.columns:
        low = col.lower()
        if low in {"year", "month"}:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif low.endswith("_ct") or low.endswith("_delay") or low.startswith("arr_"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def build_engine():
    if AZURE_SQL_CONN_STR:
        print("[INFO] Using AZURE_SQL_CONN_STR")
        return create_engine(AZURE_SQL_CONN_STR, fast_executemany=True)
    if not ODBC_CONN_STR:
        raise RuntimeError("Provide AZURE_SQL_CONN_STR or ODBC_CONN_STR in .env")
    print("[INFO] Using ODBC_CONN_STR via odbc_connect")
    params = urllib.parse.quote_plus(ODBC_CONN_STR)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

def load_csv_to_sql(csv_path: Path, engine, table_name: str, mode: str = "append"):
    print(f"[INFO] Reading {csv_path.name}")
    df = pd.read_csv(csv_path)
    df = normalize_columns(df)
    df = smart_cast(df)

    # Get schema from environment variable or from table_name
    schema = os.getenv("SCHEMA_NAME")
    if schema is None and "." in table_name:
        schema, name = table_name.split(".")
    elif schema is None:
        schema = None
        name = table_name
    else:
        name = table_name

    if schema:
        print(f"[INFO] Loading {len(df):,} rows, {len(df.columns)} cols into {schema}.{name} (mode={mode})")
    else:
        print(f"[INFO] Loading {len(df):,} rows, {len(df.columns)} cols into {name} (mode={mode})")

    # Use a very small chunk size to avoid parameter limits and memory issues
    chunk_size = 500
    total_rows = len(df)
    
    # Process in chunks with progress reporting
    for i in range(0, total_rows, chunk_size):
        end = min(i + chunk_size, total_rows)
        percentage = (i / total_rows) * 100
        print(f"[INFO] Processing rows {i:,} to {end:,} ({percentage:.1f}% complete)")
        
        chunk = df.iloc[i:end]
        try:
            chunk.to_sql(
                name=name,
                con=engine,
                schema=schema,
                if_exists="append" if i > 0 or mode == "append" else mode,
                index=False,
                method=None,  # Use pandas default method
            )
        except Exception as e:
            print(f"[ERROR] Failed to upload chunk {i}-{end}: {e}")
            raise

def main():
    engine = build_engine()
    csvs = kaggle_download_and_unzip(KAGGLE_DATASET, DOWNLOAD_DIR)
    target = csvs[0]  # first CSV; the dataset contains a single main CSV
    load_csv_to_sql(target, engine, TABLE_NAME, mode=os.getenv("LOAD_MODE", "append"))
    print("[INFO] Done ✅")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        sys.exit(1)
