"""loader.py

Handles reading and parsing various file formats into Polars DataFrames.
Supports CSV, Parquet, JSON, IPC formats.
"""

import polars as pl
from rich.console import Console

console = Console()


def load_file(file_path: str, verbose) -> pl.DataFrame:
    """
    Load file into polars DataFrame.
    If the file is not in a supported format, raise ValueError.
    """
    if file_path.endswith(".csv"):
        df = pl.read_csv(file_path)
    elif file_path.endswith(".parquet"):
        df = pl.read_parquet(file_path)
    elif file_path.endswith(".json"):
        df = pl.read_json(file_path)
    elif file_path.endswith(".ipc"):
        df = pl.read_ipc(file_path)
    else:
        raise ValueError(
            f"Unsupported file type: {file_path}. Supported formats are: .csv, .parquet, .json, .ipc"
        )

    if verbose:
        console.log(f"Loaded {file_path}")

    return df
