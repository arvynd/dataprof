"""loader.py

Handles reading and parsing various file formats into Polars DataFrames.
Supports CSV, Parquet, JSON, IPC formats.
"""

import polars as pl


def load_file(file_path: str) -> pl.DataFrame:
    """

    Load file into polars DataFrame.
    If the file is not in a supported format, raise ValueError.

    Args:
        file_path (str): Path to the file to be loaded.
        Expected file formats are .csv, .parquet, .json, and .ipc.

    Returns:
        pl.DataFrame: Loaded data as a Polars DataFrame.
    """

    if file_path.endswith(".csv"):
        return pl.read_csv("file_path")
    elif file_path.endswith(".parqet"):
        return pl.read_parquet("file_path")
    elif file_path.endswith(".json"):
        return pl.read_json("file_path")
    elif file_path.enswith(".ipc"):
        return pl.read_ipc("file_path")
    else:
        raise ValueError(
            f"Unsupported file type: {file_path}. Supported formats are: .csv, .parquet, .json, .ipc"
        )
