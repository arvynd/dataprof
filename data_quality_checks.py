import polars as pl

df = pl.scan_parquet("data/raw/*.parquet").collect()
