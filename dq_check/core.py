"""core.py
Contains the main functionality for data quality checks.
"""

import polars as pl
from rich.console import Console
from rich.table import Table
from rich.text import Text
from rich import box

console = Console()
text = Text()


def compute_basic_stats(df):
    stats_table = Table(
        title="Basic Dataset Statistics",
        title_justify="left",
        box=box.ASCII,
        title_style="#E91E63",
    )
    stats_table.add_column("Metric", style="cyan", no_wrap=True)
    stats_table.add_column("Value", style="magenta")

    stats_table.add_row("Row Count", str(df.height))
    stats_table.add_row("Column Count", str(df.width))
    stats_table.add_row("Column Names", ", ".join(df.columns))

    console.print(stats_table)


def check_null_counts(df: pl.DataFrame, threshold):
    console.print(
        f"Checking Null Thresholds with threshold set to {threshold}...",
        style="#FF9800",
    )

    table = Table(
        title="Null info",
        title_justify="left",
        box=box.ASCII,
        title_style="#E91E63",
    )

    table.add_column("Column")
    table.add_column("Null Count")
    table.add_column("Null %")

    null_counts = df.select([pl.col(c).null_count().alias(c) for c in df.columns])

    for col in df.columns:
        null_count = null_counts[col].item()
        null_pct = (null_count / df.height) * 100
        # Determine row style based on threshold
        row_style = "red" if null_pct > threshold else "green"
        table.add_row(
            f"[{row_style}]{col}[/{row_style}]",
            f"[{row_style}]{null_count}[/{row_style}]",
            f"[{row_style}]{null_pct:.2f}%[/{row_style}]",
        )

    console.print(table)

    return None


def df_to_rich_table(df, title=None):
    """Convert a pandas DataFrame to a Rich Table."""
    table = Table(show_header=True, header_style="bold magenta", title=title)
    for col in df.columns:
        table.add_column(str(col))

    for _, row in df.iterrows():
        table.add_row(*map(str, row.values))

    return table


def start_message(verbose):
    console.print(f"Starting profiling, verbosity set to {verbose}", style="#2196F3")
