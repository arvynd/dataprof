"""core.py

Core data profiling functionality for dataprof.

This module provides functions to analyze and display data quality metrics
including null counts, summary statistics, and schema information. All output
is formatted using Rich library for enhanced terminal display.

Functions are designed to work with Polars DataFrames and handle various
data types automatically.
"""

import polars as pl
from rich.console import Console
from rich.table import Table
from rich import box
import polars.selectors as cs

console = Console()


def compute_basic_stats(df: pl.DataFrame) -> None:
    """
    Display basic dataset statistics including dimensions and column names.

    Shows a formatted table with:
    - Total number of rows
    - Total number of columns
    - List of all column names

    Args:
        df: Polars DataFrame to analyze

    Returns:
        None. Prints statistics table to console.

    """
    # Rich table.
    stats_table = Table(
        title="Basic Dataset Statistics",
        title_justify="left",
        box=box.ASCII,
        title_style="#E91E63",
    )

    # Add columns.
    stats_table.add_column("Metric", style="cyan", no_wrap=True)
    stats_table.add_column("Value", style="magenta")

    # Add rows.
    stats_table.add_row("Row Count", str(df.height))
    stats_table.add_row("Column Count", str(df.width))
    stats_table.add_row("Column Names", ", ".join(df.columns))

    # Print to console.
    console.print(stats_table)


def check_null_counts(df: pl.DataFrame, threshold: float) -> None:
    """
    Analyze and display null value counts and percentages for all columns.

    Columns are color-coded based on the threshold:
    - Red: Null percentage exceeds threshold (quality issue)
    - Green: Null percentage within acceptable range

    Args:
        df: Polars DataFrame to analyze
        threshold: Percentage threshold (0-100) for flagging columns.
                   Columns with null percentage above this value are
                   highlighted in red.

    Returns:
        None. Prints formatted table with null statistics to console.

    """
    console.print(
        f"Checking Null Thresholds with threshold set to {threshold}...",
        style="#FF9800",
    )

    # TODO - Add threshold validation.

    # Rich table.
    table = Table(
        title="Null info",
        title_justify="left",
        box=box.ASCII,
        title_style="#E91E63",
    )

    # Add columns.
    table.add_column("Column")
    table.add_column("Null Count")
    table.add_column("Null %")

    # Get null counts.
    null_counts = df.select([pl.col(c).null_count().alias(c) for c in df.columns])

    # Write rows iteratively.
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

    # Print to console.
    console.print(table)

    return None


def start_message(verbose) -> None:
    """
    Print startup message indicating profiling has begun.

    Displays the verbosity level setting to inform the user
    of the detail level for subsequent output.

    Args:
        verbose: Verbosity level indicator (type/format not specified)

    Returns:
        None. Prints message to console.

    """
    console.print(f"Starting profiling, verbosity set to {verbose}", style="#2196F3")


def compute_summary_stats(df: pl.DataFrame) -> None:
    """
    Calculate and display summary statistics for numeric columns only.

    Computes and shows:
    - Maximum value
    - Mean (average) value
    - Minimum value

    Only processes columns with numeric data types. Non-numeric columns
    are automatically filtered out.

    Args:
        df: Polars DataFrame to analyze

    Returns:
        None. Prints formatted table with statistics to console.

    Note:
        If dataframe contains no numeric columns, an empty table is displayed.

    """
    console.print(
        "Printing Summary Stats...",
        style="#FF9800",
    )

    # Rich table.
    table = Table(
        title="Summary Statistics",
        title_justify="left",
        box=box.ASCII,
        title_style="#E91E63",
    )

    # Add columns.
    table.add_column("Column")
    table.add_column("Maximum")
    table.add_column("Mean")
    table.add_column("Minimum")

    # Iteratively add rows.
    for col in df.select(cs.numeric()).columns:
        table.add_row(
            f"{col}",
            f"{df.select(pl.col(col).max()).item():.2f}",
            f"{df.select(pl.col(col).mean()).item():.2f}",
            f"{df.select(pl.col(col).min()).item():.2f}",
        )

    # Print to console
    console.print(table)

    return None


def print_schema(df: pl.DataFrame) -> None:
    """
    Display the inferred schema of the DataFrame.

    Shows a table mapping each column name to its detected Polars data type.
    Useful for verifying type inference and identifying type-related issues.

    Args:
        df: Polars DataFrame whose schema to display

    Returns:
        None. Prints formatted schema table to console.

    """
    # Show inferred schema details
    console.print(
        "Inferring Schema...",
        style="#FF9800",
    )

    # Rich table.
    table = Table(
        title="Inferred Schema",
        title_justify="left",
        box=box.ASCII,
        title_style="#E91E63",
    )

    # Add columns.
    table.add_column("Column")
    table.add_column("Data Type")

    # Add rows iteratively.
    for col in df.schema.keys():
        table.add_row(f"{col}", f"{df.schema.get(col)}")

    # Print to console.
    console.print(table)

    return None


def categorical_column_info(df: pl.DataFrame):
    """
    Display overview of categorical (string) columns in the DataFrame.

    Analyzes string columns and displays a summary table showing:
    - Column name
    - Number of unique values (cardinality)
    - Most common value
    - Frequency of the most common value

    Args:
        df: Polars DataFrame to analyze

    Returns:
        None. Prints formatted table to console.
    """
    console.print("Profiling categorical columns..", style="#FF9800")

    # Rich table
    table = Table(
        title="Categorical Columns Overview",
        title_justify="left",
        box=box.ASCII,
        title_style="#E91E63",
    )

    # Define columns
    table.add_column("Column", style="cyan")
    table.add_column("Unique", style="magenta")
    table.add_column("Most Common", style="green")
    table.add_column("Frequency", style="yellow")

    # Get necessary details for each column.
    for col in df.select(cs.string(include_categorical=True)).columns:
        unique_count = df.select(pl.col(col)).n_unique()
        # Get most common value
        value_counts = (
            df.select(pl.col(col).value_counts())
            .unnest(col)
            .sort(by="count", descending=True)
        )
        # Get the most common value and frequency
        most_common = value_counts.head(1).select(col).item()
        frequency = value_counts.head(1).select(pl.col("count")).item()

        #! TODO - Show multiple options if they are all equal in count
        #! TODD - Get column percent values

        # Add row
        table.add_row(col, str(unique_count), most_common, str(frequency))

    # Print table.
    console.print(table)

    return None
