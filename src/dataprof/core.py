"""core.py

Core data profiling functionality for dataprof.

This module provides functions to analyze and display data quality metrics
including null counts, summary statistics, and schema information. All output
is formatted using Rich library for enhanced terminal display.

Functions are designed to work with Polars DataFrames and handle various
data types automatically.
"""

import polars as pl
import polars.selectors as cs
from rich import box
from rich.console import Console
from rich.table import Table

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
    console.print(
        f"Starting profiling, verbosity set to {'on' if verbose else 'off'}",
        style="#2196F3",
    )


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


def detect_outliers(df: pl.DataFrame) -> None:
    """
    Detect and display outliers in numeric columns using IQR method.

    For each numeric column, shows:
    - Number of outliers
    - Percentage of outliers
    - Lower and upper bounds

    Args:
        df: Polars DataFrame to analyze

    Returns:
        None. Prints formatted table with outlier statistics to console.
    """
    console.print(
        "Detecting outliers using IQR method...",
        style="#FF9800",
    )

    # Rich table
    table = Table(
        title="Outlier Detection (IQR Method)",
        title_justify="left",
        box=box.ASCII,
        title_style="#E91E63",
    )

    # Add columns
    table.add_column("Column", style="cyan")
    table.add_column("Outliers", style="red")
    table.add_column("Outlier %", style="red")
    table.add_column("Lower Bound", style="green")
    table.add_column("Upper Bound", style="green")

    # Get numeric columns
    numeric_cols = df.select(cs.numeric()).columns

    if not numeric_cols:
        console.print("No numeric columns found for outlier detection.", style="yellow")
        return None

    # Analyze each numeric column
    for col in numeric_cols:
        # Calculate Q1, Q3, and IQR
        q1 = df.select(pl.col(col).quantile(0.25)).item()
        q3 = df.select(pl.col(col).quantile(0.75)).item()
        iqr = q3 - q1

        # Calculate bounds
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        # Count outliers
        outliers = df.filter(
            (pl.col(col) < lower_bound) | (pl.col(col) > upper_bound)
        ).height

        outlier_pct = (outliers / df.height) * 100 if df.height > 0 else 0

        # Add row
        table.add_row(
            col,
            str(outliers),
            f"{outlier_pct:.2f}%",
            f"{lower_bound:.2f}",
            f"{upper_bound:.2f}",
        )

    # Print to console
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


def detect_duplicates(df: pl.DataFrame) -> None:
    """
    Analyze and display duplicate row information.

    Shows:
    - Total number of duplicate rows
    - Percentage of duplicate rows
    - Number of unique rows

    Args:
        df: Polars DataFrame to analyze

    Returns:
        None. Prints formatted table with duplicate statistics to console.
    """
    console.print(
        "Analyzing duplicate rows...",
        style="#FF9800",
    )

    # Rich table
    table = Table(
        title="Duplicate Analysis",
        title_justify="left",
        box=box.ASCII,
        title_style="#E91E63",
    )

    # Add columns
    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    # Calculate duplicates
    total_rows = df.height
    unique_rows = df.n_unique()
    duplicate_rows = total_rows - unique_rows
    duplicate_pct = (duplicate_rows / total_rows) * 100 if total_rows > 0 else 0

    # Add rows
    table.add_row("Total Rows", str(total_rows))
    table.add_row("Unique Rows", str(unique_rows))
    table.add_row("Duplicate Rows", str(duplicate_rows))
    table.add_row("Duplicate %", f"{duplicate_pct:.2f}%")

    # Print to console
    console.print(table)

    return None
