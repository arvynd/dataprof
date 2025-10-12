"""
Orchestration of the app.
"""

import click
from dataprof.loader import load_file
from dataprof.core import (
    compute_basic_stats,
    check_null_counts,
    start_message,
    compute_summary_stats,
    print_schema,
    categorical_column_info,
)


@click.command()
@click.option("--input", required=True, help="Input file path")
@click.option("--verbose", is_flag=True, help="Enable verbose mode")
@click.option("--null_threshold", default=10)
def main(input, verbose, null_threshold):
    """Run the CLI tool."""

    # Start message
    start_message(verbose)
    # Load Data
    df = load_file(input, verbose)

    # Compute Basic stats
    compute_basic_stats(df)

    # Compute null info
    check_null_counts(df, null_threshold)

    # Print Schema
    print_schema(df)

    # Compute summary statistics
    compute_summary_stats(df)

    # Get categorical column info
    categorical_column_info(df)


if __name__ == "__main__":
    main()
