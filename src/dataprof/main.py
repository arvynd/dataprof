"""
Orchestration of the app.
"""

import click

from dataprof.core import (
    categorical_column_info,
    check_null_counts,
    compute_basic_stats,
    compute_summary_stats,
    detect_duplicates,
    detect_outliers,
    print_schema,
    start_message,
)
from dataprof.loader import load_file


@click.command()
@click.option("--input", required=True, help="Input file path")
@click.option("--verbose", default=False, is_flag=True, help="Enable verbose mode")
@click.option("--null_threshold", default=10)
@click.option(
    "--basic-stats", "basic_stats_flag", is_flag=True, help="Compute basic stats"
)
@click.option(
    "--null-counts", "null_counts_flag", is_flag=True, help="Check null counts"
)
@click.option("--schema", "schema_flag", is_flag=True, help="Print schema")
@click.option(
    "--summary-stats",
    "summary_stats_flag",
    is_flag=True,
    help="Compute summary statistics",
)
@click.option(
    "--categorical-info",
    "categorical_info_flag",
    is_flag=True,
    help="Get categorical column info",
)
@click.option("--duplicates", "duplicates_flag", is_flag=True, help="Detect duplicates")
@click.option("--outliers", "outliers_flag", is_flag=True, help="Detect outliers")
def main(
    input,
    verbose,
    null_threshold,
    basic_stats_flag,
    null_counts_flag,
    schema_flag,
    summary_stats_flag,
    categorical_info_flag,
    duplicates_flag,
    outliers_flag,
):
    """Run the CLI tool."""

    # Start message
    start_message(verbose)
    # Load Data
    df = load_file(input, verbose)

    # Check if any specific flag is provided
    run_all = not any(
        [
            basic_stats_flag,
            null_counts_flag,
            schema_flag,
            summary_stats_flag,
            categorical_info_flag,
            duplicates_flag,
            outliers_flag,
        ]
    )

    if run_all or basic_stats_flag:
        # Compute Basic stats
        compute_basic_stats(df)

    if run_all or null_counts_flag:
        # Compute null info
        check_null_counts(df, null_threshold)

    if run_all or schema_flag:
        # Print Schema
        print_schema(df)

    if run_all or summary_stats_flag:
        # Compute summary statistics
        compute_summary_stats(df)

    if run_all or categorical_info_flag:
        # Get categorical column info
        categorical_column_info(df)

    if run_all or duplicates_flag:
        # Detect duplicates
        detect_duplicates(df)

    if run_all or outliers_flag:
        # Detect outliers
        detect_outliers(df)


if __name__ == "__main__":
    main()
