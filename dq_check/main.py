"""
Orchestration of the app.
"""

import click
from rich.console import Console
from rich.table import Table
from loader import load_file
from core import compute_basic_stats, calculate_null_metrics

console = Console()


def df_to_rich_table(df, title=None):
    """Convert a pandas DataFrame to a Rich Table."""
    table = Table(show_header=True, header_style="bold magenta", title=title)
    for col in df.columns:
        table.add_column(str(col))

    for _, row in df.iterrows():
        table.add_row(*map(str, row.values))

    return table


@click.command()
@click.option("--input", required=True, help="Input file path")
@click.option("--verbose", is_flag=True, help="Enable verbose mode")
def main(input, verbose):
    """Run the CLI tool."""
    if verbose:
        console.log("[bold green]Verbose mode is ON")

    # Load Data
    df = load_file(input)

    # Get Basic Stats
    stats = compute_basic_stats(df)
    stats_table = Table(title="Basic Dataset Statistics")
    stats_table.add_column("Metric", style="cyan", no_wrap=True)
    stats_table.add_column("Value", style="magenta")

    stats_table.add_row("Row Count", str(stats["row_count"]))
    stats_table.add_row("Column Count", str(stats["column_count"]))
    stats_table.add_row("Column Names", ", ".join(stats["column_names"]))

    console.print(stats_table)

    # Get Null counts
    console.log("Checking Nulls...")
    nulls_df = calculate_null_metrics(df)
    nulls_df = nulls_df.to_pandas()

    # Print nulls_df as a Rich Table
    console.print(df_to_rich_table(nulls_df, title="Null Value Metrics"))


if __name__ == "__main__":
    main()
