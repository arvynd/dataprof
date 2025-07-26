"""
Orchestration of the app.
"""

import click
from rich.console import Console
from rich.table import Table
from loader import load_file
from core import compute_basic_stats

console = Console()


@click.command()
@click.option("--input", required=True, help="Input file path")
@click.option("--verbose", is_flag=True, help="Enable verbose mode")
def main(input, verbose):
    """Run the CLI tool."""
    if verbose:
        console.log("[bold green]Verbose mode is ON")

    df = load_file(input)
    stats = compute_basic_stats(df)

    table = Table(title="Basic Dataset Statistics")

    table.add_column("Metric", style="cyan", no_wrap=True)
    table.add_column("Value", style="magenta")

    table.add_row("Row Count", str(stats["row_count"]))
    table.add_row("Column Count", str(stats["column_count"]))
    table.add_row("Column Names", ", ".join(stats["column_names"]))

    console.print(table)


if __name__ == "__main__":
    main()
