import polars as pl
from unittest.mock import MagicMock, patch
from rich.table import Table
from dataprof.core import compute_basic_stats

def test_compute_basic_stats():
    """
    Test that compute_basic_stats prints a table with correct basic stats.
    """
    # Create a sample DataFrame
    df = pl.DataFrame({
        "a": [1, 2, 3],
        "b": ["x", "y", "z"]
    })

    # Mock the console.print method
    with patch('dataprof.core.console.print') as mock_print:
        # Call the function
        compute_basic_stats(df)

        # Assert that print was called once
        mock_print.assert_called_once()

        # Get the table from the mock call
        args, _ = mock_print.call_args
        table = args[0]

        # Assert that the table is a rich Table
        assert isinstance(table, Table)

        # Assert table content
        assert table.row_count == 4

        # Check rows
        # Note: rich table doesn't have a direct way to get cell values.
        # We can check the private `_rows` attribute for inspection.
        # This is not ideal, but it's a way to verify the content.

        rows = table._rows

        # Row 1: Row Count
        assert "Row Count" in str(rows[0].cells)
        assert "3" in str(rows[0].cells)

        # Row 2: Column Count
        assert "Column Count" in str(rows[1].cells)
        assert "2" in str(rows[1].cells)

        # Row 3: Column Names
        assert "Column Names" in str(rows[2].cells)
        assert "a, b" in str(rows[2].cells)

        # Row 4: Estimated Size
        assert "Estimated Size" in str(rows[3].cells)
        # The exact size can vary, so we check for the presence of "MB"
        assert "MB" in str(rows[3].cells)
