import polars as pl
from unittest.mock import patch
from rich.table import Table
from dataprof.core import (
    compute_basic_stats,
    check_null_counts,
    start_message,
    compute_summary_stats,
    print_schema,
    detect_outliers,
    categorical_column_info,
    detect_duplicates,
)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _get_table_from_mock(mock_print, call_index=-1):
    """Extract the Rich Table object from a mocked console.print call."""
    args, _ = mock_print.call_args_list[call_index]
    return args[0]


def _row_values(table, row_idx):
    """Return a list of cell string values for a given row index."""
    return [str(col._cells[row_idx]) for col in table.columns]


# ---------------------------------------------------------------------------
# compute_basic_stats
# ---------------------------------------------------------------------------


class TestComputeBasicStats:
    def test_basic_output(self):
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

        with patch("dataprof.core.console.print") as mock_print:
            compute_basic_stats(df)
            mock_print.assert_called_once()

            table = _get_table_from_mock(mock_print)
            assert isinstance(table, Table)
            assert table.row_count == 3

            row0 = _row_values(table, 0)
            assert "Row Count" in row0
            assert "3" in row0

            row1 = _row_values(table, 1)
            assert "Column Count" in row1
            assert "2" in row1

            row2 = _row_values(table, 2)
            assert "Column Names" in row2
            assert "a, b" in row2

    def test_empty_dataframe(self):
        df = pl.DataFrame({"x": [], "y": []}).cast({"x": pl.Int64, "y": pl.Utf8})

        with patch("dataprof.core.console.print") as mock_print:
            compute_basic_stats(df)

            table = _get_table_from_mock(mock_print)
            assert "0" in _row_values(table, 0)  # Row Count
            assert "2" in _row_values(table, 1)  # Column Count

    def test_single_column(self):
        df = pl.DataFrame({"only_col": [10, 20]})

        with patch("dataprof.core.console.print") as mock_print:
            compute_basic_stats(df)

            table = _get_table_from_mock(mock_print)
            assert "2" in _row_values(table, 0)
            assert "1" in _row_values(table, 1)
            assert "only_col" in _row_values(table, 2)


# ---------------------------------------------------------------------------
# check_null_counts
# ---------------------------------------------------------------------------


class TestCheckNullCounts:
    def test_no_nulls(self):
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})

        with patch("dataprof.core.console.print") as mock_print:
            check_null_counts(df, threshold=10)

            # First call is the status message, second is the table
            table = _get_table_from_mock(mock_print, call_index=1)
            assert isinstance(table, Table)
            assert table.row_count == 2

            for i in range(table.row_count):
                cell_text = str(_row_values(table, i))
                assert "0.00%" in cell_text
                # Should be green (below threshold)
                assert "green" in cell_text

    def test_nulls_above_threshold(self):
        df = pl.DataFrame({"a": [1, None, None], "b": [1, 2, 3]})

        with patch("dataprof.core.console.print") as mock_print:
            check_null_counts(df, threshold=10)

            table = _get_table_from_mock(mock_print, call_index=1)

            # Column 'a' has 66.67% nulls -> above threshold -> red
            row0_text = str(_row_values(table, 0))
            assert "red" in row0_text
            assert "66.67%" in row0_text

            # Column 'b' has 0% nulls -> below threshold -> green
            assert "green" in str(_row_values(table, 1))

    def test_nulls_at_threshold_boundary(self):
        # Exactly at threshold should be green (only > threshold is red)
        df = pl.DataFrame({"a": [None] + [1] * 9})  # 10% null

        with patch("dataprof.core.console.print") as mock_print:
            check_null_counts(df, threshold=10)

            table = _get_table_from_mock(mock_print, call_index=1)
            row0_text = str(_row_values(table, 0))
            assert "green" in row0_text
            assert "10.00%" in row0_text

    def test_all_nulls(self):
        df = pl.DataFrame({"a": [None, None, None]}).cast({"a": pl.Int64})

        with patch("dataprof.core.console.print") as mock_print:
            check_null_counts(df, threshold=50)

            table = _get_table_from_mock(mock_print, call_index=1)
            row0_text = str(_row_values(table, 0))
            assert "red" in row0_text
            assert "100.00%" in row0_text

    def test_threshold_message_printed(self):
        df = pl.DataFrame({"a": [1]})

        with patch("dataprof.core.console.print") as mock_print:
            check_null_counts(df, threshold=25)

            first_call_args = mock_print.call_args_list[0]
            assert "25" in str(first_call_args)


# ---------------------------------------------------------------------------
# start_message
# ---------------------------------------------------------------------------


class TestStartMessage:
    def test_verbose_on(self):
        with patch("dataprof.core.console.print") as mock_print:
            start_message(verbose=True)

            mock_print.assert_called_once()
            msg = str(mock_print.call_args)
            assert "on" in msg

    def test_verbose_off(self):
        with patch("dataprof.core.console.print") as mock_print:
            start_message(verbose=False)

            mock_print.assert_called_once()
            msg = str(mock_print.call_args)
            assert "off" in msg


# ---------------------------------------------------------------------------
# compute_summary_stats
# ---------------------------------------------------------------------------


class TestComputeSummaryStats:
    def test_numeric_columns(self):
        df = pl.DataFrame({"val": [10, 20, 30], "score": [1.5, 2.5, 3.5]})

        with patch("dataprof.core.console.print") as mock_print:
            compute_summary_stats(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            assert isinstance(table, Table)
            assert table.row_count == 2

            row_val = _row_values(table, 0)
            assert "val" in row_val
            assert "30.00" in row_val  # max
            assert "20.00" in row_val  # mean
            assert "10.00" in row_val  # min

            row_score = _row_values(table, 1)
            assert "score" in row_score
            assert "3.50" in row_score  # max
            assert "2.50" in row_score  # mean
            assert "1.50" in row_score  # min

    def test_no_numeric_columns(self):
        df = pl.DataFrame({"name": ["a", "b"], "city": ["x", "y"]})

        with patch("dataprof.core.console.print") as mock_print:
            compute_summary_stats(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            assert table.row_count == 0

    def test_mixed_columns(self):
        df = pl.DataFrame({"num": [5, 10], "text": ["a", "b"]})

        with patch("dataprof.core.console.print") as mock_print:
            compute_summary_stats(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            # Only numeric column should appear
            assert table.row_count == 1
            row = _row_values(table, 0)
            assert "num" in row

    def test_negative_values(self):
        df = pl.DataFrame({"v": [-10, 0, 10]})

        with patch("dataprof.core.console.print") as mock_print:
            compute_summary_stats(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            row = _row_values(table, 0)
            assert "10.00" in row  # max
            assert "0.00" in row  # mean
            assert "-10.00" in row  # min


# ---------------------------------------------------------------------------
# print_schema
# ---------------------------------------------------------------------------


class TestPrintSchema:
    def test_schema_output(self):
        df = pl.DataFrame({"int_col": [1], "str_col": ["a"], "float_col": [1.0]})

        with patch("dataprof.core.console.print") as mock_print:
            print_schema(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            assert isinstance(table, Table)
            assert table.row_count == 3

            rows_text = [str(_row_values(table, i)) for i in range(3)]
            assert any("int_col" in r and "Int64" in r for r in rows_text)
            assert any("str_col" in r and "String" in r for r in rows_text)
            assert any("float_col" in r and "Float64" in r for r in rows_text)

    def test_single_column_schema(self):
        df = pl.DataFrame({"flag": [True, False]})

        with patch("dataprof.core.console.print") as mock_print:
            print_schema(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            assert table.row_count == 1
            row_text = str(_row_values(table, 0))
            assert "flag" in row_text
            assert "Boolean" in row_text


# ---------------------------------------------------------------------------
# detect_outliers
# ---------------------------------------------------------------------------


class TestDetectOutliers:
    def test_with_outliers(self):
        # Values 1-10 with an extreme outlier at 100
        df = pl.DataFrame({"vals": list(range(1, 11)) + [100]})

        with patch("dataprof.core.console.print") as mock_print:
            detect_outliers(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            assert isinstance(table, Table)
            assert table.row_count == 1

            row = _row_values(table, 0)
            assert "vals" in row
            # 100 should be detected as an outlier
            outlier_count = int(row[1])
            assert outlier_count >= 1

    def test_no_outliers(self):
        # Uniform data, no outliers
        df = pl.DataFrame({"vals": [5, 5, 5, 5, 5]})

        with patch("dataprof.core.console.print") as mock_print:
            detect_outliers(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            row = _row_values(table, 0)
            assert row[1] == "0"  # zero outliers
            assert "0.00%" in row[2]

    def test_no_numeric_columns(self):
        df = pl.DataFrame({"name": ["a", "b", "c"]})

        with patch("dataprof.core.console.print") as mock_print:
            detect_outliers(df)

            # Should print the status message and "No numeric columns" message
            assert mock_print.call_count == 2
            second_msg = str(mock_print.call_args_list[1])
            assert "No numeric columns" in second_msg

    def test_multiple_numeric_columns(self):
        df = pl.DataFrame(
            {
                "a": [1, 2, 3, 4, 5],
                "b": [10, 20, 30, 40, 50],
            }
        )

        with patch("dataprof.core.console.print") as mock_print:
            detect_outliers(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            assert table.row_count == 2

    def test_outlier_bounds(self):
        # Predictable data to verify IQR bounds
        df = pl.DataFrame({"v": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100]})

        with patch("dataprof.core.console.print") as mock_print:
            detect_outliers(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            row = _row_values(table, 0)
            lower_bound = float(row[3])
            upper_bound = float(row[4])
            # Lower bound should be below 1, upper bound below 100
            assert lower_bound < 1
            assert upper_bound < 100


# ---------------------------------------------------------------------------
# categorical_column_info
# ---------------------------------------------------------------------------


class TestCategoricalColumnInfo:
    def test_basic_categorical(self):
        df = pl.DataFrame(
            {
                "color": ["red", "blue", "red", "red", "blue"],
                "size": ["S", "M", "L", "S", "S"],
            }
        )

        with patch("dataprof.core.console.print") as mock_print:
            categorical_column_info(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            assert isinstance(table, Table)
            assert table.row_count == 2

            color_row = _row_values(table, 0)
            assert "color" in color_row
            assert "2" in color_row  # 2 unique values
            assert "red" in color_row  # most common
            assert "3" in color_row  # frequency

            size_row = _row_values(table, 1)
            assert "size" in size_row
            assert "3" in size_row  # 3 unique values
            assert "S" in size_row  # most common
            # S appears 3 times

    def test_no_string_columns(self):
        df = pl.DataFrame({"nums": [1, 2, 3]})

        with patch("dataprof.core.console.print") as mock_print:
            categorical_column_info(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            assert table.row_count == 0

    def test_single_value_column(self):
        df = pl.DataFrame({"status": ["active"] * 5})

        with patch("dataprof.core.console.print") as mock_print:
            categorical_column_info(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            row = _row_values(table, 0)
            assert "status" in row
            assert "1" in row  # 1 unique
            assert "active" in row  # most common
            assert "5" in row  # frequency


# ---------------------------------------------------------------------------
# detect_duplicates
# ---------------------------------------------------------------------------


class TestDetectDuplicates:
    def test_with_duplicates(self):
        df = pl.DataFrame(
            {
                "a": [1, 2, 2, 3, 3, 3],
                "b": ["x", "y", "y", "z", "z", "z"],
            }
        )

        with patch("dataprof.core.console.print") as mock_print:
            detect_duplicates(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            assert isinstance(table, Table)
            assert table.row_count == 4

            rows = {
                _row_values(table, i)[0]: _row_values(table, i)[1] for i in range(4)
            }
            assert rows["Total Rows"] == "6"
            assert rows["Unique Rows"] == "3"
            assert rows["Duplicate Rows"] == "3"
            assert rows["Duplicate %"] == "50.00%"

    def test_no_duplicates(self):
        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})

        with patch("dataprof.core.console.print") as mock_print:
            detect_duplicates(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            rows = {
                _row_values(table, i)[0]: _row_values(table, i)[1] for i in range(4)
            }
            assert rows["Total Rows"] == "3"
            assert rows["Unique Rows"] == "3"
            assert rows["Duplicate Rows"] == "0"
            assert rows["Duplicate %"] == "0.00%"

    def test_all_duplicates(self):
        df = pl.DataFrame({"a": [1, 1, 1], "b": ["x", "x", "x"]})

        with patch("dataprof.core.console.print") as mock_print:
            detect_duplicates(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            rows = {
                _row_values(table, i)[0]: _row_values(table, i)[1] for i in range(4)
            }
            assert rows["Total Rows"] == "3"
            assert rows["Unique Rows"] == "1"
            assert rows["Duplicate Rows"] == "2"
            assert rows["Duplicate %"] == "66.67%"

    def test_single_row(self):
        df = pl.DataFrame({"a": [42]})

        with patch("dataprof.core.console.print") as mock_print:
            detect_duplicates(df)

            table = _get_table_from_mock(mock_print, call_index=1)
            rows = {
                _row_values(table, i)[0]: _row_values(table, i)[1] for i in range(4)
            }
            assert rows["Total Rows"] == "1"
            assert rows["Unique Rows"] == "1"
            assert rows["Duplicate Rows"] == "0"
            assert rows["Duplicate %"] == "0.00%"
