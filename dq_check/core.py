"""core.py
Contains the main functionality for data quality checks.
"""

import polars as pl


def compute_basic_stats(df: pl.DataFrame) -> dict:
    """Calculate basic statistics for the dataset.

    Args:
        df : Polars DataFrame to compute statistics on.

    Returns:
           dict: A dictionary containing:
            - "row_count" (int): Total number of rows in the DataFrame.
            - "column_count" (int): Total number of columns in the DataFrame.
            - "column_names" (List[str]): List of column names.
    """

    result = {
        "row_count": df.height,
        "column_count": df.width,
        "column_names": df.columns,
    }

    return result


def calculate_null_metrics(df: pl.DataFrame):
    """Calculate null metrics for each column in the DataFrame.
    Args:
        df (pl.DataFrame): The Polars DataFrame to analyze.
        TODO : config (dict): Configuration dictionary containing null thresholds for each column.
    Returns:
    """

    total_rows = df.height
    null_summary = df.select(
        [pl.col(col).null_count().alias(f"{col}_null_count") for col in df.columns]
        + [
            (pl.col(col).null_count() / total_rows * 100).alias(f"{col}_null_pct")
            for col in df.columns
        ]
    )

    return null_summary


def calculate_uniqueness_metrics(df: pl.DataFrame):
    # For each column, calculate unique value counts and percentages; check against uniqueness thresholds and log issues.

    # TODO : config (dict): Configuration dictionary containing null thresholds for each column.
    unique_values = df.select(
        pl.col(col).n_unique().alias(f"{col}_unique_val_count") for col in df.columns
    )

    return unique_values


# validate_required_columns(df, config)
# Verify that all required columns specified in the config are present; report any missing columns.

# validate_column_types(df, config)
# Compare actual data types of columns with expected types from the config; report type mismatches.

# perform_range_checks(df, config)
# For numerical columns with defined range constraints, check if values lie within acceptable limits; flag violations.

# perform_pattern_checks(df, config)
# For columns with regex or format requirements, validate values and capture non-conforming entries.

# aggregate_results(metrics_list, violations_list)
# Combine all computed metrics and detected violations into a structured summary report.


# import polars as pl

df = pl.DataFrame(
    {
        "foo": [1, None, 3],
        "bar": [6, 7, None],
        "ham": ["a", "b", "c"],
    }
)
# print(df.null_count())

# df.null_count()i,row[0]

calculate_uniqueness_metrics(df)

# df.null_count()


# df.select(
#     [pl.col(col).null_count().alias(f"{col}_null_count") for col in df.columns]) +

# For each column, calculate unique value counts and percentages; check against uniqueness thresholds and log issues.

df.select(pl.col(col).n_unique().alias(f"{col}_unique_val_count") for col in df.columns)

df.glimpse
