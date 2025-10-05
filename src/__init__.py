"""
dataprof - Fast data profiling for tabular data files.
"""

__version__ = "0.1.0"

from dataprof.core import (
    compute_basic_stats,
    check_null_counts,
    compute_summary_stats,
    print_schema,
)

__all__ = [
    "compute_basic_stats",
    "check_null_counts",
    "compute_summary_stats",
    "print_schema",
]
