# dataprof

Fast data profiling CLI for CSV, Parquet, and other tabular data files.
Built with Polars for speed, Rich for beautiful terminal output, and Click for a smooth CLI experience.


This project is in early development.


### Installation
```bash
git clone https://github.com/Aravind-Pramod/dataprof.git
cd dataprof
uv venv
source .venv/bin/activate
uv pip install -e .
```


### Usage

```bash
# Profile a CSV file
dataprof data.csv

# Profile with custom null threshold
dataprof data.csv --threshold 10

# Verbose output
dataprof data.csv --verbose
```

