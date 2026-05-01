# Data

Store Python scripts for creating test data and generated data files here.

Run the data structure profiler with:

```bash
../.venv/bin/python ingest_data.py
```

The profiler loads supported files incrementally for validation and writes
data quality check results to `logs/data_quality_checks.jsonl` by default.

Useful options:

```bash
../.venv/bin/python ingest_data.py --validation-batch-rows 50000
../.venv/bin/python ingest_data.py --validation-max-batches 2
../.venv/bin/python ingest_data.py --quality-log logs/custom_quality_checks.jsonl
../.venv/bin/python ingest_data.py --skip-validation
```
