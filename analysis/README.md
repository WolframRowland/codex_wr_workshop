# Analysis

Run the initial taxi data analysis with:

```bash
../.venv/bin/python analyze_taxi_data.py
```

The process imports the incremental loader from `data/ingest_data.py`, cleans
taxi trip batches into a common schema, summarizes initial quality and trip
patterns, writes `outputs/initial_analysis.md`, and creates SVG charts under
`outputs/charts/`. It also writes `outputs/cleaned_data_audit.csv` with the
source file, source column, invalid row count, example invalid values, and
explanation for each data issue cleaned during the run. Audit counts are per
column/rule, so counts can overlap when one row has multiple invalid values.

By default the script analyzes the first two batches per file for quick
iteration. To scan all available rows, run:

```bash
../.venv/bin/python analyze_taxi_data.py --full
```

Useful options:

```bash
../.venv/bin/python analyze_taxi_data.py --batch-rows 50000
../.venv/bin/python analyze_taxi_data.py --max-batches 4
../.venv/bin/python analyze_taxi_data.py --output-dir outputs/sample_run
```

## Real-time style monitoring

To simulate a five-minute IoT monitoring job over newly arrived taxi events:

```bash
../.venv/bin/python realtime_iot_monitor.py
```

The monitor writes `outputs/realtime/latest_iot_insights.md` and
`outputs/realtime/latest_iot_insights.json`. Schedule the command every five
minutes, or run it continuously with:

```bash
../.venv/bin/python realtime_iot_monitor.py --watch
```

Because these TLC files do not include a true per-cab device ID, the monitor
groups device-quality alerts by the most specific source field available, such
as `VendorID`, `dispatching_base_num`, `originating_base_num`, or
`hvfhs_license_num`.

For a historical demo that intentionally picks a five-minute window with
negative fare anomalies, run:

```bash
../.venv/bin/python realtime_iot_monitor.py --window-mode anomaly-demo
```
