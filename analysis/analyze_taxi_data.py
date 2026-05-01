"""Run initial analysis on the local taxi datasets.

This process uses the incremental loader from data/ingest_data.py, cleans each
batch into a common trip schema, aggregates useful metrics, writes a Markdown
findings report, and creates simple SVG charts without extra plotting
dependencies.
"""

from __future__ import annotations

import argparse
import html
import math
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
if str(DATA_DIR) not in sys.path:
    sys.path.insert(0, str(DATA_DIR))

from ingest_data import discover_data_files, iter_dataframe_batches  # noqa: E402


CHART_COLORS = ["#2563eb", "#059669", "#dc2626", "#7c3aed", "#ea580c", "#0891b2"]


@dataclass
class DatasetStats:
    rows_loaded: int = 0
    rows_after_cleaning: int = 0
    invalid_datetime_rows: int = 0
    invalid_value_rows: int = 0
    distance_sum: float = 0.0
    distance_count: int = 0
    fare_sum: float = 0.0
    fare_count: int = 0
    tip_sum: float = 0.0
    tip_count: int = 0
    passenger_sum: float = 0.0
    passenger_count: int = 0
    pickup_hours: Counter = field(default_factory=Counter)
    pickup_boroughs: Counter = field(default_factory=Counter)
    pickup_zones: Counter = field(default_factory=Counter)


@dataclass
class CleaningIssue:
    source_file: str
    column: str
    standardized_column: str
    invalid_rows: int
    explanation: str
    example_invalid_values: List[str] = field(default_factory=list)


def normalized_columns(df: pd.DataFrame) -> Dict[str, str]:
    return {"".join(ch for ch in column.lower() if ch.isalnum()): column for column in df.columns}


def find_column(df: pd.DataFrame, candidates: Sequence[str]) -> Optional[str]:
    by_normalized = normalized_columns(df)
    for candidate in candidates:
        key = "".join(ch for ch in candidate.lower() if ch.isalnum())
        if key in by_normalized:
            return by_normalized[key]
    return None


def service_name(dataset_name: str) -> str:
    if dataset_name.startswith("yellow"):
        return "Yellow taxi"
    if dataset_name.startswith("green"):
        return "Green taxi"
    if dataset_name.startswith("fhvhv"):
        return "High-volume FHV"
    if dataset_name.startswith("fhv"):
        return "FHV"
    return dataset_name


def load_zone_lookup(data_dir: Path) -> pd.DataFrame:
    lookup_path = data_dir / "taxi_zone_lookup.csv"
    if not lookup_path.exists():
        return pd.DataFrame(columns=["LocationID", "Borough", "Zone", "service_zone"])
    lookup = pd.read_csv(lookup_path)
    lookup["LocationID"] = pd.to_numeric(lookup["LocationID"], errors="coerce")
    return lookup.dropna(subset=["LocationID"]).astype({"LocationID": "int64"})


def sample_invalid_values(values: Iterable[object], limit: int = 5) -> List[str]:
    samples = []
    seen = set()
    for value in values:
        if pd.isna(value):
            text = "<missing>"
        else:
            text = str(value)
        if text in seen:
            continue
        samples.append(text)
        seen.add(text)
        if len(samples) >= limit:
            break
    return samples


def add_cleaning_issue(
    issues: List[CleaningIssue],
    source_file: str,
    column: str,
    standardized_column: str,
    invalid_rows: int,
    explanation: str,
    example_invalid_values: Iterable[object],
) -> None:
    if invalid_rows <= 0:
        return
    issues.append(
        CleaningIssue(
            source_file=source_file,
            column=column,
            standardized_column=standardized_column,
            invalid_rows=invalid_rows,
            explanation=explanation,
            example_invalid_values=sample_invalid_values(example_invalid_values),
        )
    )


def clean_trip_batch(
    df: pd.DataFrame, source_file: str, dataset_name: str, zone_lookup: pd.DataFrame
) -> Tuple[pd.DataFrame, int, int, List[CleaningIssue]]:
    pickup_col = find_column(
        df,
        [
            "pickup_datetime",
            "tpep_pickup_datetime",
            "lpep_pickup_datetime",
            "request_datetime",
        ],
    )
    dropoff_col = find_column(
        df,
        [
            "dropoff_datetime",
            "dropOff_datetime",
            "tpep_dropoff_datetime",
            "lpep_dropoff_datetime",
        ],
    )
    pickup_location_col = find_column(df, ["PULocationID", "PUlocationID", "pickup_locationid"])
    distance_col = find_column(df, ["trip_distance", "trip_miles"])
    fare_col = find_column(df, ["total_amount", "base_passenger_fare", "fare_amount"])
    tip_col = find_column(df, ["tip_amount", "tips"])
    passenger_col = find_column(df, ["passenger_count"])

    cleaned = pd.DataFrame(index=df.index)
    cleaned["dataset"] = dataset_name
    cleaned["service"] = service_name(dataset_name)
    cleaning_issues: List[CleaningIssue] = []

    if pickup_col:
        cleaned["pickup_datetime"] = pd.to_datetime(df[pickup_col], errors="coerce")
    else:
        cleaned["pickup_datetime"] = pd.NaT

    if dropoff_col:
        cleaned["dropoff_datetime"] = pd.to_datetime(df[dropoff_col], errors="coerce")
    else:
        cleaned["dropoff_datetime"] = pd.NaT

    if pickup_location_col:
        cleaned["pickup_location_id"] = pd.to_numeric(df[pickup_location_col], errors="coerce")
    else:
        cleaned["pickup_location_id"] = pd.NA

    if distance_col:
        cleaned["distance"] = pd.to_numeric(df[distance_col], errors="coerce")
    else:
        cleaned["distance"] = pd.NA

    if fare_col:
        cleaned["fare"] = pd.to_numeric(df[fare_col], errors="coerce")
    else:
        cleaned["fare"] = pd.NA

    if tip_col:
        cleaned["tip"] = pd.to_numeric(df[tip_col], errors="coerce")
    else:
        cleaned["tip"] = pd.NA

    if passenger_col:
        cleaned["passengers"] = pd.to_numeric(df[passenger_col], errors="coerce")
    else:
        cleaned["passengers"] = pd.NA

    invalid_datetime = cleaned["pickup_datetime"].isna()
    missing_pickup_datetime = invalid_datetime.copy()
    valid_dropoff = cleaned["dropoff_datetime"].notna()
    dropoff_before_pickup = valid_dropoff & (cleaned["dropoff_datetime"] < cleaned["pickup_datetime"])
    invalid_datetime = invalid_datetime | dropoff_before_pickup
    add_cleaning_issue(
        cleaning_issues,
        source_file,
        pickup_col or "pickup_datetime",
        "pickup_datetime",
        int(missing_pickup_datetime.sum()),
        "Pickup timestamp is missing or could not be parsed, so the trip cannot be placed in time.",
        df.loc[missing_pickup_datetime, pickup_col] if pickup_col else ["<missing column>"],
    )
    timestamp_examples = (
        "pickup="
        + cleaned.loc[dropoff_before_pickup, "pickup_datetime"].astype(str)
        + "; dropoff="
        + cleaned.loc[dropoff_before_pickup, "dropoff_datetime"].astype(str)
    )
    add_cleaning_issue(
        cleaning_issues,
        source_file,
        dropoff_col or "dropoff_datetime",
        "dropoff_datetime",
        int(dropoff_before_pickup.sum()),
        "Dropoff timestamp is earlier than pickup timestamp, which would create a negative trip duration.",
        timestamp_examples,
    )

    invalid_values = pd.Series(False, index=cleaned.index)
    numeric_sources = [
        ("distance", distance_col, "Trip distance cannot be negative."),
        ("fare", fare_col, "Fare amount cannot be negative."),
        ("tip", tip_col, "Tip amount cannot be negative."),
        ("passengers", passenger_col, "Passenger count cannot be negative."),
    ]
    for column, source_column, explanation in numeric_sources:
        invalid_column_values = cleaned[column].notna() & (cleaned[column] < 0)
        invalid_values = invalid_values | invalid_column_values
        if source_column:
            example_values = df.loc[invalid_column_values, source_column]
        else:
            example_values = cleaned.loc[invalid_column_values, column]
        add_cleaning_issue(
            cleaning_issues,
            source_file,
            source_column or column,
            column,
            int(invalid_column_values.sum()),
            explanation,
            example_values,
        )

    cleaned = cleaned.loc[~invalid_datetime & ~invalid_values].copy()
    cleaned["pickup_hour"] = cleaned["pickup_datetime"].dt.hour
    cleaned["pickup_month"] = cleaned["pickup_datetime"].dt.to_period("M").astype(str)

    if not zone_lookup.empty:
        cleaned = cleaned.merge(
            zone_lookup[["LocationID", "Borough", "Zone"]],
            how="left",
            left_on="pickup_location_id",
            right_on="LocationID",
        )
    else:
        cleaned["Borough"] = pd.NA
        cleaned["Zone"] = pd.NA

    return cleaned, int(invalid_datetime.sum()), int(invalid_values.sum()), cleaning_issues


def finite_mean(total: float, count: int) -> Optional[float]:
    if count == 0:
        return None
    return total / count


def add_numeric_stats(stats: DatasetStats, cleaned: pd.DataFrame) -> None:
    for source, total_attr, count_attr in [
        ("distance", "distance_sum", "distance_count"),
        ("fare", "fare_sum", "fare_count"),
        ("tip", "tip_sum", "tip_count"),
        ("passengers", "passenger_sum", "passenger_count"),
    ]:
        values = cleaned[source].dropna()
        setattr(stats, total_attr, getattr(stats, total_attr) + float(values.sum()))
        setattr(stats, count_attr, getattr(stats, count_attr) + int(values.count()))


def update_counts(stats: DatasetStats, cleaned: pd.DataFrame) -> None:
    stats.pickup_hours.update(int(hour) for hour in cleaned["pickup_hour"].dropna())
    stats.pickup_boroughs.update(str(value) for value in cleaned["Borough"].dropna())
    stats.pickup_zones.update(str(value) for value in cleaned["Zone"].dropna())


def analyze_data(
    data_dir: Path, batch_rows: int, max_batches: Optional[int]
) -> Tuple[Dict[str, DatasetStats], List[CleaningIssue]]:
    zone_lookup = load_zone_lookup(data_dir)
    stats_by_dataset: Dict[str, DatasetStats] = {}
    cleaning_issues: List[CleaningIssue] = []

    for path in discover_data_files(data_dir):
        if path.name == "taxi_zone_lookup.csv":
            continue
        dataset = path.stem
        stats = stats_by_dataset.setdefault(dataset, DatasetStats())

        for batch_index, batch in enumerate(iter_dataframe_batches(path, batch_rows), start=1):
            stats.rows_loaded += len(batch)
            cleaned, invalid_datetime, invalid_values, batch_issues = clean_trip_batch(
                batch, path.name, dataset, zone_lookup
            )
            stats.rows_after_cleaning += len(cleaned)
            stats.invalid_datetime_rows += invalid_datetime
            stats.invalid_value_rows += invalid_values
            cleaning_issues.extend(batch_issues)
            add_numeric_stats(stats, cleaned)
            update_counts(stats, cleaned)

            if max_batches is not None and batch_index >= max_batches:
                break

    return stats_by_dataset, cleaning_issues


def fmt_number(value: float) -> str:
    if isinstance(value, float) and math.isnan(value):
        return "n/a"
    return f"{value:,.0f}"


def fmt_decimal(value: Optional[float], digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{value:,.{digits}f}"


def xml_text(value: object) -> str:
    return html.escape(str(value), quote=False)


def markdown_table(headers: Sequence[str], rows: Iterable[Sequence[object]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(lines)


def top_counter_rows(counter: Counter, limit: int) -> List[Tuple[str, int]]:
    return [(name, count) for name, count in counter.most_common(limit)]


def combined_counter(stats_by_dataset: Dict[str, DatasetStats], attr: str) -> Counter:
    combined = Counter()
    for stats in stats_by_dataset.values():
        combined.update(getattr(stats, attr))
    return combined


def aggregate_cleaning_issues(cleaning_issues: Sequence[CleaningIssue]) -> List[CleaningIssue]:
    aggregated: Dict[Tuple[str, str, str, str], CleaningIssue] = {}
    for issue in cleaning_issues:
        key = (issue.source_file, issue.column, issue.standardized_column, issue.explanation)
        existing = aggregated.get(key)
        if existing is None:
            aggregated[key] = CleaningIssue(
                source_file=issue.source_file,
                column=issue.column,
                standardized_column=issue.standardized_column,
                invalid_rows=issue.invalid_rows,
                explanation=issue.explanation,
                example_invalid_values=list(issue.example_invalid_values),
            )
            continue
        existing.invalid_rows += issue.invalid_rows
        seen = set(existing.example_invalid_values)
        for value in issue.example_invalid_values:
            if len(existing.example_invalid_values) >= 5:
                break
            if value in seen:
                continue
            existing.example_invalid_values.append(value)
            seen.add(value)
    return sorted(
        aggregated.values(),
        key=lambda issue: (issue.source_file, issue.column, issue.standardized_column, issue.explanation),
    )


def write_cleaning_audit(cleaning_issues: Sequence[CleaningIssue], output_dir: Path) -> Path:
    audit_path = output_dir / "cleaned_data_audit.csv"
    rows = [
        {
            "file": issue.source_file,
            "column": issue.column,
            "standardized_column": issue.standardized_column,
            "invalid_rows_cleaned": issue.invalid_rows,
            "example_invalid_values": "; ".join(issue.example_invalid_values),
            "explanation": issue.explanation,
        }
        for issue in aggregate_cleaning_issues(cleaning_issues)
    ]
    pd.DataFrame(
        rows,
        columns=[
            "file",
            "column",
            "standardized_column",
            "invalid_rows_cleaned",
            "example_invalid_values",
            "explanation",
        ],
    ).to_csv(audit_path, index=False)
    return audit_path


def write_bar_chart(path: Path, title: str, rows: Sequence[Tuple[str, float]], x_label: str) -> None:
    width = 920
    row_height = 42
    chart_left = 220
    chart_right = 860
    chart_top = 72
    height = max(220, chart_top + row_height * len(rows) + 64)
    max_value = max((value for _, value in rows), default=0) or 1
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="32" y="38" font-family="Arial, sans-serif" font-size="22" font-weight="700" fill="#111827">{xml_text(title)}</text>',
        f'<text x="{chart_left}" y="{height - 18}" font-family="Arial, sans-serif" font-size="12" fill="#4b5563">{xml_text(x_label)}</text>',
    ]
    for index, (label, value) in enumerate(rows):
        y = chart_top + index * row_height
        bar_width = int((chart_right - chart_left) * (value / max_value))
        color = CHART_COLORS[index % len(CHART_COLORS)]
        parts.extend(
            [
                f'<text x="32" y="{y + 23}" font-family="Arial, sans-serif" font-size="13" fill="#111827">{xml_text(label)}</text>',
                f'<rect x="{chart_left}" y="{y}" width="{bar_width}" height="24" rx="3" fill="{color}"/>',
                f'<text x="{chart_left + bar_width + 8}" y="{y + 18}" font-family="Arial, sans-serif" font-size="12" fill="#374151">{fmt_number(value)}</text>',
            ]
        )
    parts.append("</svg>")
    path.write_text("\n".join(parts), encoding="utf-8")


def write_line_chart(path: Path, title: str, hourly_counts: Counter) -> None:
    width = 920
    height = 360
    left = 64
    right = 872
    top = 72
    bottom = 300
    values = [hourly_counts.get(hour, 0) for hour in range(24)]
    max_value = max(values) or 1
    points = []
    for hour, value in enumerate(values):
        x = left + (right - left) * (hour / 23)
        y = bottom - (bottom - top) * (value / max_value)
        points.append((x, y, value))
    point_string = " ".join(f"{x:.1f},{y:.1f}" for x, y, _ in points)
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="32" y="38" font-family="Arial, sans-serif" font-size="22" font-weight="700" fill="#111827">{xml_text(title)}</text>',
        f'<line x1="{left}" y1="{bottom}" x2="{right}" y2="{bottom}" stroke="#9ca3af"/>',
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{bottom}" stroke="#9ca3af"/>',
        f'<polyline points="{point_string}" fill="none" stroke="#2563eb" stroke-width="3"/>',
    ]
    for hour, (x, y, value) in enumerate(points):
        if hour % 3 == 0:
            parts.append(
                f'<text x="{x - 8:.1f}" y="{bottom + 22}" font-family="Arial, sans-serif" font-size="12" fill="#4b5563">{hour}</text>'
            )
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3" fill="#2563eb"/>')
        if value == max_value:
            parts.append(
                f'<text x="{x + 8:.1f}" y="{y - 8:.1f}" font-family="Arial, sans-serif" font-size="12" fill="#374151">{fmt_number(value)}</text>'
            )
    parts.extend(
        [
            f'<text x="{left}" y="{height - 18}" font-family="Arial, sans-serif" font-size="12" fill="#4b5563">Pickup hour</text>',
            "</svg>",
        ]
    )
    path.write_text("\n".join(parts), encoding="utf-8")


def write_outputs(
    stats_by_dataset: Dict[str, DatasetStats],
    cleaning_issues: Sequence[CleaningIssue],
    output_dir: Path,
    max_batches: Optional[int],
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    chart_dir = output_dir / "charts"
    chart_dir.mkdir(parents=True, exist_ok=True)
    audit_path = write_cleaning_audit(cleaning_issues, output_dir)

    dataset_rows = []
    for dataset, stats in sorted(stats_by_dataset.items()):
        dataset_rows.append(
            [
                dataset,
                service_name(dataset),
                f"{stats.rows_loaded:,}",
                f"{stats.rows_after_cleaning:,}",
                f"{stats.invalid_datetime_rows:,}",
                f"{stats.invalid_value_rows:,}",
                fmt_decimal(finite_mean(stats.distance_sum, stats.distance_count)),
                fmt_decimal(finite_mean(stats.fare_sum, stats.fare_count)),
                fmt_decimal(finite_mean(stats.tip_sum, stats.tip_count)),
            ]
        )

    total_loaded = sum(stats.rows_loaded for stats in stats_by_dataset.values())
    total_clean = sum(stats.rows_after_cleaning for stats in stats_by_dataset.values())
    total_invalid_datetime = sum(stats.invalid_datetime_rows for stats in stats_by_dataset.values())
    total_invalid_values = sum(stats.invalid_value_rows for stats in stats_by_dataset.values())
    all_hours = combined_counter(stats_by_dataset, "pickup_hours")
    all_boroughs = combined_counter(stats_by_dataset, "pickup_boroughs")
    all_zones = combined_counter(stats_by_dataset, "pickup_zones")
    busiest_hour, busiest_hour_count = all_hours.most_common(1)[0] if all_hours else ("n/a", 0)
    busiest_borough, busiest_borough_count = all_boroughs.most_common(1)[0] if all_boroughs else ("n/a", 0)

    volume_rows = [
        (dataset, stats.rows_after_cleaning) for dataset, stats in sorted(stats_by_dataset.items())
    ]
    fare_rows = [
        (dataset, finite_mean(stats.fare_sum, stats.fare_count) or 0)
        for dataset, stats in sorted(stats_by_dataset.items())
    ]
    write_bar_chart(chart_dir / "clean_trip_volume_by_dataset.svg", "Clean trip volume by dataset", volume_rows, "Clean rows")
    write_bar_chart(chart_dir / "average_fare_by_dataset.svg", "Average fare by dataset", fare_rows, "Average fare")
    write_bar_chart(chart_dir / "top_pickup_boroughs.svg", "Top pickup boroughs", top_counter_rows(all_boroughs, 8), "Clean pickups")
    write_line_chart(chart_dir / "pickup_volume_by_hour.svg", "Pickup volume by hour", all_hours)

    scope_note = (
        f"Analysis used the first {max_batches} batch(es) per file."
        if max_batches is not None
        else "Analysis scanned all available batches."
    )
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    report = [
        "# Initial Taxi Data Analysis",
        "",
        f"Generated by `analysis/analyze_taxi_data.py` on {generated_at}.",
        "",
        f"Scope: {scope_note}",
        "",
        "## Summary",
        "",
        f"- Rows loaded: {total_loaded:,}",
        f"- Rows retained after cleaning: {total_clean:,}",
        f"- Rows removed for invalid timestamps: {total_invalid_datetime:,}",
        f"- Rows removed for invalid negative values: {total_invalid_values:,}",
        f"- Busiest pickup hour in the analyzed data: {busiest_hour} ({busiest_hour_count:,} clean pickups)",
        f"- Busiest pickup borough in the analyzed data: {busiest_borough} ({busiest_borough_count:,} clean pickups)",
        "",
        "## Dataset Metrics",
        "",
        markdown_table(
            [
                "Dataset",
                "Service",
                "Rows loaded",
                "Rows clean",
                "Invalid timestamps",
                "Invalid values",
                "Avg distance",
                "Avg fare",
                "Avg tip",
            ],
            dataset_rows,
        ),
        "",
        "## Top Pickup Boroughs",
        "",
        markdown_table(["Borough", "Clean pickups"], top_counter_rows(all_boroughs, 10)),
        "",
        "## Top Pickup Zones",
        "",
        markdown_table(["Zone", "Clean pickups"], top_counter_rows(all_zones, 15)),
        "",
        "## Charts",
        "",
        "- `charts/clean_trip_volume_by_dataset.svg`",
        "- `charts/average_fare_by_dataset.svg`",
        "- `charts/top_pickup_boroughs.svg`",
        "- `charts/pickup_volume_by_hour.svg`",
        "",
        "## Cleaning Audit",
        "",
        f"- `{audit_path.name}` lists each invalid data pattern cleaned during this run, including source file, source column, examples, and explanation.",
        "- Audit counts are per column/rule, so counts can overlap when the same source row has multiple invalid values.",
        "",
        "## Notes",
        "",
        "Cleaning standardizes pickup/dropoff timestamps, pickup location IDs, distance, fare, tip, and passenger fields across taxi service types.",
        "Rows are removed when pickup timestamps are missing, dropoff timestamps precede pickup timestamps, or numeric trip measures are negative.",
        "For full-file analysis, run with `--full`; for faster iteration, adjust `--max-batches` and `--batch-rows`.",
        "",
    ]
    report_path = output_dir / "initial_analysis.md"
    report_path.write_text("\n".join(report), encoding="utf-8")
    return report_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run initial analysis on local taxi data.")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument("--output-dir", type=Path, default=Path(__file__).resolve().parent / "outputs")
    parser.add_argument("--batch-rows", type=int, default=100_000)
    parser.add_argument("--max-batches", type=int, default=2)
    parser.add_argument("--full", action="store_true", help="Scan all batches instead of the sampled default.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    max_batches = None if args.full else args.max_batches
    stats_by_dataset, cleaning_issues = analyze_data(args.data_dir, args.batch_rows, max_batches)
    report_path = write_outputs(stats_by_dataset, cleaning_issues, args.output_dir, max_batches)
    print(f"Wrote analysis report to {report_path}")


if __name__ == "__main__":
    main()
