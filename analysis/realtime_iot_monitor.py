"""Monitor short taxi trip windows as if the files were IoT event feeds.

The local TLC files are historical batches, not live IoT feeds. This script
uses the most recent event time found in the scanned rows as the window end, so
it can be run against local data or scheduled every five minutes against a
freshly appended landing folder.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = REPO_ROOT / "data"
if str(DATA_DIR) not in sys.path:
    sys.path.insert(0, str(DATA_DIR))

from ingest_data import discover_data_files, iter_dataframe_batches  # noqa: E402


STANDARD_COLUMNS = [
    "source_file",
    "dataset",
    "service",
    "source_column",
    "source_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "fare",
    "tip",
    "distance",
    "payment_type",
    "store_and_fwd_flag",
]


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
        return pd.DataFrame(columns=["LocationID", "Borough", "Zone"])
    lookup = pd.read_csv(lookup_path)
    lookup["LocationID"] = pd.to_numeric(lookup["LocationID"], errors="coerce")
    return lookup.dropna(subset=["LocationID"]).astype({"LocationID": "int64"})


def choose_source_column(df: pd.DataFrame) -> Optional[str]:
    return find_column(
        df,
        [
            "taxi_id",
            "vehicle_id",
            "cab_id",
            "medallion",
            "hack_license",
            "VendorID",
            "dispatching_base_num",
            "originating_base_num",
            "hvfhs_license_num",
            "Affiliated_base_number",
        ],
    )


def standardize_batch(df: pd.DataFrame, source_file: str, dataset: str) -> pd.DataFrame:
    pickup_col = find_column(
        df,
        ["pickup_datetime", "tpep_pickup_datetime", "lpep_pickup_datetime", "request_datetime"],
    )
    dropoff_col = find_column(
        df,
        ["dropoff_datetime", "dropOff_datetime", "tpep_dropoff_datetime", "lpep_dropoff_datetime"],
    )
    source_col = choose_source_column(df)
    pickup_location_col = find_column(df, ["PULocationID", "PUlocationID", "pickup_locationid"])
    fare_col = find_column(df, ["total_amount", "base_passenger_fare", "fare_amount"])
    tip_col = find_column(df, ["tip_amount", "tips"])
    distance_col = find_column(df, ["trip_distance", "trip_miles"])
    payment_col = find_column(df, ["payment_type"])
    store_forward_col = find_column(df, ["store_and_fwd_flag"])

    standardized = pd.DataFrame(index=df.index)
    standardized["source_file"] = source_file
    standardized["dataset"] = dataset
    standardized["service"] = service_name(dataset)
    standardized["source_column"] = source_col or "dataset"
    standardized["source_id"] = df[source_col].astype(str) if source_col else dataset
    standardized["pickup_datetime"] = pd.to_datetime(df[pickup_col], errors="coerce") if pickup_col else pd.NaT
    standardized["dropoff_datetime"] = pd.to_datetime(df[dropoff_col], errors="coerce") if dropoff_col else pd.NaT
    standardized["pickup_location_id"] = (
        pd.to_numeric(df[pickup_location_col], errors="coerce") if pickup_location_col else pd.NA
    )
    standardized["fare"] = pd.to_numeric(df[fare_col], errors="coerce") if fare_col else pd.NA
    standardized["tip"] = pd.to_numeric(df[tip_col], errors="coerce") if tip_col else pd.NA
    standardized["distance"] = pd.to_numeric(df[distance_col], errors="coerce") if distance_col else pd.NA
    standardized["payment_type"] = df[payment_col].astype(str) if payment_col else pd.NA
    standardized["store_and_fwd_flag"] = df[store_forward_col].astype(str) if store_forward_col else pd.NA
    return standardized


def load_event_rows(data_dir: Path, batch_rows: int, max_batches: Optional[int]) -> pd.DataFrame:
    frames = []
    for path in discover_data_files(data_dir):
        if path.name == "taxi_zone_lookup.csv":
            continue
        dataset = path.stem
        for batch_index, batch in enumerate(iter_dataframe_batches(path, batch_rows), start=1):
            frames.append(standardize_batch(batch, path.name, dataset))
            if max_batches is not None and batch_index >= max_batches:
                break
    if not frames:
        return pd.DataFrame()
    compact_frames = [frame.dropna(axis=1, how="all") for frame in frames if not frame.empty]
    events = pd.concat(compact_frames, ignore_index=True)
    for column in STANDARD_COLUMNS:
        if column not in events.columns:
            events[column] = pd.NA
    return events[STANDARD_COLUMNS]


def determine_window(
    events: pd.DataFrame, lookback_minutes: int, end_time: Optional[str], window_mode: str
) -> Tuple[pd.Timestamp, pd.Timestamp]:
    if end_time:
        window_end = pd.Timestamp(end_time)
        window_start = window_end - pd.Timedelta(minutes=lookback_minutes)
        return window_start, window_end
    if window_mode == "anomaly-demo":
        negative_fares = events[events["fare"].notna() & (events["fare"] < 0)].copy()
        negative_fares = negative_fares.dropna(subset=["pickup_datetime"])
        if negative_fares.empty:
            raise ValueError("No negative fare events were found for anomaly-demo mode.")
        buckets = negative_fares["pickup_datetime"].dt.floor(f"{lookback_minutes}min")
        window_start = buckets.value_counts().idxmax()
        window_end = window_start + pd.Timedelta(minutes=lookback_minutes)
    else:
        window_end = events["pickup_datetime"].dropna().max()
        if pd.isna(window_end):
            raise ValueError("No valid pickup timestamps were found in the scanned events.")
        window_start = window_end - pd.Timedelta(minutes=lookback_minutes)
    return window_start, window_end


def enrich_with_zones(events: pd.DataFrame, zone_lookup: pd.DataFrame) -> pd.DataFrame:
    if events.empty or zone_lookup.empty:
        events["Borough"] = pd.NA
        events["Zone"] = pd.NA
        return events
    events = events.copy()
    events["pickup_location_id"] = pd.to_numeric(events["pickup_location_id"], errors="coerce")
    return events.merge(
        zone_lookup[["LocationID", "Borough", "Zone"]],
        how="left",
        left_on="pickup_location_id",
        right_on="LocationID",
    )


def fmt_number(value: float) -> str:
    if isinstance(value, float) and math.isnan(value):
        return "n/a"
    return f"{value:,.0f}"


def fmt_decimal(value: object, digits: int = 2) -> str:
    if value is None or pd.isna(value):
        return "n/a"
    return f"{float(value):,.{digits}f}"


def top_rows(series: pd.Series, limit: int) -> List[Dict[str, object]]:
    counts = Counter(str(value) for value in series.dropna())
    return [{"value": value, "trips": count} for value, count in counts.most_common(limit)]


def source_quality_rows(window: pd.DataFrame) -> pd.DataFrame:
    if window.empty:
        return pd.DataFrame()
    rows = []
    group_columns = ["dataset", "service", "source_column", "source_id"]
    for keys, group in window.groupby(group_columns, dropna=False):
        dataset, service, source_column, source_id = keys
        negative_fare = group["fare"].notna() & (group["fare"] < 0)
        negative_tip = group["tip"].notna() & (group["tip"] < 0)
        negative_distance = group["distance"].notna() & (group["distance"] < 0)
        invalid_time = group["pickup_datetime"].isna() | (
            group["dropoff_datetime"].notna() & (group["dropoff_datetime"] < group["pickup_datetime"])
        )
        store_forward = group["store_and_fwd_flag"].str.upper().eq("Y")
        rows.append(
            {
                "dataset": dataset,
                "service": service,
                "source_column": source_column,
                "source_id": source_id,
                "trips": len(group),
                "negative_fares": int(negative_fare.sum()),
                "negative_tips": int(negative_tip.sum()),
                "negative_distances": int(negative_distance.sum()),
                "invalid_timestamps": int(invalid_time.sum()),
                "store_and_forward": int(store_forward.sum()),
                "avg_fare": group["fare"].dropna().mean(),
                "example_negative_fares": "; ".join(str(value) for value in group.loc[negative_fare, "fare"].head(5)),
            }
        )
    quality = pd.DataFrame(rows)
    if quality.empty:
        return quality
    quality["negative_fare_rate"] = quality["negative_fares"] / quality["trips"]
    quality["negative_tip_rate"] = quality["negative_tips"] / quality["trips"]
    quality["invalid_timestamp_rate"] = quality["invalid_timestamps"] / quality["trips"]
    return quality.sort_values(
        ["negative_fares", "negative_fare_rate", "invalid_timestamps"],
        ascending=[False, False, False],
    )


def build_alerts(
    window: pd.DataFrame,
    previous_window: pd.DataFrame,
    min_cluster_count: int,
    min_cluster_rate: float,
    zone_spike_multiplier: float,
    min_zone_trips: int,
) -> List[Dict[str, object]]:
    alerts: List[Dict[str, object]] = []
    quality = source_quality_rows(window)
    if not quality.empty:
        flagged = quality[
            (quality["negative_fares"] >= min_cluster_count)
            | ((quality["negative_fare_rate"] >= min_cluster_rate) & (quality["negative_fares"] > 0))
        ]
        for _, row in flagged.iterrows():
            alerts.append(
                {
                    "type": "NEGATIVE_FARE_CLUSTER",
                    "severity": "high" if row["negative_fares"] >= min_cluster_count else "medium",
                    "source": f"{row['dataset']} {row['source_column']}={row['source_id']}",
                    "count": int(row["negative_fares"]),
                    "rate": round(float(row["negative_fare_rate"]), 4),
                    "examples": row["example_negative_fares"],
                    "explanation": "Repeated negative fare values may indicate a meter, payment, or upstream device/configuration issue.",
                }
            )

        clock_flagged = quality[quality["invalid_timestamps"] >= min_cluster_count]
        for _, row in clock_flagged.iterrows():
            alerts.append(
                {
                    "type": "CLOCK_OR_EVENT_ORDER_ISSUE",
                    "severity": "medium",
                    "source": f"{row['dataset']} {row['source_column']}={row['source_id']}",
                    "count": int(row["invalid_timestamps"]),
                    "rate": round(float(row["invalid_timestamp_rate"]), 4),
                    "explanation": "Missing pickup timestamps or dropoffs before pickups can indicate clock drift or event-order problems.",
                }
            )

        buffered = quality[quality["store_and_forward"] >= min_cluster_count]
        for _, row in buffered.iterrows():
            alerts.append(
                {
                    "type": "STORE_AND_FORWARD_SPIKE",
                    "severity": "medium",
                    "source": f"{row['dataset']} {row['source_column']}={row['source_id']}",
                    "count": int(row["store_and_forward"]),
                    "explanation": "Store-and-forward flags suggest devices may have lost connectivity before uploading trips.",
                }
            )

    if not window.empty and "Zone" in window.columns:
        current_zones = Counter(str(value) for value in window["Zone"].dropna())
        previous_zones = Counter(str(value) for value in previous_window.get("Zone", pd.Series(dtype="object")).dropna())
        for zone, count in current_zones.most_common(10):
            baseline = previous_zones.get(zone, 0)
            if count >= min_zone_trips and count >= max(1, baseline) * zone_spike_multiplier:
                alerts.append(
                    {
                        "type": "PICKUP_ZONE_SPIKE",
                        "severity": "low",
                        "source": zone,
                        "count": count,
                        "previous_count": baseline,
                        "explanation": "Pickup demand is materially above the previous window for this zone.",
                    }
                )
    return alerts


def summarize(
    events: pd.DataFrame,
    zone_lookup: pd.DataFrame,
    lookback_minutes: int,
    end_time: Optional[str],
    window_mode: str,
    min_cluster_count: int,
    min_cluster_rate: float,
    zone_spike_multiplier: float,
    min_zone_trips: int,
) -> Dict[str, object]:
    window_start, window_end = determine_window(events, lookback_minutes, end_time, window_mode)
    previous_start = window_start - pd.Timedelta(minutes=lookback_minutes)
    events = enrich_with_zones(events, zone_lookup)
    current = events[(events["pickup_datetime"] >= window_start) & (events["pickup_datetime"] < window_end)].copy()
    previous = events[(events["pickup_datetime"] >= previous_start) & (events["pickup_datetime"] < window_start)].copy()

    quality = source_quality_rows(current)
    top_quality = quality.head(10).copy()
    if not top_quality.empty:
        top_quality["negative_fare_rate"] = top_quality["negative_fare_rate"].round(4)
        top_quality["negative_tip_rate"] = top_quality["negative_tip_rate"].round(4)
        top_quality["invalid_timestamp_rate"] = top_quality["invalid_timestamp_rate"].round(4)

    alerts = build_alerts(
        current,
        previous,
        min_cluster_count,
        min_cluster_rate,
        zone_spike_multiplier,
        min_zone_trips,
    )

    negative_fare = current["fare"].notna() & (current["fare"] < 0)
    negative_tip = current["tip"].notna() & (current["tip"] < 0)
    negative_distance = current["distance"].notna() & (current["distance"] < 0)
    invalid_time = current["pickup_datetime"].isna() | (
        current["dropoff_datetime"].notna() & (current["dropoff_datetime"] < current["pickup_datetime"])
    )
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "lookback_minutes": lookback_minutes,
        "window_mode": window_mode,
        "scanned_rows": int(len(events)),
        "window_rows": int(len(current)),
        "previous_window_rows": int(len(previous)),
        "negative_fare_rows": int(negative_fare.sum()),
        "negative_tip_rows": int(negative_tip.sum()),
        "negative_distance_rows": int(negative_distance.sum()),
        "invalid_timestamp_rows": int(invalid_time.sum()),
        "avg_fare": None if current["fare"].dropna().empty else float(current["fare"].dropna().mean()),
        "avg_distance": None if current["distance"].dropna().empty else float(current["distance"].dropna().mean()),
        "top_pickup_zones": top_rows(current.get("Zone", pd.Series(dtype="object")), 8),
        "top_pickup_boroughs": top_rows(current.get("Borough", pd.Series(dtype="object")), 8),
        "volume_by_dataset": top_rows(current["dataset"], 12),
        "source_quality_leaders": top_quality.to_dict(orient="records"),
        "alerts": alerts,
        "identifier_note": (
            "These TLC files do not include a true per-cab IoT device ID. "
            "The monitor groups by the most specific available source field, such as VendorID, dispatching_base_num, "
            "originating_base_num, or hvfhs_license_num."
        ),
    }
    return summary


def markdown_table(headers: Sequence[str], rows: Iterable[Sequence[object]]) -> str:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(lines)


def write_outputs(summary: Dict[str, object], output_dir: Path) -> Tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "latest_iot_insights.json"
    md_path = output_dir / "latest_iot_insights.md"
    json_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

    alerts = summary["alerts"]
    alert_rows = [
        [
            alert["severity"],
            alert["type"],
            alert["source"],
            alert.get("count", "n/a"),
            alert["explanation"],
        ]
        for alert in alerts
    ]
    quality_rows = [
        [
            row["dataset"],
            f"{row['source_column']}={row['source_id']}",
            row["trips"],
            row["negative_fares"],
            fmt_decimal(row["negative_fare_rate"], 2),
            row["invalid_timestamps"],
            fmt_decimal(row["avg_fare"], 2),
        ]
        for row in summary["source_quality_leaders"]
    ]
    zone_rows = [[row["value"], row["trips"]] for row in summary["top_pickup_zones"]]
    dataset_rows = [[row["value"], row["trips"]] for row in summary["volume_by_dataset"]]

    report = [
        "# Real-Time IoT Taxi Insights",
        "",
        f"Generated at: {summary['generated_at']}",
        f"Window: {summary['window_start']} to {summary['window_end']} ({summary['lookback_minutes']} minutes)",
        f"Mode: {summary['window_mode']}",
        "",
        "## Summary",
        "",
        f"- Scanned rows: {fmt_number(summary['scanned_rows'])}",
        f"- Current-window rows: {fmt_number(summary['window_rows'])}",
        f"- Previous-window rows: {fmt_number(summary['previous_window_rows'])}",
        f"- Negative fare rows: {fmt_number(summary['negative_fare_rows'])}",
        f"- Negative tip rows: {fmt_number(summary['negative_tip_rows'])}",
        f"- Negative distance rows: {fmt_number(summary['negative_distance_rows'])}",
        f"- Invalid timestamp rows: {fmt_number(summary['invalid_timestamp_rows'])}",
        f"- Average fare: {fmt_decimal(summary['avg_fare'])}",
        f"- Average distance: {fmt_decimal(summary['avg_distance'])}",
        "",
        "## Alerts",
        "",
        markdown_table(["Severity", "Type", "Source", "Count", "Explanation"], alert_rows)
        if alert_rows
        else "No alert thresholds were crossed in this window.",
        "",
        "## Source Quality Leaders",
        "",
        markdown_table(
            ["Dataset", "Source", "Trips", "Negative fares", "Negative fare rate", "Invalid timestamps", "Avg fare"],
            quality_rows,
        )
        if quality_rows
        else "No source quality rows were available in this window.",
        "",
        "## Volume By Dataset",
        "",
        markdown_table(["Dataset", "Trips"], dataset_rows) if dataset_rows else "No trips in this window.",
        "",
        "## Top Pickup Zones",
        "",
        markdown_table(["Zone", "Trips"], zone_rows) if zone_rows else "No pickup zones in this window.",
        "",
        "## Identifier Note",
        "",
        str(summary["identifier_note"]),
        "",
    ]
    md_path.write_text("\n".join(report), encoding="utf-8")
    return md_path, json_path


def run_once(args: argparse.Namespace) -> Tuple[Path, Path]:
    events = load_event_rows(args.data_dir, args.batch_rows, args.max_batches)
    if events.empty:
        raise ValueError(f"No supported trip events found in {args.data_dir}.")
    summary = summarize(
        events,
        load_zone_lookup(args.data_dir),
        args.lookback_minutes,
        args.end_time,
        args.window_mode,
        args.min_cluster_count,
        args.min_cluster_rate,
        args.zone_spike_multiplier,
        args.min_zone_trips,
    )
    return write_outputs(summary, args.output_dir)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create five-minute IoT-style taxi monitoring insights.")
    parser.add_argument("--data-dir", type=Path, default=DATA_DIR)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent / "outputs" / "realtime",
    )
    parser.add_argument("--batch-rows", type=int, default=100_000)
    parser.add_argument(
        "--max-batches",
        type=int,
        default=2,
        help="Batches to scan per file. Use --full to scan all available rows.",
    )
    parser.add_argument("--full", action="store_true", help="Scan all batches instead of the sampled default.")
    parser.add_argument("--lookback-minutes", type=int, default=5)
    parser.add_argument("--end-time", help="Optional ISO timestamp for the monitoring window end.")
    parser.add_argument(
        "--window-mode",
        choices=["latest", "anomaly-demo"],
        default="latest",
        help="Use latest for scheduled monitoring, or anomaly-demo to pick a local historical window with negative fares.",
    )
    parser.add_argument("--min-cluster-count", type=int, default=3)
    parser.add_argument("--min-cluster-rate", type=float, default=0.20)
    parser.add_argument("--zone-spike-multiplier", type=float, default=2.5)
    parser.add_argument("--min-zone-trips", type=int, default=20)
    parser.add_argument("--watch", action="store_true", help="Keep running and refresh every interval.")
    parser.add_argument("--interval-seconds", type=int, default=300)
    args = parser.parse_args()
    if args.full:
        args.max_batches = None
    return args


def main() -> None:
    args = parse_args()
    while True:
        md_path, json_path = run_once(args)
        print(f"Wrote IoT insights to {md_path} and {json_path}")
        if not args.watch:
            break
        time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
