"""Ingest local data files and document their structure.

The framework discovers supported files in the data folder, loads them into
pandas DataFrames on demand or in batches, infers obvious primary and foreign
keys, writes a Markdown data dictionary, and appends data quality check results
to a JSONL log.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

import pandas as pd
import pyarrow.parquet as pq


SUPPORTED_SUFFIXES = {".csv", ".parquet", ".json", ".jsonl", ".xlsx", ".xls"}
DOCUMENTATION_FILES = {"README.md", "data_structure.md"}
KEY_NAME_TOKENS = ("id", "key", "code", "num", "number")
NONNEGATIVE_COLUMN_TOKENS = (
    "amount",
    "fare",
    "fee",
    "surcharge",
    "tax",
    "tip",
    "toll",
    "distance",
    "mile",
    "time",
    "pay",
)


DESCRIPTION_BY_COLUMN = {
    "affiliated_base_number": "Base number affiliated with the FHV dispatching base.",
    "access_a_ride_flag": "Flag indicating whether the trip was associated with Access-A-Ride service.",
    "airport_fee": "Airport access fee charged on eligible trips.",
    "base_passenger_fare": "Passenger fare before taxes, fees, tolls, and tips.",
    "bcf": "Black car fund fee.",
    "borough": "NYC borough or airport area for the taxi zone.",
    "cbd_congestion_fee": "Congestion fee for trips in the Manhattan central business district.",
    "congestion_surcharge": "NYC congestion surcharge applied to the trip.",
    "dispatching_base_num": "Dispatching base license number.",
    "dolocationid": "Drop-off taxi zone location identifier.",
    "dropoff_datetime": "Trip drop-off timestamp.",
    "dropoff_locationid": "Drop-off taxi zone location identifier.",
    "dropoff_datetime_fhv": "Trip drop-off timestamp.",
    "driver_pay": "Amount paid to the driver.",
    "ehail_fee": "Electronic hail fee, when present.",
    "extra": "Additional surcharges or extras applied to the fare.",
    "fare_amount": "Metered fare amount before fees, taxes, tolls, and tips.",
    "hvfhs_license_num": "High-volume for-hire service license number.",
    "improvement_surcharge": "NYC taxi improvement surcharge.",
    "locationid": "Unique TLC taxi zone identifier.",
    "lpep_dropoff_datetime": "Green taxi drop-off timestamp.",
    "lpep_pickup_datetime": "Green taxi pickup timestamp.",
    "mta_tax": "MTA tax charged on the trip.",
    "on_scene_datetime": "Timestamp when the driver arrived at the passenger pickup scene.",
    "originating_base_num": "Originating base license number.",
    "passenger_count": "Number of passengers reported for the trip.",
    "payment_type": "Payment method code.",
    "pickup_datetime": "Trip pickup timestamp.",
    "pickup_locationid": "Pickup taxi zone location identifier.",
    "pulocationid": "Pickup taxi zone location identifier.",
    "ratecodeid": "Final rate code applied to the trip.",
    "request_datetime": "Timestamp when the passenger requested the trip.",
    "sales_tax": "Sales tax charged on the trip.",
    "service_zone": "TLC service zone category.",
    "shared_match_flag": "Flag indicating whether a shared ride match occurred.",
    "shared_request_flag": "Flag indicating whether the passenger requested a shared ride.",
    "sr_flag": "Shared ride flag.",
    "store_and_fwd_flag": "Flag indicating whether trip data was stored before being forwarded.",
    "tip_amount": "Tip amount paid by the passenger.",
    "tips": "Tip amount paid by the passenger.",
    "tolls": "Tolls charged on the trip.",
    "tolls_amount": "Tolls charged on the trip.",
    "total_amount": "Total amount charged to the passenger.",
    "tpep_dropoff_datetime": "Yellow taxi drop-off timestamp.",
    "tpep_pickup_datetime": "Yellow taxi pickup timestamp.",
    "trip_distance": "Trip distance reported by the meter.",
    "trip_miles": "Trip distance in miles.",
    "trip_time": "Trip duration in seconds.",
    "trip_type": "Green taxi trip type code.",
    "vendorid": "TPEP provider or vendor identifier.",
    "wav_match_flag": "Flag indicating whether a wheelchair-accessible vehicle match occurred.",
    "wav_request_flag": "Flag indicating whether a wheelchair-accessible vehicle was requested.",
    "zone": "TLC taxi zone name.",
}


@dataclass
class DatasetProfile:
    name: str
    path: Path
    row_count: int
    columns: List[str]
    dtypes: Dict[str, str]
    primary_key: Optional[str]


@dataclass
class ForeignKey:
    dataset: str
    column: str
    referenced_dataset: str
    referenced_column: str
    confidence: str


def normalize_name(value: str) -> str:
    return "".join(ch for ch in value.lower() if ch.isalnum())


def dataset_name(path: Path) -> str:
    return path.stem


def discover_data_files(data_dir: Path) -> List[Path]:
    files = []
    for path in data_dir.iterdir():
        if not path.is_file() or path.name.startswith("."):
            continue
        if path.name in DOCUMENTATION_FILES:
            continue
        if path.suffix.lower() in SUPPORTED_SUFFIXES:
            files.append(path)
    return sorted(files)


def load_dataframe(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix in {".json", ".jsonl"}:
        return pd.read_json(path, lines=suffix == ".jsonl")
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError(f"Unsupported data file type: {path.suffix}")


def load_dataframes(data_dir: Path) -> Dict[str, pd.DataFrame]:
    """Load every supported data file into a pandas DataFrame."""
    return {dataset_name(path): load_dataframe(path) for path in discover_data_files(data_dir)}


def iter_dataframe_batches(path: Path, batch_rows: int) -> Iterator[pd.DataFrame]:
    """Incrementally load a data file into pandas DataFrames."""
    suffix = path.suffix.lower()
    if suffix == ".csv":
        yield from pd.read_csv(path, chunksize=batch_rows)
        return
    if suffix == ".parquet":
        parquet_file = pq.ParquetFile(path)
        for batch in parquet_file.iter_batches(batch_size=batch_rows):
            yield batch.to_pandas()
        return
    if suffix in {".json", ".jsonl"}:
        if suffix == ".jsonl":
            yield from pd.read_json(path, lines=True, chunksize=batch_rows)
            return
        yield load_dataframe(path)
        return
    if suffix in {".xlsx", ".xls"}:
        yield load_dataframe(path)
        return
    raise ValueError(f"Unsupported data file type: {path.suffix}")


def parquet_columns_and_types(path: Path) -> Tuple[int, List[str], Dict[str, str]]:
    parquet_file = pq.ParquetFile(path)
    columns = parquet_file.schema.names
    dtypes = {field.name: str(field.type) for field in parquet_file.schema_arrow}
    return parquet_file.metadata.num_rows, columns, dtypes


def tabular_columns_and_types(path: Path) -> Tuple[int, List[str], Dict[str, str]]:
    df = load_dataframe(path)
    return len(df), list(df.columns), {column: str(dtype) for column, dtype in df.dtypes.items()}


def read_column_sample(path: Path, column: str, sample_rows: int) -> pd.Series:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        parquet_file = pq.ParquetFile(path)
        batches = parquet_file.iter_batches(batch_size=sample_rows, columns=[column])
        try:
            return next(batches).to_pandas()[column]
        except StopIteration:
            return pd.Series(name=column, dtype="object")
    if suffix == ".csv":
        return pd.read_csv(path, usecols=[column], nrows=sample_rows)[column]
    return load_dataframe(path)[column].head(sample_rows)


def read_column(path: Path, column: str) -> pd.Series:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path, columns=[column])[column]
    if suffix == ".csv":
        return pd.read_csv(path, usecols=[column])[column]
    return load_dataframe(path)[column]


def looks_like_key(column: str) -> bool:
    normalized = normalize_name(column)
    return any(token in normalized for token in KEY_NAME_TOKENS)


def is_unique_non_null(values: pd.Series) -> bool:
    return not values.isna().any() and values.nunique(dropna=False) == len(values)


def infer_primary_key(path: Path, columns: Sequence[str], sample_rows: int) -> Optional[str]:
    candidates = [column for column in columns if looks_like_key(column)]
    if not candidates:
        return None

    for column in candidates:
        sample = read_column_sample(path, column, sample_rows)
        if len(sample) == 0 or not is_unique_non_null(sample):
            continue
        full_column = read_column(path, column)
        if is_unique_non_null(full_column):
            return column
    return None


def profile_dataset(path: Path, sample_rows: int) -> DatasetProfile:
    if path.suffix.lower() == ".parquet":
        row_count, columns, dtypes = parquet_columns_and_types(path)
    else:
        row_count, columns, dtypes = tabular_columns_and_types(path)
    primary_key = infer_primary_key(path, columns, sample_rows)
    return DatasetProfile(
        name=dataset_name(path),
        path=path,
        row_count=row_count,
        columns=columns,
        dtypes=dtypes,
        primary_key=primary_key,
    )


def possible_fk_match(column: str, primary_key: str) -> bool:
    normalized_column = normalize_name(column)
    normalized_pk = normalize_name(primary_key)
    if normalized_column == normalized_pk:
        return True
    return normalized_column.endswith(normalized_pk) or normalized_pk in normalized_column


def infer_foreign_keys(profiles: Sequence[DatasetProfile], sample_rows: int) -> List[ForeignKey]:
    relationships = []
    keyed_profiles = [profile for profile in profiles if profile.primary_key]

    for profile in profiles:
        for column in profile.columns:
            for referenced in keyed_profiles:
                if profile.name == referenced.name:
                    continue
                assert referenced.primary_key is not None
                if not possible_fk_match(column, referenced.primary_key):
                    continue

                source_values = read_column_sample(profile.path, column, sample_rows).dropna()
                referenced_values = set(read_column(referenced.path, referenced.primary_key).dropna())
                if source_values.empty:
                    continue
                if set(source_values).issubset(referenced_values):
                    relationships.append(
                        ForeignKey(
                            dataset=profile.name,
                            column=column,
                            referenced_dataset=referenced.name,
                            referenced_column=referenced.primary_key,
                            confidence="sampled values match referenced primary key",
                        )
                    )
    return relationships


def column_description(column: str) -> str:
    normalized = normalize_name(column)
    for description_column, description in DESCRIPTION_BY_COLUMN.items():
        if normalize_name(description_column) == normalized:
            return description
    return "No description inferred; review and update as needed."


def key_label(profile: DatasetProfile, column: str, foreign_keys: Sequence[ForeignKey]) -> str:
    labels = []
    if profile.primary_key == column:
        labels.append("**Primary key**")
    for foreign_key in foreign_keys:
        if foreign_key.dataset == profile.name and foreign_key.column == column:
            labels.append(
                f"Foreign key to `{foreign_key.referenced_dataset}.{foreign_key.referenced_column}`"
            )
    return "<br>".join(labels) if labels else ""


def markdown_table(headers: Sequence[str], rows: Iterable[Sequence[object]]) -> str:
    output = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join("---" for _ in headers) + " |",
    ]
    for row in rows:
        output.append("| " + " | ".join(str(value) for value in row) + " |")
    return "\n".join(output)


def build_documentation(profiles: Sequence[DatasetProfile], foreign_keys: Sequence[ForeignKey]) -> str:
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = [
        "# Data Structure",
        "",
        f"Generated by `data/ingest_data.py` on {generated_at}.",
        "",
        "## Source Files",
        "",
        markdown_table(
            ["Dataset", "File", "Rows", "Columns", "Primary key"],
            [
                [
                    profile.name,
                    profile.path.name,
                    f"{profile.row_count:,}",
                    len(profile.columns),
                    f"**{profile.primary_key}**" if profile.primary_key else "No single-column primary key found",
                ]
                for profile in profiles
            ],
        ),
        "",
        "## Foreign Keys",
        "",
    ]

    if foreign_keys:
        lines.append(
            markdown_table(
                ["Dataset", "Column", "References", "Confidence"],
                [
                    [
                        foreign_key.dataset,
                        foreign_key.column,
                        f"`{foreign_key.referenced_dataset}.{foreign_key.referenced_column}`",
                        foreign_key.confidence,
                    ]
                    for foreign_key in foreign_keys
                ],
            )
        )
    else:
        lines.append("No foreign keys were inferred.")

    for profile in profiles:
        lines.extend(["", f"## {profile.name}", ""])
        lines.append(f"File: `{profile.path.name}`")
        lines.append("")
        lines.append(f"Rows: {profile.row_count:,}")
        lines.append("")
        lines.append(
            f"Primary key: **{profile.primary_key}**"
            if profile.primary_key
            else "Primary key: No single-column primary key found."
        )
        lines.append("")
        lines.append(
            markdown_table(
                ["Column", "Type", "Key", "Description"],
                [
                    [
                        f"**{column}**" if profile.primary_key == column else column,
                        profile.dtypes.get(column, ""),
                        key_label(profile, column, foreign_keys),
                        column_description(column),
                    ]
                    for column in profile.columns
                ],
            )
        )

    return "\n".join(lines) + "\n"


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def append_quality_log(log_path: Path, record: Dict[str, object]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as log_file:
        log_file.write(json.dumps(record, sort_keys=True) + "\n")


def matching_datetime_pairs(columns: Sequence[str]) -> List[Tuple[str, str]]:
    pairs = []
    normalized_to_column = {normalize_name(column): column for column in columns}
    candidates = [
        ("pickup_datetime", "dropoff_datetime"),
        ("tpep_pickup_datetime", "tpep_dropoff_datetime"),
        ("lpep_pickup_datetime", "lpep_dropoff_datetime"),
        ("request_datetime", "dropoff_datetime"),
    ]
    for pickup, dropoff in candidates:
        pickup_column = normalized_to_column.get(normalize_name(pickup))
        dropoff_column = normalized_to_column.get(normalize_name(dropoff))
        if pickup_column and dropoff_column:
            pairs.append((pickup_column, dropoff_column))
    return pairs


def nonnegative_columns(columns: Sequence[str]) -> List[str]:
    matches = []
    for column in columns:
        normalized = normalize_name(column)
        if any(token in normalized for token in NONNEGATIVE_COLUMN_TOKENS):
            matches.append(column)
    return matches


def validation_record(
    profile: DatasetProfile,
    check_name: str,
    status: str,
    details: Dict[str, object],
) -> Dict[str, object]:
    return {
        "checked_at": utc_now(),
        "dataset": profile.name,
        "file": profile.path.name,
        "check": check_name,
        "status": status,
        "details": details,
    }


def validate_dataset(
    profile: DatasetProfile,
    log_path: Path,
    batch_rows: int,
    max_batches: Optional[int],
) -> List[Dict[str, object]]:
    records = [
        validation_record(
            profile,
            "metadata",
            "pass" if profile.row_count > 0 and len(profile.columns) > 0 else "fail",
            {
                "rows": profile.row_count,
                "columns": len(profile.columns),
                "primary_key": profile.primary_key,
            },
        )
    ]

    expected_columns = list(profile.columns)
    expected_column_set = set(expected_columns)
    null_counts = {column: 0 for column in expected_columns}
    duplicate_rows = 0
    invalid_datetime_pairs = {
        f"{pickup}>{dropoff}": 0 for pickup, dropoff in matching_datetime_pairs(expected_columns)
    }
    negative_values = {column: 0 for column in nonnegative_columns(expected_columns)}
    processed_rows = 0
    processed_batches = 0
    schema_mismatches = []

    for batch in iter_dataframe_batches(profile.path, batch_rows):
        processed_batches += 1
        processed_rows += len(batch)

        batch_columns = list(batch.columns)
        if batch_columns != expected_columns:
            schema_mismatches.append(
                {
                    "batch": processed_batches,
                    "missing_columns": sorted(expected_column_set - set(batch_columns)),
                    "extra_columns": sorted(set(batch_columns) - expected_column_set),
                }
            )

        for column in expected_columns:
            if column in batch.columns:
                null_counts[column] += int(batch[column].isna().sum())

        duplicate_rows += int(batch.duplicated().sum())

        for pair_key in invalid_datetime_pairs:
            pickup, dropoff = pair_key.split(">", maxsplit=1)
            if pickup in batch.columns and dropoff in batch.columns:
                invalid_datetime_pairs[pair_key] += int((batch[pickup] > batch[dropoff]).sum())

        for column in negative_values:
            if column in batch.columns and pd.api.types.is_numeric_dtype(batch[column]):
                negative_values[column] += int((batch[column] < 0).sum())

        if max_batches is not None and processed_batches >= max_batches:
            break

    records.extend(
        [
            validation_record(
                profile,
                "schema",
                "pass" if not schema_mismatches else "fail",
                {
                    "expected_columns": expected_columns,
                    "mismatches": schema_mismatches,
                    "processed_batches": processed_batches,
                },
            ),
            validation_record(
                profile,
                "nulls",
                "pass",
                {
                    "processed_rows": processed_rows,
                    "null_counts": null_counts,
                },
            ),
            validation_record(
                profile,
                "duplicates",
                "pass" if duplicate_rows == 0 else "warn",
                {
                    "processed_rows": processed_rows,
                    "duplicate_rows_within_batches": duplicate_rows,
                },
            ),
            validation_record(
                profile,
                "datetime_order",
                "pass" if sum(invalid_datetime_pairs.values()) == 0 else "warn",
                {
                    "processed_rows": processed_rows,
                    "invalid_pairs": invalid_datetime_pairs,
                },
            ),
            validation_record(
                profile,
                "nonnegative_values",
                "pass" if sum(negative_values.values()) == 0 else "warn",
                {
                    "processed_rows": processed_rows,
                    "negative_counts": negative_values,
                },
            ),
        ]
    )

    if max_batches is not None:
        records.append(
            validation_record(
                profile,
                "validation_scope",
                "warn",
                {
                    "processed_batches": processed_batches,
                    "batch_rows": batch_rows,
                    "message": "Validation was limited by --validation-max-batches.",
                },
            )
        )

    for record in records:
        append_quality_log(log_path, record)
    return records


def write_data_structure(
    data_dir: Path,
    output_path: Path,
    sample_rows: int,
    quality_log: Optional[Path],
    validation_batch_rows: int,
    validation_max_batches: Optional[int],
) -> List[DatasetProfile]:
    files = discover_data_files(data_dir)
    profiles = [profile_dataset(path, sample_rows) for path in files]
    foreign_keys = infer_foreign_keys(profiles, sample_rows)
    output_path.write_text(build_documentation(profiles, foreign_keys), encoding="utf-8")
    if quality_log:
        for profile in profiles:
            validate_dataset(
                profile=profile,
                log_path=quality_log,
                batch_rows=validation_batch_rows,
                max_batches=validation_max_batches,
            )
    return profiles


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest data files and document their structure.")
    parser.add_argument("--data-dir", type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--sample-rows", type=int, default=100_000)
    parser.add_argument("--quality-log", type=Path, default=None)
    parser.add_argument("--validation-batch-rows", type=int, default=100_000)
    parser.add_argument("--validation-max-batches", type=int, default=None)
    parser.add_argument("--skip-validation", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data_dir = args.data_dir
    output = args.output or data_dir / "data_structure.md"
    quality_log = None if args.skip_validation else args.quality_log or data_dir / "logs" / "data_quality_checks.jsonl"
    write_data_structure(
        data_dir=data_dir,
        output_path=output,
        sample_rows=args.sample_rows,
        quality_log=quality_log,
        validation_batch_rows=args.validation_batch_rows,
        validation_max_batches=args.validation_max_batches,
    )
    print(f"Wrote {output}")
    if quality_log:
        print(f"Wrote data quality checks to {quality_log}")


if __name__ == "__main__":
    main()
