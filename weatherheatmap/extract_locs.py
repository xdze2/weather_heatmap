from pathlib import Path
from typing import List

import click
import polars as pl

from .data_fields import LOC_fields
from .geo_matching import get_canton, get_departement
from .utils import list_files


def add_zone(df: pl.DataFrame):
    df = df.with_columns(
        pl.struct("LAT", "LON")
        .map_elements(
            lambda x: get_departement(x["LAT"], x["LON"]), return_dtype=pl.String
        )
        .alias("departement"),
        pl.struct("LAT", "LON")
        .map_elements(lambda x: get_canton(x["LAT"], x["LON"]), return_dtype=pl.String)
        .alias("canton"),
    )
    return df


def load_locations(agg_region_col: str = "canton"):
    output_dir = "data/locations"

    select_cols = ["NUM_POSTE", pl.col(agg_region_col).cast(pl.Utf8).str.zfill(2)]
    csv_files = list_files(folder=output_dir, file_pattern="*.csv")

    df = pl.concat([pl.scan_csv(f).select(select_cols) for f in csv_files])
    df = df.collect()
    return df


def extract_location(file_path: str):
    """Process a single CSV file and save the results to DuckDB."""

    result = pl.scan_csv(file_path, separator=";").select(LOC_fields).unique().collect()
    result = add_zone(result)
    return result


@click.command()
def main():
    """"""
    output_dir = "data/locations"
    output_prefix = "locations"

    csv_files = list_files(folder="data/2020-2023", file_pattern="*.csv.gz")

    for filepath in csv_files:
        result = extract_location(filepath)
        stem = Path(filepath).name.split(".")[0]
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        output_file = Path(output_dir, f"{output_prefix}_{stem}.csv")
        result.write_csv(output_file)
        print(f"Saved results to {output_file}", flush=True)


if __name__ == "__main__":
    main()
