from pathlib import Path

import polars as pl

from .data_fields import LOC_fields
from .extract_locs import load_locations
from .utils import list_files


def extract_compute(
    file_path: str, loc_df, region_grp_col: str = "canton", select_year: int = 2018
) -> pl.DataFrame:
    """Process a single CSV file and save the results to DuckDB."""

    used_cols = ("NUM_POSTE", "AAAAMMJJHH", "T")
    df = pl.scan_csv(file_path, separator=";").select(used_cols)

    # Join with location data, aka add "departement" column
    df = df.join(loc_df.lazy(), on="NUM_POSTE", how="left").filter(
        pl.col(region_grp_col).is_not_null()
    )

    # Group by region and aggregate
    df = df.group_by([region_grp_col, "AAAAMMJJHH"]).agg(
        pl.col("T").mean().alias("T"),
        pl.col("T").count().alias("count"),
    )

    # Add time column
    df = df.with_columns(
        (pl.col("AAAAMMJJHH").cast(pl.Utf8) + "00")
        .str.strptime(pl.Datetime, format="%Y%m%d%H%M")
        .alias("time")
    )
    # Add year and month columns
    df = df.with_columns(
        [
            pl.col("time").dt.strftime("%Y").alias("year").cast(pl.Int64),
            pl.col("time").dt.strftime("%m").alias("month").cast(pl.Int64),
        ]
    )
    # Keep only one year
    df = df.filter(pl.col("year") == select_year)
    # # Compute model on monthly data
    # Tmin = 12
    # Tmax = 35
    monthly_stats = (
        df.group_by([region_grp_col, "year", "month"])
        .agg(
            [
                # ((pl.col("T") >= Tmin) & (pl.col("T") < Tmax))
                # .sum()
                # .alias("hours_in_range"),
                pl.col("T").mean().alias("Tavg"),
                pl.col("T").count().alias("Tcount"),
                pl.col("T").max().alias("Tmax"),
                pl.col("T").min().alias("Tmin"),
                # pl.sum((pl.col(T_col_name) >= Tmin) & (pl.col(T_col_name) <= Tmax)).alias(
                #     "hours_in_range"
                # ),
                pl.count().alias("total_hours"),
                pl.col("T").is_null().sum().alias("missing_hours"),
            ]
        )
        .filter(pl.col("Tavg").is_not_null())
    )
    print(df.collect())
    # # result = hourly_stats.collect()
    result = monthly_stats.sort("year", "month").collect()
    return result


def main():

    select_year = 2023
    output_dir = f"data/agg_data_{select_year}"
    output_prefix = "agg_data"

    input_csv_files = list_files(folder="data/2020-2023", file_pattern="*.csv.gz")

    region_grp_col = "departement"
    loc_df = load_locations(agg_region_col=region_grp_col)
    for filepath in input_csv_files:
        filepath = "data/2020-2023/H_44_previous-2020-2023.csv.gz"

        result = extract_compute(
            filepath,
            loc_df=loc_df,
            region_grp_col=region_grp_col,
            select_year=select_year,
        )

        stem = Path(filepath).name.split(".")[0]
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        output_file = Path(output_dir, f"{output_prefix}_{stem}.csv")
        result.write_csv(output_file)
        print(f"Saved results to {output_file}", flush=True)
        break


if __name__ == "__main__":
    main()
