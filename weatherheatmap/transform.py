import polars as pl
from pathlib import Path

LOC_fields = ("NUM_POSTE", "NOM_USUEL", "LAT", "LON")


def transform(filepath: str):
    print(f"loading {filepath} ...")
    df = pl.read_csv(filepath, separator=";")

    df = df.with_columns(
        (pl.col("AAAAMMJJHH").cast(pl.Utf8) + "00")
        .str.strptime(pl.Datetime, format="%Y%m%d%H%M")
        .alias("time")
    )

    print("Group by location and compute summary...")

    Tmin = 12
    Tmax = 35

    # Add year and month columns
    dep = Path(filepath).stem.split("_")[0]
    df = df.with_columns(
        [
            pl.col("time").dt.strftime("%Y").alias("year").cast(pl.Int64),
            pl.col("time").dt.strftime("%m").alias("month").cast(pl.Int64),
        ]
    )

    # Group by location and month, then compute statistics
    T_col_name = "T"
    hourly_stats = df.group_by([*loc_fields, "year", "month"]).agg(
        [
            ((pl.col(T_col_name) >= Tmin) & (pl.col(T_col_name) < Tmax))
            .sum()
            .alias("hours_in_range"),
            # pl.sum((pl.col(T_col_name) >= Tmin) & (pl.col(T_col_name) <= Tmax)).alias(
            #     "hours_in_range"
            # ),
            pl.count().alias("total_hours"),
            pl.col(T_col_name).is_null().sum().alias("missing_hours"),
        ]
    )
    return hourly_stats
