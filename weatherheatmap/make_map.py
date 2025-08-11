import geopandas as gpd
import polars as pl
import pandas as pd
import matplotlib.pyplot as plt

from .utils import list_files


def main():
    dep_datafile = "data/assets/departements-version-simplifiee.geojson"
    departments = gpd.read_file(dep_datafile)

    print(departments)

    input_data_dir = "data/agg_data_2023"

    select_cols = [
        pl.col("departement").cast(pl.Utf8).str.zfill(2),
        pl.col("year").cast(pl.Int32),
        pl.col("month").cast(pl.Int32),
        pl.col("Tavg").cast(pl.Float32),
        pl.col("Tmax").cast(pl.Float32),
        pl.col("Tmin").cast(pl.Float32),
    ]
    csv_files = list_files(folder=input_data_dir, file_pattern="*.csv")

    df = pl.concat([pl.scan_csv(f).select(select_cols) for f in csv_files])
    df = df.with_columns(
        pl.col("departement").cast(pl.Utf8).str.zfill(2).alias("code"),
    )

    vmin, vmax = -40, 40  # Adjust based on your data range
    cmap = "viridis"  # Choose a colormap

    df44 = df.filter(pl.col("code") == "44").collect()
    print(df44)
    # Plot heatmap
    fig, axis = plt.subplots(3, 4, figsize=(4 * 5, 3 * 5))
    for month_idx, ax in enumerate(axis.flatten(), start=1):

        df_month = df.filter(pl.col("month") == month_idx, pl.col("year") == 2023)
        df_month = df_month.collect()

        data_pd = df_month.to_pandas()
        merged = departments.merge(
            data_pd, left_on="code", right_on="departement", how="left"
        )

        last = merged.plot(
            column="Tmax",
            vmin=vmin,
            vmax=vmax,
            cmap=cmap,
            linewidth=0.8,
            edgecolor="black",
            legend=False,
            ax=ax,
        )
        ax.set_title(f"month {month_idx}")
        ax.set_xticks([])
        ax.set_yticks([])
        ax.set_frame_on(False)
        # last[0].colorbar.remove()

    plt.tight_layout()
    # fig.colorbar(last, ax=axis.ravel().tolist(), orientation="vertical")
    plt.savefig("heatmap.png")
    print("Heatmap has been saved as heatmap.png")
