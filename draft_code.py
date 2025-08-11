import polars as pl
import polars as pl
import altair as alt
import pandas as pd


filepath = "H_31_2010-2019.csv.gz"
print(f"loading {filepath} ...")
df = pl.read_csv(filepath, separator=";")


df = df.with_columns(
    (pl.col("AAAAMMJJHH").cast(pl.Utf8) + "00")
    .str.strptime(pl.Datetime, format="%Y%m%d%H%M")
    .alias("time")
)


print(" Group by location and compute summary...")
summary = df.group_by(["NUM_POSTE", "NOM_USUEL", "LAT", "LON"]).agg(
    [pl.col("time").min().alias("start_date"), pl.col("time").max().alias("end_date")]
)

print(summary)

print("# Stats...")
Tmin = 12
Tmax = 35

df = df.with_columns(
    [
        pl.col("time").dt.strftime("%Y").alias("year").cast(pl.Int64),
        pl.col("time").dt.strftime("%m").alias("month").cast(pl.Int64),
    ]
)

# Group by location and month, then compute statistics
T_col_name = "T"
hourly_stats = df.group_by(["NUM_POSTE", "NOM_USUEL", "year", "month"]).agg(
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
print(hourly_stats)


print("# Plot...")

# Convert to Pandas for Altair


hourly_stats_pd = hourly_stats.to_pandas()

# Filter for one location (example: "A")
location_name = "AUZEVILLE-TOLOSANE-INRAE"
filtered_data = hourly_stats_pd[hourly_stats_pd["NOM_USUEL"] == location_name]

# Create Altair line chart to compare years
line_chart = (
    alt.Chart(filtered_data)
    .mark_line(point=True)
    .encode(
        x=alt.X("month:Q", title="Month"),
        y=alt.Y("hours_in_range:Q", title="Hours in Range"),
        color=alt.Color("year:Q", scale=alt.Scale(scheme="viridis"), title="Year"),
        tooltip=["year", "month", "hours_in_range", "missing_hours"],
    )
    .properties(
        width=800,
        height=500,
        title=f"Temperature Range Comparison by Year for {location_name}",
    )
)

# Save chart
line_chart.save("temperature_comparison.html")

print("Temperature comparison graph has been saved as temperature_comparison.html")

# # Convert to Pandas for Altair
# summary_pd = summary.to_pandas()

# # Create Altair scatter plot
# map_chart = (
#     alt.Chart(summary_pd)
#     .mark_circle(size=100)
#     .encode(
#         longitude="LON:Q",
#         latitude="LAT:Q",
#         tooltip=["NUM_POSTE", "start_date", "end_date"],
#         color=alt.Color("NOM_USUEL:N", legend=alt.Legend(title="Location")),
#     )
#     .properties(width=800, height=500, title="Locations with Date Ranges")
# )

# # Save chart
# map_chart.save("location_map.html")

# print("Map has been saved as location_map.html")
# # print(df)


# plt.figure(figsize=(30, 8))
# for k, dfg in df.group_by("NUM_POSTE"):
#     plt.plot(dfg["time"], dfg["T"], alpha=0.5)
#     print(k)


# plt.xlabel("Time")
# plt.ylabel("T")
# plt.title("T vs Time for each Group")
# # plt.legend()
# plt.xticks(rotation=45)
# plt.tight_layout()
# plt.savefig("T_vs_Time.png")
