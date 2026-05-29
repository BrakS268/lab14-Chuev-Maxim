import polars as pl
import duckdb
import time
import glob
import os
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

DATA_DIR = "../data"
PARQUET_PATH = "../data/agro_clean.parquet"
PLOTS_DIR = "../plots"

os.makedirs(PLOTS_DIR, exist_ok=True)


# ── Task 4: Import ────────────────────────────────────────────────────────────

def load_sensors() -> pl.DataFrame:
    files = glob.glob(f"{DATA_DIR}/sensors_*.json")
    frames = [pl.read_ndjson(f) for f in files]
    return pl.concat(frames)


def load_weather() -> pl.DataFrame:
    files = glob.glob(f"{DATA_DIR}/weather_*.json")
    frames = [pl.read_ndjson(f) for f in files]
    return pl.concat(frames)


def task4_import():
    print("\n── Task 4: Import ──────────────────────────────────")
    sensors = load_sensors()
    weather = load_weather()

    print(f"Sensors shape : {sensors.shape}")
    print(f"Weather shape : {weather.shape}")
    print("\nSensors — first 5 rows:")
    print(sensors.head(5))
    print("\nWeather — first 5 rows:")
    print(weather.head(5))
    print("\nSensors schema:")
    print(sensors.schema)
    print("\nWeather schema:")
    print(weather.schema)
    print("\nSensors null counts:")
    print(sensors.null_count())
    print("\nWeather null counts:")
    print(weather.null_count())

    return sensors, weather


# ── Task 5: Clean & Validate ──────────────────────────────────────────────────
 
def task5_clean(sensors: pl.DataFrame, weather: pl.DataFrame):
    print("\n── Task 5: Clean & Validate ────────────────────────")
 
    s_before = sensors.shape[0]
    sensors = sensors.unique()
    print(f"Sensors duplicates removed: {s_before - sensors.shape[0]}")
 
    w_before = weather.shape[0]
    weather = weather.unique()
    print(f"Weather duplicates removed: {w_before - weather.shape[0]}")
 
    sensors = sensors.with_columns(
        pl.col("timestamp").cast(pl.Datetime("us", "UTC"))
    )
    weather = weather.with_columns(
        pl.col("timestamp").cast(pl.Datetime("us", "UTC"))
    )
 
    numeric_s = ["temperature_c", "humidity_pct", "soil_moisture_pct",
                 "soil_temp_c", "nitrogen_mg_kg", "phosphorus_mg_kg",
                 "potassium_mg_kg", "ph"]
    for col in numeric_s:
        median_val = sensors[col].median()
        sensors = sensors.with_columns(
            pl.col(col).fill_null(median_val)
        )
 
    numeric_w = ["temperature_c", "humidity_pct", "pressure_hpa",
                 "wind_speed_ms", "wind_direction_deg", "rainfall_mm",
                 "solar_radiation_wm2", "dew_point_c"]
    for col in numeric_w:
        median_val = weather[col].median()
        weather = weather.with_columns(
            pl.col(col).fill_null(median_val)
        )
 
    sensors = sensors.filter(
        pl.col("temperature_c").is_between(-50, 60) &
        pl.col("humidity_pct").is_between(0, 100) &
        pl.col("ph").is_between(0, 14)
    )
 
    weather = weather.filter(
        pl.col("temperature_c").is_between(-60, 60) &
        pl.col("humidity_pct").is_between(0, 100) &
        pl.col("pressure_hpa").is_between(800, 1100)
    )
 
    print(f"Sensors after clean: {sensors.shape}")
    print(f"Weather after clean: {weather.shape}")
 
    return sensors, weather


# ── Task 6: Aggregation ───────────────────────────────────────────────────────
 
def task6_aggregate(sensors: pl.DataFrame, weather: pl.DataFrame):
    print("\n── Task 6: Aggregation ─────────────────────────────")
 
    sensor_agg = (
        sensors
        .group_by("field_id")
        .agg(
            pl.col("temperature_c").mean().alias("avg_temp"),
            pl.col("temperature_c").min().alias("min_temp"),
            pl.col("temperature_c").max().alias("max_temp"),
            pl.col("humidity_pct").mean().alias("avg_humidity"),
            pl.col("soil_moisture_pct").mean().alias("avg_soil_moisture"),
            pl.col("ph").mean().alias("avg_ph"),
            pl.col("nitrogen_mg_kg").sum().alias("sum_nitrogen"),
            pl.len().alias("count"),
        )
        .sort("field_id")
    )
    print("\nSensor aggregation by field_id:")
    print(sensor_agg)
 
    weather_agg = (
        weather
        .group_by("region")
        .agg(
            pl.col("temperature_c").mean().alias("avg_temp"),
            pl.col("pressure_hpa").mean().alias("avg_pressure"),
            pl.col("rainfall_mm").sum().alias("total_rainfall"),
            pl.col("wind_speed_ms").max().alias("max_wind"),
            pl.col("solar_radiation_wm2").mean().alias("avg_solar_rad"),
            pl.len().alias("count"),
        )
        .sort("region")
    )
    print("\nWeather aggregation by region:")
    print(weather_agg)
 
    return sensor_agg, weather_agg


# ── Task 7: Save to Parquet ───────────────────────────────────────────────────
 
def task7_parquet(sensors: pl.DataFrame, weather: pl.DataFrame):
    print("\n── Task 7: Save to Parquet ─────────────────────────")
 
    sensors.write_parquet(PARQUET_PATH)
    print(f"Saved sensors to {PARQUET_PATH} ({os.path.getsize(PARQUET_PATH):,} bytes)")
 
    weather.write_parquet(PARQUET_PATH.replace("agro_clean", "weather_clean"))
    print(f"Saved weather to {PARQUET_PATH.replace('agro_clean', 'weather_clean')}")


# ── Task 8: DuckDB ────────────────────────────────────────────────────────────
 
def task8_duckdb():
    print("\n── Task 8: DuckDB Analysis ─────────────────────────")
 
    t0 = time.perf_counter()
    con = duckdb.connect()
    result_duck = con.execute(f"""
        SELECT
            field_id,
            AVG(temperature_c)      AS avg_temp,
            AVG(humidity_pct)       AS avg_humidity,
            AVG(soil_moisture_pct)  AS avg_soil_moisture,
            AVG(ph)                 AS avg_ph,
            COUNT(*)                AS records
        FROM read_parquet('{PARQUET_PATH}')
        WHERE temperature_c > 10
          AND humidity_pct  > 40
        GROUP BY field_id
        ORDER BY avg_temp DESC
    """).df()
    t_duck = time.perf_counter() - t0
 
    print(result_duck.to_string(index=False))
 
    t0 = time.perf_counter()
    df = pl.read_parquet(PARQUET_PATH)
    result_polars = (
        df
        .filter((pl.col("temperature_c") > 10) & (pl.col("humidity_pct") > 40))
        .group_by("field_id")
        .agg(
            pl.col("temperature_c").mean().alias("avg_temp"),
            pl.col("humidity_pct").mean().alias("avg_humidity"),
            pl.col("soil_moisture_pct").mean().alias("avg_soil_moisture"),
            pl.col("ph").mean().alias("avg_ph"),
            pl.len().alias("records"),
        )
        .sort("avg_temp", descending=True)
    )
    t_polars = time.perf_counter() - t0
 
    print(f"\nDuckDB  query time: {t_duck*1000:.2f} ms")
    print(f"Polars  query time: {t_polars*1000:.2f} ms")
    print(f"Speedup factor    : {t_polars/t_duck:.2f}x (DuckDB faster)" if t_duck < t_polars
          else f"Speedup factor    : {t_duck/t_polars:.2f}x (Polars faster)")
    

# ── Task 9: Visualizations ────────────────────────────────────────────────────

def task9_visualize(sensors: pl.DataFrame, weather: pl.DataFrame):
    print("\n── Task 9: Visualizations ──────────────────────────")
 
    fig1 = go.Figure()
    for field in sensors["field_id"].unique().sort():
        subset = (
            sensors
            .filter(pl.col("field_id") == field)
            .sort("timestamp")
        )
        fig1.add_trace(go.Scatter(
            x=subset["timestamp"].to_list(),
            y=subset["temperature_c"].to_list(),
            mode="lines",
            name=field,
            line=dict(width=1.5),
        ))
 
    fig1.update_layout(
        title="Температура по полям (168 часов)",
        xaxis_title="Время",
        yaxis_title="Температура, °C",
        template="plotly_white",
        legend_title="Поле",
        height=500,
    )
    path1 = f"{PLOTS_DIR}/temp_timeseries.html"
    fig1.write_html(path1)
    print(f"Saved: {path1}")
 
    fig2 = make_subplots(rows=1, cols=2, subplot_titles=["Влажность воздуха", "Влажность почвы"])
 
    fig2.add_trace(
        go.Histogram(x=sensors["humidity_pct"].to_list(), nbinsx=30,
                     name="Влажность воздуха", marker_color="#4C78A8"),
        row=1, col=1
    )
    fig2.add_trace(
        go.Histogram(x=sensors["soil_moisture_pct"].to_list(), nbinsx=30,
                     name="Влажность почвы", marker_color="#72B7B2"),
        row=1, col=2
    )
 
    fig2.update_layout(
        title="Распределение влажности",
        template="plotly_white",
        showlegend=False,
        height=450,
    )
    path2 = f"{PLOTS_DIR}/humidity_histograms.html"
    fig2.write_html(path2)
    print(f"Saved: {path2}")
 
    weather_agg = (
        weather
        .group_by("region")
        .agg(pl.col("rainfall_mm").sum().alias("total_rainfall"))
        .sort("region")
    )
 
    fig3 = go.Figure(go.Pie(
        labels=weather_agg["region"].to_list(),
        values=weather_agg["total_rainfall"].to_list(),
        hole=0.35,
        textinfo="label+percent",
    ))
    fig3.update_layout(
        title="Распределение осадков по регионам",
        template="plotly_white",
        height=450,
    )
    path3 = f"{PLOTS_DIR}/rainfall_pie.html"
    fig3.write_html(path3)
    print(f"Saved: {path3}")
 
    sensor_cols = ["temperature_c", "humidity_pct", "soil_moisture_pct", "ph",
                   "nitrogen_mg_kg", "phosphorus_mg_kg", "potassium_mg_kg"]
    corr_data = sensors.select(sensor_cols).to_pandas().corr()
 
    fig4 = go.Figure(go.Heatmap(
        z=corr_data.values,
        x=sensor_cols,
        y=sensor_cols,
        colorscale="RdBu",
        zmid=0,
        text=corr_data.round(2).values,
        texttemplate="%{text}",
    ))
    fig4.update_layout(
        title="Корреляционная матрица датчиков полей",
        template="plotly_white",
        height=550,
        width=700,
    )
    path4 = f"{PLOTS_DIR}/correlation_heatmap.html"
    fig4.write_html(path4)
    print(f"Saved: {path4}")


if __name__ == "__main__":
    sensors_raw, weather_raw = task4_import()
    sensors_clean, weather_clean = task5_clean(sensors_raw, weather_raw)
    task6_aggregate(sensors_clean, weather_clean)
    task7_parquet(sensors_clean, weather_clean)
    task8_duckdb()
    task9_visualize(sensors_clean, weather_clean)
    print("\n✓ Pipeline complete")