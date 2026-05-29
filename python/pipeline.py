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
 
    sensors_tagged = sensors.with_columns(pl.lit("sensor").alias("source_type"))
    weather_tagged = weather.rename({
        "station_id": "sensor_id",
        "region": "field_id",
    }).with_columns(
        pl.lit(None).cast(pl.Float64).alias("soil_moisture_pct"),
        pl.lit(None).cast(pl.Float64).alias("soil_temp_c"),
        pl.lit(None).cast(pl.Float64).alias("nitrogen_mg_kg"),
        pl.lit(None).cast(pl.Float64).alias("phosphorus_mg_kg"),
        pl.lit(None).cast(pl.Float64).alias("potassium_mg_kg"),
        pl.lit(None).cast(pl.Float64).alias("ph"),
        pl.lit(None).cast(pl.Float64).alias("pressure_hpa"),
        pl.lit(None).cast(pl.Float64).alias("wind_speed_ms"),
        pl.lit(None).cast(pl.Float64).alias("wind_direction_deg"),
        pl.lit(None).cast(pl.Float64).alias("rainfall_mm"),
        pl.lit(None).cast(pl.Float64).alias("solar_radiation_wm2"),
        pl.lit(None).cast(pl.Float64).alias("dew_point_c"),
        pl.lit("weather").alias("source_type"),
    )
 
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
    

if __name__ == "__main__":
    sensors_raw, weather_raw = task4_import()
    sensors_clean, weather_clean = task5_clean(sensors_raw, weather_raw)
    task6_aggregate(sensors_clean, weather_clean)
    task7_parquet(sensors_clean, weather_clean)
    task8_duckdb()
    print("\n✓ Pipeline complete")