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


if __name__ == "__main__":
    sensors_raw, weather_raw = task4_import()
    print("\n✓ Pipeline complete")