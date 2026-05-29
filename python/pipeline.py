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


if __name__ == "__main__":
    sensors_raw, weather_raw = task4_import()
    print("\n✓ Pipeline complete")