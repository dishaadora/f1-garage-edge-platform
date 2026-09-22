# local cache configuration so repeated runs dont hit the network
#imports
import fastf1 
import pandas as pd
from pathlib import Path

CACHE_DIR = Path("./ff1_cache")
CACHE_DIR.mkdir(exist_ok = True)
fastf1.Cache.enable_cache(str(CACHE_DIR))

#pick one session 
YEAR = 2024
GRAND_PRIX = "Monza"
SESSION_TYPE = "R"

session = fastf1.get_session(YEAR, GRAND_PRIX, SESSION_TYPE)
session.load(laps=True, telemetry=True, weather=True)


driver = session.laps["Driver"].unique()[0]
driver_laps = session.laps.pick_driver(driver)
fastest_lap = driver_laps.pick_fastest()
car_data = fastest_lap.get_car_data()
pos_data = fastest_lap.get_pos_data()


OUT_DIR = Path("./session_dump")
OUT_DIR.mkdir(exist_ok=True)

car_data.to_parquet(OUT_DIR / f"{driver}_fastest_lap_car_data.parquet")
pos_data.to_parquet(OUT_DIR / f"{driver}_fastest_lap_pos_data.parquet")
session.laps.to_parquet(OUT_DIR / "all_laps.parquet")

def summarize(name: str, df:pd.DataFrame):
    print(f"\n--- {name} ---")
    print(f"rows: {len(df)}")
    print(f"columns ({len(df.columns)}) : {list(df.columns)}")
    print(df.dtypes)

summarize("car-data", car_data)
summarize("pos-data", pos_data)
summarize("laps", session.laps)

print("\n--- on-disk sizes ---")
for f in OUT_DIR.glob("*.parquet"):
    print(f"{f.name}: {f.stat().st_size / 1024:.1f} KB")
 
print("\n--- cache size (raw downloaded session data) ---")
cache_size = sum(f.stat().st_size for f in CACHE_DIR.rglob("*") if f.is_file())
print(f"total cache: {cache_size / (1024*1024):.1f} MB")