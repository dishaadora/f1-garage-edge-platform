"""
replay_session.py

Replays a historical F1 session as if it were happening live: reads
telemetry already pulled via FastF1, sorts every sample into one
combined timeline, and emits each row over HTTP at the same relative
pacing it originally occurred in (optionally sped up).

This feeds your ingest service the way a real trackside feed would --
the ingest side shouldn't be able to tell the difference.

Usage:
    python replay_session.py --speed 10 --endpoint http://localhost:8080/ingest
"""

import argparse
import time
import json
import logging
from pathlib import Path

import fastf1
import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("replay")


def load_combined_timeline(year: int, grand_prix: str, session_type: str,
                            drivers: list[str], cache_dir: str) -> pd.DataFrame:
    """Pull telemetry for the given drivers and merge it into one
    time-sorted DataFrame, tagged by driver and channel source."""
    Path(cache_dir).mkdir(exist_ok=True)
    fastf1.Cache.enable_cache(cache_dir)

    session = fastf1.get_session(year, grand_prix, session_type)
    session.load(laps=True, telemetry=True, weather=False, messages=False)

    frames = []
    for driver in drivers:
        laps = session.laps.pick_drivers(driver)
        if laps.empty:
            log.warning("No laps found for driver %s, skipping", driver)
            continue
        lap = laps.pick_fastest()
        car = lap.get_car_data().add_headers_prefix("car_")  # keep columns distinguishable
        car = car.reset_index(drop=True)
        car["Driver"] = driver
        car["Source"] = "car_data"
        # Time is already a timedelta column from FastF1
        frames.append(car[["Time", "Driver", "Source"] +
                           [c for c in car.columns if c.startswith("car_")]])

    if not frames:
        raise RuntimeError("No telemetry loaded for any requested driver")

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined = combined.sort_values("Time").reset_index(drop=True)
    log.info("Combined timeline: %d events across %d driver(s)", len(combined), len(drivers))
    return combined


def row_to_event(row: pd.Series) -> dict:
    """Serialize one telemetry row into the JSON shape the ingest
    service expects. Adjust field names to match your Go service."""
    event = {
        "driver": row["Driver"],
        "source": row["Source"],
        "session_time_seconds": row["Time"].total_seconds(),
    }
    for col in row.index:
        if col.startswith("car_"):
            val = row[col]
            # pandas/NaT-safe serialization
            event[col.replace("car_", "")] = None if pd.isna(val) else (
                val.total_seconds() if isinstance(val, pd.Timedelta) else val
            )
    return event


def send_event(endpoint: str, event: dict, timeout: float = 1.0) -> None:
    try:
        requests.post(endpoint, json=event, timeout=timeout)
    except requests.RequestException as e:
        # Don't crash the replay if the ingest side is briefly down --
        # log it and keep going, same as a real flaky uplink would.
        log.warning("Failed to send event: %s", e)


def replay(timeline: pd.DataFrame, endpoint: str, speed: float, loop: bool,
           start_offset_seconds: float = 0.0) -> None:
    if start_offset_seconds:
        timeline = timeline[timeline["Time"].dt.total_seconds() >= start_offset_seconds].reset_index(drop=True)

    while True:
        prev_time = None
        for _, row in timeline.iterrows():
            current_time = row["Time"].total_seconds()
            if prev_time is not None:
                gap = (current_time - prev_time) / speed
                if gap > 0:
                    time.sleep(gap)
            send_event(endpoint, row_to_event(row))
            prev_time = current_time

        log.info("Replay finished (%d events).", len(timeline))
        if not loop:
            break
        log.info("Looping back to start...")


def main():
    parser = argparse.ArgumentParser(description="Replay historical F1 telemetry as a live feed.")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--grand-prix", type=str, default="Monza")
    parser.add_argument("--session-type", type=str, default="R")
    parser.add_argument("--drivers", type=str, nargs="+", default=["VER"],
                         help="Driver codes to replay, e.g. VER HAM")
    parser.add_argument("--endpoint", type=str, default="http://localhost:8080/ingest")
    parser.add_argument("--speed", type=float, default=1.0,
                         help="Playback speed multiplier. 1 = real-time, 10 = 10x faster.")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--start-offset", type=float, default=0.0,
                         help="Skip ahead this many seconds into the session.")
    parser.add_argument("--cache-dir", type=str, default="./ff1_cache")
    args = parser.parse_args()

    timeline = load_combined_timeline(
        args.year, args.grand_prix, args.session_type, args.drivers, args.cache_dir
    )
    replay(timeline, args.endpoint, args.speed, args.loop, args.start_offset)


if __name__ == "__main__":
    main()