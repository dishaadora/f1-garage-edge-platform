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
                            drivers: list[str], cache_dir: str,
                            lap_numbers: list[int] | None = None,
                            lap_count: int = 5) -> pd.DataFrame:
    """Pull telemetry for the given drivers and merge it into one
    time-sorted DataFrame, tagged by driver, lap number, and channel
    source.

    lap_numbers: specific laps to replay (e.g. [3, 4, 5]). Takes
    priority over lap_count when given.
    lap_count: if lap_numbers is None, replay each driver's earliest
    N completed laps. Default 5 keeps replay/ingest fast to iterate on
    while still proving multi-lap analysis works. Pass 0 to replay
    every completed lap -- realistic, but slow to re-run while testing.
    """
    Path(cache_dir).mkdir(exist_ok=True)
    fastf1.Cache.enable_cache(cache_dir)

    session = fastf1.get_session(year, grand_prix, session_type)
    session.load(laps=True, telemetry=True, weather=False, messages=False)

    frames = []
    for driver in drivers:
        driver_laps = session.laps.pick_drivers(driver)
        if driver_laps.empty:
            log.warning("No laps found for driver %s, skipping", driver)
            continue

        if lap_numbers is not None:
            driver_laps = driver_laps[driver_laps["LapNumber"].isin(lap_numbers)]
            if driver_laps.empty:
                log.warning("Driver %s has none of the requested laps %s, skipping",
                            driver, lap_numbers)
                continue
        elif lap_count and lap_count > 0:
            # No specific laps requested -- default to the earliest
            # lap_count laps so replay/ingest stays fast during
            # development. Sort explicitly: lap order isn't guaranteed
            # by session.laps.pick_drivers()'s row order.
            driver_laps = driver_laps.sort_values("LapNumber").head(lap_count)
        # else: lap_count == 0 means "every completed lap" -- no filtering.

        # iterlaps() (not iterrows()) is required here: it yields proper
        # Lap objects with a working get_car_data() method, rather than
        # plain pandas Series.
        for _, lap in driver_laps.iterlaps():
            car = lap.get_car_data()
            car = car.reset_index(drop=True)

            if car.empty:
                log.warning("Driver %s lap %s has no telemetry, skipping",
                            driver, lap["LapNumber"])
                continue

            # Prefix telemetry columns with "car_" so combined events (from
            # multiple drivers/channels/laps) can be told apart later.
            # - "Time" is left unprefixed: it's the sort key for the
            #   combined timeline below. It's already elapsed-from-
            #   session-start, so laps concatenate in correct
            #   chronological order with no manual offset needed.
            # - "Source" is excluded entirely: FastF1's own telemetry
            #   already includes a native "Source" column (marking each
            #   row as "car" or "interpolation"), which collides with our
            #   own bookkeeping "Source" field ("car_data"/"pos_data") set
            #   below -- Go's case-insensitive JSON field matching would
            #   let whichever one appears later in the payload silently
            #   overwrite the other. Same reasoning excludes "LapNumber"
            #   as a precaution, even though FastF1's raw telemetry
            #   doesn't naturally carry that column.
            telemetry_cols = [c for c in car.columns
                               if c not in ("Time", "Source", "LapNumber")]
            car = car.rename(columns={c: f"car_{c}" for c in telemetry_cols})

            car["Driver"] = driver
            car["Source"] = "car_data"
            car["LapNumber"] = int(lap["LapNumber"])

            frames.append(car[["Time", "Driver", "Source", "LapNumber"] +
                               [f"car_{c}" for c in telemetry_cols]])

    if not frames:
        raise RuntimeError("No telemetry loaded for any requested driver/lap combination")

    combined = pd.concat(frames, ignore_index=True, sort=False)
    combined = combined.sort_values("Time").reset_index(drop=True)
    log.info("Combined timeline: %d events across %d driver(s), %d lap-segments",
              len(combined), len(drivers), len(frames))
    return combined


def _to_json_safe(val):
    """Convert a single pandas/numpy scalar into something the stdlib
    json module can actually serialize. FastF1 telemetry mixes several
    non-JSON-native types in one row (Timedelta, Timestamp, numpy
    int64/float64/bool_), so this handles the whole family at once
    rather than patching one type per bug report."""
    if pd.isna(val):
        return None
    if isinstance(val, pd.Timedelta):
        return val.total_seconds()
    if isinstance(val, pd.Timestamp):
        return val.isoformat()
    if hasattr(val, "item"):
        # Covers numpy scalar types: int64, float64, bool_, etc.
        # .item() unwraps them to a plain Python int/float/bool.
        return val.item()
    return val


def row_to_event(row: pd.Series) -> dict:
    """Serialize one telemetry row into the JSON shape the ingest
    service expects. Adjust field names to match your Go service."""
    event = {
        "driver": row["Driver"],
        "source": row["Source"],
        "lap_number": _to_json_safe(row["LapNumber"]),
        "session_time_seconds": row["Time"].total_seconds(),
    }
    for col in row.index:
        if col.startswith("car_"):
            event[col.replace("car_", "")] = _to_json_safe(row[col])
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
    parser.add_argument("--laps", type=int, nargs="+", default=None,
                         help="Specific lap numbers to replay, e.g. 3 4 5. "
                              "Overrides --lap-count when given.")
    parser.add_argument("--lap-count", type=int, default=5,
                         help="Replay each driver's earliest N completed laps "
                              "(default 5). Use 0 to replay every lap (slower). "
                              "Ignored if --laps is given.")
    parser.add_argument("--endpoint", type=str, default="http://localhost:8080/ingest")
    parser.add_argument("--speed", type=float, default=1.0,
                         help="Playback speed multiplier. 1 = real-time, 10 = 10x faster.")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--start-offset", type=float, default=0.0,
                         help="Skip ahead this many seconds into the session.")
    parser.add_argument("--cache-dir", type=str, default="./ff1_cache")
    args = parser.parse_args()

    timeline = load_combined_timeline(
        args.year, args.grand_prix, args.session_type, args.drivers, args.cache_dir,
        lap_numbers=args.laps, lap_count=args.lap_count
    )
    replay(timeline, args.endpoint, args.speed, args.loop, args.start_offset)


if __name__ == "__main__":
    main()