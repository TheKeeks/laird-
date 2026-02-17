#!/usr/bin/env python3
"""
l__ai__rd Swell Forecast Pipeline
Downloads GFS-Wave GRIB2 data from NOAA NOMADS and extracts
swell forecast for a point near Chocomount Beach, Fishers Island.

Grid point: 41.003°N, 71.600°W (nearest GFS-Wave 0.16° grid point)
Output: data/forecast.json
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone

import requests
import xarray as xr

# ── Configuration ──
TARGET_LAT = 41.002677
TARGET_LON = 360 - 71.599534  # GRIB2 uses 0–360 longitude
OUTPUT_PATH = "data/forecast.json"
NOMADS_BASE = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfswave.pl"

# Subregion bounding box (small area around our point to minimize download)
BBOX = {"toplat": 41.2, "bottomlat": 40.8, "leftlon": 288.2, "rightlon": 288.6}


def get_latest_cycle():
    """Find the most recent GFS-Wave model cycle that has data available."""
    now = datetime.now(timezone.utc)
    # GFS-Wave runs at 00, 06, 12, 18 UTC
    # Data is typically available ~3.5 hours after cycle time
    for hours_back in range(0, 24, 6):
        candidate = now - timedelta(hours=hours_back)
        cycle_hour = (candidate.hour // 6) * 6
        cycle_time = candidate.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)
        # Check if enough time has passed for data to be available
        if (now - cycle_time).total_seconds() >= 3.5 * 3600:
            return cycle_time
    return None


def download_grib(cycle_time, forecast_hour):
    """Download a single GRIB2 file for one forecast hour."""
    date_str = cycle_time.strftime("%Y%m%d")
    cycle_str = f"{cycle_time.hour:02d}"
    fhr_str = f"{forecast_hour:03d}"

    params = {
        "dir": f"/gfs.{date_str}/{cycle_str}/wave/gridded",
        "file": f"gfswave.t{cycle_str}z.atlocn.0p16.f{fhr_str}.grib2",
        "var_DIRPW": "on",
        "var_HTSGW": "on",
        "var_PERPW": "on",
        "subregion": "",
        "toplat": BBOX["toplat"],
        "bottomlat": BBOX["bottomlat"],
        "leftlon": BBOX["leftlon"],
        "rightlon": BBOX["rightlon"],
    }

    url = NOMADS_BASE
    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        return r.content
    except Exception as e:
        print(f"  Warning: Failed to download f{fhr_str}: {e}")
        return None


def extract_point(grib_bytes):
    """Extract swell height, period, and direction at our target point."""
    # Write to temp file (cfgrib needs file path)
    tmp_path = "/tmp/gfswave_temp.grib2"
    with open(tmp_path, "wb") as f:
        f.write(grib_bytes)

    try:
        ds = xr.open_dataset(tmp_path, engine="cfgrib")
        point = ds.sel(latitude=TARGET_LAT, longitude=TARGET_LON, method="nearest")

        height_m = float(point["swh"].values) if "swh" in point else None
        period_s = float(point["perpw"].values) if "perpw" in point else None
        direction_deg = float(point["dirpw"].values) if "dirpw" in point else None

        # Convert height to feet
        height_ft = round(height_m * 3.28084, 1) if height_m is not None else None

        ds.close()
        return {
            "height_ft": height_ft,
            "height_m": round(height_m, 2) if height_m else None,
            "period_s": round(period_s, 1) if period_s else None,
            "direction_deg": round(direction_deg, 0) if direction_deg else None,
        }
    except Exception as e:
        print(f"  Warning: Failed to extract point: {e}")
        return None
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


def main():
    print("l__ai__rd Swell Forecast Pipeline")
    print("=" * 40)

    cycle = get_latest_cycle()
    if not cycle:
        print("ERROR: Could not determine latest GFS-Wave cycle")
        sys.exit(1)

    print(f"Using cycle: {cycle.strftime('%Y-%m-%d %H:%M UTC')}")

    # Forecast hours: 0–120 hourly, then 123–240 every 3 hours
    forecast_hours = list(range(0, 121)) + list(range(123, 241, 3))

    forecasts = []
    success_count = 0

    for fhr in forecast_hours:
        valid_time = cycle + timedelta(hours=fhr)
        print(f"  Downloading f{fhr:03d} ({valid_time.strftime('%a %m/%d %H:%M')} UTC)...", end="")

        grib = download_grib(cycle, fhr)
        if grib is None:
            print(" SKIP")
            continue

        data = extract_point(grib)
        if data is None:
            print(" SKIP")
            continue

        forecasts.append({
            "valid_time": valid_time.isoformat(),
            "forecast_hour": fhr,
            **data,
        })
        success_count += 1
        print(f" OK ({data['height_ft']}ft)")

    print(f"\nExtracted {success_count}/{len(forecast_hours)} forecast hours")

    if success_count == 0:
        print("ERROR: No data extracted")
        sys.exit(1)

    # Write output
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "model_cycle": cycle.isoformat(),
        "grid_point": {"lat": TARGET_LAT, "lon": TARGET_LON - 360},
        "forecasts": forecasts,
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(output, f, indent=2)

    print(f"Wrote {OUTPUT_PATH} ({len(forecasts)} records)")


if __name__ == "__main__":
    main()
