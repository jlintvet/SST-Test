import numpy as np
import requests
import json
from netCDF4 import Dataset
import os
import matplotlib.cm as cm
import matplotlib.colors as mcolors
import time
from PIL import Image as PILImage
from datetime import datetime, timedelta, timezone

# --- COORDINATES: NC OFFSHORE ---
LAT_MIN, LAT_MAX = 30.0, 39.0
LON_MIN, LON_MAX = -77.5, -68.0
OUTPUT_DIR = "historical_data"
LOOKBACK_DAYS = 7   # Increased from 5 to give more buffer for provider lag
RETENTION_DAYS = 7  # Match lookback so files aren't purged before re-check

DATASETS = [
    {
        "id": "noaacwBLENDEDsstDNDaily",
        "name": "Geo-Polar Blended 5km (Gap-Free)",
        "nodes": ["https://coastwatch.noaa.gov/erddap"]
    },
    {
        "id": "noaacwLEOACSPOSSTL3SnrtKDaily",
        "name": "VIIRS+AVHRR Super-Collated 2km",
        "nodes": ["https://coastwatch.noaa.gov/erddap"]
    },
    {
        "id": "jplMURSST41",
        "name": "MUR SST 1km (Highest Resolution)",
        "nodes": ["https://coastwatch.pfeg.noaa.gov/erddap"]
    },
]

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


def cleanup_old_files():
    print("\n=== Cleaning up old files ===")
    for f in os.listdir(OUTPUT_DIR):
        path = os.path.join(OUTPUT_DIR, f)
        if f.endswith(".png") or (f.startswith("meta_") and f.endswith(".json")):
            mtime = os.path.getmtime(path)
            age_days = (time.time() - mtime) / 86400
            if age_days > (RETENTION_DAYS + 1):
                os.remove(path)
                print(f"  Purged: {f}")


def update_manifest():
    print("\n=== Updating manifest ===")
    meta_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith("meta_") and f.endswith(".json")]
    manifest_data = {}
    for f in sorted(meta_files):
        path = os.path.join(OUTPUT_DIR, f)
        try:
            with open(path, 'r', encoding='utf-8') as jf:
                meta = json.load(jf)
                day_key = meta["date"]
                if day_key not in manifest_data:
                    manifest_data[day_key] = []
                if not any(item['image'] == meta['image'] for item in manifest_data[day_key]):
                    manifest_data[day_key].append(meta)
        except Exception as e:
            print(f"  Warning: could not read {f}: {e}")
            continue

    manifest_path = os.path.join(OUTPUT_DIR, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)

    dates_found = sorted(manifest_data.keys())
    print(f"  Manifest written with {len(dates_found)} date(s): {', '.join(dates_found)}")


def process_and_save_raster(content, var_name, base_name, ts, ds_id, ds_display_name):
    try:
        with Dataset("memory", memory=content) as ds:
            possible_vars = [var_name, "analysed_sst", "sea_surface_temperature", "sst"]
            target_var = next((v for v in possible_vars if v in ds.variables), None)
            if not target_var:
                print(f"      No SST variable found. Available: {list(ds.variables.keys())}")
                return

            raw_data = np.squeeze(ds.variables[target_var][:])
            lats = ds.variables['latitude'][:]
            lons = ds.variables['longitude'][:]

            # Read ACTUAL bounds from the file
            actual_lat_min = float(lats.min())
            actual_lat_max = float(lats.max())
            actual_lon_min = float(lons.min())
            actual_lon_max = float(lons.max())

            print(f"      Actual bounds: lat {actual_lat_min:.2f}-{actual_lat_max:.2f}, "
                  f"lon {actual_lon_min:.2f}-{actual_lon_max:.2f}")
            print(f"      Grid shape: {raw_data.shape}")

            units = ds.variables[target_var].units if hasattr(ds.variables[target_var], 'units') else "celsius"
            if "K" in units.upper():
                temp_f = (raw_data - 273.15) * 1.8 + 32
            else:
                temp_f = raw_data * 1.8 + 32

            # Ensure north-to-south row order (row 0 = northernmost).
            # Leaflet ImageOverlay renders the top of the image at lat_max (north),
            # so row 0 must correspond to the northernmost latitude.
            if lats[0] < lats[-1]:
                temp_f = np.flipud(temp_f)

            masked_temp = np.ma.masked_where(
                ~np.isfinite(temp_f) | (temp_f < 30) | (temp_f > 100),
                temp_f
            )

            valid_data = masked_temp.compressed()
            if len(valid_data) == 0:
                print(f"      No valid data found, skipping.")
                return
            min_temp = float(np.percentile(valid_data, 2))
            max_temp = float(np.percentile(valid_data, 98))

            png_filename = f"{base_name}.png"
            png_path = os.path.join(OUTPUT_DIR, png_filename)

            # Convert to RGBA using PIL — no matplotlib figure padding
            colormap = cm.jet
            norm = mcolors.Normalize(vmin=min_temp, vmax=max_temp)

            filled = masked_temp.filled(np.nan)
            normalized = norm(filled)
            rgba = colormap(normalized)
            rgba_uint8 = (rgba * 255).astype(np.uint8)

            # Make all NaN/masked pixels fully transparent (alpha = 0).
            # Without this, missing/land pixels render as a solid color that
            # obscures the coastline and makes the overlay appear misaligned.
            nan_mask = ~np.isfinite(filled)
            rgba_uint8[nan_mask, 3] = 0

            img = PILImage.fromarray(rgba_uint8, mode='RGBA')
            img.save(png_path)

            # Bounds saved as [[south, west], [north, east]] — Leaflet convention
            meta = {
                "date": ts.split('T')[0],
                "timestamp": ts,
                "ds_id": ds_id,
                "ds_name": ds_display_name,
                "image": png_filename,
                "bounds": [[actual_lat_min, actual_lon_min], [actual_lat_max, actual_lon_max]],
                "min_temp": min_temp,
                "max_temp": max_temp
            }
            with open(os.path.join(OUTPUT_DIR, f"meta_{base_name}.json"), "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)
            print(f"    SAVED: {png_filename} (temps: {min_temp:.1f}F - {max_temp:.1f}F)")

    except Exception as e:
        print(f"      Error processing raster: {e}")


def already_have(base_name):
    """Return True if we already have the PNG and meta for this timestamp."""
    png_exists = os.path.exists(os.path.join(OUTPUT_DIR, f"{base_name}.png"))
    meta_exists = os.path.exists(os.path.join(OUTPUT_DIR, f"meta_{base_name}.json"))
    return png_exists and meta_exists


def fetch_history():
    # Build the cutoff datetime for the lookback window
    cutoff_dt = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)
    cutoff_str = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"\n=== Fetching SST data from {cutoff_str} to now ===")
    print(f"    Region: lat {LAT_MIN}-{LAT_MAX}, lon {LON_MIN}-{LON_MAX}\n")

    for ds in DATASETS:
        ds_id, ds_name = ds["id"], ds["name"]
        ds_nodes = ds.get("nodes", ["https://coastwatch.noaa.gov/erddap"])

        for node in ds_nodes:
            print(f"--- Scanning {ds_name} on {node} ---")
            success = False

            try:
                # Fetch all available timestamps from the server
                t_url = f"{node}/griddap/{ds_id}.json?time"
                print(f"  Querying time axis: {t_url}")
                t_resp = requests.get(t_url, timeout=30)

                if t_resp.status_code != 200:
                    print(f"  Skipping — time query returned status {t_resp.status_code}")
                    continue

                all_timestamps = [row[0] for row in t_resp.json()['table']['rows']]
                print(f"  Server has {len(all_timestamps)} total timestamps. "
                      f"Latest: {all_timestamps[-1] if all_timestamps else 'none'}")

                # Filter to only timestamps within the lookback window
                recent_ts = [ts for ts in all_timestamps if ts >= cutoff_str]
                print(f"  Timestamps within lookback window ({LOOKBACK_DAYS} days): {len(recent_ts)}")

                if not recent_ts:
                    print(f"  No data available in the lookback window — provider lag likely.")
                    # Show the most recent available date so we know how far behind it is
                    if all_timestamps:
                        print(f"  Most recent available on server: {all_timestamps[-1]}")
                    continue

                # Fetch dataset variable info once per dataset
                i_resp = requests.get(f"{node}/info/{ds_id}/index.json", timeout=20)
                if i_resp.status_code != 200:
                    print(f"  Info fetch failed: {i_resp.status_code}")
                    continue

                info = i_resp.json()
                var_name = next(
                    (r[1] for r in info['table']['rows']
                     if r[0] == 'variable' and r[1] in ["analysed_sst", "sea_surface_temperature", "sst"]),
                    "analysed_sst"
                )
                print(f"  SST variable: {var_name}")

                for ts in recent_ts:
                    clean_ts = ts.replace(":", "").replace("-", "").replace("Z", "")
                    base_name = f"sst_{ds_id}_{clean_ts}"

                    if already_have(base_name):
                        print(f"  Already have {base_name}, skipping download.")
                        continue

                    print(f"  Fetching {ts}...")
                    dl_url = (
                        f"{node}/griddap/{ds_id}.nc"
                        f"?{var_name}[({ts})][({LAT_MIN}):({LAT_MAX})][({LON_MIN}):({LON_MAX})]"
                    )
                    print(f"    URL: {dl_url}")

                    data_resp = requests.get(dl_url, timeout=120)
                    if data_resp.status_code == 200:
                        process_and_save_raster(
                            data_resp.content, var_name, base_name, ts, ds_id, ds_name
                        )
                    else:
                        print(f"    Download failed: HTTP {data_resp.status_code}")
                        # Print first 300 chars of response for ERDDAP error details
                        try:
                            print(f"    Response: {data_resp.text[:300]}")
                        except Exception:
                            pass

                success = True

            except Exception as e:
                print(f"  Error on node {node}: {e}")

            if success:
                break  # Don't try fallback nodes if primary succeeded


if __name__ == "__main__":
    cleanup_old_files()
    fetch_history()
    update_manifest()
