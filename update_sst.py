import numpy as np
import requests
import json
from netCDF4 import Dataset
import os
import matplotlib.pyplot as plt
import time

# --- COORDINATES: NC OFFSHORE ---
LAT_MIN, LAT_MAX = 33.5, 36.8   
LON_MIN, LON_MAX = -76.5, -72.5  
OUTPUT_DIR = "historical_data"
LOOKBACK_DAYS = 5
RETENTION_DAYS = 5

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
    for f in os.listdir(OUTPUT_DIR):
        path = os.path.join(OUTPUT_DIR, f)
        if f.endswith(".png") or (f.startswith("meta_") and f.endswith(".json")):
            mtime = os.path.getmtime(path)
            age_days = (time.time() - mtime) / 86400
            if age_days > (RETENTION_DAYS + 1):
                os.remove(path)
                print(f"  Purged: {f}")

def update_manifest():
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
        except:
            continue
    with open(os.path.join(OUTPUT_DIR, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)

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

            units = ds.variables[target_var].units if hasattr(ds.variables[target_var], 'units') else "celsius"
            if "K" in units.upper():
                temp_f = (raw_data - 273.15) * 1.8 + 32
            else:
                temp_f = raw_data * 1.8 + 32

            if lats[0] < lats[-1]:
                final_grid = np.flipud(temp_f)
            else:
                final_grid = temp_f

            masked_temp = np.ma.masked_where(
                ~np.isfinite(final_grid) | (final_grid < 30) | (final_grid > 100),
                final_grid
            )

            valid_data = masked_temp.compressed()
            if len(valid_data) == 0:
                print(f"      No valid data found, skipping.")
                return
            min_temp = float(np.percentile(valid_data, 2))
            max_temp = float(np.percentile(valid_data, 98))

            png_filename = f"{base_name}.png"
            png_path = os.path.join(OUTPUT_DIR, png_filename)

            fig, ax = plt.subplots(1, 1, figsize=(10, 10))
            ax.imshow(masked_temp, cmap='jet', origin='upper',
                      interpolation='bilinear', vmin=min_temp, vmax=max_temp)
            ax.axis('off')
            plt.savefig(png_path, bbox_inches='tight', pad_inches=0, dpi=150)
            plt.close(fig)

            meta = {
                "date": ts.split('T')[0],
                "timestamp": ts,
