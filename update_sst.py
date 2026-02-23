import numpy as np
import requests
import json
from netCDF4 import Dataset
import os
import matplotlib.pyplot as plt

# Settings
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0
OUTPUT_DIR = "historical_data"
STEP = 1  
LOOKBACK_DAYS = 5 

DATASETS = [
    {"id": "noaacwBLENDEDsstDNDaily", "name": "Geo-Polar Blended NRT"},
    {"id": "noa_coastwatch_acspo_v2_nrt", "name": "ACSPO NRT Global"},
    {"id": "goes19SSThourly", "name": "GOES-19 Hourly"}
]

NODES = ["https://coastwatch.noaa.gov/erddap", "https://cwcgom.aoml.noaa.gov/erddap"]

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def update_manifest():
    """Groups all successful PNGs by date for multi-dataset UI support."""
    meta_files = sorted([f for f in os.listdir(OUTPUT_DIR) if f.startswith("meta_") and f.endswith(".json")])
    manifest_data = {}

    for f in meta_files:
        path = os.path.join(OUTPUT_DIR, f)
        try:
            with open(path, 'r', encoding='utf-8') as jf:
                meta = json.load(jf)
                day_key = meta["date"]
                
                if day_key not in manifest_data:
                    manifest_data[day_key] = []
                
                if not any(item['image'] == meta['image'] for item in manifest_data[day_key]):
                    manifest_data[day_key].append(meta)
        except Exception:
            continue

    manifest_path = os.path.join(OUTPUT_DIR, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)
    
    total_files = sum(len(v) for v in manifest_data.values())
    print(f"--- Manifest Updated: {total_files} images indexed across {len(manifest_data)} days ---")

def process_and_save_raster(content, var_name, base_name, ts, ds_id, ds_display_name):
    """Converts NetCDF to a TRANSPARENT PNG."""
    try:
        with Dataset("memory", memory=content) as ds:
            data = np.squeeze(ds.variables[var_name][:])
            if data.ndim == 3: data = data[0, :, :]
            
            units = ds.variables[var_name].units
            temp_f = ((data - 273.15) * 1.8 + 32) if "K" in units.upper() else (data * 1.8 + 32)

            # Create transparency mask for invalid data/clouds
            masked_temp = np.ma.masked_where(~np.isfinite(temp_f) | (temp_f < 35) | (temp_f > 95), temp_f)

            vmin, vmax = 45, 85
            png_filename = f"{base_name}.png"
            png_path = os.path.join(OUTPUT_DIR, png_filename)

            # Save as PNG with transparency
            plt.imsave(png_path, masked_temp, vmin=vmin, vmax=vmax, cmap='jet',
