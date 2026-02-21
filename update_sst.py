import numpy as np
import requests
import json
from netCDF4 import Dataset
import os
from datetime import datetime

# Settings
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0
OUTPUT_DIR = "historical_data"
STEP = 4 
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
    """Generates manifest.json, prioritizing the best quality data per day."""
    files = sorted([f for f in os.listdir(OUTPUT_DIR) if f.startswith("sst_") and f.endswith(".json") and f != "manifest.json"])
    
    # Priority tracking: { "2026-02-19": "sst_noaacwBLENDED...json" }
    daily_best = {}

    for f in files:
        parts = f.split('_')
        if len(parts) < 3: continue
        
        ds_id = parts[1]
        date = parts[2].replace(".json", "")
        
        # If hourly GOES data, we extract just the date part for day-grouping
        if "T" in date:
            day_key = date.split('T')[0]
        else:
            day_key = date

        # Priority Ranking Logic:
        # 1. Blended (Daily)
        # 2. ACSPO (NRT)
        # 3. GOES (Hourly)
        if day_key not in daily_best:
            daily_best[day_key] = f
        else:
            current_file = daily_best[day_key]
            if "BLENDED" in f:
                daily_best[day_key] = f
            elif "acspo" in f and "BLENDED" not in current_file:
                daily_best[day_key] = f

    manifest = [{"date": d, "file": daily_best[d]} for d in sorted(daily_best.keys())]
    
    with open(os.path.join(OUTPUT_DIR, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    print(f"--- Manifest Updated: {len(manifest)} unique days indexed ---")

def process_and_save(content, var_name, output_path):
    """Processes NetCDF memory buffer and saves to a clean GeoJSON."""
    try:
        with Dataset("memory", memory=content) as ds:
            lats = ds.variables['latitude'][::STEP]
            lons = ds.variables['longitude'][::STEP]
            data = np.squeeze(ds.variables[var_name][:])
            
            # Handle different dimensionality (Time, Lat, Lon)
            if data.ndim == 3: 
                data = data[0, ::STEP, ::STEP]
            else: 
                data = data[::STEP, ::STEP]
                
            units = ds.variables[var_name].units
            # Convert to Fahrenheit
            temp_f = ((data - 273.15) * 1.8 + 32) if "K" in units.upper() else (data * 1.8 + 32)

            # Mask invalid/land data
            mask = np.isfinite(temp_f) & (temp_f > 35) & (temp_f < 95)
            idx_lats, idx_lons = np.where(mask)

            features = []
            for i, j in zip(idx_lats, idx_lons):
                features.append({
                    "type": "Feature", 
                    "geometry": {"type": "Point", "coordinates": [round(float(lons[j]), 4), round(float(lats[i]), 4)]},
                    "properties": {"t": round(float(temp_f[i, j]), 1)}
                })

            # Strictly overwrite ("w") to prevent JSON corruption
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump({"type": "FeatureCollection", "features": features}, f)
                
    except Exception as e:
        print(f"      Error processing NetCDF: {e}")

def fetch_history():
    """Scans nodes for missing data across defined datasets."""
    for node in NODES:
        for ds in DATASETS:
            ds_id = ds["id"]
            print(f"--- Scanning {ds['name']} ---")
            
            try:
                # 1. Get available times
                time_url = f"{node}/griddap/{ds_id}.json?time"
                t_resp = requests.get(time_url, timeout=15)
                if t_resp.status_code != 200: continue

                available_timestamps = [row[0] for row in t_resp.json()['table']['rows']]
                # Look at the most recent entries within our lookback window
                recent_timestamps = available_timestamps[-LOOKBACK_DAYS*2:] 

                # 2. Get the specific SST variable name
                info_url = f"{node}/info/{ds_id}/index.json"
                rows = requests.get(info_url, timeout=10).json()['table']['rows']
                var_name = next((r[1] for r in rows if r[0] == 'variable' and r[1] in ["sea_surface_temperature", "sst"]), "sst")

                for ts in recent_timestamps:
                    # Formatting filename: sst_DATASETID_TIMESTAMP.json
                    clean_ts = ts.replace(":", "").replace("-", "").replace("Z", "")
                    filename = f"sst_{ds_id}_{clean_ts}.json"
                    filepath = os.path.join(OUTPUT_DIR, filename)

                    if os.path.exists(filepath):
                        continue

                    print(f"  Gap detected! Downloading {ts}...")
                    dl_url = (f"{node}/griddap/{ds_id}.nc?"
                              f"{var_name}[({ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
                    
                    data_resp = requests.get(dl_url, timeout=60)
                    if data_resp.status_code == 200:
                        process_and_save(data_resp.content, var_name, filepath)
                        print(f"    SUCCESS: Saved {filename}")
                
            except Exception as e:
                print(f"  Skipping {ds_id}: {e}")
                continue

if __name__ == "__main__":
    fetch_history()
    update_manifest()
