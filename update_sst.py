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
LOOKBACK_DAYS = 5  # How many recent days to check for gaps

DATASETS = [
    {"id": "noaa_coastwatch_acspo_v2_nrt", "name": "ACSPO NRT Global"},
    {"id": "noaacwBLENDEDsstDNDaily", "name": "Geo-Polar Blended NRT"},
    {"id": "goes19SSThourly", "name": "GOES-19 Hourly"}
]

NODES = ["https://coastwatch.noaa.gov/erddap", "https://cwcgom.aoml.noaa.gov/erddap"]

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def update_manifest():
    files = sorted([f for f in os.listdir(OUTPUT_DIR) if f.startswith("sst_") and f.endswith(".json")])
    manifest = [{"date": f.replace("sst_", "").replace(".json", ""), "file": f} for f in files]
    with open(os.path.join(OUTPUT_DIR, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"--- Manifest Updated: {len(manifest)} files indexed ---")

def fetch_history():
    """Checks for any missing days in the recent lookback window."""
    for node in NODES:
        for ds in DATASETS:
            ds_id = ds["id"]
            print(f"--- Scanning {ds['name']} for missing days ---")
            
            try:
                # 1. Get the list of available times
                time_url = f"{node}/griddap/{ds_id}.json?time"
                t_resp = requests.get(time_url, timeout=15)
                if t_resp.status_code != 200: continue

                # Get the last few timestamps from the server
                available_timestamps = [row[0] for row in t_resp.json()['table']['rows']]
                recent_timestamps = available_timestamps[-LOOKBACK_DAYS:]

                # 2. Identify the variable name once per dataset
                info_url = f"{node}/info/{ds_id}/index.json"
                rows = requests.get(info_url, timeout=10).json()['table']['rows']
                var_name = next((r[1] for r in rows if r[0] == 'variable' and r[1] in ["sea_surface_temperature", "sst"]), "sst")

                for ts in recent_timestamps:
                    clean_date = ts.split('T')[0]
                    filename = f"sst_{clean_date}.json"
                    filepath = os.path.join(OUTPUT_DIR, filename)

                    if os.path.exists(filepath):
                        # print(f"  {clean_date} exists.")
                        continue

                    print(f"  Gap detected! Downloading {clean_date} from {ds['name']}...")
                    dl_url = (f"{node}/griddap/{ds_id}.nc?"
                              f"{var_name}[({ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
                    
                    data_resp = requests.get(dl_url, timeout=60)
                    if data_resp.status_code == 200:
                        process_and_save(data_resp.content, var_name, filepath)
                        print(f"  SUCCESS: Filled gap for {clean_date}")
                
            except Exception as e:
                # print(f"  Skipping {ds_id} on this node.")
                continue

def process_and_save(content, var_name, output_path):
    with Dataset("memory", memory=content) as ds:
        lats = ds.variables['latitude'][::STEP]
        lons = ds.variables['longitude'][::STEP]
        data = np.squeeze(ds.variables[var_name][:])
        if data.ndim == 3: data = data[0, ::STEP, ::STEP]
        else: data = data[::STEP, ::STEP]
            
        units = ds.variables[var_name].units
        temp_f = ((data - 273.15) * 1.8 + 32) if "K" in units.upper() else (data * 1.8 + 32)

        mask = np.isfinite(temp_f) & (temp_f > 35) & (temp_f < 95)
        idx_lats, idx_lons = np.where(mask)

        features = [{"type": "Feature", 
                     "geometry": {"type": "Point", "coordinates": [round(float(lons[j]), 4), round(float(lats[i]), 4)]},
                     "properties": {"t": round(float(temp_f[i, j]), 1)}} 
                    for i, j in zip(idx_lats, idx_lons)]

        with open(output_path, "w") as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)

if __name__ == "__main__":
    fetch_history()
    update_manifest()
