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

# Primary 2026 NRT IDs
DATASETS = [
    {"id": "noaa_coastwatch_acspo_v2_nrt", "name": "ACSPO NRT Global"},
    {"id": "noaacwBLENDEDsstDNDaily", "name": "Geo-Polar Blended NRT"},
    {"id": "goes19SSThourly", "name": "GOES-19 Hourly"}
]

NODES = ["https://coastwatch.noaa.gov/erddap", "https://cwcgom.aoml.noaa.gov/erddap"]

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def update_manifest():
    """Scans the directory and updates manifest.json with all available files."""
    files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith("sst_") and f.endswith(".json")]
    manifest = []
    
    # Sort files so the newest data is at the end (or beginning)
    files.sort()
    
    for f in files:
        # Extract the date/time string from the filename
        # sst_2026-02-21.json -> 2026-02-21
        date_part = f.replace("sst_", "").replace(".json", "")
        manifest.append({
            "date": date_part,
            "file": f
        })
    
    manifest_path = os.path.join(OUTPUT_DIR, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"--- Manifest Updated: {len(manifest)} files indexed ---")

def fetch_current():
    for node in NODES:
        for ds in DATASETS:
            ds_id = ds["id"]
            print(f"--- Checking {ds['name']} on {node.split('//')[1].split('.')[0]} ---")
            
            try:
                time_url = f"{node}/griddap/{ds_id}.json?time"
                t_resp = requests.get(time_url, timeout=15)
                if t_resp.status_code != 200: continue

                latest_ts = t_resp.json()['table']['rows'][-1][0]
                clean_date = latest_ts.split('T')[0]
                
                # Check if we have this specific file
                # Use simple date for daily, or full TS for hourly
                filename = f"sst_{clean_date}.json"
                filepath = os.path.join(OUTPUT_DIR, filename)
                
                if os.path.exists(filepath):
                    print(f"  {clean_date} already exists. Skipping download.")
                    continue

                # Get variable and download
                info_url = f"{node}/info/{ds_id}/index.json"
                rows = requests.get(info_url, timeout=10).json()['table']['rows']
                var_name = next((r[1] for r in rows if r[0] == 'variable' and r[1] in ["sea_surface_temperature", "sst"]), "sst")

                dl_url = (f"{node}/griddap/{ds_id}.nc?"
                          f"{var_name}[({latest_ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
                
                data_resp = requests.get(dl_url, timeout=60)
                if data_resp.status_code == 200:
                    process_and_save(data_resp.content, var_name, filepath)
                    print(f"  SUCCESS: Saved {clean_date}")
                    return True
                
            except: continue
    return False

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
    did_download = fetch_current()
    # Always update the manifest so it reflects the current state of the folder
    update_manifest()
