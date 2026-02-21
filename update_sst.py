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

# UPDATED: These are the primary 2026 NRT IDs
DATASETS = [
    {"id": "noaa_coastwatch_acspo_v2_nrt", "name": "ACSPO NRT Global"},
    {"id": "noaacwBLENDEDsstDNDaily", "name": "Geo-Polar Blended NRT"},
    {"id": "goes19SSThourly", "name": "GOES-19 Hourly (Real-Time)"}
]

# Different nodes often host different parts of the NRT stream
NODES = [
    "https://coastwatch.noaa.gov/erddap",
    "https://cwcgom.aoml.noaa.gov/erddap"
]

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def fetch_current():
    for node in NODES:
        for ds in DATASETS:
            ds_id = ds["id"]
            print(f"--- Checking {ds['name']} on {node.split('//')[1].split('.')[0]} ---")
            
            try:
                # 1. Get the latest time
                time_url = f"{node}/griddap/{ds_id}.json?time"
                t_resp = requests.get(time_url, timeout=15)
                if t_resp.status_code != 200:
                    continue

                latest_ts = t_resp.json()['table']['rows'][-1][0]
                clean_date = latest_ts.split('T')[0]
                print(f"  Found current date: {latest_ts}")

                # 2. Check variables
                info_url = f"{node}/info/{ds_id}/index.json"
                i_resp = requests.get(info_url, timeout=10)
                rows = i_resp.json()['table']['rows']
                var_name = next((r[1] for r in rows if r[0] == 'variable' and r[1] in ["sea_surface_temperature", "sst", "analysed_sst"]), "sst")

                # 3. Download
                safe_ts = latest_ts.replace(":", "-").replace("Z", "")
                filepath = os.path.join(OUTPUT_DIR, f"sst_{safe_ts}.json")
                
                if os.path.exists(filepath):
                    print(f"  Already have this data. Stopping.")
                    return

                print(f"  Downloading .nc for {clean_date}...")
                dl_url = (f"{node}/griddap/{ds_id}.nc?"
                          f"{var_name}[({latest_ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
                
                data_resp = requests.get(dl_url, timeout=60)
                if data_resp.status_code == 200:
                    process_and_save(data_resp.content, var_name, filepath)
                    print(f"  SUCCESS: Saved {latest_ts}")
                    return # Exit once we have the newest data
                
            except Exception as e:
                # Silence errors to move to the next dataset/node quickly
                continue

def process_and_save(content, var_name, output_path):
    with Dataset("memory", memory=content) as ds:
        lats = ds.variables['latitude'][::STEP]
        lons = ds.variables['longitude'][::STEP]
        data = ds.variables[var_name][:]
        
        if data.ndim == 3: data = np.squeeze(data[0, ::STEP, ::STEP])
        else: data = data[::STEP, ::STEP]
            
        units = ds.variables[var_name].units
        is_kelvin = "K" in units.upper()
        temp_c = (data - 273.15) if is_kelvin else data
        temp_f = (temp_c * 1.8) + 32

        mask = np.isfinite(temp_f) & (temp_f > 35) & (temp_f < 95)
        idx_lats, idx_lons = np.where(mask)

        features = []
        for i, j in zip(idx_lats, idx_lons):
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [round(float(lons[j]), 4), round(float(lats[i]), 4)]},
                "properties": {"t": round(float(temp_f[i, j]), 1)}
            })

        with open(output_path, "w") as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)

if __name__ == "__main__":
    fetch_current()
