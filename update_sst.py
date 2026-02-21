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
STEP = 4  # Increased step to handle higher frequency data faster

# IDs used by the NOAA CoastWatch "Live" map for 2026
DATASETS = [
    {"id": "noaacwVIIRSj01SSTDaily3P", "name": "VIIRS J01 L3S NRT"},
    {"id": "noaacwVIIRSnppSSTDaily3P", "name": "VIIRS NPP L3S NRT"},
    {"id": "noaacwVIIRSj02SSTDaily3P", "name": "VIIRS J02 L3S NRT"}
]

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def fetch_current():
    for ds in DATASETS:
        ds_id = ds["id"]
        print(f"--- Checking {ds['name']} ---")
        
        try:
            # We use the .json?time endpoint which is more reliable for NRT
            time_url = f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.json?time"
            resp = requests.get(time_url, timeout=15)
            
            if resp.status_code != 200:
                print(f"  ID {ds_id} not responsive on this node.")
                continue

            # Get the very last timestamp in the array
            all_times = resp.json()['table']['rows']
            latest_ts = all_times[-1][0]
            clean_date = latest_ts.split('T')[0]
            
            print(f"  Server reports latest data: {latest_ts}")

            # Check if we already processed this specific date/time
            # Using timestamp in filename to allow multiple updates per day
            safe_ts = latest_ts.replace(":", "-").replace("Z", "")
            filepath = os.path.join(OUTPUT_DIR, f"sst_{safe_ts}.json")
            
            if os.path.exists(filepath):
                print(f"  Snapshot {latest_ts} already exists. Skipping.")
                continue

            # Get variable name
            info_url = f"https://coastwatch.noaa.gov/erddap/info/{ds_id}/index.json"
            info_resp = requests.get(info_url, timeout=10)
            rows = info_resp.json()['table']['rows']
            var_names = [row[1] for row in rows if row[0] == 'variable']
            var_name = next((v for v in ["sea_surface_temperature", "sst", "analysed_sst"] if v in var_names), "sst")

            print(f"  Downloading fresh swath: {latest_ts}...")
            url = (f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.nc?"
                   f"{var_name}[({latest_ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
            
            data_resp = requests.get(url, timeout=60)
            if data_resp.status_code == 200:
                process_data(data_resp.content, var_name, filepath)
                print(f"  SUCCESS: {latest_ts} saved.")
                return 
            else:
                print(f"  Server error {data_resp.status_code} on download.")

        except Exception as e:
            print(f"  Error: {e}")

def process_data(content, var_name, output_path):
    with Dataset("memory", memory=content) as ds:
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        # Level 2/3S data often has a 'time' dimension we need to squeeze
        data = ds.variables[var_name][:]
        if data.ndim == 3:
            data = np.squeeze(data[0, :, :])
            
        # Apply stepping for performance
        lats = lats[::STEP]
        lons = lons[::STEP]
        data = data[::STEP, ::STEP]
        
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
                "geometry": {"type": "Point", "coordinates": [round(float(lons[j]), 4), round(float(lats[i]) , 4)]},
                "properties": {"t": round(float(temp_f[i, j]), 1)}
            })

        with open(output_path, "w") as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)

if __name__ == "__main__":
    fetch_current()
