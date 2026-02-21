import numpy as np
import requests
import json
from netCDF4 import Dataset
import os
import time
from datetime import datetime

# Settings
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0
MAX_DAYS = 8 # Increased to bridge the gap
OUTPUT_DIR = "historical_data"
STEP = 3 

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# UPDATED: Using Near Real-Time (NRT) IDs for 2026
DATASETS = [
    {"id": "noaacwVIIRSj01SSTDaily3P", "res": "high"}, # 1st choice: High Res
    {"id": "noaacwLEOACSPOSSTL3SnrtCDaily", "res": "mid"}, # 2nd choice: NRT Daily
    {"id": "noaacwL3CollatednppC", "res": "high"} # 3rd choice: Backup
]

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def get_variable_name(ds_id):
    try:
        info_url = f"https://coastwatch.noaa.gov/erddap/info/{ds_id}/index.json"
        resp = requests.get(info_url, headers=HEADERS, timeout=10)
        rows = resp.json()['table']['rows']
        var_names = [row[1] for row in rows if row[0] == 'variable']
        for v in ["sea_surface_temperature", "sst", "analysed_sst"]:
            if v in var_names: return v
    except: pass
    return "sea_surface_temperature"

def fetch_history():
    # We want to check ALL datasets until we find the one with the freshest date
    freshest_date_found = None
    
    for ds in DATASETS:
        ds_id = ds["id"]
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Checking: {ds_id}")
        try:
            # We check the time coverage specifically
            info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.json?time"
            resp = requests.get(info_url, headers=HEADERS, timeout=12)
            if resp.status_code != 200: continue
            
            all_timestamps = [row[0] for row in resp.json()['table']['rows']]
            target_timestamps = all_timestamps[-MAX_DAYS:]
            
            last_date = target_timestamps[-1].split('T')[0]
            print(f"  Server's latest data for this ID: {last_date}")

            var_name = get_variable_name(ds_id)
            
            for ts in target_timestamps:
                clean_date = ts.split('T')[0]
                filepath = os.path.join(OUTPUT_DIR, f"sst_{clean_date}.json")
                
                if os.path.exists(filepath):
                    continue
                
                print(f"  New data found! Processing {clean_date}...")
                download_and_process(ds_id, ts, var_name, filepath)
            
            # If we found data for today or yesterday, we can stop
            if last_date >= datetime.now().strftime('%Y-%m-%d'):
                print("  Successfully reached current date.")
                return

        except Exception as e:
            print(f"  Error checking {ds_id}: {e}")
            continue

def download_and_process(ds_id, ts, var, output_path):
    # Narrowing the bounding box slightly can also prevent "Hanging"
    url = (f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.nc?"
           f"{var}[({ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        if response.status_code == 200:
            with Dataset("memory", memory=response.content) as ds:
                lats = ds.variables['latitude'][::STEP]
                lons = ds.variables['longitude'][::STEP]
                data = np.squeeze(ds.variables[var][:, ::STEP, ::STEP])
                units = ds.variables[var].units

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
            print(f"    Saved {clean_date} with {len(features)} points.")
            return True
    except Exception as e:
        print(f"    Processing Error: {e}")
    return False

if __name__ == "__main__":
    fetch_history()
