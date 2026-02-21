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
MAX_DAYS = 7 
OUTPUT_DIR = "historical_data"
INTERVAL_SECONDS = 3600  # 1 Hour

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# 2026 Verified Dataset IDs
DATASETS = [
    {"id": "noaacwLEOACSPOSSTL3SnrtCDaily", "res": "mid"},
    {"id": "noaacwVIIRSj01SSTDaily3P", "res": "high"},
    {"id": "noaacwL3CollatednppC", "res": "high"}
]

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def get_variable_name(ds_id):
    try:
        info_url = f"https://coastwatch.noaa.gov/erddap/info/{ds_id}/index.json"
        resp = requests.get(info_url, headers=HEADERS, timeout=20)
        rows = resp.json()['table']['rows']
        var_names = [row[1] for row in rows if row[0] == 'variable']
        
        for v in ["sea_surface_temperature", "sst", "analysed_sst"]:
            if v in var_names:
                return v
    except:
        pass
    return "sea_surface_temperature"

def fetch_history():
    for ds in DATASETS:
        ds_id = ds["id"]
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Checking Dataset: {ds_id}")
        
        try:
            info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.json?time"
            resp = requests.get(info_url, headers=HEADERS, timeout=20)
            
            if resp.status_code != 200:
                continue
            
            all_timestamps = [row[0] for row in resp.json()['table']['rows']]
            target_timestamps = all_timestamps[-MAX_DAYS:]
            
            var_name = get_variable_name(ds_id)
            manifest = []

            for ts in target_timestamps:
                clean_date = ts.split('T')[0]
                filename = f"sst_{clean_date}.json"
                filepath = os.path.join(OUTPUT_DIR, filename)
                
                # --- NEW: SKIP IF FILE EXISTS ---
                if os.path.exists(filepath):
                    print(f"  Skipping {clean_date} (Already downloaded)")
                    manifest.append({"date": clean_date, "file": filename})
                    continue
                
                print(f"  Downloading new data for {clean_date}...")
                if download_data(ds_id, ts, var_name, filepath):
                    manifest.append({"date": clean_date, "file": filename})
                    time.sleep(1)

            if manifest:
                with open(os.path.join(OUTPUT_DIR, "manifest.json"), "w") as f:
                    json.dump(manifest, f, indent=2)
                return # Stop after successfully processing a dataset

        except Exception as e:
            print(f"  Error: {e}")
            continue

def download_data(ds_id, ts, var, output_path):
    url = (
        f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.nc?"
        f"{var}[({ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]"
    )
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        if response.status_code == 200:
            process_and_save(response.content, var, output_path)
            return True
    except:
        pass
    return False

def process_and_save(content, var_name, output_path):
    with Dataset("memory", memory=content) as ds:
        raw_val = np.squeeze(ds.variables[var_name][:])
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        units = ds.variables[var_name].units
        
        features = []
        for i in range(len(lats)): 
            for j in range(len(lons)):
                val = raw_val[i, j]
                if np.isfinite(val) and val > -30000:
                    is_kelvin = "K" in units.upper() or "KELVIN" in units.upper()
                    temp_c = (float(val) - 273.15) if is_kelvin else float(val)
                    temp_f = (temp_c * 1.8) + 32
                    
                    if 35 < temp_f < 95:
                        features.append({
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                            "properties": {"t": round(temp_f, 1)}
                        })
        
        output = {"type": "FeatureCollection", "features": features}
        with open(output_path, "w") as f:
            json.dump(output, f)

# --- UPDATED MAIN BLOCK ---
if __name__ == "__main__":
    print("SST Historical Tracker Started.")
    while True:
        fetch_history()
        print(f"Cycle complete. Waiting {INTERVAL_SECONDS // 60} minutes for next check...")
        time.sleep(INTERVAL_SECONDS)
