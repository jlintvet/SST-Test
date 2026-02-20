import numpy as np
import requests
import json
from netCDF4 import Dataset
import os
import time

# Settings
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0
MAX_DAYS = 7 
OUTPUT_DIR = "historical_data"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Expanded list of Dataset IDs for 2026 redundancy
DATASETS = [
    {"id": "noaacwVIIRSj01SSTDaily3P", "res": "high"},   # Original target
    {"id": "noaacwNPPVIIRSchlaDaily", "res": "high"},    # VIIRS backup
    {"id": "noaacwLEOACSPOSSTL3SnrtCDaily", "res": "mid"}, # Reliable L3S
    {"id": "noaacrwsstDaily", "res": "low"}              # CoralTemp (Global backup)
]

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def fetch_history():
    success_count = 0
    # Try datasets until one gives us a valid timeline
    for ds in DATASETS:
        ds_id = ds["id"]
        info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.json?time"
        
        print(f"Checking dataset timeline: {ds_id}...")
        try:
            resp = requests.get(info_url, headers=HEADERS, timeout=20)
            if resp.status_code == 404:
                print(f"  404: Dataset {ds_id} not found. Trying next...")
                continue
            
            all_timestamps = [row[0] for row in resp.json()['table']['rows']]
            target_timestamps = all_timestamps[-MAX_DAYS:]
            print(f"  Found {len(target_timestamps)} updates. Fetching...")
            
            manifest = []
            for ts in target_timestamps:
                clean_date = ts.split('T')[0]
                filename = f"sst_{clean_date}.json"
                filepath = os.path.join(OUTPUT_DIR, filename)
                
                if download_timestamp(ds_id, ts, filepath):
                    manifest.append({"date": clean_date, "file": filename})
                    success_count += 1
                    time.sleep(1)

            # Save the index of what we found
            if manifest:
                with open(os.path.join(OUTPUT_DIR, "manifest.json"), "w") as f:
                    json.dump(manifest, f, indent=2)
                print(f"Success! Saved {success_count} days of data.")
                return # Stop if we successfully got history from a dataset

        except Exception as e:
            print(f"  Error checking {ds_id}: {e}")
            continue

    print("CRITICAL: All datasets returned 404 or errors.")

def download_timestamp(ds_id, ts, output_path):
    url = (
        f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.nc?"
        f"sea_surface_temperature[({ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]"
    )
    try:
        response = requests.get(url, headers=HEADERS, timeout=60)
        if response.status_code == 200:
            process_and_save(response.content, output_path)
            return True
    except:
        pass
    return False

def process_and_save(content, output_path):
    with Dataset("memory", memory=content) as ds:
        # Some datasets use 'sst', others 'sea_surface_temperature'
        var_name = "sea_surface_temperature" if "sea_surface_temperature" in ds.variables else "sst"
        raw_val = np.squeeze(ds.variables[var_name][:])
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        units = ds.variables[var_name].units
        
        features = []
        for i in range(len(lats)): 
            for j in range(len(lons)):
                val = raw_val[i, j]
                if np.isfinite(val) and val > -30000:
                    temp_c = (float(val) - 273.15) if "K" in units else float(val)
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

if __name__ == "__main__":
    fetch_history()
