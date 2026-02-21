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
INTERVAL_SECONDS = 3600 

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

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
        resp = requests.get(info_url, headers=HEADERS, timeout=15)
        rows = resp.json()['table']['rows']
        var_names = [row[1] for row in rows if row[0] == 'variable']
        for v in ["sea_surface_temperature", "sst", "analysed_sst"]:
            if v in var_names: return v
    except: pass
    return "sea_surface_temperature"

def fetch_history():
    for ds in DATASETS:
        ds_id = ds["id"]
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Checking: {ds_id}")
        try:
            info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.json?time"
            resp = requests.get(info_url, headers=HEADERS, timeout=15)
            if resp.status_code != 200: continue
            
            all_timestamps = [row[0] for row in resp.json()['table']['rows']]
            target_timestamps = all_timestamps[-MAX_DAYS:]
            var_name = get_variable_name(ds_id)
            
            success_count = 0
            for ts in target_timestamps:
                clean_date = ts.split('T')[0]
                filepath = os.path.join(OUTPUT_DIR, f"sst_{clean_date}.json")
                
                if os.path.exists(filepath):
                    success_count += 1
                    continue
                
                print(f"  Processing {clean_date}...")
                if download_and_process(ds_id, ts, var_name, filepath):
                    success_count += 1
            
            if success_count > 0:
                print(f"  Done with {ds_id}.")
                return 
        except Exception as e:
            print(f"  Error on {ds_id}: {e}")
            continue

def download_and_process(ds_id, ts, var, output_path):
    url = (f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.nc?"
           f"{var}[({ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
    try:
        response = requests.get(url, headers=HEADERS, timeout=45)
        if response.status_code == 200:
            # Use NumPy for lightning-fast processing
            with Dataset("memory", memory=response.content) as ds:
                lats = ds.variables['latitude'][:]
                lons = ds.variables['longitude'][:]
                data = np.squeeze(ds.variables[var][:])
                units = ds.variables[var].units

                # Vectorized Conversion
                is_kelvin = "K" in units.upper()
                temp_c = (data - 273.15) if is_kelvin else data
                temp_f = (temp_c * 1.8) + 32

                # Create a mask for valid data (non-nan and within range)
                # This replaces the nested for-loops
                mask = np.isfinite(temp_f) & (temp_f > 35) & (temp_f < 95)
                idx_lats, idx_lons = np.where(mask)

                features = []
                for i, j in zip(idx_lats, idx_lons):
                    features.append({
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                        "properties": {"t": round(float(temp_f[i, j]), 1)}
                    })

                with open(output_path, "w") as f:
                    json.dump({"type": "FeatureCollection", "features": features}, f)
            return True
    except Exception as e:
        print(f"    Download/Process Error: {e}")
    return False

if __name__ == "__main__":
    print("SST Tracker Active (Vectorized Mode)")
    while True:
        fetch_history()
        print(f"Sleeping... Next check at {datetime.fromtimestamp(time.time()+INTERVAL_SECONDS).strftime('%H:%M:%S')}")
        time.sleep(INTERVAL_SECONDS)
