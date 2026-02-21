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
STEP = 3 

# NEW: Prioritizing "nrt" (Near Real-Time) IDs
DATASETS = [
    {"id": "noaa_coastwatch_acspo_v2_nrt", "name": "ACSPO NRT"}, # Most current 2026 ID
    {"id": "noaacwVIIRSj01SSTDaily3P", "name": "VIIRS Daily"},
    {"id": "noaacwLEOACSPOSSTL3SnrtCDaily", "name": "LEO NRT"}
]

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def fetch_current():
    for ds in DATASETS:
        ds_id = ds["id"]
        print(f"--- Checking {ds['name']} ({ds_id}) ---")
        
        try:
            # Step 1: Get the absolute latest timestamp available on the server
            info_url = f"https://coastwatch.noaa.gov/erddap/info/{ds_id}/index.json"
            resp = requests.get(info_url, timeout=10)
            rows = resp.json()['table']['rows']
            
            # Find the time_coverage_end attribute
            latest_ts = next(row[4] for row in rows if row[2] == 'time_coverage_end')
            clean_date = latest_ts.split('T')[0]
            
            print(f"  Server reports latest data is from: {clean_date}")
            
            # Step 2: Check if we already have it
            filepath = os.path.join(OUTPUT_DIR, f"sst_{clean_date}.json")
            if os.path.exists(filepath):
                print(f"  Already have {clean_date}. Skipping.")
                continue

            # Step 3: Get the variable name (usually 'sst' or 'sea_surface_temperature')
            var_names = [row[1] for row in rows if row[0] == 'variable']
            var_name = next((v for v in ["sea_surface_temperature", "sst", "analysed_sst"] if v in var_names), "sst")

            # Step 4: Download the .nc file for that specific latest timestamp
            print(f"  Downloading fresh data for {clean_date}...")
            url = (f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.nc?"
                   f"{var_name}[({latest_ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
            
            data_resp = requests.get(url, timeout=60)
            if data_resp.status_code == 200:
                process_data(data_resp.content, var_name, filepath)
                print(f"  SUCCESS: {clean_date} saved.")
                return # Stop once we've successfully pulled the latest data
            else:
                print(f"  Failed download (Status {data_resp.status_code})")

        except Exception as e:
            print(f"  Error on {ds_id}: {e}")

def process_data(content, var_name, output_path):
    with Dataset("memory", memory=content) as ds:
        lats = ds.variables['latitude'][::STEP]
        lons = ds.variables['longitude'][::STEP]
        data = np.squeeze(ds.variables[var_name][:, ::STEP, ::STEP])
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
