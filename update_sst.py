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

# These are the "Bulletproof" NRT IDs for the 2026 NOAA Catalog
DATASETS = [
    {"id": "noaacwVIIRSnrtDaily", "name": "VIIRS NRT Daily"},
    {"id": "noaacwNPPVIIRSnrtDaily", "name": "NPP NRT Daily"},
    {"id": "noaacwNRTswathL2SST", "name": "Direct Swath NRT"}
]

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def fetch_current():
    for ds in DATASETS:
        ds_id = ds["id"]
        print(f"--- Attempting: {ds['name']} ({ds_id}) ---")
        
        # Check the metadata first to see if the dataset exists on this node
        check_url = f"https://coastwatch.noaa.gov/erddap/info/{ds_id}/index.json"
        try:
            r = requests.get(check_url, timeout=10)
            if r.status_code != 200:
                print(f"  [!] ID not found on this node. Moving to next...")
                continue
            
            # If we reach here, the dataset IS on the server.
            # Now get the latest time.
            time_url = f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.json?time"
            t_resp = requests.get(time_url, timeout=10)
            latest_ts = t_resp.json()['table']['rows'][-1][0]
            clean_date = latest_ts.split('T')[0]
            
            print(f"  Found Live Data: {latest_ts}")

            # Check if we already have this specific timestamp
            safe_ts = latest_ts.replace(":", "-").replace("Z", "")
            filepath = os.path.join(OUTPUT_DIR, f"sst_{safe_ts}.json")
            if os.path.exists(filepath):
                print(f"  Data for {latest_ts} already saved locally.")
                return # Stop, we are current

            # Determine Variable Name
            rows = r.json()['table']['rows']
            var_name = next((row[1] for row in rows if row[0] == 'variable' and row[1] in ["sea_surface_temperature", "sst", "analysed_sst"]), "sst")

            # Download .nc
            print(f"  Downloading {clean_date}...")
            dl_url = (f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.nc?"
                      f"{var_name}[({latest_ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
            
            data_resp = requests.get(dl_url, timeout=60)
            if data_resp.status_code == 200:
                process_and_save(data_resp.content, var_name, filepath)
                print(f"  SUCCESS: Saved {latest_ts}")
                
                # Update a 'latest.json' for your frontend
                with open(os.path.join(OUTPUT_DIR, "latest_status.json"), "w") as f:
                    json.dump({"last_updated": latest_ts, "file": f"sst_{safe_ts}.json"}, f)
                return
                
        except Exception as e:
            print(f"  Connection error or timeout: {e}")

def process_and_save(content, var_name, output_path):
    with Dataset("memory", memory=content) as ds:
        lats = ds.variables['latitude'][::STEP]
        lons = ds.variables['longitude'][::STEP]
        data = ds.variables[var_name][:]
        
        # Handle 3D (time, lat, lon) or 2D (lat, lon)
        if data.ndim == 3:
            data = np.squeeze(data[0, ::STEP, ::STEP])
        else:
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
                "geometry": {"type": "Point", "coordinates": [round(float(lons[j]), 4), round(float(lats[i]), 4)]},
                "properties": {"t": round(float(temp_f[i, j]), 1)}
            })

        with open(output_path, "w") as f:
            json.dump({"type": "FeatureCollection", "features": features}, f)

if __name__ == "__main__":
    fetch_current()
