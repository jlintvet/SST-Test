import numpy as np
import requests
import json
from netCDF4 import Dataset
import os
from datetime import datetime

# Settings
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0
MAX_DAYS = 7  # <--- Change this for more/less history
OUTPUT_DIR = "historical_data"

DATASETS = [
    {"id": "noaacwVIIRSj01SSTDaily3P", "res": "high"},
    {"id": "noaacwLEOACSPOSSTL3SnrtCDaily", "res": "low"}
]

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def fetch_history():
    # Step 1: Get available timestamps from the primary dataset
    ds_id = DATASETS[0]["id"]
    info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.json?time"
    
    try:
        resp = requests.get(info_url, timeout=20)
        # ERDDAP returns timestamps in ISO format: "2024-02-18T12:00:00Z"
        all_timestamps = [row[0] for row in resp.json()['table']['rows']]
        
        # Get the last X timestamps
        target_timestamps = all_timestamps[-MAX_DAYS:]
        print(f"Found {len(target_timestamps)} updates to fetch.")
        
        manifest = []

        for ts in target_timestamps:
            # Clean timestamp for filename (remove T, Z, and colons)
            clean_date = ts.split('T')[0] 
            filename = f"sst_{clean_date}.json"
            filepath = os.path.join(OUTPUT_DIR, filename)
            
            print(f"Processing {clean_date}...")
            
            # Fetch data for this specific timestamp
            success = download_timestamp(ts, filepath)
            
            if success:
                manifest.append({"date": clean_date, "file": filename})

        # Save a manifest file so the UI knows the order of files
        with open(os.path.join(OUTPUT_DIR, "manifest.json"), "w") as f:
            json.dump(manifest, f)
        print("Done! Historical data and manifest.json ready.")

    except Exception as e:
        print(f"Failed to fetch timeline: {e}")

def download_timestamp(ts, output_path):
    # Try datasets in order of resolution preference
    for ds in DATASETS:
        ds_id = ds["id"]
        is_low_res = (ds["res"] == "low")
        url = (
            f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.nc?"
            f"sea_surface_temperature[({ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]"
        )
        
        try:
            response = requests.get(url, timeout=120)
            if response.status_code == 200:
                process_and_save(response.content, is_low_res, output_path)
                return True
        except:
            continue
    return False

def process_and_save(content, is_low_res, output_path):
    with Dataset("memory", memory=content) as ds:
        var_name = "sea_surface_temperature"
        raw_val = np.squeeze(ds.variables[var_name][:])
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        units = ds.variables[var_name].units
        
        features = []
        for i in range(len(lats)): 
            for j in range(len(lons)):
                val = raw_val[i, j]
                if np.isfinite(val) and val > -30000:
                    # Unit Conversion
                    temp_c = (float(val) - 273.15) if "K" in units else float(val)
                    temp_f = (temp_c * 1.8) + 32
                    
                    if 35 < temp_f < 95:
                        features.append({
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                            "properties": {"temp_f": round(temp_f, 1)}
                        })
        
        output = {"type": "FeatureCollection", "features": features}
        with open(output_path, "w") as f:
            json.dump(output, f)

if __name__ == "__main__":
    fetch_history()
