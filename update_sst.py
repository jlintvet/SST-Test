import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Coordinates for VA Beach / Hatteras Box
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0

# Tier 1: 750m VIIRS (High Resolution)
# Tier 2: 2km L3S (Stable Fallback)
DATASETS = [
    {"id": "noaacwVIIRSj01SSTDaily3P", "res": "high"},
    {"id": "noaacwLEOACSPOSSTL3SnrtCDaily", "res": "low"}
]

def fetch_and_convert():
    for ds in DATASETS:
        ds_id = ds["id"]
        is_low_res = (ds["res"] == "low")
        print(f"Trying {ds['res']}-res dataset: {ds_id}...")
        
        try:
            # Step 1: Discover Timestamp
            info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.json?time"
            info_resp = requests.get(info_url, timeout=20)
            if info_resp.status_code != 200:
                continue
            
            latest_ts = info_resp.json()['table']['rows'][-1][0]
            
            # Step 2: Request Data (Handling VIIRS vs L3S variable names)
            var_name = "sea_surface_temperature" if is_low_res else "sea_surface_temperature"
            url = (
                f"https://coastwatch.noaa.gov/erddap/griddap/{ds_id}.nc?"
                f"{var_name}[({latest_ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]"
            )
            
            response = requests.get(url, timeout=120)
            if response.status_code == 200:
                print(f"Success with {ds['res']}-res data!")
                process_data(response.content, is_low_res)
                return # Stop after first successful fetch
            
        except Exception as e:
            print(f"Skipping {ds_id} due to error.")

    print("CRITICAL: All datasets failed.")

def process_data(content, is_low_res):
    with Dataset("memory", memory=content) as ds:
        # VIIRS and L3S have slightly different internal structures
        var_name = "sea_surface_temperature"
        raw_val = np.squeeze(ds.variables[var_name][:])
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        # VIIRS (High-res) is often Kelvin, L3S is Celsius. 
        # We check the units attribute to be safe.
        units = ds.variables[var_name].units
        
        features = []
        for i in range(len(lats)): 
            for j in range(len(lons)):
                val = raw_val[i, j]
                if np.isfinite(val) and val > -30000:
                    # Convert to Fahrenheit based on native units
                    if "K" in units: # Kelvin
                        temp_f = (float(val) - 273.15) * 1.8 + 32
                    else: # Celsius
                        temp_f = (float(val) * 1.8) + 32
                    
                    if 35 < temp_f < 95: # Basic ocean filter
                        features.append({
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                            "properties": {
                                "temp_f": round(temp_f, 1),
                                "is_low_res": is_low_res # Pass the flag to the map
                            }
                        })
        
        output = {"type": "FeatureCollection", "features": features}
        with open("sst_data.json", "w") as f:
            json.dump(output, f, allow_nan=False)
        print(f"Created map with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
