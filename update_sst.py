import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Coordinates for VA Beach / Hatteras
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0
DATASET_ID = "jplMURSST41" 

def fetch_and_convert():
    info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.json?time"
    
    try:
        info_resp = requests.get(info_url, timeout=30)
        latest_time_str = info_resp.json()['table']['rows'][-1][0]

        data_url = (
            f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
            f"analysed_sst[({latest_time_str})][({LAT_MIN}):({LAT_MAX})][({LON_MIN}):({LON_MAX})]"
        )
        
        response = requests.get(data_url, timeout=120)
        if response.status_code == 200:
            process_data(response.content)
            
    except Exception as e:
        print(f"Error: {e}")

def process_data(content):
    with Dataset("memory", memory=content) as ds:
        sst_raw = ds.variables['analysed_sst'][0, :, :]
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        features = []
        for i in range(0, len(lats), 3): 
            for j in range(0, len(lons), 3):
                val = sst_raw[i, j]
                
                # CRITICAL FIX: Only proceed if val is a finite number
                if np.isfinite(val):
                    temp_f = (float(val) - 273.15) * 9/5 + 32
                    
                    # Double check the calculated temp is also finite
                    if np.isfinite(temp_f):
                        features.append({
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                            "properties": {"temp_f": round(temp_f, 1)}
                        })
        
        output = {
            "type": "FeatureCollection",
            "features": features
        }
        
        # Save to file and prevent any future NaNs from being written
        file_path = os.path.join(os.getcwd(), "sst_data.json")
        with open(file_path, "w") as f:
            # allow_nan=False will raise a Python error if a NaN slips through,
            # which is better than breaking your website!
            json.dump(output, f, allow_nan=False)
            
        print(f"Success! Cleaned JSON created with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
