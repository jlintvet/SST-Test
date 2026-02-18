import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Broad box covering the Sounds and the Offshore Canyons
LAT_MIN, LAT_MAX = 34.0, 37.0
LON_MIN, LON_MAX = -76.8, -74.0 # Expanded West to include the sounds
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
        # REMOVED STEPS: Processing every single 1km pixel for a solid look
        for i in range(len(lats)): 
            for j in range(len(lons)):
                val = sst_raw[i, j]
                
                if np.isfinite(val):
                    temp_f = (float(val) - 273.15) * 9/5 + 32
                    features.append({
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                        "properties": {"temp_f": round(temp_f, 1)}
                    })
        
        output = {"type": "FeatureCollection", "features": features}
        with open("sst_data.json", "w") as f:
            json.dump(output, f, allow_nan=False)
        print(f"Success! Created solid map with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
