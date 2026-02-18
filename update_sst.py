import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Coordinates for VA Beach / Hatteras Box
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0
DATASET_ID = "jplMURSST41" 

def fetch_and_convert():
    # Fetching only the absolute latest timestamp
    info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.json?time"
    
    try:
        info_resp = requests.get(info_url, timeout=30)
        latest_time_str = info_resp.json()['table']['rows'][-1][0]
        print(f"Fetching latest data for: {latest_time_str}")

        data_url = (
            f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
            f"analysed_sst[({latest_time_str})][({LAT_MIN}):({LAT_MAX})][({LON_MIN}):({LON_MAX})]"
        )
        
        response = requests.get(data_url, timeout=120)
        if response.status_code == 200:
            process_data(response.content)
            
    except Exception as e:
        print(f"Error during fetch: {e}")

def process_data(content):
    with Dataset("memory", memory=content) as ds:
        sst_raw = ds.variables['analysed_sst'][0, :, :]
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        features = []
        # Removed step=3 to process EVERY pixel for high density
        for i in range(len(lats)): 
            for j in range(len(lons)):
                val = sst_raw[i, j]
                
                if np.isfinite(val):
                    # KELVIN TO FAHRENHEIT CONVERSION
                    temp_f = (float(val) - 273.15) * 1.8 + 32
                    
                    features.append({
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                        "properties": {"temp_f": round(temp_f, 1)}
                    })
        
        output = {
            "type": "FeatureCollection",
            "features": features
        }
        
        file_path = os.path.join(os.getcwd(), "sst_data.json")
        with open(file_path, "w") as f:
            json.dump(output, f, allow_nan=False)
            
        print(f"Success! High-density JSON created with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
