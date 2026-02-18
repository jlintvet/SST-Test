import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Oregon Inlet / Hatteras Box
LAT_MIN, LAT_MAX = 34.0, 37.0
LON_MIN, LON_MAX = -76.8, -74.0
DATASET_ID = "jplMURSST41" 

def fetch_and_convert():
    print(f"Fetching 3-day window for {DATASET_ID}...")
    
    # Direct request for the last 3 days of data
    # [(latest-2):(latest)] is the standard ERDDAP way to grab a trailing window
    data_url = (
        f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
        f"analysed_sst[(latest-2):(latest)][({LAT_MIN}):({LAT_MAX})][({LON_MIN}):({LON_MAX})]"
    )
    
    print(f"Requesting URL: {data_url}")
    
    try:
        response = requests.get(data_url, timeout=180)
        
        if response.status_code == 200:
            print("Connection successful. Processing 3-day stack...")
            process_data(response.content)
        else:
            print(f"NOAA Server returned error {response.status_code}")
            print(f"Details: {response.text[:200]}") # Print first 200 chars of error
            
    except Exception as e:
        print(f"Network error: {e}")

def process_data(content):
    with Dataset("memory", memory=content) as ds:
        # Stack shape will be [3, Lats, Lons]
        sst_stack = ds.variables['analysed_sst'][:, :, :]
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        # Calculate mean across the 3 days, ignoring any NaNs (clouds)
        with np.errstate(all='ignore'):
            sst_avg = np.nanmean(sst_stack, axis=0)
        
        features = []
        # No skipping - full resolution
        for i in range(len(lats)): 
            for j in range(len(lons)):
                val = sst_avg[i, j]
                if np.isfinite(val):
                    temp_f = (float(val) - 273.15) * 9/5 + 32
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
            
        print(f"Success! Generated 3-day average with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
