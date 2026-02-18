import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# VA Beach / Hatteras Box including the sounds
LAT_MIN, LAT_MAX = 34.0, 37.0
LON_MIN, LON_MAX = -76.8, -74.0
DATASET_ID = "jplMURSST41" 

def fetch_and_convert():
    # Step 1: Get the last 3 time indices
    info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.json?time"
    print("Checking time indices for 3-day average...")
    
    try:
        info_resp = requests.get(info_url, timeout=30)
        # Grab the last three timestamps
        rows = info_resp.json()['table']['rows']
        latest_times = [rows[-1][0], rows[-2][0], rows[-3][0]]
        print(f"Averaging data for: {latest_times}")

        # Step 2: Request 3 days of data
        # Format: [(-3):(latest)] grabs the 3-day window
        data_url = (
            f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
            f"analysed_sst[(-3):(latest)][({LAT_MIN}):({LAT_MAX})][({LON_MIN}):({LON_MAX})]"
        )
        
        response = requests.get(data_url, timeout=180)
        if response.status_code == 200:
            process_data(response.content)
        else:
            print(f"Fetch failed: {response.status_code}")
            
    except Exception as e:
        print(f"Error: {e}")

def process_data(content):
    with Dataset("memory", memory=content) as ds:
        # sst_raw now has 3 slices: [time, lat, lon]
        sst_stack = ds.variables['analysed_sst'][:, :, :]
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        # Step 3: Perform the average across the time axis (axis 0)
        # nanmean ignores cloud gaps (NaNs) in any single day
        with np.errstate(all='ignore'):
            sst_avg = np.nanmean(sst_stack, axis=0)
        
        features = []
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
            "metadata": {"type": "3-Day Average Cloud Filtered"},
            "features": features
        }
        
        with open("sst_data.json", "w") as f:
            json.dump(output, f, allow_nan=False)
        print(f"Success! Created 3-day average with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
