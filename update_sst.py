import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Oregon Inlet / Hatteras Box (High-Res)
LAT_MIN, LAT_MAX = 34.0, 37.0
LON_MIN, LON_MAX = -76.8, -74.0

# The high-fidelity VIIRS NOAA-20 ID
DATASET_ID = "noaacwVIIRSj01SSTDaily3P"

def fetch_and_convert():
    # Step 1: Discover the last 3 available timestamps to avoid 404
    info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.json?time"
    print("Discovering latest high-res timestamps...")
    
    try:
        info_resp = requests.get(info_url, timeout=30)
        if info_resp.status_code != 200:
            print("Server metadata busy. Retrying with generic latest...")
            latest_times = ["(latest)", "(latest-1)", "(latest-2)"]
        else:
            rows = info_resp.json()['table']['rows']
            latest_times = [f"({r[0]})" for r in rows[-3:]]
        
        features_list = []
        
        # Step 2: Fetch each of the 3 days individually
        # This prevents the "Start=NaN" error by using explicit timestamps
        combined_data = []
        for time_str in latest_times:
            url = (
                f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
                f"sea_surface_temperature[{time_str}][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]"
            )
            print(f"Fetching pass: {time_str}")
            resp = requests.get(url, timeout=60)
            if resp.status_code == 200:
                combined_data.append(resp.content)
        
        if not combined_data:
            print("Could not retrieve any valid satellite passes.")
            return

        process_stack(combined_data)
            
    except Exception as e:
        print(f"Error: {e}")

def process_stack(data_contents):
    # Use the first pass to set up the grid
    with Dataset("memory", memory=data_contents[0]) as ds:
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        # Create a 3D stack for averaging
        stack = np.full((len(data_contents), len(lats), len(lons)), np.nan)
        
    # Fill the stack with SST data from all successful passes
    for idx, content in enumerate(data_contents):
        with Dataset("memory", memory=content) as ds:
            stack[idx, :, :] = ds.variables['sea_surface_temperature'][0, :, :]

    # Average the stack while ignoring clouds (NaNs)
    with np.errstate(all='ignore'):
        sst_avg = np.nanmean(stack, axis=0)

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

    output = {"type": "FeatureCollection", "features": features}
    with open("sst_data.json", "w") as f:
        json.dump(output, f, allow_nan=False)
    print(f"Success! High-res 3-day average created with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
