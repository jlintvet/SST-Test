import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# VA Beach / Hatteras Box
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.8, -73.0

# The L3S Super-collated Dataset (2km resolution)
DATASET_ID = "noaacwLEOACSPOSSTL3SnrtCDaily"

def fetch_and_convert():
    print(f"Connecting to High-Res L3S: {DATASET_ID}")
    
    # Step 1: Get the exact last 3 available timestamps from metadata
    info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.json?time"
    
    try:
        info_resp = requests.get(info_url, timeout=30)
        if info_resp.status_code != 200:
            print("Server metadata busy. Cannot find time indices.")
            return
            
        # Get the last 3 rows of the 'time' variable
        rows = info_resp.json()['table']['rows']
        latest_times = [f"({r[0]})" for r in rows[-3:]]
        print(f"Averaging timestamps: {latest_times}")

        combined_data = []
        # Step 2: Fetch each exact time slice
        for ts in latest_times:
            url = (
                f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
                f"sea_surface_temperature[{ts}][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]"
            )
            print(f"Requesting: {ts}")
            resp = requests.get(url, timeout=60)
            if resp.status_code == 200:
                combined_data.append(resp.content)
            else:
                print(f"  -> Error {resp.status_code} for {ts}")

        if not combined_data:
            print("No data slices were found.")
            return

        process_stack(combined_data)
            
    except Exception as e:
        print(f"Error: {e}")

def process_stack(data_contents):
    with Dataset("memory", memory=data_contents[0]) as ds:
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        stack = np.full((len(data_contents), len(lats), len(lons)), np.nan)
        
    for idx, content in enumerate(data_contents):
        with Dataset("memory", memory=content) as ds:
            # L3S uses standard variable names
            stack[idx, :, :] = ds.variables['sea_surface_temperature'][0, :, :]

    # 3-Day Average to eliminate cloud artifacts
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
    print(f"Success! Created 2km cloud-filtered map with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
