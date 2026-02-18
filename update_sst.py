import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# VA Beach / Hatteras Box
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.8, -73.0

# The High-Fidelity 750m/1km Dataset ID
DATASET_ID = "noaacwVIIRSj01SSTDaily3P"

def fetch_and_convert():
    # Step 1: Discover available timestamps to avoid 404s
    info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.json?time"
    print("Checking available high-res timestamps...")
    
    try:
        info_resp = requests.get(info_url, timeout=30)
        if info_resp.status_code != 200:
            print("Server metadata busy. Cannot find time indices.")
            return
            
        rows = info_resp.json()['table']['rows']
        latest_times = [f"({r[0]})" for r in rows[-3:]] # Take last 3 successful passes
        print(f"Averaging: {latest_times}")

        combined_data = []
        for ts in latest_times:
            # High-res VIIRS data request
            url = (f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
                   f"sea_surface_temperature[{ts}][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
            resp = requests.get(url, timeout=90)
            if resp.status_code == 200:
                combined_data.append(resp.content)
            else:
                print(f"  -> Pass skipped: {ts} (Status {resp.status_code})")

        if combined_data:
            process_stack(combined_data)
            
    except Exception as e:
        print(f"Error during fetch: {e}")

def process_stack(data_contents):
    # netCDF4 automatically applies scale_factor and add_offset to VIIRS data
    with Dataset("memory", memory=data_contents[0]) as ds:
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        stack = np.full((len(data_contents), len(lats), len(lons)), np.nan)
        
    for idx, content in enumerate(data_contents):
        with Dataset("memory", memory=content) as ds:
            # VIIRS is natively in Kelvin
            stack[idx, :, :] = np.squeeze(ds.variables['sea_surface_temperature'][:])

    # 3-Day Average to fill in cloud gaps
    with np.errstate(all='ignore'):
        sst_avg_k = np.nanmean(stack, axis=0)

    features = []
    # No pixel skipping (every point included)
    for i in range(len(lats)):
        for j in range(len(lons)):
            val_k = sst_avg_k[i, j]
            
            # Filter valid water temps (> 32F in Kelvin)
            if np.isfinite(val_k) and val_k > 273.15:
                # CONVERSION: Kelvin to Fahrenheit
                # (K - 273.15) * 1.8 + 32
                temp_f = (val_k - 273.15) * 1.8 + 32
                
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                    "properties": {"temp_f": round(temp_f, 1)}
                })

    output = {"type": "FeatureCollection", "features": features}
    with open("sst_data.json", "w") as f:
        json.dump(output, f, allow_nan=False)
    print(f"Success! Restored high-density map with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
