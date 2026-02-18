import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# VA Beach / Hatteras Box
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.8, -73.0
DATASET_ID = "noaacwLEOACSPOSSTL3SnrtCDaily"

def fetch_and_convert():
    info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.json?time"
    try:
        # Step 1: Discover the last 3 available time slices
        info_resp = requests.get(info_url, timeout=30)
        latest_times = [f"({r[0]})" for r in info_resp.json()['table']['rows'][-3:]]
        
        combined_data = []
        for ts in latest_times:
            url = (f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
                   f"sea_surface_temperature[{ts}][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
            print(f"Requesting: {ts}")
            resp = requests.get(url, timeout=60)
            if resp.status_code == 200:
                combined_data.append(resp.content)

        if combined_data:
            process_stack(combined_data)
        else:
            print("No data found for the requested times.")
    except Exception as e:
        print(f"Error: {e}")

def process_stack(data_contents):
    # Use the first slice to get the coordinate grid and unpacking attributes
    with Dataset("memory", memory=data_contents[0]) as ds:
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        # Access attributes to unpack raw integers into real Celsius values
        var = ds.variables['sea_surface_temperature']
        scale = getattr(var, 'scale_factor', 1.0)
        offset = getattr(var, 'add_offset', 0.0)
        
        stack = np.full((len(data_contents), len(lats), len(lons)), np.nan)
        
    # Fill stack with unpacked Celsius data
    for idx, content in enumerate(data_contents):
        with Dataset("memory", memory=content) as ds:
            raw_val = ds.variables['sea_surface_temperature'][0, :, :]
            # Formula: Real Value = (Raw * Scale) + Offset
            stack[idx, :, :] = (raw_val * scale) + offset

    # Calculate 3-Day Average in Celsius
    with np.errstate(all='ignore'):
        sst_avg_c = np.nanmean(stack, axis=0)

    features = []
    for i in range(len(lats)):
        for j in range(len(lons)):
            val_c = sst_avg_c[i, j]
            
            # Filter: Check if it's a valid number and above freezing (prevents land errors)
            if np.isfinite(val_c) and val_c > -2:
                # CONVERSION: Celsius to Fahrenheit
                temp_f = (val_c * 9/5) + 32
                
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                    "properties": {"temp_f": round(temp_f, 1)}
                })

    output = {"type": "FeatureCollection", "features": features}
    
    with open("sst_data.json", "w") as f:
        json.dump(output, f, allow_nan=False)
    print(f"Success! Unpacked data in Fahrenheit with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
