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
        info_resp = requests.get(info_url, timeout=30)
        latest_times = [f"({r[0]})" for r in info_resp.json()['table']['rows'][-3:]]
        
        combined_data = []
        for ts in latest_times:
            url = (f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
                   f"sea_surface_temperature[{ts}][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
            resp = requests.get(url, timeout=60)
            if resp.status_code == 200:
                combined_data.append(resp.content)

        if combined_data:
            process_stack(combined_data)
    except Exception as e:
        print(f"Error: {e}")

def process_stack(data_contents):
    with Dataset("memory", memory=data_contents[0]) as ds:
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        # Access the variable attributes for manual unpacking
        var = ds.variables['sea_surface_temperature']
        # Try to get attributes, default to 1 and 0 if not present
        scale = getattr(var, 'scale_factor', 1.0)
        offset = getattr(var, 'add_offset', 0.0)
        
        stack = np.full((len(data_contents), len(lats), len(lons)), np.nan)
        
    for idx, content in enumerate(data_contents):
        with Dataset("memory", memory=content) as ds:
            raw_val = ds.variables['sea_surface_temperature'][0, :, :]
            # MANUAL UNPACKING: Real_Value = (Raw * Scale) + Offset
            stack[idx, :, :] = (raw_val * scale) + offset

    # 3-Day Average (already unpacked into Celsius)
    with np.errstate(all='ignore'):
        sst_avg_c = np.nanmean(stack, axis=0)

    features = []
    for i in range(len(lats)):
        for j in range(len(lons)):
            val_c = sst_avg_c[i, j]
            if np.isfinite(val_c) and val_c > -5: # Filter out errors
                # Convert Celsius to Fahrenheit
                temp_f = (val_c * 9/5) + 32
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                    "properties": {"temp_f": round(temp_f, 1)}
                })

    output = {"type": "FeatureCollection", "features": features}
    with open("sst_data.json", "w") as f:
        json.dump(output, f, allow_nan=False)
    print(f"Success! Unpacked high-res average created with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
