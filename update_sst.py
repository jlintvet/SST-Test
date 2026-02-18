import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# VA Beach / Hatteras Box
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.8, -73.0

# Using the VIIRS 750m High-Res Dataset
DATASET_ID = "noaacwVIIRSj01SSTDaily3P"

def fetch_and_convert():
    print(f"Requesting data directly using relative indices (bypassing busy metadata)...")
    
    combined_data = []
    # We try indices 0, 1, and 2 (the 3 most recent successful passes)
    for i in range(3):
        # Using relative indexing (latest-N) is more robust than date strings
        ts = f"(latest-{i})"
        url = (f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
               f"sea_surface_temperature[{ts}][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
        
        print(f"Attempting pass: {ts}")
        try:
            resp = requests.get(url, timeout=120) # Increased timeout for high-res data
            if resp.status_code == 200:
                print(f"  -> Success for {ts}")
                combined_data.append(resp.content)
            else:
                print(f"  -> {ts} not available (Status {resp.status_code})")
        except Exception as e:
            print(f"  -> Connection error for {ts}: {e}")

    if combined_data:
        process_stack(combined_data)
    else:
        print("CRITICAL: No data could be retrieved from the server.")

def process_stack(data_contents):
    # netCDF4 handles the internal unpacking (scale/offset)
    with Dataset("memory", memory=data_contents[0]) as ds:
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        stack = np.full((len(data_contents), len(lats), len(lons)), np.nan)
        
    for idx, content in enumerate(data_contents):
        with Dataset("memory", memory=content) as ds:
            # VIIRS is Kelvin
            stack[idx, :, :] = np.squeeze(ds.variables['sea_surface_temperature'][:])

    # 3-Day Average to eliminate cloud gaps
    with np.errstate(all='ignore'):
        sst_avg_k = np.nanmean(stack, axis=0)

    features = []
    # FULL DENSITY: 35,000+ points
    for i in range(len(lats)):
        for j in range(len(lons)):
            val_k = sst_avg_k[i, j]
            if np.isfinite(val_k) and val_k > 270:
                # KELVIN TO FAHRENHEIT
                temp_f = (val_k - 273.15) * 1.8 + 32
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                    "properties": {"temp_f": round(temp_f, 1)}
                })

    output = {"type": "FeatureCollection", "features": features}
    with open("sst_data.json", "w") as f:
        json.dump(output, f, allow_nan=False)
    print(f"Success! High-Res map created with {len(features)} points in Fahrenheit.")

if __name__ == "__main__":
    fetch_and_convert()
