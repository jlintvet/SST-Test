import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# VA Beach / Hatteras Box
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.8, -73.0

# The high-fidelity VIIRS ID that provided your 35k points
DATASET_ID = "noaacwVIIRSj01SSTDaily3P"

def fetch_and_convert():
    print(f"Connecting to High-Res Dataset: {DATASET_ID}")
    
    # Step 1: Discover the last 3 available time slices to avoid 404s
    info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.json?time"
    
    try:
        info_resp = requests.get(info_url, timeout=30)
        if info_resp.status_code != 200:
            print("Server metadata busy. Cannot fetch high-res timestamps.")
            return
            
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
            resp = requests.get(url, timeout=90)
            if resp.status_code == 200:
                combined_data.append(resp.content)
            else:
                print(f"  -> Error {resp.status_code} for {ts}")

        if not combined_data:
            print("No high-res data found.")
            return

        process_stack(combined_data)
            
    except Exception as e:
        print(f"Error: {e}")

def process_stack(data_contents):
    # Use the first slice to get the coordinate grid and unpacking attributes
    with Dataset("memory", memory=data_contents[0]) as ds:
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        # Access attributes for VIIRS unpacking (Scale: 0.01, Offset: 273.15)
        var = ds.variables['sea_surface_temperature']
        scale = getattr(var, 'scale_factor', 0.01)
        offset = getattr(var, 'add_offset', 273.15)
        
        # Create 3D stack: [Time, Lat, Lon]
        stack = np.full((len(data_contents), len(lats), len(lons)), np.nan)
        
    # Fill stack with unpacked data
    for idx, content in enumerate(data_contents):
        with Dataset("memory", memory=content) as ds:
            # Squeeze to ensure we have a 2D [Lat, Lon] array
            raw_val = np.squeeze(ds.variables['sea_surface_temperature'][:])
            stack[idx, :, :] = (raw_val * scale) + offset

    # Calculate 3-Day Average in Kelvin, ignoring cloud NaNs
    with np.errstate(all='ignore'):
        sst_avg_k = np.nanmean(stack, axis=0)

    features = []
    # Loop through every single pixel (no more range steps)
    for i in range(len(lats)):
        for j in range(len(lons)):
            val_k = sst_avg_k[i, j]
            
            # Filter valid ocean water (approx 40F to 90F in Kelvin)
            if np.isfinite(val_k) and val_k > 275:
                # CONVERSION: Kelvin to Fahrenheit
                temp_f = (val_k - 273.15) * 9/5 + 32
                
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                    "properties": {"temp_f": round(temp_f, 1)}
                })

    output = {"type": "FeatureCollection", "features": features}
    
    with open("sst_data.json", "w") as f:
        json.dump(output, f, allow_nan=False)
    print(f"Success! High-Density average created with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
