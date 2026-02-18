import numpy as np
import requests
import json
from netCDF4 import Dataset
import os
from datetime import datetime, timedelta

# VA Beach / Hatteras Box
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.8, -73.0

# The high-fidelity VIIRS NOAA-20 ID - standard for East Coast
DATASET_ID = "noaacwVIIRSj01SSTDaily3P"

def fetch_and_convert():
    print(f"Initiating High-Res 3-Day Average for {DATASET_ID}...")
    
    # Step 1: Generate the last 3 days of timestamps (e.g., 2026-02-17T12:00:00Z)
    # Most NOAA Daily products are indexed at 12:00:00Z
    now = datetime.utcnow()
    timestamps = []
    for i in range(1, 4):  # Try yesterday, day before, and 3 days ago
        day = now - timedelta(days=i)
        timestamps.append(day.strftime('%Y-%m-%dT12:00:00Z'))

    combined_data = []
    
    # Step 2: Fetch each day explicitly
    for ts in timestamps:
        url = (
            f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
            f"sea_surface_temperature[({ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]"
        )
        print(f"Attempting: {ts}")
        
        try:
            response = requests.get(url, timeout=45)
            if response.status_code == 200:
                print(f"  -> Found data for {ts}")
                combined_data.append(response.content)
            else:
                print(f"  -> No data for {ts} (Status {response.status_code})")
        except Exception as e:
            print(f"  -> Connection error on {ts}")

    if not combined_data:
        print("CRITICAL: No high-res data found in the 3-day window.")
        return

    process_stack(combined_data)

def process_stack(data_contents):
    # Use the first available pass to define the grid
    with Dataset("memory", memory=data_contents[0]) as ds:
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        # Create 3D stack: [Number of days found, Lats, Lons]
        stack = np.full((len(data_contents), len(lats), len(lons)), np.nan)
        
    for idx, content in enumerate(data_contents):
        with Dataset("memory", memory=content) as ds:
            # Squeeze to ensure we only have [Lat, Lon]
            val = np.squeeze(ds.variables['sea_surface_temperature'][:])
            stack[idx, :, :] = val

    # Calculate 3-day nanmean to fill cloud gaps
    with np.errstate(all='ignore'):
        sst_avg = np.nanmean(stack, axis=0)

    features = []
    for i in range(len(lats)):
        for j in range(len(lons)):
            val = sst_avg[i, j]
            if np.isfinite(val) and val > 270: # Ensure it's not a land mask value
                temp_f = (float(val) - 273.15) * 9/5 + 32
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                    "properties": {"temp_f": round(temp_f, 1)}
                })

    output = {"type": "FeatureCollection", "features": features}
    with open("sst_data.json", "w") as f:
        json.dump(output, f, allow_nan=False)
    print(f"Success! High-res average created with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
