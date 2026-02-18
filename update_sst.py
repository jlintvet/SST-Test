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
    # Step 1: Discover available timestamps to avoid 404s
    info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.json?time"
    print("Finding the latest high-res satellite passes...")
    
    try:
        info_resp = requests.get(info_url, timeout=30)
        if info_resp.status_code != 200:
            print("Server busy. Cannot find time indices.")
            return
            
        rows = info_resp.json()['table']['rows']
        # Take the last 3 successful passes for the 3-day average
        latest_times = [f"({r[0]})" for r in rows[-3:]]
        print(f"Averaging timestamps: {latest_times}")

        combined_data = []
        for ts in latest_times:
            url = (f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
                   f"sea_surface_temperature[{ts}][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
            print(f"Requesting pass: {ts}")
            resp = requests.get(url, timeout=90)
            if resp.status_code == 200:
                combined_data.append(resp.content)

        if combined_data:
            process_stack(combined_data)
            
    except Exception as e:
        print(f"Error: {e}")

def process_stack(data_contents):
    # netCDF4 automatically applies scale/offset (unpacks) VIIRS data
    with Dataset("memory", memory=data_contents[0]) as ds:
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        # Create 3D stack: [Time, Lat, Lon]
        stack = np.full((len(data_contents), len(lats), len(lons)), np.nan)
        
    for idx, content in enumerate(data_contents):
        with Dataset("memory", memory=content) as ds:
            # VIIRS data is Kelvin
            stack[idx, :, :] = np.squeeze(ds.variables['sea_surface_temperature'][:])

    # 3-Day Average to fill cloud gaps
    with np.errstate(all='ignore'):
        sst_avg_k = np.nanmean(stack, axis=0)

    features = []
    # Loop through every single pixel (Full 35k+ Density)
    for i in range(len(lats)):
        for j in range(len(lons)):
            val_k = sst_avg_k[i, j]
            
            # Filter valid sea water (> 32°F in Kelvin)
            if np.isfinite(val_k) and val_k > 273.15:
                # CONVERSION: Kelvin to Fahrenheit
                # Formula: (K - 273.15) * 1.8 + 32
                temp_f = (val_k - 273.15) * 1.8 + 32
                
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                    "properties": {"temp_f": round(temp_f, 1)}
                })

    output = {"type": "FeatureCollection", "features": features}
    with open("sst_data.json", "w") as f:
        json.dump(output, f, allow_nan=False)
    print(f"Success! Restored 750m density with {len(features)} points in Fahrenheit.")

if __name__ == "__main__":
    fetch_and_convert()
