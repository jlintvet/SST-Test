import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Coordinates for VA Beach / Hatteras
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0

DATASET_ID = "noaacwLEOACSPOSSTL3SnrtCDaily"

def fetch_and_convert():
    # Step 1: Get the latest time index
    # We request the 'time' variable metadata to see the last available index
    info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.json?time"
    print(f"Checking latest time at: {info_url}")
    
    try:
        info_resp = requests.get(info_url, timeout=30)
        if info_resp.status_code != 200:
            print("Could not fetch time metadata. Server might be down.")
            return
        
        # Parse the JSON to find the last time index
        time_data = info_resp.json()
        # In ERDDAP JSON, data is in ['table']['rows']
        # We take the last row [-1], which is the most recent time
        latest_time_str = time_data['table']['rows'][-1][0]
        print(f"Latest timestamp found: {latest_time_str}")

        # Step 2: Fetch the SST data using the explicit timestamp
        # Using [({latest_time_str})] ensures the server knows exactly which slice we want
        data_url = (
            f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
            f"sea_surface_temperature[({latest_time_str})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]"
        )
        
        print(f"Requesting Data: {data_url}")
        response = requests.get(data_url, timeout=120)
        
        if response.status_code == 200:
            process_data(response.content)
        else:
            print(f"Data fetch failed: {response.status_code}")
            print(response.text)
            
    except Exception as e:
        print(f"Error: {e}")

def process_data(content):
    with Dataset("memory", memory=content) as ds:
        # Index [0] is the single time slice we requested
        sst_raw = ds.variables['sea_surface_temperature'][0, :, :]
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        features = []
        for i in range(len(lats)): 
            for j in range(len(lons)):
                val = sst_raw[i, j]
                if not np.isnan(val):
                    temp_f = (float(val) - 273.15) * 9/5 + 32
                    features.append({
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                        "properties": {"temp_f": round(temp_f, 1)}
                    })
        
        output = {
            "type": "FeatureCollection",
            "metadata": {"generated": str(np.datetime64('now')), "source": DATASET_ID},
            "features": features
        }
        
        with open("sst_data.json", "w") as f:
            json.dump(output, f)
            
        print(f"Success! Created sst_data.json with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
