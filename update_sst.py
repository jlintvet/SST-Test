import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Coordinates for VA Beach / Hatteras Box
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0

# The stable L3S dataset
DATASET_ID = "noaacwLEOACSPOSSTL3SnrtCDaily"

def fetch_and_convert():
    # Step 1: Discover the actual timestamp for "latest"
    print(f"Discovering latest timestamp for {DATASET_ID}...")
    info_url = f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.json?time"
    
    try:
        info_resp = requests.get(info_url, timeout=30)
        if info_resp.status_code != 200:
            print("Failed to get timestamp metadata.")
            return
        
        # Extract the very last timestamp string from the metadata
        latest_ts = info_resp.json()['table']['rows'][-1][0]
        print(f"Targeting specific pass: {latest_ts}")

        # Step 2: Request the data using the explicit timestamp
        # Most CoastWatch nodes prefer [Time][North:South][West:East]
        url = (
            f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
            f"sea_surface_temperature[({latest_ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]"
        )
        
        print("Requesting high-density data...")
        response = requests.get(url, timeout=120)
        
        if response.status_code == 200:
            process_data(response.content)
        else:
            print(f"Direct request failed (Status {response.status_code}). Trying inverted latitude...")
            # Fallback: South to North
            url_alt = url.replace(f"({LAT_MAX}):({LAT_MIN})", f"({LAT_MIN}):({LAT_MAX})")
            response = requests.get(url_alt, timeout=120)
            if response.status_code == 200:
                process_data(response.content)
            else:
                print(f"Final attempt failed. Server Message: {response.text[:100]}")
            
    except Exception as e:
        print(f"Execution error: {e}")

def process_data(content):
    with Dataset("memory", memory=content) as ds:
        # Squeeze to ensure 2D [Lat, Lon]
        raw_val = np.squeeze(ds.variables['sea_surface_temperature'][:])
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        features = []
        # FULL DENSITY: Every single pixel processed for 35k+ points
        for i in range(len(lats)): 
            for j in range(len(lons)):
                val_c = raw_val[i, j]
                
                # Check for land mask / missing data
                if np.isfinite(val_c) and val_c > -5:
                    # CONVERSION: Celsius to Fahrenheit
                    temp_f = (float(val_c) * 1.8) + 32
                    
                    features.append({
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                        "properties": {"temp_f": round(temp_f, 1)}
                    })
        
        output = {"type": "FeatureCollection", "features": features}
        with open("sst_data.json", "w") as f:
            json.dump(output, f, allow_nan=False)
            
        print(f"Success! High-density Fahrenheit JSON created with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
