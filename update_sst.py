import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# VA Beach / Hatteras Fishing Box
# Simplified constraints to avoid 400 errors
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0

# Stabilized NOAA L3S ID
DATASET_ID = "noaacwLEOACSPOSSTL3SnrtCDaily"

def fetch_and_convert():
    print(f"Connecting to NOAA L3S: {DATASET_ID}")
    
    # Constructing the URL with explicit coordinate constraints
    # Added [(latest)] for time and used the standard ERDDAP [(min):(max)] syntax
    url = (
        f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
        f"sea_surface_temperature[(latest)][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]"
    )
    
    print(f"Requesting URL: {url}")
    
    try:
        response = requests.get(url, timeout=120)
        
        if response.status_code == 200:
            print("Successfully connected. Processing data...")
            process_data(response.content)
        else:
            # If 400, the server likely dislikes the specific coordinate bracket
            print(f"Server returned {response.status_code}. Response body: {response.text}")
            
    except Exception as e:
        print(f"Network error: {e}")

def process_data(content):
    with Dataset("memory", memory=content) as ds:
        # Note: L3S datasets often have a 'time' dimension we must index
        sst_raw = ds.variables['sea_surface_temperature'][0, :, :]
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        features = []
        for i in range(len(lats)): 
            for j in range(len(lons)):
                val = sst_raw[i, j]
                if not np.isnan(val):
                    # Convert Kelvin to Fahrenheit
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
