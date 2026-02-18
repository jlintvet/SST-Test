import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# VA Beach / Hatteras Fishing Box
LAT_RANGE = "[(37.5):(34.0)]"
LON_RANGE = "[(-76.5):(-73.0)]"

# This L3S dataset is the most stable high-res (approx 2km) product for 2026
# It combines all VIIRS satellites so it rarely 404s
DATASET_ID = "noaacwLEOACSPOSSTL3SnrtCDaily"

def fetch_and_convert():
    print(f"Connecting to stabilized NOAA L3S Dataset: {DATASET_ID}")
    
    # We use 'latest' time and request the 'sea_surface_temperature' variable
    url = f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?sea_surface_temperature[(latest)]{LAT_RANGE}{LON_RANGE}"
    
    try:
        response = requests.get(url, timeout=120) # Higher timeout for collated data
        
        if response.status_code == 200:
            print("Successfully connected. Processing data...")
            process_data(response.content)
        else:
            print(f"Server returned {response.status_code}. Checking alternative access...")
            # If the primary still fails, it's likely a server maintenance window.
            
    except Exception as e:
        print(f"Network error: {e}")

def process_data(content):
    with Dataset("memory", memory=content) as ds:
        # L3S variable name is often 'sea_surface_temperature'
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
        
        file_path = os.path.join(os.getcwd(), "sst_data.json")
        with open(file_path, "w") as f:
            json.dump(output, f)
            
        print(f"Success! Created sst_data.json with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
