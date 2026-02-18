import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Coordinates for VA Beach / Hatteras Box
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0

# The most stable High-Res (2km) dataset on the NOAA East Coast Node
DATASET_ID = "noaacwLEOACSPOSSTL3SnrtCDaily"

def fetch_and_convert():
    # Direct 'latest' call. Note: Lat order must be (MAX):(MIN)
    url = (
        f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
        f"sea_surface_temperature[(latest)][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]"
    )
    
    print(f"Requesting latest stable high-res pass...")
    try:
        response = requests.get(url, timeout=120)
        
        if response.status_code == 200:
            process_data(response.content)
        else:
            print(f"Server returned {response.status_code}. The node might be busy.")
            
    except Exception as e:
        print(f"Connection error: {e}")

def process_data(content):
    with Dataset("memory", memory=content) as ds:
        # Squeeze to get 2D [Lat, Lon]
        raw_val = np.squeeze(ds.variables['sea_surface_temperature'][:])
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        features = []
        # FULL DENSITY: Every single pixel processed (No range steps)
        for i in range(len(lats)): 
            for j in range(len(lons)):
                val_c = raw_val[i, j]
                
                # Filter valid sea water (L3S is natively in Celsius)
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
