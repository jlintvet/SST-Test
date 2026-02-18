import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Coordinates for VA Beach / Hatteras Box
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0

# The standard High-Res 750m IDs for East Coast
DATASET_ID = "noaacwVIIRSj01SSTDaily3P"

def fetch_and_convert():
    # Direct 'latest' call to avoid metadata crashes
    # Note: Latitudes must be (MAX):(MIN) for ERDDAP descending order
    url = (
        f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
        f"sea_surface_temperature[(latest)][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]"
    )
    
    print(f"Requesting latest high-res pass...")
    try:
        response = requests.get(url, timeout=120)
        
        # Fallback logic: if j01 is down, try NPP
        if response.status_code == 404:
            print("Primary high-res node busy. Trying Suomi-NPP fallback...")
            url = url.replace("j01", "npp")
            response = requests.get(url, timeout=120)

        if response.status_code == 200:
            process_data(response.content)
        else:
            print(f"Both high-res nodes returned {response.status_code}. Server may be undergoing maintenance.")
            
    except Exception as e:
        print(f"Connection error: {e}")

def process_data(content):
    with Dataset("memory", memory=content) as ds:
        # Squeeze removes the extra 'time' dimension
        raw_val = np.squeeze(ds.variables['sea_surface_temperature'][:])
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        # Get unpacking attributes (VIIRS is stored as packed integers)
        var = ds.variables['sea_surface_temperature']
        scale = getattr(var, 'scale_factor', 0.01)
        offset = getattr(var, 'add_offset', 273.15)
        
        features = []
        # FULL DENSITY: Every pixel processed (replaces step=3)
        for i in range(len(lats)): 
            for j in range(len(lons)):
                val = raw_val[i, j]
                
                # Check for standard fill values
                if np.isfinite(val) and val > -30000:
                    # 1. Unpack to Kelvin: (raw * scale) + offset
                    temp_k = (val * scale) + offset
                    # 2. Convert to Fahrenheit
                    temp_f = (temp_k - 273.15) * 1.8 + 32
                    
                    # Ocean-only filter (ignores land-mask artifacts)
                    if 35 < temp_f < 95:
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
