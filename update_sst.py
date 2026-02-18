import numpy as np
import requests
import json
from netCDF4 import Dataset
import os
from datetime import datetime, timedelta

# VA Beach / Hatteras Box
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.8, -73.0

# Using the 1km MUR Dataset - The gold standard for stability
DATASET_ID = "jplMURSST41"

def fetch_and_convert():
    print(f"Fetching 3-day high-res window for {DATASET_ID}...")
    
    # MUR is updated daily; we grab the last 3 days to fill clouds
    # We use explicit date indices which ERDDAP handles better than 'latest'
    url = (
        f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
        f"analysed_sst[(latest-2):(latest)][({LAT_MIN}):({LAT_MAX})][({LON_MIN}):({LON_MAX})]"
    )
    
    print(f"Requesting URL: {url}")
    
    try:
        response = requests.get(url, timeout=180)
        if response.status_code == 200:
            process_data(response.content)
        else:
            print(f"Server Error {response.status_code}: {response.text[:100]}")
    except Exception as e:
        print(f"Connection failed: {e}")

def process_data(content):
    with Dataset("memory", memory=content) as ds:
        # stack is [Time, Lat, Lon]
        sst_stack = ds.variables['analysed_sst'][:, :, :]
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        # Average across the 3 days to eliminate cloud gaps
        with np.errstate(all='ignore'):
            sst_avg_k = np.nanmean(sst_stack, axis=0)
        
        features = []
        # No skipping (Full 35,000+ point density)
        for i in range(len(lats)): 
            for j in range(len(lons)):
                val_k = sst_avg_k[i, j]
                
                if np.isfinite(val_k) and val_k > 270:
                    # CONVERSION: Kelvin to Fahrenheit
                    temp_f = (float(val_k) - 273.15) * 1.8 + 32
                    
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
