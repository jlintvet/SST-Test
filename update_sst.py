import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# VA Beach / Hatteras Box
LAT_MIN, LAT_MAX = 34.0, 37.0
LON_MIN, LON_MAX = -76.8, -74.0

# Official NOAA Blended L4 Dataset (Very stable)
DATASET_ID = "noaacwACSPOSSTL4Daily"

def fetch_and_convert():
    print(f"Fetching 3-day window from stabilized NOAA Dataset: {DATASET_ID}...")
    
    # Using explicit index [(latest-2):(latest)] for the 3-day stack
    # Variable name for this dataset is 'analysed_sst'
    url = (
        f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
        f"analysed_sst[(latest-2):(latest)][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]"
    )
    
    print(f"Requesting URL: {url}")
    
    try:
        response = requests.get(url, timeout=180)
        
        if response.status_code == 200:
            print("Successfully connected. Processing 3-day stack...")
            process_data(response.content)
        else:
            print(f"Server returned error {response.status_code}")
            print(f"Details: {response.text[:200]}")
            
    except Exception as e:
        print(f"Network error: {e}")

def process_data(content):
    with Dataset("memory", memory=content) as ds:
        # Pull the stack: [Time, Lat, Lon]
        sst_stack = ds.variables['analysed_sst'][:, :, :]
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        # Average the 3 days to eliminate transient cloud artifacts
        with np.errstate(all='ignore'):
            sst_avg = np.nanmean(sst_stack, axis=0)
        
        features = []
        for i in range(len(lats)): 
            for j in range(len(lons)):
                val = sst_avg[i, j]
                if np.isfinite(val):
                    # Convert Kelvin to Fahrenheit
                    temp_f = (float(val) - 273.15) * 9/5 + 32
                    features.append({
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                        "properties": {"temp_f": round(temp_f, 1)}
                    })
        
        output = {
            "type": "FeatureCollection",
            "metadata": {"source": DATASET_ID, "method": "3-Day Average"},
            "features": features
        }
        
        file_path = os.path.join(os.getcwd(), "sst_data.json")
        with open(file_path, "w") as f:
            json.dump(output, f, allow_nan=False)
            
        print(f"Success! Created cloud-filtered JSON with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
