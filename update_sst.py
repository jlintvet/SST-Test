import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# The high-fidelity bounding box for Oregon Inlet / Hatteras
LAT_RANGE = "[(37.5):(34.0)]"
LON_RANGE = "[(-76.5):(-73.0)]"
DATASET_ID = "noaacwVIIRSj01SSTDaily3P" # NOAA-20 Satellite

def fetch_and_convert():
    # Try the last 2 passes in case the most recent is still processing (404)
    time_queries = ["[(latest)]", "[(-2)]"]
    
    for time_query in time_queries:
        URL = f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?sea_surface_temperature{time_query}{LAT_RANGE}{LON_RANGE}"
        print(f"Trying NOAA URL: {URL}")
        
        try:
            response = requests.get(URL, timeout=45)
            if response.status_code == 200:
                print("Connection Successful!")
                process_data(response.content)
                return # Exit once we have data
            else:
                print(f"Pass failed with status {response.status_code}. Trying next...")
        except Exception as e:
            print(f"Connection error: {e}")
            
    print("All attempts failed. NOAA server may be down or data is unavailable.")

def process_data(content):
    with Dataset("memory", memory=content) as ds:
        sst = ds.variables['sea_surface_temperature'][0, :, :]
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        features = []
        for i in range(0, len(lats), 2): 
            for j in range(0, len(lons), 2):
                val = sst[i, j]
                if not np.isnan(val):
                    temp_f = (float(val) - 273.15) * 9/5 + 32
                    features.append({
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                        "properties": {"temp_f": round(temp_f, 2)}
                    })
        
        output = {"type": "FeatureCollection", "features": features}
        with open("sst_data.json", "w") as f:
            json.dump(output, f)
        print(f"Success! Saved sst_data.json with {len(features)} points.")

if __name__ == "__main__":
    fetch_and_convert()
