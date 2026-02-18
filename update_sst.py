import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Coordinates for VA Beach / Hatteras Box
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0
DATASET_ID = "jplMURSST41" 

def fetch_and_convert():
    # Directly requesting 'latest' avoids the JSON metadata crash
    print(f"Requesting latest high-res pass for {DATASET_ID}...")
    
    data_url = (
        f"https://coastwatch.noaa.gov/erddap/griddap/{DATASET_ID}.nc?"
        f"analysed_sst[(latest)][({LAT_MIN}):({LAT_MAX})][({LON_MIN}):({LON_MAX})]"
    )
    
    try:
        # We increase timeout because pulling 35,000+ points in one go is heavy
        response = requests.get(data_url, timeout=150)
        
        if response.status_code == 200:
            print("Data received. Processing high-density grid...")
            process_data(response.content)
        else:
            print(f"Server refused request. Status: {response.status_code}")
            print("The NOAA node might be down. Try again in 10 minutes.")
            
    except Exception as e:
        print(f"Connection error: {e}")

def process_data(content):
    with Dataset("memory", memory=content) as ds:
        # Squeeze removes the empty 'time' dimension
        sst_raw = np.squeeze(ds.variables['analysed_sst'][:])
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        features = []
        # FULL DENSITY: range(len()) captures every single pixel
        for i in range(len(lats)): 
            for j in range(len(lons)):
                val = sst_raw[i, j]
                
                if np.isfinite(val) and val > 270:
                    # KELVIN TO FAHRENHEIT
                    temp_f = (float(val) - 273.15) * 1.8 + 32
                    
                    features.append({
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                        "properties": {"temp_f": round(temp_f, 1)}
                    })
        
        output = {"type": "FeatureCollection", "features": features}
        
        # Save to the root directory for your map to find
        file_path = "sst_data.json"
        with open(file_path, "w") as f:
            json.dump(output, f, allow_nan=False)
            
        print(f"Success! Map updated with {len(features)} points in Fahrenheit.")

if __name__ == "__main__":
    fetch_and_convert()
