import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# High-fidelity coordinates for VA Beach/Hatteras
URL = "https://coastwatch.noaa.gov/erddap/griddap/noaacwVIIRSnppSSTDaily3U.nc?sea_surface_temperature[(latest)][(37.5):(34.0)][(-76.5):(-73.0)]"

def fetch_and_convert():
    print("Fetching NOAA VIIRS 750m Data...")
    try:
        response = requests.get(URL, timeout=60)
        
        # Fixed: Correct attribute is status_code
        if response.status_code != 200:
            print(f"Server Error: {response.status_code}")
            return

        with Dataset("memory", memory=response.content) as ds:
            sst = ds.variables['sea_surface_temperature'][0, :, :]
            lats = ds.variables['latitude'][:]
            lons = ds.variables['longitude'][:]
            
            features = []
            # Step 2 maintains ~1.5km resolution
            for i in range(0, len(lats), 2): 
                for j in range(0, len(lons), 2):
                    val = sst[i, j]
                    if not np.isnan(val):
                        temp_c = float(val) - 273.15
                        temp_f = (temp_c * 9/5) + 32
                        features.append({
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                            "properties": {"temp_f": round(temp_f, 2)}
                        })
            
            output = {"type": "FeatureCollection", "features": features}
            
            with open("sst_data.json", "w") as f:
                json.dump(output, f)
                
            print(f"Success! Created sst_data.json with {len(features)} points.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_and_convert()
