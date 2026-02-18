import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Coordinates for VA Beach / Hatteras
# Slicing slightly wider to ensure we catch the Gulf Stream edge
URL = "https://coastwatch.noaa.gov/erddap/griddap/noaacwVIIRSnppSSTDaily3U.nc?sea_surface_temperature[(latest)][(37.5):(34.0)][(-76.5):(-73.0)]"

def fetch_and_convert():
    print("Fetching data from NOAA...")
    response = requests.get(URL)
    if response.status_status != 200:
        print(f"Error: {response.status_code}")
        return

    # Open from memory
    with Dataset("memory", memory=response.content) as ds:
        sst = ds.variables['sea_surface_temperature'][0, :, :]
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        # Create a simple JSON structure
        # We only export points that aren't masked by clouds
        features = []
        for i in range(0, len(lats), 5): # Step by 5 to keep file size manageable
            for j in range(0, len(lons), 5):
                val = sst[i, j]
                if not np.isnan(val):
                    features.append({
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                        "properties": {"temp_c": round(float(val), 2)}
                    })
        
        geojson = {"type": "FeatureCollection", "features": features}
        
        with open("sst_data.json", "w") as f:
            json.dump(geojson, f)
    print("Successfully created sst_data.json")

if __name__ == "__main__":
    fetch_and_convert()
