import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Coordinates for VA Beach / Hatteras corridor
# We use 'latest' to always get the most recent satellite pass
URL = "https://coastwatch.noaa.gov/erddap/griddap/noaacwVIIRSnppSSTDaily3U.nc?sea_surface_temperature[(latest)][(37.5):(34.0)][(-76.5):(-73.0)]"

def fetch_and_convert():
    print("Fetching high-fidelity VIIRS data from NOAA...")
    try:
        # Added timeout=60 because NOAA servers can be slow with large NetCDF slices
        response = requests.get(URL, timeout=60)
        
        # Check if the connection was successful
        if response.status_code != 200:
            print(f"NOAA Server Error: {response.status_code}")
            return

        # Open the NetCDF data directly from the memory buffer
        with Dataset("memory", memory=response.content) as ds:
            # Extract variables
            sst = ds.variables['sea_surface_temperature'][0, :, :]
            lats = ds.variables['latitude'][:]
            lons = ds.variables['longitude'][:]
            
            features = []
            # We step by 2 to balance high-fidelity with file size
            # Every 2nd pixel at 750m resolution is still extremely sharp (~1.5km)
            for i in range(0, len(lats), 2): 
                for j in range(0, len(lons), 2):
                    val = sst[i, j]
                    # Only include data points that aren't masked by clouds (NaN)
                    if not np.isnan(val):
                        # Convert Kelvin to Celsius (NOAA sends Kelvin by default)
                        temp_c = float(val) - 273.15
                        features.append({
                            "type": "Feature",
                            "geometry": {
                                "type": "Point", 
                                "coordinates": [float(lons[j]), float(lats[i])]
                            },
                            "properties": {
                                "temp_c": round(temp_c, 2),
                                "temp_f": round((temp_c * 9/5) + 32, 2)
                            }
                        })
            
            geojson = {
                "type": "FeatureCollection", 
                "metadata": {"generated": str(np.datetime64('now'))},
                "features": features
            }
            
            # Save to the root directory so the GitHub Action can find and commit it
            with open("sst_data.json", "w") as f:
                json.dump(geojson, f)
                
            print(f"Success! Captured {len(features)} clear-sky data points.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    fetch_and_convert()
