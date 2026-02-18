import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Coordinates for VA Beach / Hatteras corridor
# Slicing: 37.5N (VA Beach) down to 34.0N (Hatteras/Ocracoke)
URL = "https://coastwatch.noaa.gov/erddap/griddap/noaacwVIIRSnppSSTDaily3U.nc?sea_surface_temperature[(latest)][(37.5):(34.0)][(-76.5):(-73.0)]"

def fetch_and_convert():
    print("Fetching high-fidelity VIIRS 750m data from NOAA...")
    try:
        # Requesting NetCDF data from NOAA ERDDAP
        response = requests.get(URL, timeout=60)
        
        # Verify connection (Corrected attribute: status_code)
        if response.status_code != 200:
            print(f"NOAA Server Error: {response.status_code}")
            return

        # Load the data from the response memory buffer
        with Dataset("memory", memory=response.content) as ds:
            sst = ds.variables['sea_surface_temperature'][0, :, :]
            lats = ds.variables['latitude'][:]
            lons = ds.variables['longitude'][:]
            
            features = []
            # We use a step of 2 to keep the JSON file size efficient 
            # while maintaining high-fidelity (~1.5km resolution).
            for i in range(0, len(lats), 2): 
                for j in range(0, len(lons), 2):
                    val = sst[i, j]
                    
                    # Filter out NaN values (pixels covered by clouds)
                    if not np.isnan(val):
                        # Convert Kelvin to Celsius and Fahrenheit
                        temp_c = float(val) - 273.15
                        temp_f = (temp_c * 9/5) + 32
                        
                        features.append({
                            "type": "Feature",
                            "geometry": {
                                "type": "Point", 
                                "coordinates": [float(lons[j]), float(lats[i])]
                            },
                            "properties": {
                                "temp_f": round(temp_f, 2),
                                "temp_c": round(temp_c, 2)
                            }
                        })
            
            # Construct the GeoJSON object
            geojson = {
                "type": "FeatureCollection", 
                "metadata": {
                    "region": "VA Beach to Hatteras",
                    "generated_at": str(np.datetime64('now'))
                },
                "features": features
            }
            
            # Write to file
            with open("sst_data.json", "w") as f:
                json.dump(geojson, f)
                
            print(f"Success! Generated sst_data.json with {len(features)} points.")

    except Exception as e:
        print(
