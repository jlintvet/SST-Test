import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Coordinates for VA Beach / Hatteras
URL = "https://coastwatch.noaa.gov/erddap/griddap/noaacwVIIRSnppSSTDaily3U.nc?sea_surface_temperature[(latest)][(37.5):(34.0)][(-76.5):(-73.0)]"

def fetch_and_convert():
    print("Fetching high-fidelity VIIRS data...")
    try:
        response = requests.get(URL, timeout=60)
        if response.status_code != 200:
            print(f"NOAA Server Error: {response.status_code}")
            return

        with Dataset("memory", memory=response.content) as ds:
            sst = ds.variables['sea_surface_temperature'][0, :, :]
            lats = ds.variables['latitude'][:]
            lons = ds.variables['longitude'][:]
            
            total_pixels = sst.size
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
            
            # Calculate visibility %
            visibility = round((len(features) / (total_pixels / 4)) * 100, 2)
            print(f"Visibility: {visibility}% ({len(features)} points found)")

            output = {
                "type": "FeatureCollection", 
                "metadata": {
                    "visibility_percent": visibility,
                    "generated": str(np.datetime64('now'))
                },
                "features": features
            }
            
            # Use absolute path to ensure GitHub finds it
            file_path = os.path.join(os.getcwd(), "sst_data.json")
            with open(file_path, "w") as f:
                json.dump(output, f)
            print(f"File saved to: {file_path}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_and_convert()
