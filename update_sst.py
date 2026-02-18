import numpy as np
import requests
import json
from netCDF4 import Dataset
import os

# Fishing Box: VA Beach (37.5N) to Hatteras (34.0N)
LAT_RANGE = "[(37.5):(34.0)]"
LON_RANGE = "[(-76.5):(-73.0)]"

# Attempt these IDs in order. 
# 'nrt' datasets are more stable for 'latest' queries.
DATASET_IDS = [
    "noaacwVIIRSnrtSSTDaily3P",  # Primary: Near Real-Time Aggregated (750m)
    "noaacwVIIRSnppSSTDaily3P"   # Fallback: Suomi-NPP satellite (750m)
]

def fetch_and_convert():
    print("Initiating NOAA High-Fidelity SST Fetch...")
    
    success = False
    for dataset_id in DATASET_IDS:
        if success:
            break
            
        url = f"https://coastwatch.noaa.gov/erddap/griddap/{dataset_id}.nc?sea_surface_temperature[(latest)]{LAT_RANGE}{LON_RANGE}"
        print(f"Trying Dataset: {dataset_id}")
        
        try:
            # High timeout because ERDDAP aggregation can be slow
            response = requests.get(url, timeout=90)
            
            if response.status_code == 200:
                print(f"Successfully connected to {dataset_id}")
                process_data(response.content)
                success = True
            else:
                print(f"Dataset {dataset_id} returned error {response.status_code}")
                
        except Exception as e:
            print(f"Error connecting to {dataset_id}: {e}")

    if not success:
        print("CRITICAL: All NOAA datasets returned 404 or timed out. Server may be in maintenance.")

def process_data(content):
    try:
        with Dataset("memory", memory=content) as ds:
            # Extract SST, Lats, and Lons
            sst_raw = ds.variables['sea_surface_temperature'][0, :, :]
            lats = ds.variables['latitude'][:]
            lons = ds.variables['longitude'][:]
            
            features = []
            # Step 2 maintains ~1.5km fidelity while keeping JSON size small
            for i in range(0, len(lats), 2): 
                for j in range(0, len(lons), 2):
                    val = sst_raw[i, j]
                    
                    # Only map clear-sky pixels (non-NaN)
                    if not np.isnan(val):
                        # Convert Kelvin to Fahrenheit
                        temp_f = (float(val) - 273.15) * 9/5 + 32
                        features.append({
                            "type": "Feature",
                            "geometry": {
                                "type": "Point", 
                                "coordinates": [float(lons[j]), float(lats[i])]
                            },
                            "properties": {
                                "temp_f": round(temp_f, 2)
                            }
                        })
            
            # Construct GeoJSON
            output = {
                "type": "FeatureCollection",
                "metadata": {
                    "generated_at": str(np.datetime64('now')),
                    "description": "750m VIIRS SST - VA Beach to Hatteras"
                },
                "features": features
            }
            
            # Save to root directory
            file_path = os.path.join(os.getcwd(), "sst_data.json")
            with open(file_path, "w") as f:
                json.dump(output, f)
                
            print(f"Success! Created sst_data.json with {len(features)} points.")
            
    except Exception as e:
        print(f"Processing error: {e}")

if __name__ == "__main__":
    fetch_and_convert()
