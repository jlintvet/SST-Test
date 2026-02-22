import numpy as np
import requests
import json
from netCDF4 import Dataset
import os
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

# Settings
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0
OUTPUT_DIR = "historical_data"
# We reduce STEP to 1 because PNGs can handle much higher resolution than JSON
STEP = 1 
LOOKBACK_DAYS = 5 

DATASETS = [
    {"id": "noaacwBLENDEDsstDNDaily", "name": "Geo-Polar Blended NRT"},
    {"id": "noa_coastwatch_acspo_v2_nrt", "name": "ACSPO NRT Global"},
    {"id": "goes19SSThourly", "name": "GOES-19 Hourly"}
]

NODES = ["https://coastwatch.noaa.gov/erddap", "https://cwcgom.aoml.noaa.gov/erddap"]

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def update_manifest():
    """Generates manifest.json referencing the PNG images and their bounds."""
    # Look for the metadata json files we create alongside PNGs
    files = sorted([f for f in os.listdir(OUTPUT_DIR) if f.startswith("meta_") and f.endswith(".json")])
    daily_best = {}

    for f in files:
        parts = f.split('_')
        ds_id, ts_val = parts[1], parts[2].replace(".json", "")
        day_key = ts_val.split('T')[0] if "T" in ts_val else ts_val

        # Priority: Blended > ACSPO > GOES
        if day_key not in daily_best or "BLENDED" in f or ("acspo" in f and "BLENDED" not in daily_best[day_key]):
            with open(os.path.join(OUTPUT_DIR, f), 'r') as meta_file:
                daily_best[day_key] = json.load(meta_file)

    manifest = [daily_best[d] for d in sorted(daily_best.keys())]
    with open(os.path.join(OUTPUT_DIR, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"--- Manifest Updated: {len(manifest)} images indexed ---")

def process_and_save_raster(content, var_name, base_path, timestamp):
    """Converts NetCDF data to a colorized PNG image."""
    try:
        with Dataset("memory", memory=content) as ds:
            # We want full resolution for the image
            data = np.squeeze(ds.variables[var_name][:])
            if data.ndim == 3: data = data[0, :, :]
            
            units = ds.variables[var_name].units
            temp_f = ((data - 273.15) * 1.8 + 32) if "K" in units.upper() else (data * 1.8 + 32)

            # Define color scale (e.g., 40F to 85F)
            vmin, vmax = 45, 85
            
            # Create the image buffer
            # 'origin=lower' is important because NetCDF usually stores data south-to-north
            plt.imsave(f"{base_path}.png", temp_f, vmin=vmin, vmax=vmax, cmap='jet', origin='upper')

            # Create metadata so the frontend knows where to overlay this PNG
            meta = {
                "date": timestamp.split('T')[0],
                "image": os.path.basename(f"{base_path}.png"),
                "bounds": [[LAT_MIN, LON_MIN], [LAT_MAX, LON_MAX]],
                "temp_range": [vmin, vmax]
            }
            
            with open(f"meta_{os.path.basename(base_path)}.json", "w") as f:
                json.dump(meta, os.path.join(OUTPUT_DIR, f))

    except Exception as e:
        print(f"      Error creating raster: {e}")

def fetch_history():
    for node in NODES:
        for ds in DATASETS:
            ds_id = ds["id"]
            try:
                time_url = f"{node}/griddap/{ds_id}.json?time"
                t_resp = requests.get(time_url, timeout=15)
                if t_resp.status_code != 200: continue
                
                ts = t_resp.json()['table']['rows'][-1][0] # Get latest
                clean_ts = ts.replace(":", "").replace("-", "").replace("Z", "")
                base_filename = f"sst_{ds_id}_{clean_ts}"
                
                if os.path.exists(os.path.join(OUTPUT_DIR, f"{base_filename}.png")):
                    continue

                print(f"--- Processing Raster for {ds['name']} ---")
                info_url = f"{node}/info/{ds_id}/index.json"
                rows = requests.get(info_url, timeout=10).json()['table']['rows']
                var_name = next((r[1] for r in rows if r[0] == 'variable' and r[1] in ["sea_surface_temperature", "sst"]), "sst")

                dl_url = (f"{node}/griddap/{ds_id}.nc?"
                          f"{var_name}[({ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
                
                data_resp = requests.get(dl_url, timeout=60)
                if data_resp.status_code == 200:
                    process_and_save_raster(data_resp.content, var_name, os.path.join(OUTPUT_DIR, base_filename), ts)
            
            except Exception as e:
                print(f"  Error: {e}")

if __name__ == "__main__":
    fetch_history()
    update_manifest()
