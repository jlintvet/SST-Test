import numpy as np
import requests
import json
from netCDF4 import Dataset
import os
import matplotlib.pyplot as plt

# Settings
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0
OUTPUT_DIR = "historical_data"
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
    """Groups all successful PNGs by date so the UI can reference multiple snapshots."""
    # Look for all metadata files generated during the run
    meta_files = sorted([f for f in os.listdir(OUTPUT_DIR) if f.startswith("meta_") and f.endswith(".json")])
    
    # Structure: { "2026-02-22": [ {goes_13:00}, {goes_14:00}, ... ] }
    manifest_data = {}

    for f in meta_files:
        path = os.path.join(OUTPUT_DIR, f)
        try:
            with open(path, 'r', encoding='utf-8') as jf:
                meta = json.load(jf)
                day_key = meta["date"]
                
                if day_key not in manifest_data:
                    manifest_data[day_key] = []
                
                # Check for duplicates before adding to prevent bloat
                if not any(item['image'] == meta['image'] for item in manifest_data[day_key]):
                    manifest_data[day_key].append(meta)
        except Exception as e:
            continue

    # Write the final grouped manifest
    manifest_path = os.path.join(OUTPUT_DIR, "manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)
    
    # Calculate total count for the log
    total_files = sum(len(v) for v in manifest_data.values())
    print(f"--- Manifest Updated: {total_files} images indexed across {len(manifest_data)} days ---")

def process_and_save_raster(content, var_name, base_name, ts, ds_id, ds_display_name):
    """Converts NetCDF to a TRANSPARENT PNG."""
    try:
        with Dataset("memory", memory=content) as ds:
            data = np.squeeze(ds.variables[var_name][:])
            if data.ndim == 3: data = data[0, :, :]
            
            units = ds.variables[var_name].units
            temp_f = ((data - 273.15) * 1.8 + 32) if "K" in units.upper() else (data * 1.8 + 32)

            # MASKING: Identify clouds/NaNs to create transparency
            masked_temp = np.ma.masked_where(~np.isfinite(temp_f) | (temp_f < 35) | (temp_f > 95), temp_f)

            vmin, vmax = 45, 85
            png_filename = f"{base_name}.png"
            png_path = os.path.join(OUTPUT_DIR, png_filename)

            # Use transparency-aware saving
            plt.imsave(png_path, masked_temp, vmin=vmin, vmax=vmax, cmap='jet', origin='upper')

            meta = {
                "date": ts.split('T')[0],
                "timestamp": ts,
                "ds_id": ds_id,
                "ds_name": ds_display_name,
                "image": png_filename,
                "bounds": [[LAT_MIN, LON_MIN], [LAT_MAX, LON_MAX]]
            }
            
            meta_path = os.path.join(OUTPUT_DIR, f"meta_{base_name}.json")
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)

    except Exception as e:
        print(f"      Error processing {ds_id}: {e}")

def fetch_history():
    for node in NODES:
        for ds in DATASETS:
            ds_id, ds_display_name = ds["id"], ds["name"]
            print(f"--- Scanning {ds_display_name} ---")
            try:
                time_url = f"{node}/griddap/{ds_id}.json?time"
                t_resp = requests.get(time_url, timeout=30)
                if t_resp.status_code != 200: continue
                
                available_ts = [row[0] for row in t_resp.json()['table']['rows']]
                recent_ts = available_ts[-LOOKBACK_DAYS:]

                for ts in recent_ts:
                    clean_ts = ts.replace(":", "").replace("-", "").replace("Z", "")
                    base_name = f"sst_{ds_id}_{clean_ts}"
                    
                    if os.path.exists(os.path.join(OUTPUT_DIR, f"{base_name}.png")):
                        continue

                    print(f"  Gap detected! Downloading {ts}...")
                    info_url = f"{node}/info/{ds_id}/index.json"
                    rows = requests.get(info_url, timeout=20).json()['table']['rows']
                    var_name = next((r[1] for r in rows if r[0] == 'variable' and r[1] in ["sea_surface_temperature", "sst"]), "sst")

                    dl_url = (f"{node}/griddap/{ds_id}.nc?"
                              f"{var_name}[({ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
                    
                    data_resp = requests.get(dl_url, timeout=120)
                    if data_resp.status_code == 200:
                        process_and_save_raster(data_resp.content, var_name, base_name, ts, ds_id, ds_display_name)
            
            except Exception as e:
                print(f"  Error: {e}")

if __name__ == "__main__":
    fetch_history()
    update_manifest()import numpy as np
import requests
import json
from netCDF4 import Dataset
import os
import matplotlib.pyplot as plt

# Settings
LAT_MIN, LAT_MAX = 34.0, 37.5
LON_MIN, LON_MAX = -76.5, -73.0
OUTPUT_DIR = "historical_data"
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
    """Generates manifest.json referencing the best PNG images per day."""
    meta_files = sorted([f for f in os.listdir(OUTPUT_DIR) if f.startswith("meta_") and f.endswith(".json")])
    daily_best = {}

    for f in meta_files:
        path = os.path.join(OUTPUT_DIR, f)
        try:
            with open(path, 'r') as jf:
                meta = json.load(jf)
                day_key = meta["date"]
                ds_id = meta["ds_id"]

                if day_key not in daily_best:
                    daily_best[day_key] = meta
                else:
                    current_ds = daily_best[day_key]["ds_id"]
                    if "BLENDED" in ds_id:
                        daily_best[day_key] = meta
                    elif "acspo" in ds_id and "BLENDED" not in current_ds:
                        daily_best[day_key] = meta
        except Exception as e:
            print(f"Error reading meta file {f}: {e}")

    manifest = [daily_best[d] for d in sorted(daily_best.keys())]
    with open(os.path.join(OUTPUT_DIR, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"--- Manifest Updated: {len(manifest)} images indexed ---")

def process_and_save_raster(content, var_name, base_name, ts, ds_id):
    """Converts NetCDF data to a smooth PNG."""
    try:
        with Dataset("memory", memory=content) as ds:
            data = np.squeeze(ds.variables[var_name][:])
            if data.ndim == 3: data = data[0, :, :]
            
            units = ds.variables[var_name].units
            temp_f = ((data - 273.15) * 1.8 + 32) if "K" in units.upper() else (data * 1.8 + 32)

            vmin, vmax = 45, 85
            
            png_filename = f"{base_name}.png"
            png_path = os.path.join(OUTPUT_DIR, png_filename)
            plt.imsave(png_path, temp_f, vmin=vmin, vmax=vmax, cmap='jet', origin='upper')

            meta = {
                "date": ts.split('T')[0],
                "ds_id": ds_id,
                "image": png_filename,
                "bounds": [[LAT_MIN, LON_MIN], [LAT_MAX, LON_MAX]]
            }
            
            # FIXED: Corrected path handling for metadata file
            meta_filename = f"meta_{base_name}.json"
            meta_path = os.path.join(OUTPUT_DIR, meta_filename)
            with open(meta_path, "w") as f:
                json.dump(meta, f, indent=2)

    except Exception as e:
        print(f"      Error creating raster: {e}")

def fetch_history():
    for node in NODES:
        for ds in DATASETS:
            ds_id = ds["id"]
            print(f"--- Scanning {ds['name']} ---")
            try:
                # INCREASED TIMEOUT: 30s for the initial connection
                time_url = f"{node}/griddap/{ds_id}.json?time"
                t_resp = requests.get(time_url, timeout=30)
                if t_resp.status_code != 200: continue
                
                available_ts = [row[0] for row in t_resp.json()['table']['rows']]
                recent_ts = available_ts[-LOOKBACK_DAYS:]

                for ts in recent_ts:
                    clean_ts = ts.replace(":", "").replace("-", "").replace("Z", "")
                    base_name = f"sst_{ds_id}_{clean_ts}"
                    
                    if os.path.exists(os.path.join(OUTPUT_DIR, f"{base_name}.png")):
                        continue

                    print(f"  Downloading {ts} from {ds['name']}...")
                    info_url = f"{node}/info/{ds_id}/index.json"
                    rows = requests.get(info_url, timeout=20).json()['table']['rows']
                    var_name = next((r[1] for r in rows if r[0] == 'variable' and r[1] in ["sea_surface_temperature", "sst"]), "sst")

                    dl_url = (f"{node}/griddap/{ds_id}.nc?"
                              f"{var_name}[({ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
                    
                    # INCREASED TIMEOUT: 90s for the actual data download
                    data_resp = requests.get(dl_url, timeout=90)
                    if data_resp.status_code == 200:
                        process_and_save_raster(data_resp.content, var_name, base_name, ts, ds_id)
                        print(f"    SUCCESS: Saved {base_name}.png")
            
            except Exception as e:
                print(f"  Connection Issue: {e}")

if __name__ == "__main__":
    fetch_history()
    update_manifest()
