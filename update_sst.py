import numpy as np
import requests
import json
from netCDF4 import Dataset
import os
import matplotlib.pyplot as plt
import time

# --- COORDINATES: NC OFFSHORE & GULF STREAM CORE ---
LAT_MIN, LAT_MAX = 33.5, 36.8   
LON_MIN, LON_MAX = -76.5, -72.5  
OUTPUT_DIR = "historical_data"
LOOKBACK_DAYS = 5 
RETENTION_DAYS = 14  # Keeps the repo from getting bloated

DATASETS = [
    {"id": "noaacwBLENDEDsstDNDaily", "name": "Geo-Polar Blended NRT"},
    {"id": "noa_coastwatch_acspo_v2_nrt", "name": "ACSPO NRT Global"},
    {"id": "goes19SSThourly", "name": "GOES-19 Hourly"}
]

NODES = ["https://coastwatch.noaa.gov/erddap", "https://cwcgom.aoml.noaa.gov/erddap"]

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def cleanup_old_files():
    """Removes files older than RETENTION_DAYS to keep the repo slim."""
    print(f"--- Running Cleanup (Retention: {RETENTION_DAYS} days) ---")
    now = time.time()
    cutoff = now - (RETENTION_DAYS * 86400)
    count = 0
    if not os.path.exists(OUTPUT_DIR):
        return
    for f in os.listdir(OUTPUT_DIR):
        if f.endswith(".png") or f.startswith("meta_"):
            file_path = os.path.join(OUTPUT_DIR, f)
            if os.path.getmtime(file_path) < cutoff:
                os.remove(file_path)
                count += 1
    if count > 0:
        print(f"  Removed {count} old files.")

def update_manifest():
    """Rebuilds the manifest.json file based on current meta files."""
    meta_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith("meta_") and f.endswith(".json")]
    manifest_data = {}
    for f in sorted(meta_files):
        path = os.path.join(OUTPUT_DIR, f)
        try:
            with open(path, 'r', encoding='utf-8') as jf:
                meta = json.load(jf)
                day_key = meta["date"]
                if day_key not in manifest_data:
                    manifest_data[day_key] = []
                if not any(item['image'] == meta['image'] for item in manifest_data[day_key]):
                    manifest_data[day_key].append(meta)
        except Exception as e:
            print(f"      Error reading {f}: {e}")
            continue
            
    with open(os.path.join(OUTPUT_DIR, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)
    print(f"--- Manifest Updated: {len(meta_files)} images indexed ---")

def process_and_save_raster(content, var_name, base_name, ts, ds_id, ds_display_name):
    """Converts NetCDF content to a high-contrast, transparent PNG."""
    try:
        with Dataset("memory", memory=content) as ds:
            possible_vars = [var_name, "sea_surface_temperature", "sst", "analysed_sst"]
            target_var = next((v for v in possible_vars if v in ds.variables), None)
            
            if not target_var:
                return

            data = np.squeeze(ds.variables[target_var][:])
            if data.ndim == 3: 
                data = data[0, :, :]
            
            # Unit Conversion to Fahrenheit
            units = ds.variables[target_var].units if hasattr(ds.variables[target_var], 'units') else "K"
            if "K" in units.upper():
                temp_f = (data - 273.15) * 1.8 + 32
            else:
                temp_f = (data * 1.8) + 32

            # --- THE FLIP FIX ---
            # np.flipud flips the array vertically so North is at the top of the PNG
            data_fixed = np.flipud(temp_f)

            # Apply Mask (Land and extreme cloud noise)
            masked_temp = np.ma.masked_where(~np.isfinite(data_fixed) | (data_fixed < 30) | (data_fixed > 100), data_fixed)

            png_filename = f"{base_name}.png"
            png_path = os.path.join(OUTPUT_DIR, png_filename)
            
            # Save with 58F to 82F range for sharp Stream edges
            plt.imsave(png_path, masked_temp, vmin=58, vmax=82, cmap='jet', origin='upper')

            meta = {
                "date": ts.split('T')[0],
                "timestamp": ts,
                "ds_id": ds_id,
                "ds_name": ds_display_name,
                "image": png_filename,
                "bounds": [[LAT_MIN, LON_MIN], [LAT_MAX, LON_MAX]]
            }
            
            with open(os.path.join(OUTPUT_DIR, f"meta_{base_name}.json"), "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)
            print(f"    SUCCESS: Saved {png_filename}")
            
    except Exception as e:
        print(f"      Processing Error: {e}")

def fetch_history():
    """Queries NOAA for the most recent data across all datasets."""
    for node in NODES:
        for ds in DATASETS:
            ds_id, ds_name = ds["id"], ds["name"]
            lookback = 10 if "BLENDED" in ds_id else LOOKBACK_DAYS
            print(f"--- Scanning {ds_name} ---")
            try:
                t_url = f"{node}/griddap/{ds_id}.json?time"
                t_resp = requests.get(t_url, timeout=30)
                if t_resp.status_code != 200:
                    continue
                
                recent_ts = [row[0] for row in t_resp.json()['table']['rows']][-lookback:]

                for ts in recent_ts:
                    clean_ts = ts.replace(":", "").replace("-", "").replace("Z", "")
                    base_name = f"sst_{ds_id}_{clean_ts}"
                    
                    if os.path.exists(os.path.join(OUTPUT_DIR, f"{base_name}.png")):
                        continue

                    print(f"  Downloading {ts}...")
                    i_url = f"{node}/info/{ds_id}/index.json"
                    info_resp = requests.get(i_url, timeout=20)
                    if info_resp.status_code != 200:
                        continue
                        
                    info = info_resp.json()
                    var_name = next((r[1] for r in info['table']['rows'] if r[0] == 'variable' and r[1] in ["sea_surface_temperature", "sst", "analysed_sst"]), "sst")

                    # Correct URL construction
                    dl_url = f"{node}/griddap/{ds_id}.nc?{var_name}[({ts})][({LAT_MIN}):({LAT_MAX})][({LON_MIN}):({LON_MAX})]"
                    data_resp = requests.get(dl_url, timeout=120)
                    if data_resp.status_code == 200:
                        process_and_save_raster(data_resp.content, var_name, base_name, ts, ds_id, ds_name)
            except Exception as e:
                print(f"  Connection Error: {e}")

if __name__ == "__main__":
    cleanup_old_files() 
    fetch_history()
    update_manifest
