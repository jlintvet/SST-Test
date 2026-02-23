import numpy as np
import requests
import json
from netCDF4 import Dataset
import os
import matplotlib.pyplot as plt
import time

# --- COORDINATES: NC OFFSHORE ---
LAT_MIN, LAT_MAX = 33.5, 36.8   
LON_MIN, LON_MAX = -76.5, -72.5  
OUTPUT_DIR = "historical_data"
LOOKBACK_DAYS = 5 
RETENTION_DAYS = 14 

DATASETS = [
    {"id": "noaacwBLENDEDsstDNDaily", "name": "Geo-Polar Blended NRT"},
    {"id": "noa_coastwatch_acspo_v2_nrt", "name": "ACSPO NRT Global"},
    {"id": "goes19SSThourly", "name": "GOES-19 Hourly"}
]

NODES = ["https://coastwatch.noaa.gov/erddap", "https://cwcgom.aoml.noaa.gov/erddap"]

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def cleanup_old_files():
    now = time.time()
    cutoff = now - (RETENTION_DAYS * 86400)
    for f in os.listdir(OUTPUT_DIR):
        if f.endswith(".png") or f.startswith("meta_"):
            path = os.path.join(OUTPUT_DIR, f)
            if os.path.getmtime(path) < cutoff:
                os.remove(path)

def update_manifest():
    meta_files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith("meta_") and f.endswith(".json")]
    manifest_data = {}
    for f in sorted(meta_files):
        path = os.path.join(OUTPUT_DIR, f)
        try:
            with open(path, 'r', encoding='utf-8') as jf:
                meta = json.load(jf)
                day_key = meta["date"]
                if day_key not in manifest_data: manifest_data[day_key] = []
                if not any(item['image'] == meta['image'] for item in manifest_data[day_key]):
                    manifest_data[day_key].append(meta)
        except: continue
    with open(os.path.join(OUTPUT_DIR, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)

def process_and_save_raster(content, var_name, base_name, ts, ds_id, ds_display_name):
    try:
        with Dataset("memory", memory=content) as ds:
            possible_vars = [var_name, "sea_surface_temperature", "sst", "analysed_sst"]
            target_var = next((v for v in possible_vars if v in ds.variables), None)
            if not target_var: return

            raw_data = np.squeeze(ds.variables[target_var][:])
            lats = ds.variables['latitude'][:]
            
            units = ds.variables[target_var].units if hasattr(ds.variables[target_var], 'units') else "K"
            temp_f = ((raw_data - 273.15) * 1.8 + 32) if "K" in units.upper() else (raw_data * 1.8 + 32)

            # --- SMART ORIENTATION ---
            # If latitudes go from small to large (33 -> 36), the data is "upside down" for an image.
            # We flip it so the first row is the highest latitude (North).
            if lats[0] < lats[-1]:
                final_grid = np.flipud(temp_f)
            else:
                final_grid = temp_f

            masked_temp = np.ma.masked_where(~np.isfinite(final_grid) | (final_grid < 30) | (final_grid > 100), final_grid)
            png_filename = f"{base_name}.png"
            png_path = os.path.join(OUTPUT_DIR, png_filename)
            
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
            print(f"    FIXED & OVERWRITTEN: {png_filename}")

    except Exception as e:
        print(f"      Error: {e}")

def fetch_history():
    for node in NODES:
        for ds in DATASETS:
            ds_id, ds_name = ds["id"], ds["name"]
            print(f"--- Scanning {ds_name} ---")
            try:
                t_resp = requests.get(f"{node}/griddap/{ds_id}.json?time", timeout=30)
                if t_resp.status_code != 200: continue
                recent_ts = [row[0] for row in t_resp.json()['table']['rows']][-LOOKBACK_DAYS:]

                for ts in recent_ts:
                    clean_ts = ts.replace(":", "").replace("-", "").replace("Z", "")
                    base_name = f"sst_{ds_id}_{clean_ts}"
                    
                    # --- OVERWRITE ENABLED ---
                    # We removed the 'if os.path.exists... continue' line.
                    # This forces the script to re-download and fix the orientation.

                    print(f"  Processing {ts}...")
                    i_resp = requests.get(f"{node}/info/{ds_id}/index.json", timeout=20)
                    if i_resp.status_code != 200: continue
                    info = i_resp.json()
                    var_name = next((r[1] for r in info['table']['rows'] if r[0] == 'variable' and r[1] in ["sea_surface_temperature", "sst", "analysed_sst"]), "sst")

                    dl_url = f"{node}/griddap/{ds_id}.nc?{var_name}[({ts})][({LAT_MIN}):({LAT_MAX})][({LON_MIN}):({LON_MAX})]"
                    data_resp = requests.get(dl_url, timeout=120)
                    if data_resp.status_code == 200:
                        process_and_save_raster(data_resp.content, var_name, base_name, ts, ds_id, ds_name)
            except Exception as e:
                print(f"  Error: {e}")

if __name__ == "__main__":
    cleanup_old_files()
    fetch_history()
    update_manifest()
