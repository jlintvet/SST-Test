import numpy as np
import requests
import json
from netCDF4 import Dataset
import os
import matplotlib.pyplot as plt

# --- FIXED COORDINATES: NC OFFSHORE & GULF STREAM CORE ---
LAT_MIN, LAT_MAX = 33.5, 36.8   # North from Cape Lookout through Hatteras to Oregon Inlet
LON_MIN, LON_MAX = -76.5, -72.5  # West to East capturing the Shelf Break and Stream
OUTPUT_DIR = "historical_data"
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
        except Exception: continue

    with open(os.path.join(OUTPUT_DIR, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)
    print(f"--- Manifest Updated: {len(meta_files)} images indexed ---")

def process_and_save_raster(content, var_name, base_name, ts, ds_id, ds_display_name):
    try:
        with Dataset("memory", memory=content) as ds:
            possible_vars = [var_name, "sea_surface_temperature", "sst", "analysed_sst"]
            target_var = next((v for v in possible_vars if v in ds.variables), None)
            
            if not target_var: return

            data = np.squeeze(ds.variables[target_var][:])
            # Fixed the syntax error here
            if data.ndim == 3: 
                data = data[0, :, :]
            
            units = ds.variables[target_var].units if hasattr(ds.variables[target_var], 'units') else "K"
            temp_f = ((data - 273.15) * 1.8 + 32) if "K" in units.upper() else (data * 1.8 + 32)

            # Transparency Mask for Land/Clouds
            masked_temp = np.ma.masked_where(~np.isfinite(temp_f) | (temp_f < 30) | (temp_f > 100), temp_f)

            png_filename = f"{base_name}.png"
            png_path = os.path.join(OUTPUT_DIR, png_filename)
            
            # --- HIGH CONTRAST SETTINGS (58°F - 82°F) ---
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
        print(f"      Error processing {ds_id}: {e}")

def fetch_history():
    for node in NODES:
        for ds in DATASETS:
            ds_id, ds_name = ds["id"], ds["name"]
            lookback = 10 if "BLENDED" in ds_id else LOOKBACK_DAYS
            print(f"--- Scanning {ds_name} ---")
            try:
                t_resp = requests.get(f"{node}/griddap/{ds_id}.json?time", timeout=30)
                if t_resp.status_code != 200: continue
                recent_ts = [row[0] for row in t_resp.json()['table']['rows']][-lookback:]

                for ts in recent_ts:
                    clean_ts = ts.replace(":", "").replace("-", "").replace("Z", "")
                    base_name = f"sst_{ds_id}_{clean_ts}"
                    if os.path.exists(os.path.join(OUTPUT_DIR, f"{base_name}.png")): continue

                    print(f"  Downloading {ts}...")
                    info = requests.get(f"{node}/info/{ds_id}/index.json", timeout=20).json()
                    var_name = next((r[1] for r in info['table']['rows'] if r[0] == 'variable' and r[1] in ["sea_surface_temperature", "sst", "analysed_sst"]), "sst")

                    # LATITUDE FIX: Min to Max prevents vertical flip
                    dl_url = f"{node}/griddap/{ds_id}.nc?{var_name}[({ts})][({LAT_MIN}):({LAT_MAX})][({LON_MIN}):({LON_MAX})]"
                    data_resp = requests.get(dl_url, timeout=120)
                    if data_resp.status_code == 200:
                        process_and_save_raster(data_resp.content, var_name, base_name, ts, ds_id, ds_name)
            except Exception as e:
                print(f"  Error: {e}")

if __name__ == "__main__":
    fetch_history()
    update_manifest()
