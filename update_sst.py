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
RETENTION_DAYS = 5

DATASETS = [
    {
        "id": "noaacwBLENDEDsstDNDaily",
        "name": "Geo-Polar Blended NRT (Day+Night)",
        "nodes": ["https://coastwatch.noaa.gov/erddap", "https://cwcgom.aoml.noaa.gov/erddap"]
    },
    {
        "id": "noaacrwsstDaily",
        "name": "CoralTemp 5km Daily",
        "nodes": ["https://coastwatch.noaa.gov/erddap"]
    },
    {
        "id": "jplMURSST41",
        "name": "MUR SST 1km Daily (NASA JPL)",
        "nodes": ["https://coastwatch.pfeg.noaa.gov/erddap"]
    },
]

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
                print(f"  Purged: {f}")

def update_manifest():
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
        except:
            continue
    with open(os.path.join(OUTPUT_DIR, "manifest.json"), "w", encoding="utf-8") as f:
        json.dump(manifest_data, f, indent=2)

def process_and_save_raster(content, var_name, base_name, ts, ds_id, ds_display_name):
    try:
        with Dataset("memory", memory=content) as ds:
            possible_vars = [var_name, "analysed_sst", "sea_surface_temperature", "sst"]
            target_var = next((v for v in possible_vars if v in ds.variables), None)
            if not target_var:
                print(f"      No SST variable found. Available: {list(ds.variables.keys())}")
                return

            raw_data = np.squeeze(ds.variables[target_var][:])
            lats = ds.variables['latitude'][:]

            units = ds.variables[target_var].units if hasattr(ds.variables[target_var], 'units') else "celsius"
            if "K" in units.upper():
                temp_f = (raw_data - 273.15) * 1.8 + 32
            else:
                temp_f = raw_data * 1.8 + 32

            if lats[0] < lats[-1]:
                final_grid = np.flipud(temp_f)
            else:
                final_grid = temp_f

            masked_temp = np.ma.masked_where(
                ~np.isfinite(final_grid) | (final_grid < 30) | (final_grid > 100),
                final_grid
            )

            valid_data = masked_temp.compressed()
            if len(valid_data) == 0:
                print(f"      No valid data found, skipping.")
                return
            min_temp = float(np.percentile(valid_data, 2))
            max_temp = float(np.percentile(valid_data, 98))

            png_filename = f"{base_name}.png"
            png_path = os.path.join(OUTPUT_DIR, png_filename)

            fig, ax = plt.subplots(1, 1, figsize=(10, 10))
            ax.imshow(masked_temp, cmap='jet', origin='upper',
                      interpolation='bicubic', vmin=min_temp, vmax=max_temp)
            ax.axis('off')
            plt.savefig(png_path, bbox_inches='tight', pad_inches=0, dpi=150)
            plt.close(fig)

            meta = {
                "date": ts.split('T')[0],
                "timestamp": ts,
                "ds_id": ds_id,
                "ds_name": ds_display_name,
                "image": png_filename,
                "bounds": [[LAT_MIN, LON_MIN], [LAT_MAX, LON_MAX]],
                "min_temp": min_temp,
                "max_temp": max_temp
            }
            with open(os.path.join(OUTPUT_DIR, f"meta_{base_name}.json"), "w", encoding="utf-8") as f:
                json.dump(meta, f, indent=2)
            print(f"    SAVED: {png_filename} (temps: {min_temp:.1f}F - {max_temp:.1f}F)")

    except Exception as e:
        print(f"      Error: {e}")

def fetch_history():
    for ds in DATASETS:
        ds_id, ds_name = ds["id"], ds["name"]
        ds_nodes = ds.get("nodes", ["https://coastwatch.noaa.gov/erddap"])
        for node in ds_nodes:
            print(f"--- Scanning {ds_name} on {node} ---")
            success = False
            try:
                t_resp = requests.get(f"{node}/griddap/{ds_id}.json?time", timeout=30)
                if t_resp.status_code != 200:
                    print(f"  Skipping — status {t_resp.status_code}")
                    continue

                recent_ts = [row[0] for row in t_resp.json()['table']['rows']][-LOOKBACK_DAYS:]

                for ts in recent_ts:
                    clean_ts = ts.replace(":", "").replace("-", "").replace("Z", "")
                    base_name = f"sst_{ds_id}_{clean_ts}"

                    print(f"  Fetching {ts}...")
                    i_resp = requests.get(f"{node}/info/{ds_id}/index.json", timeout=20)
                    if i_resp.status_code != 200:
                        print(f"    Info fetch failed: {i_resp.status_code}")
                        continue
                    info = i_resp.json()
                    var_name = next(
                        (r[1] for r in info['table']['rows']
                         if r[0] == 'variable' and r[1] in ["analysed_sst", "sea_surface_temperature", "sst"]),
                        "analysed_sst"
                    )

                    dl_url = f"{node}/griddap/{ds_id}.nc?{var_name}[({ts})][({LAT_MIN}):({LAT_MAX})][({LON_MIN}):({LON_MAX})]"
                    data_resp = requests.get(dl_url, timeout=120)
                    if data_resp.status_code == 200:
                        process_and_save_raster(data_resp.content, var_name, base_name, ts, ds_id, ds_name)
                    else:
                        print(f"    Download failed: {data_resp.status_code}")

                success = True

            except Exception as e:
                print(f"  Error: {e}")

            if success:
                break

if __name__ == "__main__":
    cleanup_old_files()
    fetch_history()
    update_manifest()
