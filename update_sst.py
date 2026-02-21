def fetch_history():
    """Checks for any missing days in the recent lookback window."""
    for node in NODES:
        for ds in DATASETS:
            ds_id = ds["id"]
            print(f"--- Scanning {ds['name']} ({ds_id}) ---")
            
            try:
                # 1. Get available timestamps
                time_url = f"{node}/griddap/{ds_id}.json?time"
                t_resp = requests.get(time_url, timeout=15)
                if t_resp.status_code != 200: continue

                available_timestamps = [row[0] for row in t_resp.json()['table']['rows']]
                # Look at the most recent entries
                recent_timestamps = available_timestamps[-LOOKBACK_DAYS:]

                # 2. Get variable name
                info_url = f"{node}/info/{ds_id}/index.json"
                rows = requests.get(info_url, timeout=10).json()['table']['rows']
                var_name = next((r[1] for r in rows if r[0] == 'variable' and r[1] in ["sea_surface_temperature", "sst"]), "sst")

                for ts in recent_timestamps:
                    # Create a unique filename using Date + Dataset ID
                    # For GOES hourly, we use the full timestamp to prevent overwrites
                    clean_ts = ts.replace(":", "").replace("-", "").replace("Z", "")
                    clean_date = ts.split('T')[0]
                    
                    # Use full timestamp for GOES-19, just date for Daily Blended
                    suffix = clean_ts if "hourly" in ds_id else clean_date
                    filename = f"sst_{ds_id}_{suffix}.json"
                    filepath = os.path.join(OUTPUT_DIR, filename)

                    if os.path.exists(filepath):
                        continue

                    print(f"  Gap detected! Downloading {ts} from {ds['name']}...")
                    dl_url = (f"{node}/griddap/{ds_id}.nc?"
                              f"{var_name}[({ts})][({LAT_MAX}):({LAT_MIN})][({LON_MIN}):({LON_MAX})]")
                    
                    data_resp = requests.get(dl_url, timeout=60)
                    if data_resp.status_code == 200:
                        process_and_save(data_resp.content, var_name, filepath)
                        print(f"  SUCCESS: Saved {filename}")
                
            except Exception as e:
                print(f"  Error accessing {ds_id}: {e}")
                continue
