def process_data(content):
    with Dataset("memory", memory=content) as ds:
        sst_raw = ds.variables['analysed_sst'][0, :, :]
        lats = ds.variables['latitude'][:]
        lons = ds.variables['longitude'][:]
        
        features = []
        for i in range(0, len(lats), 3): 
            for j in range(0, len(lons), 3):
                val = sst_raw[i, j]
                
                # IMPROVED FILTER: Check if it is a real number and not a masked value
                if np.isreal(val) and not np.isnan(val) and val > 0:
                    temp_f = (float(val) - 273.15) * 9/5 + 32
                    
                    # Ensure temp_f itself is a valid number before adding
                    if np.isfinite(temp_f):
                        features.append({
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [float(lons[j]), float(lats[i])]},
                            "properties": {"temp_f": round(temp_f, 1)}
                        })
        
        output = {
            "type": "FeatureCollection",
            "features": features
        }
        
        with open("sst_data.json", "w") as f:
            # Added an extra check to catch any accidental NaNs
            json.dump(output, f, allow_nan=False) 
            
        print(f"Success! Cleaned JSON created with {len(features)} points.")
