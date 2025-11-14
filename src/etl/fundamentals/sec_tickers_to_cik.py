import os
import pandas as pd
import json

# -----------------------------------------------------------
# Read local JSON file using path relative to this script
# -----------------------------------------------------------

# Get absolute path of this script (e.g., /home/.../ds5110/src/etl/fundamentals/sec_tickers_to_cik.py)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Build relative path from this script to the data folder
json_path = os.path.join(BASE_DIR, "../../../data/raw/sec_filing/company_tickers.json")
out_path  = os.path.join(BASE_DIR, "../../../data/raw/sec_filing/SP500_cik_mapping.csv")

# Normalize (resolve ../../)
json_path = os.path.normpath(json_path)
out_path  = os.path.normpath(out_path)

# Ensure file exists
if not os.path.exists(json_path):
    raise FileNotFoundError(f"JSON file not found at {json_path}")
with open(json_path, "r") as file:
    mapping = json.load(file)

# Convert the mapping to a DataFrame
df_map = pd.DataFrame(mapping).T
df_map["cik"] = df_map["cik_str"].astype(int)
df_map.rename(columns={"ticker": "symbol", "title": "name"}, inplace=True)

# Ensure directory exists
os.makedirs(os.path.dirname(out_path), exist_ok=True)

# Save the DataFrame to CSV
df_map.to_csv(out_path, index=False)

print(f"SEC tickers to CIK mapping saved to {out_path}")
print(df_map.head())