import requests
import time
import pandas as pd
from pathlib import Path
from tqdm import tqdm

HEADERS = {
    "User-Agent": "Chen Yang <email@example.com>",
    "Accept-Encoding": "gzip"
}

MAPPING_PATH = Path("data/raw/sec_filing/SP500_cik_mapping.csv")
SP500_LIST_PATH = Path("data/raw/S&P500.csv")
OUTPUT_DIR = Path("data/raw/fundamental/edgar/companyfacts")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RATE_LIMIT_SECONDS = 0.12

def fetch_json(cik):
    cik_padded = str(cik).zfill(10)
    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_padded}.json"
    out_file = OUTPUT_DIR / f"CIK{cik_padded}.json"

    # skip existing files (resume)
    if out_file.exists():
        return True

    time.sleep(RATE_LIMIT_SECONDS)

    try:
        r = requests.get(url, headers=HEADERS)
    except Exception as e:
        print(f"[ERROR] Request failed for CIK {cik_padded}: {e}")
        return False

    if r.status_code == 200:
        out_file.write_bytes(r.content)
        return True

    else:
        print(f"[ERROR] CIK {cik_padded} → {r.status_code}")
        return False
        

def main():
    # Load S&P 500 symbols
    df_sp = pd.read_csv(SP500_LIST_PATH)
    if "Symbol" not in df_sp.columns:
        raise ValueError("S&P500.csv must contain column: Symbol")

    symbols_needed = df_sp["Symbol"].str.upper().tolist()

    # Load Symbol → CIK mapping
    df_map = pd.read_csv(MAPPING_PATH)
    df_map["Symbol"] = df_map["symbol"].str.upper()

    # Filter to keep only symbols in S&P500.csv
    df_final = df_map[df_map["Symbol"].isin(symbols_needed)]

    print(f"S&P500.csv contains {len(symbols_needed)} symbols")
    print(f"Found {len(df_final)} symbols with valid CIK mapping")

    success = 0

    with tqdm(total=len(df_final), desc="Downloading companyfacts", unit="file") as pbar:
        for _, row in df_final.iterrows():
            symbol = row["Symbol"]
            cik = row["cik"]

            ok = fetch_json(cik)
            success += 1 if ok else 0

            pbar.update(1)
            pbar.set_postfix({
                "symbol": symbol,
                "success": f"{success}/{len(df_final)}"
            })

    print(f"\nDone. Successfully downloaded {success}/{len(df_final)} files.")
        
if __name__ == "__main__":
    main()