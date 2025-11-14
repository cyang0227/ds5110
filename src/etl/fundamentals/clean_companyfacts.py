import json
import pandas as pd
from pathlib import Path

INPUT_DIR = Path("data/raw/fundamentals/edgar/companyfacts")
OUTPUT_PATH = Path("data/curated/fundamentals/edgar_fundamentals.parquet")
OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

START_YEAR = 2017

FIN_MAP = {
    "net_income": "NetIncomeLoss",
    "assets": "Assets",
    "equity": "StockholdersEquity",
    "revenue": "Revenues",
    "gross_profit": "GrossProfit",
    "operating_income": "OperatingIncomeLoss",
    "eps": "EarningsPerShareDiluted"
}

MAP_PATH = Path("data/raw/sec_filing/SP500_cik_mapping.csv")
df_map = pd.read_csv(MAP_PATH)
df_map["symbol"] = df_map["symbol"].str.upper()
CIK_TO_SYMBOL = dict(zip(df_map["cik"].astype(str).str.zfill(10), df_map["symbol"]))


def extract_from_json(fp: Path):
    cik_padded = fp.stem.replace("CIK", "")  # e.g., 0000320193
    symbol = CIK_TO_SYMBOL.get(cik_padded)

    # This file is not in the mapping (rare), skip
    if not symbol:
        print(f"[WARN] No symbol found for CIK {cik_padded}")
        return []

    try:
        data = json.loads(fp.read_text())
    except Exception as e:
        print("[ERROR] cannot read:", fp.name, e)
        return []

    facts = data.get("facts", {}).get("us-gaap", {})
    rows = []

    for metric, tag in FIN_MAP.items():
        if tag not in facts:
            continue

        items = facts[tag].get("units", {}).get("USD", [])
        if not items:
            continue

        for it in items:
            end = it.get("end")
            val = it.get("val")
            if not end or val is None:
                continue

            year = int(end[:4])
            if year < START_YEAR:
                continue

            rows.append({
                "symbol": symbol,
                "metric": metric,
                "period_end": end,
                "value": val,
                "period_type": it.get("fp"),
                "form": it.get("form")
            })

    return rows


def main():
    all_rows = []

    for fp in INPUT_DIR.glob("CIK*.json"):
        rows = extract_from_json(fp)
        all_rows.extend(rows)

    df = pd.DataFrame(all_rows)
    df.to_parquet(OUTPUT_PATH)

    print("Saved:", OUTPUT_PATH, "| rows:", len(df))


if __name__ == "__main__":
    main()
