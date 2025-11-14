import json
import pandas as pd
from pathlib import Path

INPUT_DIR = Path("data/raw/fundamental/edgar/companyfacts")
OUTPUT = Path("data/curated/fundamental/fundamentals_clean.parquet")

FIN_MAP = {
    "net_income": "NetIncomeLoss",
    "assets": "Assets",
    "equity": "StockholdersEquity",
    "revenue": "Revenues",
    "gross_profit": "GrossProfit",
    "operating_profit": "OperatingIncomeLoss",
    "eps": "EarningsPerShareDiluted",
    "shares_outstanding": "EntityCommonStockSharesOutstanding",
}

# Filing priority
PRIORITY = {
    "10-K/A": 1,
    "10-K": 2,
}
DEFAULT_PRIORITY = 9


def extract_one_file(path: Path, cik_map):
    cik = path.stem.replace("CIK", "")
    symbol = cik_map.get(cik)
    if not symbol:
        return []

    try:
        data = json.loads(path.read_text())
    except:
        return []

    facts = data.get("facts", {}).get("us-gaap", {})
    rows = []

    for metric, tag in FIN_MAP.items():
        units = facts.get(tag, {}).get("units", {}).get("USD", [])
        for item in units:
            form = item.get("form")
            if form not in ("10-K", "10-K/A"):
                continue

            end = item.get("end")
            val = item.get("val")
            if not end or val is None:
                continue

            year = int(end[:4])
            if year < 2017:
                continue

            rows.append({
                "symbol": symbol,
                "metric": metric,
                "period_end": end,
                "value": val,
                "form": form,
                "priority": PRIORITY.get(form, DEFAULT_PRIORITY),
                "year": year,
            })

    return rows


def main():

    # load cik→symbol mapping
    cik_df = pd.read_csv("data/raw/sec_filing/SP500_cik_mapping.csv")
    cik_df["cik"] = cik_df["cik"].astype(str).str.zfill(10)
    cik_map = dict(zip(cik_df["cik"], cik_df["symbol"]))

    # extract
    all_rows = []
    for fp in INPUT_DIR.glob("CIK*.json"):
        all_rows.extend(extract_one_file(fp, cik_map))

    df = pd.DataFrame(all_rows)
    print("Raw rows:", len(df))

    if df.empty:
        print("No 10-K data found.")
        return

    # Keep only best 10-K per symbol + metric + year
    df_sorted = df.sort_values(["symbol", "metric", "year", "priority"])
    df_clean = df_sorted.groupby(
        ["symbol", "metric", "year"], as_index=False
    ).first()

    # mark annual
    df_clean["period_type"] = "A"

    df_clean = df_clean[["symbol", "metric", "period_end", "period_type", "value"]]

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_parquet(OUTPUT)
    print("Saved:", OUTPUT)


if __name__ == "__main__":
    main()
