# ======================================================
# DS5110 - Fetch S&P 500 Daily Price Data from Yahoo Finance
# By Chen Yang
# ======================================================

import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from typing import List
import time
from tqdm import tqdm
import argparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class SP500PriceFetcherBatch:
    def __init__(self, start_date: str = "2017-01-01", end_date: str = None, data_dir: str = "data"):
        self.start_date = start_date
        self.end_date = end_date if end_date else datetime.now().strftime("%Y-%m-%d")
        self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "raw" / "prices" / "source=yahoo"
        self.tmp_dir = self.raw_dir / "tmp"
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        self.all_data = []

    def load_sp500_symbols(self, csv_path: str = "S&P500.csv") -> pd.DataFrame:
        try:
            df = pd.read_csv(csv_path, encoding="utf-8")
            logger.info(f"Loaded {len(df)} S&P 500 symbols from {csv_path}")
            return df
        except Exception as e:
            logger.error(f"Error loading S&P 500 symbols: {e}")
            raise

    def fetch_symbol_data(self, symbol: str) -> pd.DataFrame:
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(
                start=self.start_date,
                end=self.end_date,
                interval="1d",
                auto_adjust=False
            )
            if df.empty:
                logger.warning(f"No data found for symbol: {symbol}")
                return pd.DataFrame()

            df.reset_index(inplace=True)
            if "Adj Close" in df.columns:
                df.rename(columns={"Adj Close": "adj_close"}, inplace=True)
            else:
                df["adj_close"] = df["Close"]

            df = df.rename(columns={
                "Date": "trade_date",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Dividends": "dividends",
                "Stock Splits": "split_ratio"
            })
            df["symbol"] = symbol

            price_cols = ["open", "high", "low", "close", "adj_close", "dividends", "split_ratio"]
            df[price_cols] = df[price_cols].round(2)

            df = df[["symbol", "trade_date", "open", "high", "low", "close", "adj_close",
                     "volume", "dividends", "split_ratio"]]
            return df

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {e}")
            return pd.DataFrame()

    def collect_data(self, symbols: List[str], delay: float = 0.5):
        total = len(symbols)
        success, failed = 0, []
        logger.info(f"Starting fetching data for {total} symbols...")
        done_symbols = {f.stem.split("_")[0] for f in self.tmp_dir.glob("*.parquet")}
        symbols_to_run = [s for s in symbols if s not in done_symbols]
        logger.info(f"Resume mode: {len(done_symbols)} done, {len(symbols_to_run)} remaining.")

        for symbol in tqdm(symbols_to_run, desc="Fetching Progress", ncols=80):
            try:
                df = self.fetch_symbol_data(symbol)
                if not df.empty:
                    success += 1
                    tmp_file = self.tmp_dir / f"{symbol}_{self.end_date}.parquet"
                    df.to_parquet(tmp_file, index=False, engine="pyarrow")
                else:
                    failed.append(symbol)
                time.sleep(delay)
            except Exception as e:
                logger.error(f"Exception for {symbol}: {e}")
                failed.append(symbol)

        logger.info("=" * 60)
        logger.info(f"Successful: {success}, Failed: {len(failed)}")
        if failed:
            logger.warning(f"Failed symbols: {failed[:10]}{' ...' if len(failed) > 10 else ''}")
        self.load_tmp_data()

    def load_tmp_data(self):
        tmp_files = list(self.tmp_dir.glob("*.parquet"))
        if not tmp_files:
            logger.warning("No temporary data found to load.")
            return
        dfs = [pd.read_parquet(f) for f in tmp_files]
        self.all_data = dfs
        logger.info(f"Loaded {len(dfs)} temp files ({sum(len(df) for df in dfs)} records).")

    def save_all_data(self):
        if not self.all_data:
            logger.warning("No data to save.")
            return
        combined_df = pd.concat(self.all_data, ignore_index=True)
        numeric_cols = ["open", "high", "low", "close", "adj_close", "dividends", "split_ratio"]
        combined_df[numeric_cols] = combined_df[numeric_cols].round(2)
        combined_df["year"] = pd.to_datetime(combined_df["trade_date"]).dt.year
        combined_df["month"] = pd.to_datetime(combined_df["trade_date"]).dt.month

        for (year, month), group in combined_df.groupby(["year", "month"]):
            partition_path = self.raw_dir / f"year={year}" / f"month={month:02d}"
            partition_path.mkdir(parents=True, exist_ok=True)
            save_df = group.drop(columns=["year", "month"]).sort_values(["symbol", "trade_date"])
            filename = f"part_{year}-{month:02d}_{save_df.trade_date.min().strftime('%Y%m%d')}_{save_df.trade_date.max().strftime('%Y%m%d')}.parquet"
            save_df.to_parquet(partition_path / filename, index=False, engine="pyarrow")
            logger.info(f"Saved: {filename} ({len(save_df)} records)")

        logger.info(" All monthly partitions saved successfully!")

    def generate_summary_report(self):
        if not self.raw_dir.exists():
            logger.warning("No raw data directory found.")
            return
        logger.info("=" * 60)
        logger.info("Summary Report:")
        for year_dir in sorted(self.raw_dir.glob("year=*")):
            year = year_dir.name.split("=")[1]
            for month_dir in sorted(year_dir.glob("month=*")):
                month = month_dir.name.split("=")[1]
                files = list(month_dir.glob("*.parquet"))
                if not files:
                    continue
                df = pd.read_parquet(files[0])
                logger.info(f" Year {year}, Month {month}, Files: {len(files)}, Symbols: {df.symbol.nunique()}, Records: {len(df)}")

# ================================
# Detect latest date for incremental update
# ================================
def detect_latest_date(raw_dir: Path) -> str:
    """Return the max trade_date in all existing parquet files."""
    parquet_files = sorted(raw_dir.glob("**/*.parquet"))
    if not parquet_files:
        logger.info("No existing parquet files found; full refresh mode.")
        return "2017-01-01"
    dfs = []
    for f in parquet_files[-3:]:  # only check the last 3 files for efficiency
        try:
            df = pd.read_parquet(f, columns=["trade_date"])
            dfs.append(df)
        except Exception:
            pass
    if not dfs:
        return "2017-01-01"
    last_date = max(pd.concat(dfs)["trade_date"])
    next_day = (pd.to_datetime(last_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"Detected latest date: {last_date}, next fetch starts from {next_day}")
    return next_day

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--incremental", action="store_true", help="Run incremental update mode")
    args = parser.parse_args()

    fetcher = SP500PriceFetcherBatch(data_dir="data")
    if args.incremental:
        fetcher.start_date = detect_latest_date(fetcher.raw_dir)
        fetcher.end_date = None

    sp500_df = fetcher.load_sp500_symbols("data/raw/S&P500.csv")
    symbols = sp500_df["Symbol"].tolist()
    fetcher.collect_data(symbols, delay=0.5)
    fetcher.save_all_data()
    fetcher.generate_summary_report()

if __name__ == "__main__":
    main()
