import yfinance as yf
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from typing import List
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SP500PriceFetcherBatch:
    '''
    Fetches historical price data for all S&P 500 companies in batches.
    Args:
        start_date (str): The start date'YYYY-MM-DD'
        end_date (str): The end date 'YYYY-MM-DD'. Defaults to current date if None.
        data_dir (str): Directory to save the fetched data.
    '''
    def __init__(self, start_date: str = "2017-01-01", end_date: str = None, data_dir: str = "data"):
        self.start_date = start_date
        self.end_date = end_date if end_date else datetime.now().strftime("%Y-%m-%d")
        self.data_dir = Path(data_dir)
        self.raw_dir = self.data_dir / "raw" / "price" / "source=yahoo"

        self.raw_dir.mkdir(parents=True, exist_ok=True)

        self.all_data = []

    def load_sp500_symbols(self, csv_path: str = "S&P500.csv") -> pd.DataFrame:
        '''
        Loads S&P 500 symbols from a CSV file.
        Args:
            csv_path (str): Path to the CSV file containing S&P 500 symbols.
        Returns:
            pd.DataFrame: DataFrame containing S&P 500 symbols.
        '''
        try:
            df = pd.read_csv(csv_path, encoding = 'utf-8')
            logger.info(f"Loaded {len(df)} S&P 500 symbols from {csv_path}")
            return df
        except Exception as e:
            logger.error(f"Error loading S&P 500 symbols: {e}")
            raise

    def fetch_symbol_data(self, symbol: str) -> pd.DataFrame:
        '''
        Fetches historical price data for a given symbol.
        Args:
            symbol (str): Stock symbol.
        Returns:
            pd.DataFrame: DataFrame containing price data for the symbol.
        '''
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(
                start = self.start_date,
                end = self.end_date,
                interval = "1d",
                auto_adjust = False
            )

            if df.empty:
                logger.warning(f"No data found for symbol: {symbol}")
                return pd.DataFrame()
            
            df.reset_index(inplace=True)

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

            df['symbol'] = symbol

            columns = ["symbol", "trade_date", "open", "high", "low", "close", "volume", "dividends", "split_ratio"]
            df = df[columns]
            
            logger.info(f"Fetched data for symbol: {symbol}, {len(df)} records")
            return df
        except Exception as e:
            logger.error(f"Error fetching data for symbol {symbol}: {e}")
            return pd.DataFrame()
        
    def collect_data(self, symbols: List[str], delay: float = 0.5):
        '''
        Collects data for all symbols in batches.
        Args:
            symbols (List[str]): List of stock symbols.
            delay (float): Delay between requests to avoid rate limiting.
        '''
        total = len(symbols)
        success_count = 0
        failed_count = 0
        failed_symbols = []

        logger.info(f"Starting fetching data for {total} symbols...")
        logger.info(f"Start Time: {self.start_date}, End Time: {self.end_date}")

        for i, symbol in enumerate(symbols, 1):
            try:
                df = self.fetch_symbol_data(symbol)
                if not df.empty:
                    success_count += 1
                    self.all_data.append(df)
                else:
                    failed_count += 1
                    failed_symbols.append(symbol)

                if i % 10 == 0:
                    logger.info(f"Progress: {i}/{total} symbols ({(i/total)*100:.1f}%)")

                time.sleep(delay)

            except Exception as e:
                logger.error(f"Exception for symbol {symbol}: {e}")
                failed_count += 1
                failed_symbols.append(symbol)
                continue
        
        logger.info("=" * 60)
        logger.info(f"Total symbols processed: {total}")
        logger.info(f"Successful fetches: {success_count}")
        logger.info(f"Failed fetches: {failed_count}")

        if failed_symbols:
            logger.info("Failed symbols:")
            if len(failed_symbols) > 20:
                logger.warning(f"... and {len(failed_symbols) - 20} more failed")

    def save_all_data(self):
        '''
        Saves data to a YYYY-MM-DD.parquet file.
        '''
        if not self.all_data:
            logger.warning("No data to save.")
            return
        
        logger.info("=" * 60)
        logger.info("Saving all fetched data...")

        combined_df = pd.concat(self.all_data, ignore_index=True)
        logger.info(f"Total {len(combined_df)} records to save.")

        combined_df['year'] = pd.to_datetime(combined_df['trade_date']).dt.year
        combined_df['month'] = pd.to_datetime(combined_df['trade_date']).dt.month

        saved_files = 0

        for (year, month), group_df in combined_df.groupby(['year', 'month']):
            partition_path = self.raw_dir / f"year={year}" / f"month={month:02d}"
            partition_path.mkdir(parents=True, exist_ok=True)

            save_df = group_df.drop(columns=['year', 'month'])
            save_df = save_df.sort_values(by=['symbol', 'trade_date'])

            min_date = save_df['trade_date'].min()
            max_date = save_df['trade_date'].max()

            filename = f"part_{year}-{month:02d}_{min_date.strftime('%Y%m%d')}_{max_date.strftime('%Y%m%d')}.parquet"

            file_path = partition_path / filename
            save_df.to_parquet(file_path, index=False, engine='pyarrow')

            unique_symbols = save_df['symbol'].nunique()
            logger.info(
                f"Saved {file_path}\n"
                f"  Records: {len(save_df)}, Unique Symbols: {unique_symbols}, \n"
                f"  Date Range: {min_date} to {max_date}"
            )
            saved_files += 1
        
        logger.info("=" * 60)
        logger.info(f"Total files saved: {saved_files}")

    def generate_summary_report(self):
        '''
        Generates a summary report of the fetched data.
        '''
        
        if self.raw_dir.exists():
            for year_dir in sorted(self.raw_dir.glob("year=*")):
                year = year_dir.name.split('=')[1]
                for month_dir in sorted(year_dir.glob("month=*")):
                    month = month_dir.name.split('=')[1]
                    files = list(month_dir.glob("*.parquet"))

                    if files:
                        df = pd.read_parquet(files[0])
                        symbols_count = df['symbol'].nunique()
                        records_count = len(df)
                        logger.info(
                            f" Year: {year}, Month: {month}, Files: {len(files)}, "
                            f"Symbols: {symbols_count}, Records: {records_count}"
                        )

def main():
    fetcher = SP500PriceFetcherBatch(start_date="2017-01-01",
                                     end_date=None,
                                        data_dir="data")
    sp500_df = fetcher.load_sp500_symbols(csv_path="S&P500.csv")
    symbols = sp500_df['Symbol'].tolist()

    fetcher.collect_data(symbols=symbols, delay=0.5)
    fetcher.save_all_data()
    fetcher.generate_summary_report()

if __name__ == "__main__":
    main()