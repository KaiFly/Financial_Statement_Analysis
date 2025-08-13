import os
import logging
import datetime
import json
import argparse
import pandas as pd
from vnstock import Listing
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# ==============================================================================
# CONFIGURATION - THAY ĐỔI CÁC THAM SỐ TẠI ĐÂY
# ==============================================================================
CONFIG = {
    "start_year": 2017,
    "max_workers": 10,  # Số luồng chạy song song
    "output_dir": "output_data",
    "company_list_filename": "company_list.csv",
    "raw_data_filename": "raw_financials.parquet",
    "final_data_filename": "final_financial_statements.parquet",
    "mapping_filepath": "account_mapping.json"
}

# ==============================================================================
# LOGIC SCRAPING (CLASS)
# ==============================================================================
class CafeFScraper:
    """Scrapes financial statements for a single company from cafef.vn."""
    BASE_URL = "https://s.cafef.vn/bao-cao-tai-chinh/{}/{}/{}/0/0/0/0/luu-chuyen-tien-te-gian-tiep-.chn"
    REPORT_TYPES = ['bsheet', 'incsta', 'cashflow']

    def __init__(self, symbol: str, start_year: int):
        self.symbol = symbol.upper()
        self.start_year = start_year
        self.end_year = datetime.datetime.now().year

    def _fetch_data_for_year(self, report_type: str, year: int) -> pd.Series | None:
        url = self.BASE_URL.format(self.symbol, report_type, year)
        try:
            web_data = pd.read_html(url)
            data_column = web_data[4].iloc[:, 4]
            return data_column if not data_column.isna().all() else None
        except Exception as e:
            logging.debug(f"Could not fetch {url}. Error: {e}")
            return None

    def _get_categories(self, report_type: str) -> pd.Series | None:
        url = self.BASE_URL.format(self.symbol, report_type, self.end_year)
        try:
            return pd.read_html(url)[4].iloc[:, 0]
        except Exception:
            return None

    def scrape_all_reports(self) -> pd.DataFrame | None:
        company_reports = []
        report_map = {'bsheet': 'Balance Sheet', 'incsta': 'Income Statement', 'cashflow': 'Cash Flow Statement'}
        
        for report_type in self.REPORT_TYPES:
            categories = self._get_categories(report_type)
            if categories is None: continue

            yearly_data = {year: self._fetch_data_for_year(report_type, year) for year in range(self.start_year, self.end_year + 1)}
            yearly_data = {k: v for k, v in yearly_data.items() if v is not None}

            if not yearly_data: continue

            df_wide = pd.DataFrame(yearly_data)
            df_wide['account'] = categories
            df_long = df_wide.melt(id_vars=['account'], var_name='report_date', value_name='value')
            df_long['report_type'] = report_map.get(report_type)
            company_reports.append(df_long)
        
        if not company_reports: return None
            
        final_df = pd.concat(company_reports, ignore_index=True)
        final_df['symbol'] = self.symbol
        return final_df

# ==============================================================================
# LOGIC HELPER (FUNCTIONS)
# ==============================================================================
def get_company_listing() -> pd.DataFrame:
    """Fetches a list of companies from HSX and HNX."""
    logging.info("Fetching company list...")
    listing = Listing()
    df_symbols = listing.symbols_by_exchange()
    df_short = df_symbols[df_symbols['exchange'].isin(['HSX', 'HNX']) & (df_symbols['type'] == 'STOCK')]
    
    df_industries = listing.symbols_by_industries()
    df_industry_names = df_industries[['symbol', 'icb_name2']].rename(columns={'icb_name2': 'industry'})
    
    df_final = pd.merge(df_short, df_industry_names, how='left', on='symbol')
    return df_final[['symbol', 'exchange', 'organ_name', 'industry']].dropna(subset=['symbol'])

def transform_data(raw_df: pd.DataFrame, company_info_df: pd.DataFrame, mapping_dict: dict) -> pd.DataFrame:
    """Cleans and transforms raw scraped data."""
    logging.info("Transforming raw data...")
    df = pd.merge(raw_df, company_info_df, on='symbol', how='left')
    df.dropna(subset=['symbol', 'report_date', 'account'], inplace=True)
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df['report_date'] = df['report_date'].astype(str)
    
    df['account_vi'] = df['account']
    df['account_en'] = df['account'].apply(lambda x: mapping_dict.get(x, {}).get('english'))
    df['account'] = df['account'].apply(lambda x: mapping_dict.get(x, {}).get('english_format'))
    
    final_cols = ['symbol', 'exchange', 'organ_name', 'industry', 'report_type', 'report_date', 'account', 'value', 'account_vi', 'account_en']
    return df[final_cols].sort_values(by=['symbol', 'report_type', 'report_date'])

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
def main():
    """Main pipeline orchestrator."""
    # --- Setup ---
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    parser = argparse.ArgumentParser(description="A simple pipeline to scrape financial data from CafeF.")
    parser.add_argument('--limit', type=int, help="Limit the number of companies to scrape for testing.")
    args = parser.parse_args()
    
    output_dir = CONFIG['output_dir']
    os.makedirs(output_dir, exist_ok=True)

    # --- Step 1: Get Company List ---
    company_df = get_company_listing()
    company_list_path = os.path.join(output_dir, CONFIG['company_list_filename'])
    company_df.to_csv(company_list_path, index=False)
    logging.info(f"Saved company list of {len(company_df)} companies to {company_list_path}")

    # --- Step 2: Scrape Data Concurrently ---
    symbols_to_scrape = company_df['symbol'].tolist()
    if args.limit:
        symbols_to_scrape = symbols_to_scrape[:args.limit]
        logging.info(f"Scraping limited to {args.limit} companies.")

    all_results = []
    with ThreadPoolExecutor(max_workers=CONFIG['max_workers']) as executor:
        future_to_symbol = {
            executor.submit(CafeFScraper(symbol, CONFIG['start_year']).scrape_all_reports): symbol
            for symbol in symbols_to_scrape
        }
        progress = tqdm(as_completed(future_to_symbol), total=len(symbols_to_scrape), desc="Scraping Financials")
        for future in progress:
            result_df = future.result()
            if result_df is not None:
                all_results.append(result_df)
    
    if not all_results:
        logging.warning("Scraping finished, but no data was collected.")
        return

    raw_df = pd.concat(all_results, ignore_index=True)
    raw_data_path = os.path.join(output_dir, CONFIG['raw_data_filename'])
    raw_df.to_parquet(raw_data_path, index=False)
    logging.info(f"Saved raw scraped data to {raw_data_path}")

    # --- Step 3: Transform Data ---
    with open(CONFIG['mapping_filepath'], 'r', encoding='utf-8') as f:
        account_map = json.load(f)
        
    final_df = transform_data(raw_df, company_df, account_map)
    final_data_path = os.path.join(output_dir, CONFIG['final_data_filename'])
    final_df.to_parquet(final_data_path, index=False)
    logging.info(f"Successfully transformed data and saved to {final_data_path}")
    logging.info("Pipeline finished.")

if __name__ == "__main__":
    main()