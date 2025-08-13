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
    "start_year": 2015,
    "end_year": 2024,
    "max_workers": 12,  # Số luồng chạy song song
    "output_dir": "output_data",
    "company_list_filename": "company_list.csv",
    "raw_data_filename": "raw_financials_suffix.parquet",
    "final_data_filename": "final_financial_statements_suffix.parquet",
    "final_data_filename_csv": "final_financial_statements_suffix.csv",
    "mapping_filepath": "account_mapping.json"
}
financial_statement_schemas = ['company_code', 'exchange', 'company_name', 'industry', 'report_type', 'report_date', 'account', 'value', 'account_vi', 'account_en']

# ==============================================================================
# LOGIC SCRAPING (CLASS) - PHIÊN BẢN CẬP NHẬT
# ==============================================================================
class CafeFScraper:
    """Scrapes financial statements for a single company from cafef.vn."""
    BASE_URL = "https://s.cafef.vn/bao-cao-tai-chinh/{}/{}/{}/0/0/0/0/bao-cao-tai-chinh-.chn"
    # CHANGED: Added 'cashflowdirect' for completeness
    ALL_REPORT_TYPES = ['bsheet', 'incsta', 'cashflow', 'cashflowdirect']

    # CHANGED: __init__ now accepts a list of report types
    def __init__(self, symbol: str, start_year: int, report_types: list[str]):
        """
        Initializes the scraper.

        Args:
            symbol (str): The company stock symbol.
            start_year (int): The starting year for scraping data.
            report_types (list[str]): A list of report types to scrape
                                     (e.g., ['bsheet', 'incsta']).
        """
        self.symbol = symbol.upper()
        self.start_year = start_year
        self.end_year = datetime.datetime.now().year
        # CHANGED: The list of reports to scrape is now passed directly
        self.report_types_to_scrape = report_types

    def _fetch_report_table(self, report_type: str, year: int) -> pd.DataFrame | None:
        """
        Fetches the entire financial report table for a given year.
        Returns the specific DataFrame table, or None if it fails.
        """
        url = self.BASE_URL.format(self.symbol, report_type, year)
        try:
            web_data = pd.read_html(url)
            table = web_data[4]
            if table.shape[0] > 1 and table.shape[1] > 1:
                return table
            return table
        except Exception as e:
            logging.debug(f"Could not fetch or parse table from {url}. Error: {e}")
            return None

    def scrape_all_reports(self) -> pd.DataFrame | None:
        """
        Scrapes financial reports for the company based on the list of report
        types provided during initialization.
        """
        company_reports = []
        report_map = {'bsheet': 'Balance Sheet',
                      'incsta': 'Income Statement',
                      'cashflow': 'Cash Flow Statement',
                      'cashflowdirect': 'Direct Cash Flow Statement'
                     }
        
        # This loop now iterates over the list provided in __init__
        for report_type in self.report_types_to_scrape:
            yearly_tables = {}
            for year in range(self.start_year, self.end_year + 1):
                table = self._fetch_report_table(report_type, year)
                if table is not None:
                    yearly_tables[year] = table
            
            if not yearly_tables:
                logging.debug(f"No data found for {self.symbol} - {report_type} in any year.")
                continue
            first_available_table = list(yearly_tables.values())[0]
            categories = first_available_table.iloc[:, 0]

            yearly_data = {
                year: table.iloc[:, 4]
                for year, table in yearly_tables.items()
            }
            
            df_wide = pd.DataFrame(yearly_data)
            df_wide['account'] = categories
            df_long = df_wide.melt(id_vars=['account'], var_name='report_date', value_name='value')
            df_long['report_type'] = report_map.get(report_type)
            company_reports.append(df_long)
        
        if not company_reports:
            return None
            
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
    df.rename(columns={'symbol': 'company_code', 'organ_name': 'company_name'}, inplace=True)
    df.dropna(subset=['company_code', 'report_date', 'account'], inplace=True)
    # df['value'] = pd.to_numeric(df['value'], errors='coerce')
    df['report_date'] = df['report_date'].astype(str)
    
    df['account_vi'] = df['account']
    df['account_en'] = df['account'].apply(lambda x: mapping_dict.get(x, {}).get('english'))
    df['account'] = df['account'].apply(lambda x: mapping_dict.get(x, {}).get('english_format'))
    
    return df[financial_statement_schemas].sort_values(by=['company_code', 'report_type', 'report_date'])

# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
def main():
    """Main pipeline orchestrator."""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # Define the list of all possible report types for argparse
    ALL_REPORTS = ['bsheet', 'incsta', 'cashflow', 'cashflowdirect']
    
    parser = argparse.ArgumentParser(description="A pipeline to scrape financial data from CafeF.")

    parser.add_argument('--reload', type=int, default=0, help="Reload the companies list to scrape: 1-reload, 0-load.")
    parser.add_argument('--limit', type=int, help="Limit the number of companies to scrape for testing.")
    parser.add_argument('--single-thread', action='store_true', help="Run the scraper in a single thread (sequentially).")
    
    # CHANGED: argparse now accepts multiple values for --report-type and defaults to all
    parser.add_argument('--report-type',
                        nargs='*',  # Accept 0 or more values
                        choices=ALL_REPORTS,
                        default=ALL_REPORTS,  # Default to the full list if flag is not used
                        help=f"Scrape specific report types. Can provide multiple (e.g., 'bsheet incsta'). "
                             f"If not specified, all types are scraped: {', '.join(ALL_REPORTS)}.")

    args = parser.parse_args()
    
    output_dir = CONFIG['output_dir']
    os.makedirs(output_dir, exist_ok=True)

    # --- Step 1: Get Company List ---
    company_list_path = os.path.join(output_dir, CONFIG['company_list_filename'])
    try:
        if args.reload == 1:
            company_df = get_company_listing()
            company_df.to_csv(company_list_path, index=False)
            logging.info(f"Saved company list of {len(company_df)} companies to {company_list_path}")
        else:
            company_df = pd.read_csv(company_list_path)
            logging.info(f"Loaded company list of {len(company_df)} companies to {company_list_path}")
    except Exception as e:
        logging.warning(f"Failed to fetch new company list: {e}. Trying to load from existing file.")
        if os.path.exists(company_list_path):
            company_df = pd.read_csv(company_list_path)
            logging.info(f"Loaded {len(company_df)} companies from {company_list_path}")
        else:
            logging.error("No existing company list found. Exiting.")
            return

    # --- Step 2: Scrape Data ---
    symbols_to_scrape = company_df['symbol'].tolist()
    if args.limit:
        symbols_to_scrape = symbols_to_scrape[:args.limit]
        logging.info(f"Scraping limited to {args.limit} companies.")

    all_results = []
    
    # The report types to scrape are now in args.report_type (which is a list)
    logging.info(f"Target report types: {', '.join(args.report_type)}")
    
    if args.single_thread:
        logging.info("Running in single-thread mode.")
        # symbols_to_scrape = ['SSI']
        for symbol in tqdm(symbols_to_scrape, desc="Scraping Financials (Single-Thread)"):
            # logging.info(f"Running in single-thread on {symbol}")
            # CHANGED: Pass the list args.resport_type to the scraper
            scraper = CafeFScraper(symbol, CONFIG['start_year'], report_types=args.report_type)
            result_df = scraper.scrape_all_reports()
            if result_df is not None:
                all_results.append(result_df)
    else:
        logging.info(f"Running in multi-thread mode with {CONFIG['max_workers']} workers.")
        with ThreadPoolExecutor(max_workers=CONFIG['max_workers']) as executor:
            future_to_symbol = {
                # CHANGED: Pass the list args.report_type to the scraper
                executor.submit(CafeFScraper(symbol, CONFIG['start_year'], report_types=args.report_type).scrape_all_reports): symbol
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
    # raw_data_path = os.path.join(output_dir, CONFIG['raw_data_filename'])
    # raw_df.to_parquet(raw_data_path, index=False)
    # logging.info(f"Saved raw scraped data to {raw_data_path}")

    # --- Step 3: Transform Data ---
    with open(CONFIG['mapping_filepath'], 'r', encoding='utf-8') as f:
        account_map = json.load(f)
        
    final_df = transform_data(raw_df, company_df, account_map)

    suffix = args.report_type[0] if len(args.report_type) == 1 else ""
    final_data_path = os.path.join(output_dir, CONFIG['final_data_filename'].replace('_suffix', f'_{suffix}'))
    final_csv_data_path = os.path.join(output_dir, CONFIG['final_data_filename_csv'].replace('_suffix', f'_{suffix}'))
    
    final_df.to_parquet(final_data_path, index=False)
    # final_df.to_csv(final_csv_data_path, sep="\t", index=False)
    
    logging.info(f"Successfully transformed data and saved to {final_data_path} and {final_csv_data_path}")
    logging.info("Pipeline finished.")

if __name__ == "__main__":
    main()