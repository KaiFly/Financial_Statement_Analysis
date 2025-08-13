# Financial Statement Scraper for CafeF

This script automatically downloads financial statements for companies listed on Vietnamese stock exchanges from `cafef.vn`.

It fetches the company list, downloads the data, and then cleans and standardizes the financial statement item names. The final output is a `final_financial_statements.parquet` file in the `output_data` directory, ready for analysis.

## Quick Start

1.  **Install Dependencies:**
    Open a terminal and run the following command in the project directory:
    ```bash
    pip install -r requirements.txt
    ```

2.  **Run the Script:**
    To run the pipeline for a limited number of companies (e.g., 5 for a test run):
    ```bash
    python financial_statement_pipeline.py --limit 5
    ```
    To run for all companies, simply remove the `--limit` flag.

## Customization

* **To change the start year or the number of downloader threads:** Open the `run_pipeline.py` file and edit the `CONFIG` dictionary at the top.
* **To edit or add account translations:** Modify the `account_mapping.json` file.