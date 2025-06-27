import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import gspread
from gspread_dataframe import set_with_dataframe
from datetime import datetime
import time
import re
import os
import random
import logging
from typing import Optional

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Utility Functions ---
def clean_price(price_text: str) -> str:
    if not price_text or not isinstance(price_text, str):
        return ""
    price_text = price_text.replace("Sale Price", "").replace("Current Price", "")
    price_text = price_text.replace("reg", "").replace("was", "").replace("now", "")
    price_patterns = re.findall(r'[\$£€¥]?(\d{1,3}(?:[,\.]\d{3})*(?:[,\.]\d{2})?)', price_text)
    if price_patterns:
        main_price = price_patterns[0]
        if ',' in main_price and '.' in main_price:
            if main_price.rfind(',') > main_price.rfind('.'):
                main_price = main_price.replace('.', '').replace(',', '.')
            else:
                main_price = main_price.replace(',', '')
        elif ',' in main_price and len(main_price.split(',')[-1]) == 2:
            main_price = main_price.replace(',', '.')
        else:
            main_price = main_price.replace(',', '')
        try:
            float(main_price)
            return main_price
        except ValueError:
            return ""
    return ""

def get_soup_with_retry(url: str, max_retries: int = 3, base_delay: float = 1.0) -> Optional[BeautifulSoup]:
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept-Language': 'en-US, en;q=0.9',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1}/{max_retries} for URL: {url}")
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            return BeautifulSoup(response.content, "html.parser")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt)
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"All {max_retries} attempts failed for {url}")
                return None
    return None

# --- Google Sheets Setup ---
def setup_google_sheets_client():
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        client = gspread.service_account(filename='credentials.json', scopes=scopes)  # type: ignore
        logger.info("Successfully authenticated with Google Sheets API")
        return client
    except FileNotFoundError:
        logger.error("'credentials.json' not found. Please ensure the Google Cloud service account credentials file is in the same directory.")
        return None
    except Exception as e:
        logger.error(f"An error occurred during Google Sheets authentication: {e}")
        return None

def get_or_create_worksheet(client, sheet_name, worksheet_name):
    try:
        sheet = client.open(sheet_name)
        logger.info(f"Opened existing spreadsheet: {sheet_name}")
    except gspread.SpreadsheetNotFound:
        logger.info(f"Spreadsheet '{sheet_name}' not found. Creating a new one.")
        sheet = client.create(sheet_name)
        logger.info(f"Created new spreadsheet: {sheet_name}")
    try:
        worksheet = sheet.worksheet(worksheet_name)
        logger.info(f"Found existing worksheet: {worksheet_name}")
    except gspread.WorksheetNotFound:
        logger.info(f"Worksheet '{worksheet_name}' not found. Creating a new one.")
        worksheet = sheet.add_worksheet(title=worksheet_name, rows="1000", cols="20")
    return worksheet

# --- Scraping Functions (Amazon, eBay, Walmart, Target) ---
# (Copy all scraping functions from scraper-manus.py here)
# For brevity, only the main scrape_product_data function is shown, but you should copy all helpers as in scraper-manus.py

def scrape_product_data(product_url):
    logger.info(f"Fetching page for: {product_url}")
    soup = get_soup_with_retry(product_url)
    if not soup:
        logger.error(f"Failed to fetch page: {product_url}")
        return None
    try:
        if "amazon.com" in product_url:
            # (Copy the Amazon scraping logic from scraper-manus.py)
            pass
        elif "ebay.com" in product_url:
            # (Copy the eBay scraping logic from scraper-manus.py)
            pass
        elif "walmart.com" in product_url:
            # (Copy the Walmart scraping logic from scraper-manus.py)
            pass
        elif "target.com" in product_url:
            # (Copy the Target scraping logic from scraper-manus.py)
            pass
        else:
            logger.warning(f"Unsupported URL: {product_url}")
            return None
    except Exception as e:
        logger.error(f"Error scraping {product_url}: {e}")
        return None

# --- Main Execution ---
def run_scraper():
    # Allow for environment variable overrides for sheet/worksheet names
    GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "Retailer Data")
    WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "Product_Scrapes")
    # Product URLs (can be set via env or hardcoded)
    PRODUCT_URLS = [
        # Add your product URLs here, or load from env/config
        "https://www.amazon.com/Graco-DuoGlider-Connect-Stroller-Glacier/dp/B01GHVJHMW/",
        "https://www.amazon.com/CeraVe-Moisturizing-Cream-Daily-Moisturizer/dp/B00TTD9BRC/",
        "https://www.ebay.com/itm/196527782223",
        "https://www.ebay.com/itm/135835897421",
        "https://www.walmart.com/ip/DreamFish-Womens-Nightgown-Short-Sleeve-Sleepshirt-V-Neck-Sleepwear-Casual-Loungewear-Ladies-Sleepwear/5055405942",
        "https://www.target.com/p/patriotic-vanilla-mini-cupcakes-10oz-12ct-favorite-day-8482/-/A-87306034",
    ]
    logger.info("Starting enhanced scraper...")
    gspread_client = setup_google_sheets_client()
    if gspread_client:
        worksheet = get_or_create_worksheet(gspread_client, GOOGLE_SHEET_NAME, WORKSHEET_NAME)
        all_products_data = []
        successful_scrapes = 0
        failed_scrapes = 0
        for i, url in enumerate(PRODUCT_URLS):
            logger.info(f"Scraping product {i+1}/{len(PRODUCT_URLS)}: {url}")
            product_data = scrape_product_data(url)
            if product_data and product_data.get('name'):
                all_products_data.append(product_data)
                successful_scrapes += 1
                logger.info(f"✓ Successfully scraped: {product_data['name']} - Price: {product_data['price']}")
            else:
                failed_scrapes += 1
                logger.warning(f"✗ Failed to scrape: {url}")
            delay = 2 if product_data else 4
            time.sleep(delay)
        logger.info(f"Scraping completed: {successful_scrapes} successful, {failed_scrapes} failed")
        if not all_products_data:
            logger.error("No data was scraped. Exiting.")
        else:
            df = pd.DataFrame(all_products_data)
            df.replace('', np.nan, inplace=True)
            df.dropna(subset=['name'], inplace=True)
            df = df[['timestamp', 'website_name', 'name', 'price', 'availability', 'promo', 'url']]
            df.fillna('', inplace=True)
            logger.info("\nScraped Data Preview:")
            print(df.to_string())
            is_sheet_empty = not worksheet.get_all_values()
            if is_sheet_empty:
                logger.info("\nWorksheet is empty. Writing headers and data to Google Sheet...")
                set_with_dataframe(worksheet, df)
            else:
                logger.info("\nWorksheet has data. Appending new rows...")
                worksheet.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')
            logger.info(f"\n✓ Successfully wrote {len(df)} rows to '{WORKSHEET_NAME}' in '{GOOGLE_SHEET_NAME}'")
    else:
        logger.error("Failed to authenticate with Google Sheets. Please check your credentials.json file.")

if __name__ == '__main__':
    run_scraper() 