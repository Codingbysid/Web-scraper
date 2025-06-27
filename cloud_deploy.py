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
# --- Part 2: Enhanced Scraping Functions ---

# --- Amazon Scraping Functions ---
def get_amazon_title(soup):
    try:
        title_element = soup.find("span", attrs={"id": "productTitle"})
        if title_element:
            return title_element.text.strip()
        return ""
    except AttributeError:
        return ""

def get_amazon_price(soup):
    try:
        # Check for the standard price first
        price_whole = soup.find("span", attrs={'class': 'a-price-whole'}) 
        price_fraction = soup.find("span", attrs={'class': 'a-price-fraction'}) 
        if price_whole and price_fraction:
            raw_price = price_whole.text + price_fraction.text
            return clean_price(raw_price)
        
        # If the standard price isn't found, check for a deal price
        deal_price = soup.find("span", attrs={"id": "priceblock_dealprice"}) 
        if deal_price:
            return clean_price(deal_price.text.strip())
        
        # Check for other price patterns
        price_element = soup.find("span", class_="a-price-current")
        if price_element:
            return clean_price(price_element.text.strip())
            
        return ""
    except AttributeError:
        return ""

def get_amazon_availability(soup):
    try:
        availability_div = soup.find("div", attrs={"id": "availability"}) 
        if availability_div:
            availability_text = availability_div.find("span")
            if availability_text:
                return availability_text.text.strip()
        return "Not Available"
    except AttributeError:
        return "Not Available"

def get_amazon_promo_flag(soup):
    try:
        # Check for deal price as promo indicator
        if soup.find("span", attrs={'id': 'priceblock_dealprice'}):
            return "Yes"
        # Check for savings badge
        if soup.find("span", class_="a-color-price"):
            return "Yes"
        return "No"
    except AttributeError:
        return "No"

def scrape_amazon_product_data(soup, product_url):
    """
    Scrapes a single Amazon product page for the required details.
    """
    if not soup:
        return None
    
    # Check if Amazon blocked the request
    if "api-services-support@amazon.com" in soup.text:
        logger.warning(f"Request blocked by Amazon for URL: {product_url}")
        return None

    return {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'website_name': 'Amazon',
        'name': get_amazon_title(soup),
        'price': get_amazon_price(soup),
        'availability': get_amazon_availability(soup),
        'promo': get_amazon_promo_flag(soup),
        'url': product_url
    }

# --- eBay Scraping Functions ---
def get_ebay_title(soup):
    try:
        # Common selectors for eBay titles
        title = soup.find("h1", class_="x-item-title__mainTitle")
        if title:
            return title.text.strip()
        title = soup.find("h1", id="itemTitle") # Older selector
        if title:
            return title.text.strip().replace("Details about", "").strip()
        return ""
    except Exception:
        return ""

def get_ebay_price(soup):
    try:
        # Common selectors for eBay prices (Buy It Now)
        price = soup.find("div", class_="x-price-primary")
        if price:
            price_span = price.find("span", class_="ux-textspans")
            if price_span:
                return clean_price(price_span.text.strip())
        
        # For auction prices or other formats
        price = soup.find("span", id="prcIsum")
        if price:
            return clean_price(price.text.strip())
        
        price = soup.find("span", class_="notranslate") # Another common price class
        if price:
            return clean_price(price.text.strip())
        return ""
    except AttributeError:
        return ""

def get_ebay_availability(soup):
    try:
        # Check for "Sold" or "Out of stock" messages
        sold_status = soup.find("span", class_="vi-qty-pur-txt")
        if sold_status and "sold" in sold_status.text.lower():
            return "Sold"
        
        quantity_input = soup.find("input", id="qtyTextBox")
        if quantity_input and quantity_input.get('value') == '0':
            return "Out of Stock"

        # Check for "Quantity available"
        qty_available = soup.find("span", id="qtySubTxt")
        if qty_available:
            return qty_available.text.strip()

        return "In Stock" # Default if no specific message found
    except AttributeError:
        return "Unknown"

def get_ebay_promo_flag(soup):
    try:
        # Look for "Best Offer" or "Sale" indicators
        best_offer = soup.find("span", class_="vi-bbox-btn__text", string=re.compile(r"Best Offer"))
        if best_offer:
            return "Yes (Best Offer)"
        
        sale_price = soup.find("span", class_="vi-original-price") # Check for original price crossed out
        if sale_price:
            return "Yes (Sale)"
        
        return "No"
    except AttributeError:
        return "No"

def scrape_ebay_product_data(soup, product_url):
    """
    Scrapes a single eBay product page for the required details.
    """
    if not soup:
        return None

    # eBay might block requests, check for common signs
    if "Access Denied" in soup.text or "captcha" in soup.text.lower():
        logger.warning(f"Request blocked by eBay for URL: {product_url}")
        return None

    return {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'website_name': 'eBay',
        'name': get_ebay_title(soup),
        'price': get_ebay_price(soup),
        'availability': get_ebay_availability(soup),
        'promo': get_ebay_promo_flag(soup),
        'url': product_url
    }

# --- Walmart Scraping Functions ---
def get_walmart_title(soup):
    try:
        # Common selectors for Walmart titles
        title = soup.find("h1", attrs={"itemprop": "name"})
        if title:
            return title.text.strip()
        title = soup.find("h1", class_="product-title-text") # Another common title class
        if title:
            return title.text.strip()
        return ""
    except AttributeError:
        return ""

def get_walmart_price(soup):
    try:
        # Common selectors for Walmart prices
        price = soup.find("span", attrs={"itemprop": "price"})
        if price:
            return clean_price(price.text.strip())
        price = soup.find("span", class_="price-characteristic") # Another common price class
        if price:
            return clean_price(price.text.strip())
        return ""
    except AttributeError:
        return ""

def get_walmart_availability(soup):
    try:
        # Check for out of stock messages
        out_of_stock = soup.find("div", class_="out-of-stock-message")
        if out_of_stock:
            return "Out of Stock"
        
        # Check for in stock messages or add to cart button
        add_to_cart_button = soup.find("button", class_="add-to-cart-button")
        if add_to_cart_button:
            return "In Stock"
        
        return "Unknown"
    except AttributeError:
        return "Unknown"

def get_walmart_promo_flag(soup):
    try:
        # Look for sale badges or discounted prices
        promo_badge = soup.find("span", class_="price-badge-text")
        if promo_badge and "sale" in promo_badge.text.lower():
            return "Yes"
        
        # Check for original price crossed out
        old_price = soup.find("span", class_="strike-through")
        if old_price:
            return "Yes"
        
        return "No"
    except AttributeError:
        return "No"

def scrape_walmart_product_data(soup, product_url):
    """
    Scrapes a single Walmart product page for the required details.
    """
    if not soup:
        return None

    # Walmart might block requests, check for common signs
    if "Access Denied" in soup.text or "captcha" in soup.text.lower():
        logger.warning(f"Request blocked by Walmart for URL: {product_url}")
        return None

    return {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'website_name': 'Walmart',
        'name': get_walmart_title(soup),
        'price': get_walmart_price(soup),
        'availability': get_walmart_availability(soup),
        'promo': get_walmart_promo_flag(soup),
        'url': product_url
    }

# --- Enhanced Target Scraping Functions ---
def get_target_title(soup):
    try:
        # Updated selectors based on current Target HTML structure
        title = soup.find("h1", attrs={"data-test": "product-title"})
        if title:
            return title.text.strip()
        
        # Alternative title selectors
        title = soup.find("h1", class_="styles__StyledHeading-sc-1fx9mxj-0")
        if title:
            return title.text.strip()
            
        # Look for any h1 with product-like content
        title = soup.find("h1")
        if title and len(title.text.strip()) > 5:  # Basic validation
            return title.text.strip()
            
        return ""
    except AttributeError:
        return ""

def get_target_price(soup):
    try:
        # Target often uses JavaScript to load prices dynamically
        # Look for multiple possible price locations
        
        # Method 1: Look for spans with price-like text patterns
        all_spans = soup.find_all("span")
        price_spans = []
        for span in all_spans:
            if span.text and span.text.strip():
                text = span.text.strip()
                # Look for price patterns: $X.XX, $X, etc.
                if re.search(r'\$\d+\.?\d*', text):
                    price_spans.append(text)
        
        # If we found price-like spans, clean and return the first valid one
        for price_text in price_spans:
            cleaned = clean_price(price_text)
            if cleaned and float(cleaned) > 0:
                return cleaned
        
        # Method 2: Look for common Target price selectors
        price_selectors = [
            {"tag": "span", "class": re.compile(r"h-text-xl.*font-bold")},
            {"tag": "div", "attrs": {"data-test": "product-price"}},
            {"tag": "span", "class": "h-text-bs"},
            {"tag": "span", "class": "price-current"},
            {"tag": "span", "class": "sr-only"},
        ]
        
        for selector in price_selectors:
            if "class" in selector:
                elements = soup.find_all(selector["tag"], class_=selector["class"])
            elif "attrs" in selector:
                elements = soup.find_all(selector["tag"], attrs=selector["attrs"])
            else:
                continue
            
            for element in elements:
                if element.text and "$" in element.text:
                    cleaned = clean_price(element.text.strip())
                    if cleaned:
                        return cleaned
        
        # Method 3: Look in script tags for JSON data (sometimes prices are in structured data)
        scripts = soup.find_all("script", type="application/ld+json")
        for script in scripts:
            try:
                import json
                data = json.loads(script.string)
                if isinstance(data, dict):
                    # Look for price in structured data
                    price = data.get("offers", {}).get("price") or data.get("price")
                    if price:
                        cleaned = clean_price(str(price))
                        if cleaned:
                            return cleaned
            except (json.JSONDecodeError, AttributeError):
                continue
        
        # Method 4: Look for price in meta tags
        meta_price = soup.find("meta", attrs={"property": "product:price:amount"})
        if meta_price and meta_price.get("content"):
            cleaned = clean_price(meta_price.get("content"))
            if cleaned:
                return cleaned
        
        # Method 5: Check for price in any text content containing dollar signs
        page_text = soup.get_text()
        dollar_matches = re.findall(r'\$\d+\.?\d*', page_text)
        for match in dollar_matches:
            cleaned = clean_price(match)
            if cleaned and 0.01 <= float(cleaned) <= 10000:  # Reasonable price range
                return cleaned
        
        return ""
    except (AttributeError, ValueError, ImportError):
        return ""

def get_target_availability(soup):
    try:
        # Check for out of stock messages
        out_of_stock = soup.find("div", class_="styles__StyledAvailability-sc-1fx9mxj-0", 
                                string=re.compile(r"Out of stock", re.IGNORECASE))
        if out_of_stock:
            return "Out of Stock"
        
        # Check for in stock messages or add to cart button
        add_to_cart_button = soup.find("button", attrs={"data-test": "add-to-cart-button"})
        if add_to_cart_button:
            return "In Stock"
        
        # Look for availability text
        availability_text = soup.find("div", class_="styles__StyledAvailability-sc-1fx9mxj-0")
        if availability_text:
            text = availability_text.text.strip().lower()
            if "in stock" in text:
                return "In Stock"
            elif "out of stock" in text:
                return "Out of Stock"
            elif "limited stock" in text:
                return "Limited Stock"

        # Default to checking for presence of add to cart functionality
        if soup.find("button", string=re.compile(r"Add to cart", re.IGNORECASE)):
            return "In Stock"

        return "Unknown"
    except AttributeError:
        return "Unknown"

def get_target_promo_flag(soup):
    try:
        # Look for sale indicators in the price area
        # Check for crossed-out regular price (indicates sale)
        crossed_out_price = soup.find("span", class_="line-through")
        if crossed_out_price:
            return "Yes"
        
        # Look for "Sale" text
        sale_text = soup.find("span", class_="text-red-600")
        if sale_text and "sale" in sale_text.text.lower():
            return "Yes"
        
        # Look for discount indicators
        discount_indicators = soup.find_all("span", string=re.compile(r"save|off|%", re.IGNORECASE))
        if discount_indicators:
            return "Yes"
        
        # Check for multiple price elements (usually indicates sale vs regular price)
        price_elements = soup.find_all("span", class_=re.compile(r"h-text-xl.*font-bold|text-gray-500"))
        if len(price_elements) > 1:
            return "Yes (Potential Sale)"
        
        return "No"
    except AttributeError:
        return "No"

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
