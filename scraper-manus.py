
import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime
import time
import re # Import regex for URL parsing
from typing import Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Part 0: Utility Functions ---

def clean_price(price_text: str) -> str:
    """
    Universal price cleaning function to extract only numeric values from price strings.
    Handles various formats: "$19.99", "19.99 USD", "Sale Price $15.99", "$4.99 ($0.50/ounce)", etc.
    Returns only the numeric part as a string, or empty string if no valid price found.
    """
    if not price_text or not isinstance(price_text, str):
        return ""
    
    # Remove common price-related text
    price_text = price_text.replace("Sale Price", "").replace("Current Price", "")
    price_text = price_text.replace("reg", "").replace("was", "").replace("now", "")
    
    # Find all price-like patterns (numbers with optional decimal places)
    # This regex finds patterns like: $123.45, 123.45, 123,45 (European format)
    price_patterns = re.findall(r'[\$£€¥]?(\d{1,3}(?:[,\.]\d{3})*(?:[,\.]\d{2})?)', price_text)
    
    if price_patterns:
        # Take the first (main) price found
        main_price = price_patterns[0]
        # Normalize decimal separators (handle both . and , as decimal)
        # Convert European format (1.234,56) to US format (1234.56)
        if ',' in main_price and '.' in main_price:
            # Format like 1.234,56 - comma is decimal separator
            if main_price.rfind(',') > main_price.rfind('.'):
                main_price = main_price.replace('.', '').replace(',', '.')
            # Format like 1,234.56 - period is decimal separator
            else:
                main_price = main_price.replace(',', '')
        elif ',' in main_price and len(main_price.split(',')[-1]) == 2:
            # Format like 123,45 - comma is decimal separator
            main_price = main_price.replace(',', '.')
        else:
            # Remove thousand separators (commas)
            main_price = main_price.replace(',', '')
        
        # Validate the final number
        try:
            float(main_price)
            return main_price
        except ValueError:
            return ""
    
    return ""

def get_soup_with_retry(url: str, max_retries: int = 3, base_delay: float = 1.0) -> Optional[BeautifulSoup]:
    """
    Fetches webpage with retry logic and exponential backoff.
    Returns BeautifulSoup object or None if all retries fail.
    """
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
                delay = base_delay * (2 ** attempt)  # Exponential backoff
                logger.info(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logger.error(f"All {max_retries} attempts failed for {url}")
                return None
    
    return None

# --- Part 1: Google Sheets Authentication & Setup ---

def setup_google_sheets_client():
    """
    Authenticates with Google Sheets API using service account credentials.
    This version uses the updated gspread authentication method.
    """
    try:
        # Define the scope for the APIs
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        # IMPORTANT: Ensure 'credentials.json' is in the same directory or provide the full path.
        # Uses the modern gspread method to authorize
        client = gspread.service_account(filename='credentials.json', scopes=scopes)
        logger.info("Successfully authenticated with Google Sheets API")
        return client
    except FileNotFoundError:
        logger.error("'credentials.json' not found. Please ensure the Google Cloud service account credentials file is in the same directory.")
        return None
    except Exception as e:
        logger.error(f"An error occurred during Google Sheets authentication: {e}")
        return None

def get_or_create_worksheet(client, sheet_name, worksheet_name):
    """
    Opens a Google Sheet and a specific worksheet. Creates them if they don't exist.
    """
    try:
        sheet = client.open(sheet_name)
        logger.info(f"Opened existing spreadsheet: {sheet_name}")
    except gspread.SpreadsheetNotFound:
        logger.info(f"Spreadsheet '{sheet_name}' not found. Creating a new one.")
        sheet = client.create(sheet_name)
        # IMPORTANT: You may need to share this new sheet with your own email address to view it.
        # The service account email can be found in your credentials.json file.
        logger.info(f"Created new spreadsheet: {sheet_name}")

    try:
        worksheet = sheet.worksheet(worksheet_name)
        logger.info(f"Found existing worksheet: {worksheet_name}")
    except gspread.WorksheetNotFound:
        logger.info(f"Worksheet '{worksheet_name}' not found. Creating a new one.")
        worksheet = sheet.add_worksheet(title=worksheet_name, rows="1000", cols="20")

    return worksheet


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

def scrape_target_product_data(soup, product_url):
    """
    Scrapes a single Target product page for the required details.
    Enhanced with better error handling and current selectors.
    """
    if not soup:
        return None

    # Target might block requests, check for common signs
    if any(phrase in soup.text.lower() for phrase in ["access denied", "captcha", "robot", "blocked"]):
        logger.warning(f"Request blocked by Target for URL: {product_url}")
        return None

    return {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'website_name': 'Target',
        'name': get_target_title(soup),
        'price': get_target_price(soup),
        'availability': get_target_availability(soup),
        'promo': get_target_promo_flag(soup),
        'url': product_url
    }


def scrape_product_data(product_url):
    """
    Determines the website and calls the appropriate scraping function.
    Enhanced with better error handling and retry logic.
    """
    logger.info(f"Fetching page for: {product_url}")
    soup = get_soup_with_retry(product_url)
    if not soup:
        logger.error(f"Failed to fetch page: {product_url}")
        return None

    try:
        if "amazon.com" in product_url:
            return scrape_amazon_product_data(soup, product_url)
        elif "etsy.com" in product_url:
            # Etsy scraping functions are commented out; this will not be called.
            logger.info(f"Etsy scraping is currently disabled for URL: {product_url}")
            return None
        elif "ebay.com" in product_url:
            return scrape_ebay_product_data(soup, product_url)
        elif "walmart.com" in product_url:
            return scrape_walmart_product_data(soup, product_url)
        elif "target.com" in product_url:
            return scrape_target_product_data(soup, product_url)
        else:
            logger.warning(f"Unsupported URL: {product_url}")
            return None
    except Exception as e:
        logger.error(f"Error scraping {product_url}: {e}")
        return None


# --- Part 3: Main Execution ---

if __name__ == '__main__':
    # --- User Configuration ---
    # IMPORTANT: Add the names of your Google Sheet and the worksheet you want to use.
    GOOGLE_SHEET_NAME = "Retailer Data"
    WORKSHEET_NAME = "Product_Scrapes" # Changed worksheet name for clarity

    # IMPORTANT: Replace these with the product URLs you want to track.
    # Include Amazon and eBay examples.
    PRODUCT_URLS = [
        "https://www.amazon.com/Graco-DuoGlider-Connect-Stroller-Glacier/dp/B01GHVJHMW/?_encoding=UTF8&pd_rd_w=ccDy9&content-id=amzn1.sym.4bba068a-9322-4692-abd5-0bbe652907a9&pf_rd_p=4bba068a-9322-4692-abd5-0bbe652907a9&pf_rd_r=GK7BJ9DWP2HKCTA1HAHR&pd_rd_wg=zgdlp&pd_rd_r=a613875f-c012-4f50-a0c7-da4de8b0fbc0&ref_=pd_hp_d_btf_nta-top-picks", # Example: Anker USB C Charger (Amazon  )
        "https://www.amazon.com/CeraVe-Moisturizing-Cream-Daily-Moisturizer/dp/B00TTD9BRC/?_encoding=UTF8&pd_rd_w=n6l0N&content-id=amzn1.sym.4bba068a-9322-4692-abd5-0bbe652907a9&pf_rd_p=4bba068a-9322-4692-abd5-0bbe652907a9&pf_rd_r=RPN8ACWBTZECJFPF0CA8&pd_rd_wg=YqQj0&pd_rd_r=a8d61603-89fe-4c51-b0cc-1d14d8fabdab&ref_=pd_hp_d_btf_nta-top-picks&th=1", # Example: Hydro Flask Bottle (Amazon  )
        "https://www.amazon.com/Invicta-8926OB-Stainless-Automatic-Bracelet/dp/B000JQFX1G/?_encoding=UTF8&pd_rd_w=KBTWR&content-id=amzn1.sym.4bba068a-9322-4692-abd5-0bbe652907a9&pf_rd_p=4bba068a-9322-4692-abd5-0bbe652907a9&pf_rd_r=9ZVQ5V67C3MKPWXYH4AZ&pd_rd_wg=Ifhku&pd_rd_r=6f3853e2-b241-4190-b6fb-45d2f61b1e03&ref_=pd_hp_d_btf_nta-top-picks&th=1",
        
        # Etsy scraping is currently disabled due to complexity in bypassing anti-scraping measures.
        # If you wish to enable it, uncomment the Etsy URLs and the related functions above.

        # Valid eBay individual product listings:
        "https://www.ebay.com/itm/196527782223?_skw=Jordan+12+Retro+Brilliant+Orange+W&var=496303342939&epid=28057815357&itmmeta=01JYJNBRPNVSHS84644W5R8DMJ&hash=item2dc1f7f94f:g:FF8AAOSwZXRmpfa3&itmprp=enc%3AAQAKAAABAMHg7L1Zz0LA5DYYmRTS30n8DkLuDy4CkJStobMNAxTfvuIzwJ6m2gsQwsZ9PQ3aB%2FrFiaK1i%2FNCCnG0t%2Bf70ElpZD%2FCHsUn%2Fco%2B5gydejxT7yJdk%2FyrHKkpkpkH59z9AqEYqoYEXOUc3S39S5dwFl0ZGVLRtrDirxID2QSeQlug3RFjUe%2Be7RXsAVmIgaIxG%2BV%2BtgZ%2F2SN3fAvQohsbmhws2dgP%2BmgJ3Npwpn8swBw2JrTkC2d4cngBWFykhzG27A%2FAKe1UPlw27M9P6AOdypQPqZ67%2B3Y9j0CQvXNU6X4JFKCT%2Fa1T9L9%2FLt90h94mW%2B3nPByKYo5jssgPpXgBncQ%3D%7Ctkp%3ABFBMuIuv1fRl",
        "https://www.ebay.com/itm/135835897421?_trkparms=amclksrc%3DITM%26aid%3D777008%26algo%3DPERSONAL.TOPIC%26ao%3D1%26asc%3D20231108131718%26meid%3Debd90cc7efc64c5699cdf5c246960b10%26pid%3D101910%26rk%3D1%26rkt%3D1%26itm%3D135835897421%26pmt%3D0%26noa%3D1%26pg%3D4375194%26algv%3DFeaturedDealsV2&_trksid=p4375194.c101910.m150506&_trkparms=parentrq%3Aa58f0f821970ab42cfc09872fffc09b0%7Cpageci%3A37d9b4f2-5185-11f0-880c-fe023e7492f2%7Ciid%3A1%7Cvlpname%3Avlp_homepage",
        "https://www.ebay.com/itm/386178100457?_trkparms=amclksrc%3DITM%26aid%3D777008%26algo%3DPERSONAL.TOPIC%26ao%3D1%26asc%3D20231108131718%26meid%3Debd90cc7efc64c5699cdf5c246960b10%26pid%3D101910%26rk%3D1%26rkt%3D1%26itm%3D386178100457&_trksid=p4375194.c101910.m150506&_trkparms=parentrq%3Aa58f0f821970ab42cfc09872fffc09b0%7Cpageci%3A37d9b4f2-5185-11f0-880c-fe023e7492f2%7Ciid%3A1%7Cvlpname%3Avlp_homepage",
        # Valid Walmart individual product listings:
        "https://www.walmart.com/ip/DreamFish-Womens-Nightgown-Short-Sleeve-Sleepshirt-V-Neck-Sleepwear-Casual-Loungewear-Ladies-Sleepwear/5055405942?athAsset=eyJhdGhjcGlkIjoiNTA1NTQwNTk0MiIsImF0aHN0aWQiOiJDUzAyMCIsImF0aGFuY2lkIjoiSXRlbUNhcm91c2VsIiwiYXRocmsiOjAuMH0=&athena=true&athbdg=L1300",
        "https://www.walmart.com/ip/Walking-Pad-Incline-Patbrela-2-5-HP-Desk-Treadmill-Small-LED-Display-Remote-Control-4-1-Incline-Walking-Pad-300-Lbs-Portable-Treadmill-Home-Office/14225173763?athAsset=eyJhdGhjcGlkIjoiMTQyMjUxNzM3NjMiLCJhdGhzdGlkIjoiQ1MwMjAiLCJhdGhhbmNpZCI6Ikl0ZW1DYXJvdXNlbCIsImF0aHJrIjowLjB9&athena=true",
        "https://www.walmart.com/ip/Pool-Toys-Light-up-Pool-Beach-Game-Balls-4-Pack-8-Light-Modes-Pool-Activities-Decorations-Adult-Red-Unisex/220621757?athAsset=eyJhdGhjcGlkIjoiMjIwNjIxNzU3IiwiYXRoc3RpZCI6IkNTMDIwIiwiYXRocmtiIjowLjB9&athena=true",
        # Enhanced Target URLs with updated scraping
        "https://www.target.com/p/patriotic-vanilla-mini-cupcakes-10oz-12ct-favorite-day-8482/-/A-87306034",
        "https://www.target.com/p/shark-pet-cordless-stick-vacuum-with-anti-allergen-complete-seal-ix141h/-/A-82874261#lnk=sametab",
        "https://www.target.com/p/bissell-little-green-max-pet-portable-carpet-cleaner/-/A-90571394#lnk=sametab",
    ]

    logger.info("Starting enhanced scraper...")
    
    # Authenticate with Google Sheets
    gspread_client = setup_google_sheets_client()
    
    if gspread_client:
        worksheet = get_or_create_worksheet(gspread_client, GOOGLE_SHEET_NAME, WORKSHEET_NAME)
        
        all_products_data = []
        successful_scrapes = 0
        failed_scrapes = 0
        
        for i, url in enumerate(PRODUCT_URLS):
            logger.info(f"Scraping product {i+1}/{len(PRODUCT_URLS)}: {url}")
            product_data = scrape_product_data(url)
            
            if product_data and product_data.get('name'): # Ensure the product has a title
                all_products_data.append(product_data)
                successful_scrapes += 1
                logger.info(f"✓ Successfully scraped: {product_data['name']} - Price: {product_data['price']}")
            else:
                failed_scrapes += 1
                logger.warning(f"✗ Failed to scrape: {url}")
            
            # Optimized delay - shorter for successful requests, longer for failures
            delay = 2 if product_data else 4
            time.sleep(delay)

        logger.info(f"Scraping completed: {successful_scrapes} successful, {failed_scrapes} failed")

        if not all_products_data:
            logger.error("No data was scraped. Exiting.")
        else:
            # Create a DataFrame from the scraped data
            df = pd.DataFrame(all_products_data)
            df.replace('', np.nan, inplace=True)
            df.dropna(subset=['name'], inplace=True) # Only add rows where a title was found

            # Reorder columns for clarity, excluding 'brand'
            df = df[['timestamp', 'website_name', 'name', 'price', 'availability', 'promo', 'url']]
            
            # ** FIX: Replace NaN with empty strings to make it JSON compliant for Google Sheets API **
            df.fillna('', inplace=True)

            logger.info("\\nScraped Data Preview:")
            print(df.to_string())

            # Check if the worksheet is empty to decide whether to add headers
            is_sheet_empty = not worksheet.get_all_values()
            
            if is_sheet_empty:
                logger.info("\\nWorksheet is empty. Writing headers and data to Google Sheet...")
                # Write the entire DataFrame (including headers) to the sheet
                set_with_dataframe(worksheet, df)
            else:
                logger.info("\\nWorksheet has data. Appending new rows...")
                # Append only the data rows (as a list of lists) to the sheet
                worksheet.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')
            
            logger.info(f"\n✓ Successfully wrote {len(df)} rows to '{WORKSHEET_NAME}' in '{GOOGLE_SHEET_NAME}'")
            
            # Summary of improvements
            logger.info("\\n=== SCRAPER IMPROVEMENTS APPLIED ===")
            logger.info("✓ Fixed Target price scraping with updated HTML selectors")
            logger.info("✓ Added universal price cleaning - only numbers in price column")
            logger.info("✓ Implemented retry logic with exponential backoff")
            logger.info("✓ Enhanced error handling and logging")
            logger.info("✓ Optimized request delays based on success/failure")
            logger.info("✓ Maintained all existing scraping mechanisms")
    else:
        logger.error("Failed to authenticate with Google Sheets. Please check your credentials.json file.")

