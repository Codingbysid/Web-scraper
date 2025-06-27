import requests
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from datetime import datetime
import time
import re
import random

# --- Part 1: Google Sheets Authentication & Setup ---

def setup_google_sheets_client():
    """
    Authenticates with Google Sheets API using service account credentials.
    """
    try:
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        # Uses the modern gspread method to authorize and avoid deprecation warnings.
        client = gspread.service_account(filename='credentials.json', scopes=scopes)
        return client
    except FileNotFoundError:
        print("\nError: 'credentials.json' not found.")
        print("Please ensure the Google Cloud service account credentials file is in the same directory or provide the correct path.\n")
        return None
    except Exception as e:
        print(f"An error occurred during Google Sheets authentication: {e}")
        return None

def get_or_create_worksheet(client, sheet_name, worksheet_name):
    """
    Opens a Google Sheet and a specific worksheet. Creates them if they don't exist.
    """
    try:
        sheet = client.open(sheet_name)
    except gspread.SpreadsheetNotFound:
        print(f"Spreadsheet '{sheet_name}' not found. Creating a new one.")
        sheet = client.create(sheet_name)
        print(f"Sharing new sheet with service account: {client.auth.service_account_email}")
        sheet.share(client.auth.service_account_email, perm_type='user', role='writer')

    try:
        worksheet = sheet.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        print(f"Worksheet '{worksheet_name}' not found. Creating a new one.")
        worksheet = sheet.add_worksheet(title=worksheet_name, rows="1000", cols="20")
    return worksheet


# --- Part 2: Web Scraping Functions ---

# A list of User-Agents to rotate through. This helps avoid getting blocked.
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.2 Safari/605.1.15',
]

def get_soup(url):
    """Fetches and parses a URL, returns a BeautifulSoup object with a rotating user-agent."""
    try:
        headers = {
            'User-Agent': random.choice(USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        webpage = requests.get(url, headers=headers)
        webpage.raise_for_status()
        return BeautifulSoup(webpage.content, "html.parser")
    except requests.exceptions.RequestException as e:
        print(f"Failed to retrieve page {url}. Error: {e}")
        return None

# --- Scraper for Amazon ---
def scrape_amazon_data(soup, url):
    if "api-services-support@amazon.com" in soup.text:
        print(f"Request blocked by Amazon for URL: {url}.")
        return None
    def get_title(s):
        try: return s.find("span", id='productTitle').text.strip()
        except: return ""
    def get_price(s):
        try: return s.find("span", class_='a-price-whole').text + s.find("span", class_='a-price-fraction').text
        except: return ""
    def get_brand(s):
        try: return s.find('tr', class_='po-brand').find_all('span')[1].text.strip()
        except: return ""
    def get_availability(s):
        try: return s.find("div", id='availability').find("span").text.strip()
        except: return "Not Available"
    return {'name': get_title(soup), 'price': get_price(soup), 'brand': get_brand(soup), 'availability': get_availability(soup)}

# --- Scraper for eBay ---
def scrape_ebay_data(soup, url):
    def get_title(s):
        try: return s.find("h1", class_="x-item-title__mainTitle").find("span", class_="ux-textspans--BOLD").text.strip()
        except: return ""
    def get_price(s):
        try: return s.find("div", class_="x-price-primary").find("span", class_="ux-textspans").text.strip()
        except: return ""
    def get_brand(s):
        try: return s.find("div", class_="ux-layout-section-evo__item--table-view").find(string=re.compile("Brand")).find_next('div').find('span').text.strip()
        except: return "N/A"
    def get_availability(s):
        try:
            qty_string = s.find(string=re.compile(r"\d+ sold|\d+ available|Last One", re.IGNORECASE))
            return qty_string if qty_string else "In Stock"
        except: return "In Stock"
    return {'name': get_title(soup), 'price': get_price(soup), 'brand': get_brand(soup), 'availability': get_availability(soup)}

# --- Scraper for Etsy ---
def scrape_etsy_data(soup, url):
    def get_title(s):
        try: return s.find("h1", class_="wt-text-body-03").text.strip()
        except: return ""
    def get_price(s):
        try:
            price_text = s.find("p", class_=re.compile("wt-text-title-03")).text.strip()
            return re.sub(r"[^\d.]", "", price_text)
        except: return ""
    def get_brand(s): # Shop Name as "brand"
        try: return s.find('a', class_="wt-text-link-no-underline").find_all("span")[0].text.strip()
        except: return ""
    def get_availability(s):
        try:
            stock_element = s.find(string=re.compile(r"in stock|Only \d+ left", re.IGNORECASE))
            return stock_element.strip() if stock_element else "In Stock"
        except: return "In Stock"
    return {'name': get_title(soup), 'price': get_price(soup), 'brand': get_brand(soup), 'availability': get_availability(soup)}

# --- Part 3: Main Execution ---

if __name__ == '__main__':
    GOOGLE_SHEET_NAME = "Retailer Data"
    WORKSHEET_NAME = "Sheet1"

    # --- User Configuration ---
    # Updated with new, valid URLs.
    PRODUCT_URLS = {
        "Amazon": [
            "https://www.amazon.com/dp/B08P2H5LW2", # Anker USB C Charger
            "https://www.amazon.com/dp/B0862269YP", # Sony WH-1000XM4
        ],
        "eBay": [
            "https://www.ebay.com/itm/305545892582", # Google Pixel 8 Pro
            "https://www.ebay.com/itm/126133036662", # Meta Quest 3
        ],
        "Etsy": [ # NOTE: Etsy still has strong anti-bot measures. Success is not guaranteed.
            "https://www.etsy.com/listing/715039122/recycled-newspaper-round-pencil-holder",
            "https://www.etsy.com/listing/1188448849/personalized-leather-keychain-custom",
        ]
    }

    print("Starting multi-retailer scraper...")
    
    gspread_client = setup_google_sheets_client()
    if not gspread_client: exit()

    worksheet = get_or_create_worksheet(gspread_client, GOOGLE_SHEET_NAME, WORKSHEET_NAME)
    all_products_data = []

    SCRAPER_MAP = {
        "Amazon": scrape_amazon_data, "eBay": scrape_ebay_data, "Etsy": scrape_etsy_data
    }

    for retailer, urls in PRODUCT_URLS.items():
        print(f"\n--- Scraping {retailer} ---")
        for i, url in enumerate(urls):
            print(f"Scraping product {i+1}/{len(urls)}: {url}")
            soup = get_soup(url)
            if soup:
                product_data = SCRAPER_MAP[retailer](soup, url)
                if product_data and product_data.get('name'):
                    product_data.update({
                        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'website': retailer, 'url': url
                    })
                    all_products_data.append(product_data)
            time.sleep(random.uniform(2.5, 4.5)) # Use a random delay

    if not all_products_data:
        print("\nNo data was scraped. Exiting.")
    else:
        df = pd.DataFrame(all_products_data)
        
        cols = ['timestamp', 'website', 'name', 'brand', 'price', 'availability', 'url']
        df = df.reindex(columns=cols)
        
        # The pandas warnings are informational and don't break the script.
        # This fillna call is crucial to prevent JSON errors when writing to Google Sheets.
        df.fillna('', inplace=True) 

        print("\n--- Scraped Data ---")
        print(df)

        is_sheet_empty = not worksheet.get_all_values()
        if is_sheet_empty:
            print("\nWorksheet is empty. Writing headers and data...")
            set_with_dataframe(worksheet, df)
        else:
            print("\nWorksheet has data. Appending new rows...")
            worksheet.append_rows(df.values.tolist(), value_input_option='USER_ENTERED')
        
        print(f"\nSuccessfully wrote {len(df)} rows to '{WORKSHEET_NAME}' in '{GOOGLE_SHEET_NAME}'.")

    print("\nScraping completed!")
