import schedule
import time
import subprocess
import sys
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_scraper():
    """Run the scraper script"""
    try:
        logger.info("Starting scheduled scraper run...")
        result = subprocess.run([sys.executable, 'scraper-manus.py'], 
                              capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            logger.info("Scraper completed successfully")
            logger.info(f"Output: {result.stdout}")
        else:
            logger.error(f"Scraper failed with return code {result.returncode}")
            logger.error(f"Error: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        logger.error("Scraper timed out after 5 minutes")
    except Exception as e:
        logger.error(f"Error running scraper: {e}")

def main():
    logger.info("Starting scheduler...")
    
    # Schedule the scraper to run at 12:00 PM India time (UTC+5:30)
    # Since schedule library uses local time, we need to account for timezone
    schedule.every().day.at("12:00").do(run_scraper)
    
    logger.info("Scheduler set to run scraper daily at 12:00 PM India time")
    logger.info("Press Ctrl+C to stop the scheduler")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")

if __name__ == "__main__":
    main() 