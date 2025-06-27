# Retail Scraper - Automated Daily Execution

This project scrapes product data from multiple retailers (Amazon, eBay, Etsy, Walmart, Target) and automatically updates Google Sheets daily at 12:00 PM India time.

## Setup Options

### Option 1: Local Scheduler (Laptop must be on)

**Pros:** Simple setup, no cost
**Cons:** Laptop must be running 24/7

#### Setup Steps:
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up Google Sheets credentials:
   - Create a Google Cloud project
   - Enable Google Sheets API
   - Create a service account
   - Download `credentials.json` to your project folder
   - Share your Google Sheet with the service account email

3. Run the scheduler:
   ```bash
   python scheduler.py
   ```

4. The scraper will run automatically every day at 12:00 PM India time.

### Option 2: Cloud Deployment (Recommended)

**Pros:** No laptop needed, runs 24/7, more reliable
**Cons:** May have costs (though many free tiers available)

#### Cloud Deployment Options:

##### A. Railway (Recommended - Free tier available)
1. Create account at [railway.app](https://railway.app)
2. Connect your GitHub repository
3. Add environment variables:
   - `GOOGLE_SHEETS_CREDENTIALS`: Your credentials.json content
4. Deploy and set up cron job

##### B. Heroku (Free tier discontinued)
1. Create account at [heroku.com](https://heroku.com)
2. Install Heroku CLI
3. Deploy using:
   ```bash
   heroku create your-app-name
   git push heroku main
   heroku addons:create scheduler:standard
   ```

##### C. Google Cloud Functions
1. Create Google Cloud project
2. Enable Cloud Functions API
3. Deploy function with trigger

##### D. GitHub Actions (Free tier available)
1. Create `.github/workflows/scraper.yml`:
   ```yaml
   name: Daily Scraper
   on:
     schedule:
       - cron: '30 6 * * *'  # 12:00 PM India time (UTC+5:30)
   jobs:
     scrape:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v2
         - name: Set up Python
           uses: actions/setup-python@v2
           with:
             python-version: '3.9'
         - name: Install dependencies
           run: pip install -r requirements.txt
         - name: Run scraper
           run: python cloud_deploy.py
           env:
             GOOGLE_SHEETS_CREDENTIALS: ${{ secrets.GOOGLE_SHEETS_CREDENTIALS }}
   ```

## Files Overview

- `scraper-manus.py`: Your working scraper
- `scheduler.py`: Local scheduler for daily execution
- `cloud_deploy.py`: Cloud-ready version
- `requirements.txt`: Python dependencies
- `credentials.json`: Google Sheets API credentials

## Configuration

### Customizing Product URLs
Edit the `PRODUCT_URLS` dictionary in your scraper file to add/remove products.

### Changing Schedule Time
For local scheduler: Edit the time in `scheduler.py`
For cloud: Edit the cron expression in your deployment platform

### Google Sheets Setup
1. Create a new Google Sheet named "Retailer Data"
2. The script will automatically create a worksheet named "Sheet1"
3. Data columns: timestamp, website, name, brand, price, availability, url

## Troubleshooting

### Local Scheduler Issues
- Ensure your laptop doesn't sleep
- Check `scheduler.log` for errors
- Make sure timezone is set correctly

### Cloud Deployment Issues
- Check platform logs
- Verify environment variables
- Ensure credentials.json is properly configured

### Scraping Issues
- Some sites may block automated requests
- Consider using proxies for large-scale scraping
- Rotate user agents and add delays

## Security Notes

- Never commit `credentials.json` to version control
- Use environment variables for cloud deployment
- Regularly rotate API keys

## Cost Considerations

- **Local:** Free (electricity cost)
- **Railway:** Free tier available
- **Heroku:** Paid plans only
- **Google Cloud:** Free tier available
- **GitHub Actions:** Free tier available

## Recommended Setup

For beginners: Start with **GitHub Actions** (free, reliable)
For production: Use **Railway** or **Google Cloud Functions** 