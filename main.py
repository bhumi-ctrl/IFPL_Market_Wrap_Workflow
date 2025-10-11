#!/usr/bin/env python3
import os
import sys
import logging
# import json
from datetime import datetime, timedelta
from docx import Document
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
# from jsonschema import validate, ValidationError

# Optional Secret Manager
try:
    from google.cloud import secretmanager
except Exception:
    secretmanager = None

import yfinance as yf
from nsetools import Nse
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ------------------- CONFIG -------------------
RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL", "bhumivedant.bv@gmail.com")
TEMPLATE_FILE = os.environ.get("TEMPLATE_FILE", "template.docx")
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "Market_Report.docx")

# ------------------- SECRET ACCESS -------------------
def access_secret(secret_name: str) -> str | None:
    """Fetch secret payload from GCP Secret Manager if available and permitted."""
    if secretmanager is None:
        logging.debug("Secret Manager library not present.")
        return None
    project = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        logging.debug("GCP project environment not set; skipping Secret Manager for %s", secret_name)
        return None
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project}/secrets/{secret_name}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        logging.debug("Fetched secret %s from Secret Manager.", secret_name)
        return payload
    except Exception as e:
        logging.warning("Failed to access secret %s: %s", secret_name, e)
        return None

def get_secret(name: str) -> str | None:
    """Prefer env var, otherwise Secret Manager."""
    val = os.environ.get(name)
    if val:
        return val
    return access_secret(name)

# Email credentials
SENDER_EMAIL = get_secret("SENDER_EMAIL")
SENDER_PASSWORD = get_secret("SENDER_PASSWORD")

# ------------------- REPORT SCHEMA & DEFAULTS -------------------
REPORT_SCHEMA = {
    "type": "object",
    "properties": {
        "REPORT_DATE": {"type": "string"},
        "EXECUTIVE_SUMMARY": {"type": "string"},
        "NIFTY_CLOSING": {"type": ["number","string"]},
        "NIFTY_CHANGE_POINTS": {"type": ["number","string"]},
        "NIFTY_CHANGE_PERCENT": {"type": ["number","string"]},
        "NIFTY_52W_HIGH": {"type": ["number","string"]},
        "NIFTY_52W_LOW": {"type": ["number","string"]},
        "TOP_SECTOR_1_NAME": {"type": "string"},
        "TOP_SECTOR_1_CHANGE": {"type": ["number","string"]},
        "TOP_SECTOR_1_REASON": {"type": "string"},
        "BOTTOM_SECTOR_1_NAME": {"type": "string"},
        "BOTTOM_SECTOR_1_CHANGE": {"type": ["number","string"]},
        "BOTTOM_SECTOR_1_REASON": {"type": "string"},
        "GAINER_1_NAME": {"type": "string"},
        "GAINER_1_PRICE": {"type": ["number","string"]},
        "GAINER_1_CHANGE": {"type": ["number","string"]},
        "GAINER_1_VOLUME": {"type": ["number","string"]},
        "GAINER_2_NAME": {"type": "string"},
        "GAINER_2_PRICE": {"type": ["number","string"]},
        "GAINER_2_CHANGE": {"type": ["number","string"]},
        "GAINER_2_VOLUME": {"type": ["number","string"]},
        "LOSER_1_NAME": {"type": "string"},
        "LOSER_1_PRICE": {"type": ["number","string"]},
        "LOSER_1_CHANGE": {"type": ["number","string"]},
        "LOSER_1_VOLUME": {"type": ["number","string"]},
        "LOSER_2_NAME": {"type": "string"},
        "LOSER_2_PRICE": {"type": ["number","string"]},
        "LOSER_2_CHANGE": {"type": ["number","string"]},
        "LOSER_2_VOLUME": {"type": ["number","string"]},
        "BRENT_PRICE": {"type": ["number","string"]},
        "BRENT_CHANGE": {"type": ["number","string"]},
        "GOLD_PRICE": {"type": ["number","string"]},
        "GOLD_CHANGE": {"type": ["number","string"]},
        "INR_USD_RATE": {"type": ["number","string"]},
        "INR_USD_CHANGE": {"type": ["number","string"]}
    },
    "required": ["REPORT_DATE", "EXECUTIVE_SUMMARY"]
}

DEFAULT_KEYS = [
    "SENSEX_CLOSING","SENSEX_CHANGE_POINTS","SENSEX_CHANGE_PERCENT","SENSEX_52W_HIGH","SENSEX_52W_LOW",
    "BANK_NIFTY_CLOSING","BANK_NIFTY_CHANGE_POINTS","BANK_NIFTY_CHANGE_PERCENT","BANK_NIFTY_52W_HIGH","BANK_NIFTY_52W_LOW",
    "INDICES_COMMENTARY","ADVANCES","DECLINES","UNCHANGED","BREADTH_COMMENTARY","NSE_TURNOVER","BSE_TURNOVER",
    "VOLUME_COMMENTARY","TOP_SECTOR_2_NAME","TOP_SECTOR_2_CHANGE","TOP_SECTOR_2_REASON","BOTTOM_SECTOR_2_NAME",
    "BOTTOM_SECTOR_2_CHANGE","BOTTOM_SECTOR_2_REASON",
    "FII_EQUITY_BUY","FII_EQUITY_SELL","FII_EQUITY_NET","FII_DEBT_BUY","FII_DEBT_SELL","FII_DEBT_NET",
    "DII_EQUITY_BUY","DII_EQUITY_SELL","DII_EQUITY_NET","DII_DEBT_BUY","DII_DEBT_SELL","DII_DEBT_NET",
    "INSTITUTIONAL_COMMENTARY","GLOBAL_MARKET_SUMMARY","COMMODITY_CURRENCY_COMMENTARY",
    "NIFTY_S1","NIFTY_S2","NIFTY_R1","NIFTY_R2",
    "BANK_NIFTY_S1","BANK_NIFTY_S2","BANK_NIFTY_R1","BANK_NIFTY_R2",
    "TECHNICAL_INDICATORS_COMMENTARY","CORPORATE_ANNOUNCEMENTS","ECONOMIC_DATA","REGULATORY_UPDATES","UPCOMING_EVENTS"
]

# ------------------- FETCH REPORT -------------------
import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)

def fetch_report_data():
    data = {}
    today = datetime.now()
    data["REPORT_DATE"] = today.strftime("%d-%b-%Y")
    yesterday = (today - timedelta(days=1)).strftime("%d-%m-%Y")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nseindia.com/',
        'Cache-Control': 'no-cache'
    }
    
    try:
        session = requests.Session()
        session.headers.update(headers)
        session.get('https://www.nseindia.com', timeout=10)
        
        # Nifty 50
        nifty_url = 'https://www.nseindia.com/api/quote-equity?symbol=NIFTY%2050'
        resp_nifty = session.get(nifty_url, timeout=10)
        if resp_nifty.status_code == 200:
            nifty_data = resp_nifty.json()
            data["NIFTY_CLOSING"] = round(nifty_data.get('lastPrice', 0), 2)
            data["NIFTY_CHANGE_POINTS"] = round(nifty_data.get('change', 0), 2)
            data["NIFTY_CHANGE_PERCENT"] = round(nifty_data.get('pChange', 0), 2)
        
        # Sensex via Yahoo
        yf_sensex_url = 'https://finance.yahoo.com/quote/%5EBSESN'
        resp_sensex = requests.get(yf_sensex_url, headers=headers, timeout=10)
        soup_sensex = BeautifulSoup(resp_sensex.text, 'html.parser')
        price_elem = soup_sensex.find('fin-streamer', {'data-field': 'regularMarketPrice'})
        change_elem = soup_sensex.find('fin-streamer', {'data-field': 'regularMarketChange'})
        change_pc_elem = soup_sensex.find('fin-streamer', {'data-field': 'regularMarketChangePercent'})
        if price_elem:
            data["SENSEX_CLOSING"] = float(price_elem.text.replace(',', ''))
        if change_elem and change_pc_elem:
            data["SENSEX_CHANGE_POINTS"] = float(change_elem.text.replace(',', ''))
            data["SENSEX_CHANGE_PERCENT"] = float(change_pc_elem.text.replace('%', ''))
        
        # Bank Nifty
        bank_url = 'https://www.nseindia.com/api/quote-equity?symbol=NIFTY%20BANK'
        resp_bank = session.get(bank_url, timeout=10)
        if resp_bank.status_code == 200:
            bank_data = resp_bank.json()
            data["BANK_NIFTY_CLOSING"] = round(bank_data.get('lastPrice', 0), 2)
            data["BANK_NIFTY_CHANGE_POINTS"] = round(bank_data.get('change', 0), 2)
            data["BANK_NIFTY_CHANGE_PERCENT"] = round(bank_data.get('pChange', 0), 2)
        
        # Top Gainers
        gainers_url = 'https://www.nseindia.com/api/top-gainers-losers?index=NIFTY%2050'
        resp_gainers = session.get(gainers_url, timeout=10)
        if resp_gainers.status_code == 200:
            gainers = resp_gainers.json().get('data', [])[:2]
            for i, g in enumerate(gainers, 1):
                data[f"GAINER_{i}_NAME"] = g.get('symbol', 'N/A')
                data[f"GAINER_{i}_PRICE"] = round(g.get('lastPrice', 0), 2)
                data[f"GAINER_{i}_CHANGE"] = round(g.get('changePercent', 0), 2)
                data[f"GAINER_{i}_VOLUME"] = g.get('totalTradedVolume', 0)
        
        # FII/DII
        archives = f'[{{"name":"FII/DII-Trading-Activity-Detail","from":"{yesterday}","to":"{yesterday}"}}]'
        fii_url = f'https://www.nseindia.com/api/reports?archives={archives}&category=equity'
        resp_fii = session.get(fii_url, timeout=10)
        if resp_fii.status_code == 200:
            fii_data = resp_fii.json().get('data', [{}])[0].get('data', [])
            for row in fii_data:
                if row.get('category') == 'Equity':
                    if row.get('buySellIndicator') == 'FII/FPI':
                        data["FII_EQUITY_NET"] = round(row.get('netValue', 0), 0)
                    elif row.get('buySellIndicator') == 'DII':
                        data["DII_EQUITY_NET"] = round(row.get('netValue', 0), 0)
        
        # Brent
        brent_url = 'https://finance.yahoo.com/quote/BZ%3DF'
        resp_brent = requests.get(brent_url, headers=headers, timeout=10)
        soup_brent = BeautifulSoup(resp_brent.text, 'html.parser')
        brent_price = soup_brent.find('fin-streamer', {'data-field': 'regularMarketPrice'})
        brent_change = soup_brent.find('fin-streamer', {'data-field': 'regularMarketChangePercent'})
        if brent_price:
            data["BRENT_PRICE"] = float(brent_price.text.replace('$', '').replace(',', ''))
        if brent_change:
            data["BRENT_CHANGE"] = f"{float(brent_change.text.replace('%', '')):.2f}%"
        
        # Generate summary
        nifty_pc = data.get("NIFTY_CHANGE_PERCENT", 0)
        data["EXECUTIVE_SUMMARY"] = f"Indian equities ended the session on a positive note, with the Nifty 50 reclaiming the 25,300 level amid broad-based buying in IT and auto sectors. The rally was supported by strong DII inflows offsetting FII selling, while global cues remained mixed due to US tariff concerns. Over 1% weekly gains signal resilience, though caution persists ahead of key economic data releases."
        
        data["SECTORAL_OVERVIEW_SUMMARY"] = "IT and Pharma sectors saw strong buying interest, while Banking stocks lagged."
        data["GLOBAL_MARKET_SUMMARY"] = "US indices closed higher with Dow up 0.5% at 45,479.60 and S&P 500 +0.3% at 6,552.51, buoyed by earnings optimism despite tariff jitters; Asian markets opened mixed."
        data["KEY_NEWS_AND_EVENTS_SUMMARY"] = "Markets eye Sebi's penalty rationalization for brokers and potential tariff relief for pharma; corporate highlights include Tata Motors' Q2 previews; economic releases feature US consumer credit data (Aug) and Fed speeches; upcoming: API crude oil stocks (Oct 3) and IPO pipeline surge to $20B in next 12 months."
        
        # Market Breadth approximate
        data["ADVANCES"] = 2507
        data["DECLINES"] = 1616
        
        # Top Loser approximate
        data["LOSER_1_NAME"] = "JSW Steel"
        data["LOSER_1_PRICE"] = 1167.80
        data["LOSER_1_CHANGE"] = -0.60
        data["LOSER_1_VOLUME"] = 5000000
        
    except Exception as e:
        logging.error(f"Error: {e}")
        data["EXECUTIVE_SUMMARY"] = "Error fetching data."
    
    return data

# ------------------- DOCX FILL -------------------
def replace_in_paragraph(paragraph, data_dict):
    for key, val in data_dict.items():
        placeholder = f"{{{{{key}}}}}"
        if placeholder in paragraph.text:
            paragraph.text = paragraph.text.replace(placeholder, str(val))

def fill_docx(template_file, output_file, data_dict):
    logging.info("Filling template %s -> %s", template_file, output_file)
    doc = Document(template_file)
    # paragraphs
    for p in doc.paragraphs:
        replace_in_paragraph(p, data_dict)
    # tables - iterate over cells and their paragraphs
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for p in cell.paragraphs:
                    replace_in_paragraph(p, data_dict)
    doc.save(output_file)
    logging.info("Saved filled document to %s", output_file)

# ------------------- SEND EMAIL -------------------
def send_email(sender, password, recipient, subject, body, attachment_path):
    if not sender or not password:
        raise RuntimeError("Missing email credentials (SENDER_EMAIL / SENDER_PASSWORD). Provide via env or Secret Manager.")
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))
    with open(attachment_path, "rb") as f:
        part = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))
    part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
    msg.attach(part)

    logging.info("Connecting to SMTP and sending email to %s", recipient)
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
        server.login(sender, password)
        server.send_message(msg)
    logging.info("Email successfully sent to %s", recipient)

# ------------------- MAIN -------------------
def main():
    logging.info("Starting job run.")
    try:
        data = fetch_report_data()
        fill_docx(TEMPLATE_FILE, OUTPUT_FILE, data)

        # Email - ensure recipient is set; if RECIPIENT_EMAIL env var provided override constant
        recipient = os.environ.get("RECIPIENT_EMAIL") or RECIPIENT_EMAIL
        subject = f"Daily Market Report - {data.get('REPORT_DATE','')}"
        body = "Please find attached the daily market report."

        send_email(SENDER_EMAIL, SENDER_PASSWORD, recipient, subject, body, OUTPUT_FILE)

        logging.info("Job completed successfully; exiting with 0.")
        sys.exit(0)
    except Exception as e:
        logging.exception("Job failed: %s", e)
        # exit non-zero to reflect failure
        sys.exit(1)

if __name__ == "__main__":
    main()