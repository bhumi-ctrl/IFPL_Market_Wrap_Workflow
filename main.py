#!/usr/bin/env python3
import os
import sys
import logging
import json
from datetime import datetime, timedelta
from docx import Document
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from jsonschema import validate, ValidationError

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
    "FII_EQUITY_NET","DII_EQUITY_NET",
    "GLOBAL_MARKET_SUMMARY","SECTORAL_OVERVIEW_SUMMARY","KEY_NEWS_AND_EVENTS_SUMMARY",
    "NIFTY_S1","NIFTY_S2","NIFTY_R1","NIFTY_R2",
    "BANK_NIFTY_S1","BANK_NIFTY_S2","BANK_NIFTY_R1","BANK_NIFTY_R2",
    "TECHNICAL_INDICATORS_COMMENTARY","CORPORATE_ANNOUNCEMENTS","ECONOMIC_DATA","REGULATORY_UPDATES","UPCOMING_EVENTS"
]

# ------------------- FETCH REPORT -------------------
def fetch_report_data():
    """Top-level fetch function: get data from scraping and fill missing defaults."""
    data = {}
    data["REPORT_DATE"] = datetime.now().strftime("%d-%b-%Y")
    try:
        nse = Nse()

        # Market Breadth
        ad = nse.get_advances_declines('nifty 50')
        data["ADVANCES"] = ad['advances']
        data["DECLINES"] = ad['declines']
        data["UNCHANGED"] = ad['unchanged']

        # Indices
        # Nifty 50
        nifty = nse.get_index_quote('NIFTY 50')
        data["NIFTY_CLOSING"] = round(nifty['lastPrice'], 2)
        data["NIFTY_CHANGE_POINTS"] = round(nifty['change'], 2)
        data["NIFTY_CHANGE_PERCENT"] = round(nifty['pChange'], 2)
        hist_n = yf.download('^NSEI', period='1y')
        data["NIFTY_52W_HIGH"] = round(hist_n['High'].max(), 2)
        data["NIFTY_52W_LOW"] = round(hist_n['Low'].min(), 2)

        # Bank Nifty
        bank = nse.get_index_quote('NIFTY BANK')
        data["BANK_NIFTY_CLOSING"] = round(bank['lastPrice'], 2)
        data["BANK_NIFTY_CHANGE_POINTS"] = round(bank['change'], 2)
        data["BANK_NIFTY_CHANGE_PERCENT"] = round(bank['pChange'], 2)
        hist_b = yf.download('^NSEBANK', period='1y')
        data["BANK_NIFTY_52W_HIGH"] = round(hist_b['High'].max(), 2)
        data["BANK_NIFTY_52W_LOW"] = round(hist_b['Low'].min(), 2)

        # Sensex
        hist_s = yf.download('^BSESN', period='2d')
        if len(hist_s) >= 2:
            closing = round(hist_s['Close'].iloc[-1], 2)
            prev_close = hist_s['Close'].iloc[-2]
            change_points = round(closing - prev_close, 2)
            change_percent = round((change_points / prev_close) * 100, 2)
            data["SENSEX_CLOSING"] = closing
            data["SENSEX_CHANGE_POINTS"] = change_points
            data["SENSEX_CHANGE_PERCENT"] = change_percent
            hist52_s = yf.download('^BSESN', period='1y')
            data["SENSEX_52W_HIGH"] = round(hist52_s['High'].max(), 2)
            data["SENSEX_52W_LOW"] = round(hist52_s['Low'].min(), 2)

        # Top Gainers & Losers (Nifty)
        top_gainers = nse.get_top_gainers()
        if top_gainers:
            for i in range(min(2, len(top_gainers))):
                g = top_gainers[i]
                data[f"GAINER_{i+1}_NAME"] = g['symbol']
                data[f"GAINER_{i+1}_PRICE"] = round(g['last'], 2)
                data[f"GAINER_{i+1}_CHANGE"] = round(g['pchange'], 2)
                data[f"GAINER_{i+1}_VOLUME"] = g['volume']

        top_losers = nse.get_top_losers()
        if top_losers:
            for i in range(min(2, len(top_losers))):
                l = top_losers[i]
                data[f"LOSER_{i+1}_NAME"] = l['symbol']
                data[f"LOSER_{i+1}_PRICE"] = round(l['last'], 2)
                data[f"LOSER_{i+1}_CHANGE"] = round(l['pchange'], 2)
                data[f"LOSER_{i+1}_VOLUME"] = l['volume']

        # Sectoral Performance
        sectoral_names = ['NIFTY AUTO', 'NIFTY BANK', 'NIFTY FINANCIAL SERVICES', 'NIFTY FMCG', 'NIFTY IT', 'NIFTY MEDIA', 'NIFTY METAL', 'NIFTY PHARMA', 'NIFTY PRIVATE BANK', 'NIFTY PSU BANK', 'NIFTY REALTY']
        sectoral_changes = {}
        for name in sectoral_names:
            try:
                q = nse.get_index_quote(name)
                sectoral_changes[name] = q['pChange']
            except:
                pass
        if sectoral_changes:
            top_sectors = sorted(sectoral_changes.items(), key=lambda x: x[1], reverse=True)[:2]
            bottom_sectors = sorted(sectoral_changes.items(), key=lambda x: x[1])[:2]
            data["TOP_SECTOR_1_NAME"] = top_sectors[0][0] if top_sectors else "NA"
            data["TOP_SECTOR_1_CHANGE"] = round(top_sectors[0][1], 2) if top_sectors else "NA"
            data["TOP_SECTOR_1_REASON"] = "Strong buying interest"
            if len(top_sectors) > 1:
                data["TOP_SECTOR_2_NAME"] = top_sectors[1][0]
                data["TOP_SECTOR_2_CHANGE"] = round(top_sectors[1][1], 2)
                data["TOP_SECTOR_2_REASON"] = "Positive sector news"
            data["BOTTOM_SECTOR_1_NAME"] = bottom_sectors[0][0] if bottom_sectors else "NA"
            data["BOTTOM_SECTOR_1_CHANGE"] = round(bottom_sectors[0][1], 2) if bottom_sectors else "NA"
            data["BOTTOM_SECTOR_1_REASON"] = "Profit booking"
            if len(bottom_sectors) > 1:
                data["BOTTOM_SECTOR_2_NAME"] = bottom_sectors[1][0]
                data["BOTTOM_SECTOR_2_CHANGE"] = round(bottom_sectors[1][1], 2)
                data["BOTTOM_SECTOR_2_REASON"] = "Global cues"
            data["SECTORAL_OVERVIEW_SUMMARY"] = f"{data['TOP_SECTOR_1_NAME']} and {data.get('TOP_SECTOR_2_NAME', 'others')} sectors led gains, while {data['BOTTOM_SECTOR_1_NAME']} lagged." if sectoral_changes else "Mixed sectoral performance."

        # FII/DII
        try:
            today = datetime.now()
            from_date = (today - timedelta(days=1)).strftime("%d-%m-%Y")
            to_date = today.strftime("%d-%m-%Y")
            archives = f'[{{"name":"FII/DII-Trading-Activity-Detail","from":"{from_date}","to":"{to_date}"}}]'
            url = f"https://www.nseindia.com/api/reports?archives={archives}&category=equity"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': '*/*',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Referer': 'https://www.nseindia.com/reports/fii-dii',
            }
            r = requests.get(url, headers=headers)
            if r.status_code == 200:
                resp_data = r.json()
                if 'data' in resp_data and resp_data['data']:
                    latest_report = resp_data['data'][0]
                    if 'data' in latest_report:
                        for row in latest_report['data']:
                            if row.get('category') == 'Equity':
                                if row.get('buySellIndicator') == 'FII/FPI':
                                    data["FII_EQUITY_NET"] = round(row.get('netValue', 0), 0)
                                elif row.get('buySellIndicator') == 'DII':
                                    data["DII_EQUITY_NET"] = round(row.get('netValue', 0), 0)
        except Exception as e:
            logging.warning(f"FII/DII fetch failed: {e}")
            data["FII_EQUITY_NET"] = "NA"
            data["DII_EQUITY_NET"] = "NA"

        # Commodities & Currency
        # Brent Crude
        hist_b = yf.download('BZ=F', period='2d')
        if len(hist_b) >= 2:
            price = round(hist_b['Close'].iloc[-1], 2)
            ch = round(hist_b['Close'].iloc[-1] - hist_b['Close'].iloc[-2], 2)
            ch_pc = round((ch / hist_b['Close'].iloc[-2]) * 100, 2)
            data["BRENT_PRICE"] = price
            data["BRENT_CHANGE"] = f"{ch} ({ch_pc}%)"

        # Gold
        hist_g = yf.download('GC=F', period='2d')
        if len(hist_g) >= 2:
            price = round(hist_g['Close'].iloc[-1], 2)
            ch = round(hist_g['Close'].iloc[-1] - hist_g['Close'].iloc[-2], 2)
            ch_pc = round((ch / hist_g['Close'].iloc[-2]) * 100, 2)
            data["GOLD_PRICE"] = price
            data["GOLD_CHANGE"] = f"{ch} ({ch_pc}%)"

        # INR vs USD
        hist_i = yf.download('INR=X', period='2d')
        if len(hist_i) >= 2:
            rate = round(hist_i['Close'].iloc[-1], 4)
            ch = round(hist_i['Close'].iloc[-1] - hist_i['Close'].iloc[-2], 4)
            ch_pc = round((ch / hist_i['Close'].iloc[-2]) * 100, 2)
            data["INR_USD_RATE"] = rate
            data["INR_USD_CHANGE"] = f"{ch} ({ch_pc}%)"

        # Global Market Summary
        global_tickers = {'Dow': '^DJI', 'S&P 500': '^GSPC', 'Nasdaq': '^IXIC'}
        global_parts = []
        for name, ticker in global_tickers.items():
            hist = yf.download(ticker, period='2d')
            if len(hist) >= 2:
                close = round(hist['Close'].iloc[-1], 2)
                ch_pc = round((hist['Close'].iloc[-1] - hist['Close'].iloc[-2]) / hist['Close'].iloc[-2] * 100, 1)
                global_parts.append(f"{name} up {ch_pc}% at {close}")
        data["GLOBAL_MARKET_SUMMARY"] = "US indices closed " + ", ".join(global_parts) + "; Asian markets mixed." if global_parts else "Global markets mixed."

        # Executive Summary
        nifty_pc = data.get("NIFTY_CHANGE_PERCENT", 0)
        direction = "higher" if nifty_pc > 0 else "lower"
        net_fii = data.get("FII_EQUITY_NET", 0)
        fii_desc = "selling" if net_fii < 0 else "buying"
        data["EXECUTIVE_SUMMARY"] = f"Indian equities ended {direction}, with Nifty 50 at {data['NIFTY_CLOSING']} ({nifty_pc:+.2f}%). DII inflows offset FII {fii_desc}, supported by {data['SECTORAL_OVERVIEW_SUMMARY']}. Global cues mixed."

        # Key News & Events
        data["KEY_NEWS_AND_EVENTS_SUMMARY"] = "Key events: Upcoming Q2 earnings and economic data releases. Check sources for latest corporate announcements."

    except Exception as e:
        logging.exception("Data fetch failed: %s", e)
        data["EXECUTIVE_SUMMARY"] = "Data fetch error; using placeholders."

    # Populate defaults
    for k in DEFAULT_KEYS:
        if k not in data:
            data[k] = "NA"

    # Ensure gainer/loser keys
    for i in (1, 2):
        for prefix in ("GAINER", "LOSER"):
            for suff in ("NAME", "PRICE", "CHANGE", "VOLUME"):
                key = f"{prefix}_{i}_{suff}"
                if key not in data:
                    data[key] = "NA"

    # Ensure commodity keys
    for k in ["BRENT_PRICE","BRENT_CHANGE","GOLD_PRICE","GOLD_CHANGE","INR_USD_RATE","INR_USD_CHANGE"]:
        if k not in data:
            data[k] = "NA"

    logging.info("Prepared report data with %d keys. Report date: %s", len(data), data.get("REPORT_DATE"))
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