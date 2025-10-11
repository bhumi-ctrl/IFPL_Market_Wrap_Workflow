import os
import sys
import json
import logging
from datetime import datetime
from docx import Document
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import requests
from requests.adapters import HTTPAdapter, Retry
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Optional GCP Secret Manager
try:
    from google.cloud import secretmanager
except Exception:
    secretmanager = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ------------------- CONFIG -------------------
RECIPIENT_EMAIL = os.environ.get("RECIPIENT_EMAIL", "bhumivedant.bv@gmail.com")
TEMPLATE_FILE = os.environ.get("TEMPLATE_FILE", "template.docx")
OUTPUT_FILE = os.environ.get("OUTPUT_FILE", "Market_Report.docx")

# Secrets - prefer environment variables; if not present, try Secret Manager
def access_secret(secret_name: str) -> str | None:
    """Fetch secret payload from GCP Secret Manager if available."""
    if secretmanager is None:
        logging.debug("google-cloud-secret-manager not installed or not available.")
        return None
    project = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        logging.debug("No GCP project env var set; cannot fetch secret %s", secret_name)
        return None
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project}/secrets/{secret_name}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        logging.debug("Fetched secret %s from Secret Manager.", secret_name)
        return payload
    except Exception as e:
        logging.warning("Could not access secret %s: %s", secret_name, e)
        return None

def get_secret(name: str) -> str | None:
    val = os.environ.get(name)
    if val:
        return val
    return access_secret(name)

SENDER_EMAIL = get_secret("SENDER_EMAIL")
SENDER_PASSWORD = get_secret("SENDER_PASSWORD")
# Other secrets (if required)
# GEMINI_API_KEY = get_secret("GEMINI_API_KEY")
# N8N_DATABASE_PASSWORD = get_secret("N8N_DATABASE_PASSWORD")
# N8N_ENCRYPTION_KEY = get_secret("N8N_ENCRYPTION_KEY")

# ------------------- HTTP session & helpers -------------------
REQUEST_TIMEOUT = (5, 7)  # (connect_timeout, read_timeout) in seconds
session = requests.Session()
retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)
session.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; indian-wrap-bot/1.0; +https://example.com)"
})

def safe_get_json(url: str, params=None, headers=None, timeout=REQUEST_TIMEOUT):
    try:
        resp = session.get(url, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.error("Request failed for %s: %s", url, e)
        return None

# Use tenacity for retrying entire function on transient exceptions
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10),
       retry=retry_if_exception_type(Exception))
def fetch_nifty_data():
    # NOTE: NSE often requires cookies/headers in real world; this is a basic attempt.
    url = "https://www.nseindia.com/api/quote-equity?symbol=NIFTY"
    data = safe_get_json(url)
    if not data or "priceInfo" not in data:
        raise Exception("NIFTY API returned invalid data")
    p = data["priceInfo"]
    return {
        "NIFTY_CLOSING": p.get("lastPrice", "NA"),
        "NIFTY_CHANGE_POINTS": p.get("change", "NA"),
        "NIFTY_CHANGE_PERCENT": p.get("pChange", "NA"),
        "NIFTY_52W_HIGH": p.get("dayHigh", "NA"),
        "NIFTY_52W_LOW": p.get("dayLow", "NA")
    }

@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=1, max=4),
       retry=retry_if_exception_type(Exception))
def fetch_sector_performance():
    url = "https://www.nseindia.com/api/sector-performance"
    data = safe_get_json(url)
    if not data or "data" not in data:
        logging.warning("Sector performance not available; returning defaults.")
        return {k: "NA" for k in [
            "TOP_SECTOR_1_NAME","TOP_SECTOR_1_CHANGE","TOP_SECTOR_1_REASON",
            "BOTTOM_SECTOR_1_NAME","BOTTOM_SECTOR_1_CHANGE","BOTTOM_SECTOR_1_REASON"]}
    sectors = data["data"]
    if not sectors:
        raise Exception("Empty sector data")
    top = sectors[0]
    bottom = sectors[-1]
    return {
        "TOP_SECTOR_1_NAME": top.get("sectorName", "NA"),
        "TOP_SECTOR_1_CHANGE": top.get("change", "NA"),
        "TOP_SECTOR_1_REASON": top.get("reason", "NA"),
        "BOTTOM_SECTOR_1_NAME": bottom.get("sectorName", "NA"),
        "BOTTOM_SECTOR_1_CHANGE": bottom.get("change", "NA"),
        "BOTTOM_SECTOR_1_REASON": bottom.get("reason", "NA")
    }

def fetch_top_gainers_losers():
    url = "https://www.nseindia.com/api/top-gainers"
    data = safe_get_json(url)
    if not data or "data" not in data:
        logging.warning("Top gainers API failed; returning placeholders.")
        return {}
    arr = data["data"]
    gainers = arr[:2] if len(arr) >= 2 else arr
    losers = arr[-2:] if len(arr) >= 2 else arr
    out = {}
    for i, g in enumerate(gainers):
        out.update({
            f"GAINER_{i+1}_NAME": g.get("symbol", "NA"),
            f"GAINER_{i+1}_PRICE": g.get("lastPrice", "NA"),
            f"GAINER_{i+1}_CHANGE": g.get("change", "NA"),
            f"GAINER_{i+1}_VOLUME": g.get("quantityTraded", "NA")
        })
    for i, l in enumerate(losers):
        out.update({
            f"LOSER_{i+1}_NAME": l.get("symbol", "NA"),
            f"LOSER_{i+1}_PRICE": l.get("lastPrice", "NA"),
            f"LOSER_{i+1}_CHANGE": l.get("change", "NA"),
            f"LOSER_{i+1}_VOLUME": l.get("quantityTraded", "NA")
        })
    return out

def fetch_institutional_activity():
    url = "https://www.nseindia.com/api/fii-dii"
    data = safe_get_json(url)
    if not data:
        logging.warning("FII/DII API failed.")
        return {k: "NA" for k in [
            "FII_EQUITY_BUY","FII_EQUITY_SELL","FII_EQUITY_NET",
            "FII_DEBT_BUY","FII_DEBT_SELL","FII_DEBT_NET",
            "DII_EQUITY_BUY","DII_EQUITY_SELL","DII_EQUITY_NET",
            "DII_DEBT_BUY","DII_DEBT_SELL","DII_DEBT_NET"
        ]}
    fii = data.get("fii", {}) or {}
    dii = data.get("dii", {}) or {}
    return {
        "FII_EQUITY_BUY": fii.get("equityBuy", "NA"),
        "FII_EQUITY_SELL": fii.get("equitySell", "NA"),
        "FII_EQUITY_NET": fii.get("equityNet", "NA"),
        "FII_DEBT_BUY": fii.get("debtBuy", "NA"),
        "FII_DEBT_SELL": fii.get("debtSell", "NA"),
        "FII_DEBT_NET": fii.get("debtNet", "NA"),
        "DII_EQUITY_BUY": dii.get("equityBuy", "NA"),
        "DII_EQUITY_SELL": dii.get("equitySell", "NA"),
        "DII_EQUITY_NET": dii.get("equityNet", "NA"),
        "DII_DEBT_BUY": dii.get("debtBuy", "NA"),
        "DII_DEBT_SELL": dii.get("debtSell", "NA"),
        "DII_DEBT_NET": dii.get("debtNet", "NA")
    }

def fetch_commodities_and_currency():
    url = "https://www.nseindia.com/api/commodities-currency"
    data = safe_get_json(url)
    if not data:
        logging.warning("Commodities/currency API failed.")
        return {"BRENT_PRICE":"NA","BRENT_CHANGE":"NA","GOLD_PRICE":"NA","GOLD_CHANGE":"NA","INR_USD_RATE":"NA","INR_USD_CHANGE":"NA"}
    brent = data.get("brentCrude", {}) or {}
    gold = data.get("gold", {}) or {}
    inrusd = data.get("inrUsd", {}) or {}
    return {
        "BRENT_PRICE": brent.get("price", "NA"),
        "BRENT_CHANGE": brent.get("change", "NA"),
        "GOLD_PRICE": gold.get("price", "NA"),
        "GOLD_CHANGE": gold.get("change", "NA"),
        "INR_USD_RATE": inrusd.get("rate", "NA"),
        "INR_USD_CHANGE": inrusd.get("change", "NA")
    }

def fetch_report_data():
    data = {
        "REPORT_DATE": datetime.now().strftime("%d-%b-%Y"),
        "EXECUTIVE_SUMMARY": "Indian markets closed mixed today with tech stocks leading gains.",
        **fetch_nifty_data(),
        **fetch_sector_performance(),
        **fetch_top_gainers_losers(),
        **fetch_institutional_activity(),
        **fetch_commodities_and_currency()
    }
    # Add defaults for common placeholders in template so replacement doesn't break
    defaults = [
        "SENSEX_CLOSING","SENSEX_CHANGE_POINTS","SENSEX_CHANGE_PERCENT","SENSEX_52W_HIGH","SENSEX_52W_LOW",
        "BANK_NIFTY_CLOSING","BANK_NIFTY_CHANGE_POINTS","BANK_NIFTY_CHANGE_PERCENT","BANK_NIFTY_52W_HIGH","BANK_NIFTY_52W_LOW",
        "INDICES_COMMENTARY","ADVANCES","DECLINES","UNCHANGED","BREADTH_COMMENTARY","NSE_TURNOVER","BSE_TURNOVER",
        "VOLUME_COMMENTARY","TOP_SECTOR_2_NAME","TOP_SECTOR_2_CHANGE","TOP_SECTOR_2_REASON","BOTTOM_SECTOR_2_NAME",
        "BOTTOM_SECTOR_2_CHANGE","BOTTOM_SECTOR_2_REASON",
        "GAINER_1_NAME","GAINER_1_PRICE","GAINER_1_CHANGE","GAINER_1_VOLUME",
        "GAINER_2_NAME","GAINER_2_PRICE","GAINER_2_CHANGE","GAINER_2_VOLUME",
        "LOSER_1_NAME","LOSER_1_PRICE","LOSER_1_CHANGE","LOSER_1_VOLUME",
        "LOSER_2_NAME","LOSER_2_PRICE","LOSER_2_CHANGE","LOSER_2_VOLUME",
        "INSTITUTIONAL_COMMENTARY","GLOBAL_MARKET_SUMMARY","COMMODITY_CURRENCY_COMMENTARY",
        "NIFTY_S1","NIFTY_S2","NIFTY_R1","NIFTY_R2",
        "BANK_NIFTY_S1","BANK_NIFTY_S2","BANK_NIFTY_R1","BANK_NIFTY_R2",
        "TECHNICAL_INDICATORS_COMMENTARY","CORPORATE_ANNOUNCEMENTS","ECONOMIC_DATA","REGULATORY_UPDATES","UPCOMING_EVENTS"
    ]
    for k in defaults:
        if k not in data:
            data[k] = "NA"
    return data

# ------------------- DOCX FILL -------------------
def replace_in_paragraph(paragraph, data_dict):
    for key, val in data_dict.items():
        placeholder = f"{{{{{key}}}}}"
        if placeholder in paragraph.text:
            paragraph.text = paragraph.text.replace(placeholder, str(val))

def fill_docx(template_file, output_file, data_dict):
    doc = Document(template_file)
    # Paragraphs
    for p in doc.paragraphs:
        replace_in_paragraph(p, data_dict)
    # Tables
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                # cells can have multiple paragraphs
                for p in cell.paragraphs:
                    replace_in_paragraph(p, data_dict)
    doc.save(output_file)
    logging.info("Saved filled docx to %s", output_file)

# ------------------- SEND EMAIL -------------------
def send_email(sender, password, recipient, subject, body, attachment_path):
    if not sender or not password:
        raise Exception("Missing email credentials (SENDER_EMAIL / SENDER_PASSWORD)")

    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))
    with open(attachment_path, "rb") as f:
        part = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))
    part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
    msg.attach(part)

    with smtplib.SMTP_SSL("smtp.gmail.com", 587, timeout=30) as server:
        server.login(sender, password)
        server.send_message(msg)
    logging.info("Email sent to %s", recipient)

# ------------------- MAIN -------------------
def main():
    try:
        logging.info("Starting job run.")
        data = fetch_report_data()
        logging.info("Fetched data keys: %s", ", ".join(list(data.keys())[:10]))

        fill_docx(TEMPLATE_FILE, OUTPUT_FILE, data)

        send_email(
            SENDER_EMAIL,
            SENDER_PASSWORD,
            RECIPIENT_EMAIL,
            subject=f"Daily Market Report - {data['REPORT_DATE']}",
            body="Please find attached the daily market report.",
            attachment_path=OUTPUT_FILE
        )
        logging.info("Job completed successfully; exiting.")
        sys.exit(0)  # explicit success exit
    except Exception as e:
        logging.exception("Job failed with exception: %s", e)
        # Exit non-zero so CI/Cloud Run logs show failure
        sys.exit(1)

if __name__ == "__main__":
    main()
