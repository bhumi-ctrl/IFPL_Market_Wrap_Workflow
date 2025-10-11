import os
import json
import requests
from datetime import datetime
from docx import Document
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

# ------------------- CONFIG -------------------
RECIPIENT_EMAIL = "recipient@example.com"
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")  # Gmail address
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD")  # App password
TEMPLATE_FILE = "template.docx"
OUTPUT_FILE = "Market_Report.docx"

# ------------------- DATA FETCHING -------------------

def fetch_nifty_data():
    url = "https://www.nseindia.com/api/quote-equity?symbol=NIFTY"
    response = requests.get(url)
    data = response.json()
    return {
        "NIFTY_CLOSING": data["priceInfo"]["lastPrice"],
        "NIFTY_CHANGE_POINTS": data["priceInfo"]["change"],
        "NIFTY_CHANGE_PERCENT": data["priceInfo"]["pChange"],
        "NIFTY_52W_HIGH": data["priceInfo"]["dayHigh"],
        "NIFTY_52W_LOW": data["priceInfo"]["dayLow"]
    }

def fetch_sector_performance():
    url = "https://www.nseindia.com/api/sector-performance"
    response = requests.get(url)
    data = response.json()
    sectors = data["data"]
    top_sector = sectors[0]
    bottom_sector = sectors[-1]
    return {
        "TOP_SECTOR_1_NAME": top_sector["sectorName"],
        "TOP_SECTOR_1_CHANGE": top_sector["change"],
        "TOP_SECTOR_1_REASON": top_sector["reason"],
        "BOTTOM_SECTOR_1_NAME": bottom_sector["sectorName"],
        "BOTTOM_SECTOR_1_CHANGE": bottom_sector["change"],
        "BOTTOM_SECTOR_1_REASON": bottom_sector["reason"]
    }

def fetch_top_gainers_losers():
    url = "https://www.nseindia.com/api/top-gainers"
    response = requests.get(url)
    data = response.json()
    gainers = data["data"][:2]
    losers = data["data"][-2:]
    gainers_info = {}
    for i, g in enumerate(gainers):
        gainers_info.update({
            f"GAMER_{i+1}_NAME": g["symbol"],
            f"GAMER_{i+1}_PRICE": g["lastPrice"],
            f"GAMER_{i+1}_CHANGE": g["change"],
            f"GAMER_{i+1}_VOLUME": g["quantityTraded"]
        })
    
    losers_info = {}
    for i, l in enumerate(losers):
        losers_info.update({
            f"LOSER_{i+1}_NAME": l["symbol"],
            f"LOSER_{i+1}_PRICE": l["lastPrice"],
            f"LOSER_{i+1}_CHANGE": l["change"],
            f"LOSER_{i+1}_VOLUME": l["quantityTraded"]
        })
    return {**gainers_info, **losers_info}

def fetch_institutional_activity():
    url = "https://www.nseindia.com/api/fii-dii"
    response = requests.get(url)
    data = response.json()
    fii = data["fii"]
    dii = data["dii"]
    return {
        "FII_EQUITY_BUY": fii["equityBuy"],
        "FII_EQUITY_SELL": fii["equitySell"],
        "FII_EQUITY_NET": fii["equityNet"],
        "FII_DEBT_BUY": fii["debtBuy"],
        "FII_DEBT_SELL": fii["debtSell"],
        "FII_DEBT_NET": fii["debtNet"],
        "DII_EQUITY_BUY": dii["equityBuy"],
        "DII_EQUITY_SELL": dii["equitySell"],
        "DII_EQUITY_NET": dii["equityNet"],
        "DII_DEBT_BUY": dii["debtBuy"],
        "DII_DEBT_SELL": dii["debtSell"],
        "DII_DEBT_NET": dii["debtNet"]
    }

def fetch_commodities_and_currency():
    url = "https://www.nseindia.com/api/commodities-currency"
    response = requests.get(url)
    data = response.json()
    return {
        "BRENT_PRICE": data["brentCrude"]["price"],
        "BRENT_CHANGE": data["brentCrude"]["change"],
        "GOLD_PRICE": data["gold"]["price"],
        "GOLD_CHANGE": data["gold"]["change"],
        "INR_USD_RATE": data["inrUsd"]["rate"],
        "INR_USD_CHANGE": data["inrUsd"]["change"]
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
    return data

# ------------------- DOCX FILL -------------------

def fill_docx(template_file, output_file, data_dict):
    doc = Document(template_file)
    for p in doc.paragraphs:
        for key, val in data_dict.items():
            if f"{{{{{key}}}}}" in p.text:
                p.text = p.text.replace(f"{{{{{key}}}}}", str(val))
    doc.save(output_file)

# ------------------- SEND EMAIL -------------------

def send_email(sender, password, recipient, subject, body, attachment_path):
    msg = MIMEMultipart()
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = subject

    msg.attach(MIMEText(body, "plain"))

    with open(attachment_path, "rb") as f:
        part = MIMEApplication(f.read(), Name=os.path.basename(attachment_path))
    part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
    msg.attach(part)

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(sender, password)
        server.send_message(msg)

# ------------------- MAIN -------------------

if __name__ == "__main__":
    data = fetch_report_data()
    fill_docx(TEMPLATE_FILE, OUTPUT_FILE, data)
    send_email(
        SENDER_EMAIL,
        SENDER_PASSWORD,
        RECIPIENT_EMAIL,
        subject=f"Daily Market Report - {data['REPORT_DATE']}",
        body="Please find attached the daily market report.",
        attachment_path=OUTPUT_FILE
    )
