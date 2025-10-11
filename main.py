import yfinance as yf
import requests
from datetime import datetime
from docx import Document
from docx2html import convert
import pdfkit
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import os

# --- CONFIG ---
SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD")
RECIPIENT_EMAILS = os.environ.get("RECIPIENT_EMAILS", "").split(",")

TEMPLATE_PATH = "template.docx"
OUTPUT_DOCX = "report.docx"
OUTPUT_PDF = "report.pdf"

# --- FETCH MARKET DATA ---
def fetch_index_data():
    indices = {"NIFTY": "^NSEI", "SENSEX": "^BSESN", "BANK_NIFTY": "^NSEBANK"}
    data = {}
    for label, ticker in indices.items():
        t = yf.Ticker(ticker)
        hist = t.history(period="1d")
        if not hist.empty:
            last_close = hist["Close"].iloc[-1]
            data[f"{label}_CLOSING"] = f"{last_close:.2f}"
        else:
            data[f"{label}_CLOSING"] = "N/A"
    return data

# --- FETCH COMMODITIES & CURRENCY ---
def fetch_commodities_currency():
    symbols = {"BRENT": "BZ=F", "CRUDE_OIL": "CL=F", "GOLD": "GC=F", "INR_USD": "USDINR=X"}
    data = {}
    for key, symbol in symbols.items():
        t = yf.Ticker(symbol)
        hist = t.history(period="1d")
        if not hist.empty:
            last_close = hist["Close"].iloc[-1]
            data[f"{key}_PRICE"] = f"{last_close:.2f}"
            if len(hist["Close"]) > 1:
                change = last_close - hist["Close"].iloc[-2]
                data[f"{key}_CHANGE"] = f"{change:.2f}"
            else:
                data[f"{key}_CHANGE"] = "0.00"
        else:
            data[f"{key}_PRICE"] = data[f"{key}_CHANGE"] = "N/A"
    return data

# --- FETCH OTHER MARKET DATA (Top Gainers/Losers, Sector, FII/DII etc) ---
def fetch_other_data():
    # Placeholder: fetch from free public APIs or parse NSE/BSE JSON endpoints
    # Ensure all your template placeholders are mapped here
    data = {}
    # Example:
    data["GAINER_1_NAME"] = "TCS"
    data["GAINER_1_PRICE"] = "4500.00"
    data["GAINER_1_CHANGE"] = "2.5"
    data["GAINER_1_VOLUME"] = "123456"
    # Repeat for all placeholders...
    return data

# --- MERGE ALL DATA ---
def get_all_data():
    data = {}
    data.update(fetch_index_data())
    data.update(fetch_commodities_currency())
    data.update(fetch_other_data())
    data["REPORT_DATE"] = datetime.now().strftime("%d-%b-%Y")
    return data

# --- FILL TEMPLATE ---
def fill_template(template_path, output_path, data: dict):
    doc = Document(template_path)
    for para in doc.paragraphs:
        for key, val in data.items():
            placeholder = f"{{{{{key}}}}}"
            if placeholder in para.text:
                para.text = para.text.replace(placeholder, str(val))
    doc.save(output_path)

# --- CONVERT DOCX TO PDF (Linux-Compatible) ---
def docx_to_pdf(docx_path, pdf_path):
    html_content = convert(docx_path)
    pdfkit.from_string(html_content, pdf_path)

# --- SEND EMAIL ---
def send_email(pdf_path):
    with open(pdf_path, "rb") as f:
        pdf_bytes = f.read()

    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = ",".join(RECIPIENT_EMAILS)
    msg["Subject"] = f"Indian Market Wrap - {datetime.now().strftime('%d-%b-%Y')}"
    msg.attach(MIMEText("Attached is today's market wrap.", "plain"))

    attachment = MIMEApplication(pdf_bytes, _subtype="pdf")
    attachment.add_header("Content-Disposition", "attachment", filename="MarketWrap.pdf")
    msg.attach(attachment)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.send_message(msg)
    print("✅ Email sent successfully!")

# --- MAIN ---
if __name__ == "__main__":
    data = get_all_data()
    fill_template(TEMPLATE_PATH, OUTPUT_DOCX, data)
    docx_to_pdf(OUTPUT_DOCX, OUTPUT_PDF)
    send_email(OUTPUT_PDF)
