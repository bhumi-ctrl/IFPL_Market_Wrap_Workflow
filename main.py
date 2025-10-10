import os
from datetime import datetime
from docx import Document
from docx2pdf import convert
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

# --- CONFIG ---
TEMPLATE_PATH = os.environ.get("TEMPLATE_PATH", "template.docx")
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD")
SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
RECIPIENT_EMAILS = os.environ.get("RECIPIENT_EMAILS", "").split(",")

# --- MARKET DATA ---
import yfinance as yf
def fetch_market_data():
    out = {}
    try:
        nifty = yf.Ticker("^NSEI").history(period="1d")["Close"].iloc[-1]
        sensex = yf.Ticker("^BSESN").history(period="1d")["Close"].iloc[-1]
        now = datetime.now()
        out["DATE"] = now.strftime("%d-%b-%Y")
        out["NIFTY"] = f"{nifty:.2f}"
        out["SENSEX"] = f"{sensex:.2f}"
    except:
        out["DATE"] = datetime.now().strftime("%d-%b-%Y")
        out["NIFTY"] = out["SENSEX"] = "N/A"
    return out

# --- FILL TEMPLATE ---
def fill_template():
    data = fetch_market_data()
    doc = Document(TEMPLATE_PATH)
    replacements = {
        "DATE": data["DATE"],
        "NIFTY": data["NIFTY"],
        "SENSEX": data["SENSEX"],
        "Executive Summary": f"Nifty closed at {data['NIFTY']} and Sensex at {data['SENSEX']}."
    }
    for p in doc.paragraphs:
        for key, val in replacements.items():
            if key in p.text:
                p.text = p.text.replace(key, val)
    doc_path = "/tmp/filled.docx"
    doc.save(doc_path)
    pdf_path = "/tmp/filled.pdf"
    convert(doc_path, pdf_path)
    return pdf_path

# --- SEND EMAIL ---
def send_email(pdf_path):
    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = ", ".join(RECIPIENT_EMAILS)
    msg["Subject"] = f"Indian Market Wrap — {datetime.now().strftime('%d-%b-%Y')}"
    msg.attach(MIMEText("Attached is your daily market wrap.", "plain"))
    with open(pdf_path, "rb") as f:
        attach = MIMEApplication(f.read(), _subtype="pdf")
        attach.add_header("Content-Disposition", "attachment", filename=os.path.basename(pdf_path))
        msg.attach(attach)
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_PASSWORD)
        server.send_message(msg)

# --- MAIN ---
if __name__ == "__main__":
    pdf_file = fill_template()
    send_email(pdf_file)
    print("✅ Job completed")
