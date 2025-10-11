import os
import yfinance as yf
import requests
from datetime import datetime
from docx import Document
import pdfkit
from docx2html import convert
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import openai

# ---------------- CONFIG ----------------
SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD")
RECIPIENT_EMAILS = os.environ.get("RECIPIENT_EMAILS", "").split(",")

TEMPLATE_PATH = "template.docx"
OUTPUT_DOCX = "MarketWrap.docx"
OUTPUT_PDF = "MarketWrap.pdf"

openai.api_key = os.environ.get("GEMINI_API_KEY")  # For commentary generation

# ---------------- FETCH MARKET DATA ----------------
def get_all_data():
    data = {}

    # 1️⃣ Report date
    data["REPORT_DATE"] = datetime.now().strftime("%d-%b-%Y")

    # 2️⃣ Market Indices
    indices = {"NIFTY": "^NSEI", "SENSEX": "^BSESN", "BANK_NIFTY": "^NSEBANK"}
    for label, ticker in indices.items():
        t = yf.Ticker(ticker)
        hist = t.history(period="2d")  # last 2 days for change calculation
        if not hist.empty:
            last_close = hist["Close"].iloc[-1]
            prev_close = hist["Close"].iloc[-2] if len(hist) > 1 else last_close
            change_points = last_close - prev_close
            change_percent = (change_points / prev_close * 100) if prev_close != 0 else 0
            data[f"{label}_CLOSING"] = f"{last_close:.2f}"
            data[f"{label}_CHANGE_POINTS"] = f"{change_points:.2f}"
            data[f"{label}_CHANGE_PERCENT"] = f"{change_percent:.2f}%"
            data[f"{label}_52W_HIGH"] = f"{t.info.get('fiftyTwoWeekHigh', 'N/A')}"
            data[f"{label}_52W_LOW"] = f"{t.info.get('fiftyTwoWeekLow', 'N/A')}"
        else:
            for suffix in ["CLOSING", "CHANGE_POINTS", "CHANGE_PERCENT", "52W_HIGH", "52W_LOW"]:
                data[f"{label}_{suffix}"] = "N/A"

    # 3️⃣ Commodities & Currency
    commodities = {"BRENT_PRICE": "BZ=F", "GOLD_PRICE": "GC=F", "INR_USD_RATE": "USDINR=X"}
    for key, symbol in commodities.items():
        t = yf.Ticker(symbol)
        hist = t.history(period="2d")
        if not hist.empty:
            last_close = hist["Close"].iloc[-1]
            prev_close = hist["Close"].iloc[-2] if len(hist) > 1 else last_close
            change = last_close - prev_close
            data[key] = f"{last_close:.2f}"
            data[f"{key}_CHANGE"] = f"{change:.2f}"
        else:
            data[key] = "N/A"
            data[f"{key}_CHANGE"] = "N/A"

    # 4️⃣ Top Gainers / Losers (placeholder example)
    for i in range(1, 3):
        data[f"GAINER_{i}_NAME"] = f"Gainer{i}"
        data[f"GAINER_{i}_PRICE"] = f"N/A"
        data[f"GAINER_{i}_CHANGE"] = f"N/A"
        data[f"GAINER_{i}_VOLUME"] = f"N/A"
        data[f"LOSER_{i}_NAME"] = f"Loser{i}"
        data[f"LOSER_{i}_PRICE"] = f"N/A"
        data[f"LOSER_{i}_CHANGE"] = f"N/A"
        data[f"LOSER_{i}_VOLUME"] = f"N/A"

    # 5️⃣ Sector Performance placeholders
    for i in range(1, 3):
        data[f"TOP_SECTOR_{i}_NAME"] = f"Sector{i}"
        data[f"TOP_SECTOR_{i}_CHANGE"] = f"{i}.5%"
        data[f"TOP_SECTOR_{i}_REASON"] = f"Reason {i}"
        data[f"BOTTOM_SECTOR_{i}_NAME"] = f"Sector{i+2}"
        data[f"BOTTOM_SECTOR_{i}_CHANGE"] = f"-{i}.5%"
        data[f"BOTTOM_SECTOR_{i}_REASON"] = f"Reason {i+2}"

    # 6️⃣ Institutional Activity placeholders
    for key in ["FII_EQUITY_BUY", "FII_EQUITY_SELL", "FII_EQUITY_NET",
                "FII_DEBT_BUY", "FII_DEBT_SELL", "FII_DEBT_NET",
                "DII_EQUITY_BUY", "DII_EQUITY_SELL", "DII_EQUITY_NET",
                "DII_DEBT_BUY", "DII_DEBT_SELL", "DII_DEBT_NET"]:
        data[key] = "N/A"

    # 7️⃣ Commentary (OpenAI / Gemini API)
    try:
        prompt = f"Generate executive summary and commentaries for market indices: Nifty {data['NIFTY_CLOSING']}, Sensex {data['SENSEX_CLOSING']}, Bank Nifty {data['BANK_NIFTY_CLOSING']}, top gainers {data['GAINER_1_NAME']}, top losers {data['LOSER_1_NAME']}."
        response = openai.Completion.create(model="gpt-4", prompt=prompt, max_tokens=150)
        summary = response.choices[0].text.strip()
        commentary_fields = ["EXECUTIVE_SUMMARY", "INDICES_COMMENTARY", "BREADTH_COMMENTARY",
                             "VOLUME_COMMENTARY", "INSTITUTIONAL_COMMENTARY", "COMMODITY_CURRENCY_COMMENTARY",
                             "TECHNICAL_INDICATORS_COMMENTARY", "GLOBAL_MARKET_SUMMARY"]
        for field in commentary_fields:
            data[field] = summary
    except:
        for field in ["EXECUTIVE_SUMMARY", "INDICES_COMMENTARY", "BREADTH_COMMENTARY",
                      "VOLUME_COMMENTARY", "INSTITUTIONAL_COMMENTARY", "COMMODITY_CURRENCY_COMMENTARY",
                      "TECHNICAL_INDICATORS_COMMENTARY", "GLOBAL_MARKET_SUMMARY"]:
            data[field] = "N/A"

    # 8️⃣ News & Events placeholders
    for key in ["CORPORATE_ANNOUNCEMENTS", "ECONOMIC_DATA", "REGULATORY_UPDATES", "UPCOMING_EVENTS"]:
        data[key] = "N/A"

    return data

# ---------------- FILL TEMPLATE ----------------
def fill_template(template_path, output_path, data: dict):
    doc = Document(template_path)
    for para in doc.paragraphs:
        for key, val in data.items():
            placeholder = f"{{{{{key}}}}}"
            if placeholder in para.text:
                para.text = para.text.replace(placeholder, str(val))
    doc.save(output_path)

# ---------------- CONVERT DOCX TO PDF ----------------
def docx_to_pdf(docx_path, pdf_path):
    html_content = convert(docx_path)
    pdfkit.from_string(html_content, pdf_path)

# ---------------- SEND EMAIL ----------------
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

# ---------------- MAIN ----------------
if __name__ == "__main__":
    data = get_all_data()
    fill_template(TEMPLATE_PATH, OUTPUT_DOCX, data)
    docx_to_pdf(OUTPUT_DOCX, OUTPUT_PDF)
    send_email(OUTPUT_PDF)
    print("✅ Market wrap generated and sent successfully!")
