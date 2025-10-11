import yfinance as yf
import requests
from datetime import datetime
from docx import Document
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import os
from docx import Document
from docx2html import convert
import pdfkit

# ---------------- CONFIG ----------------
SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD")
RECIPIENT_EMAILS = os.environ.get("RECIPIENT_EMAILS", "").split(",")

TEMPLATE_PATH = "template.docx"
OUTPUT_DOCX = "report.docx"
OUTPUT_PDF = "report.pdf"

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

# ---------------- FETCH MARKET DATA ----------------
def fetch_indices():
    indices = {"NIFTY": "^NSEI", "SENSEX": "^BSESN", "BANK_NIFTY": "^NSEBANK"}
    data = {}
    for label, ticker in indices.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="1d")
            if not hist.empty:
                last_close = hist["Close"].iloc[-1]
                prev_close = hist["Close"].iloc[-2] if len(hist) > 1 else last_close
                change_points = last_close - prev_close
                change_percent = (change_points / prev_close) * 100 if prev_close != 0 else 0
                data[f"{label}_CLOSING"] = f"{last_close:.2f}"
                data[f"{label}_CHANGE_POINTS"] = f"{change_points:.2f}"
                data[f"{label}_CHANGE_PERCENT"] = f"{change_percent:.2f}"
                data[f"{label}_52W_HIGH"] = f"{t.info.get('fiftyTwoWeekHigh', 0):.2f}"
                data[f"{label}_52W_LOW"] = f"{t.info.get('fiftyTwoWeekLow', 0):.2f}"
            else:
                data[f"{label}_CLOSING"] = "N/A"
        except Exception:
            data[f"{label}_CLOSING"] = data[f"{label}_CHANGE_POINTS"] = data[f"{label}_CHANGE_PERCENT"] = "N/A"
    return data

# ---------------- FETCH TOP GAINERS/LOSERS ----------------
def fetch_top_gainers_losers():
    headers = {"User-Agent": "Mozilla/5.0"}
    data = {}
    try:
        # NSE Top gainers
        resp = requests.get("https://www.nseindia.com/api/live-analysis-variations", headers=headers, timeout=10)
        json_data = resp.json()
        gainers = json_data.get("topGainers", [])[:2]
        losers = json_data.get("topLosers", [])[:2]

        for i, g in enumerate(gainers, 1):
            data[f"GAINER_{i}_NAME"] = g.get("symbol")
            data[f"GAINER_{i}_PRICE"] = g.get("ltP")
            data[f"GAINER_{i}_CHANGE"] = g.get("ptsC")
            data[f"GAINER_{i}_VOLUME"] = g.get("trdVol")

        for i, l in enumerate(losers, 1):
            data[f"LOSER_{i}_NAME"] = l.get("symbol")
            data[f"LOSER_{i}_PRICE"] = l.get("ltP")
            data[f"LOSER_{i}_CHANGE"] = l.get("ptsC")
            data[f"LOSER_{i}_VOLUME"] = l.get("trdVol")
    except Exception:
        # default placeholders
        for i in range(1, 3):
            data[f"GAINER_{i}_NAME"] = data[f"LOSER_{i}_NAME"] = "N/A"
            data[f"GAINER_{i}_PRICE"] = data[f"LOSER_{i}_PRICE"] = "N/A"
            data[f"GAINER_{i}_CHANGE"] = data[f"LOSER_{i}_CHANGE"] = "N/A"
            data[f"GAINER_{i}_VOLUME"] = data[f"LOSER_{i}_VOLUME"] = "N/A"
    return data

# ---------------- FETCH COMMODITIES & CURRENCY ----------------
def fetch_commodities_currency():
    data = {}
    try:
        url = "https://query1.finance.yahoo.com/v7/finance/quote?symbols=GC=F,CL=F,USDINR=X"
        resp = requests.get(url, timeout=10).json()
        quotes = resp.get("quoteResponse", {}).get("result", [])
        for q in quotes:
            symbol = q.get("symbol")
            if symbol == "GC=F":
                data["GOLD_PRICE"] = q.get("regularMarketPrice", "N/A")
                data["GOLD_CHANGE"] = q.get("regularMarketChangePercent", "N/A")
            elif symbol == "CL=F":
                data["BRENT_PRICE"] = q.get("regularMarketPrice", "N/A")
                data["BRENT_CHANGE"] = q.get("regularMarketChangePercent", "N/A")
            elif symbol == "USDINR=X":
                data["INR_USD_RATE"] = q.get("regularMarketPrice", "N/A")
                data["INR_USD_CHANGE"] = q.get("regularMarketChangePercent", "N/A")
    except Exception:
        data.update({
            "GOLD_PRICE":"N/A", "GOLD_CHANGE":"N/A",
            "BRENT_PRICE":"N/A", "BRENT_CHANGE":"N/A",
            "INR_USD_RATE":"N/A", "INR_USD_CHANGE":"N/A"
        })
    return data

# ---------------- FETCH COMMENTARY ----------------
def fetch_commentary(all_data):
    try:
        headers = {"Authorization": f"Bearer {GEMINI_API_KEY}"}
        prompt = f"Generate executive summary and market commentaries based on the following data: {all_data}"
        resp = requests.post(
            "https://api.gemini.com/v1/completions",
            headers=headers,
            json={"prompt": prompt, "max_tokens": 500}
        ).json()
        text = resp.get("choices", [{}])[0].get("text", "")
        return {"EXECUTIVE_SUMMARY": text, "INDICES_COMMENTARY": text, "BREADTH_COMMENTARY": text,
                "VOLUME_COMMENTARY": text, "INSTITUTIONAL_COMMENTARY": text,
                "COMMODITY_CURRENCY_COMMENTARY": text, "TECHNICAL_INDICATORS_COMMENTARY": text,
                "GLOBAL_MARKET_SUMMARY": text}
    except Exception:
        return {k: "N/A" for k in [
            "EXECUTIVE_SUMMARY", "INDICES_COMMENTARY", "BREADTH_COMMENTARY",
            "VOLUME_COMMENTARY", "INSTITUTIONAL_COMMENTARY",
            "COMMODITY_CURRENCY_COMMENTARY", "TECHNICAL_INDICATORS_COMMENTARY",
            "GLOBAL_MARKET_SUMMARY"
        ]}

# ---------------- FILL TEMPLATE ----------------
def fill_template(doc_path, pdf_path, data):
    doc = Document(doc_path)
    # Replace placeholders
    for para in doc.paragraphs:
        for key, val in data.items():
            placeholder = f"{{{{{key}}}}}"
            if placeholder in para.text:
                para.text = para.text.replace(placeholder, str(val))
    temp_docx = "/tmp/temp.docx"
    doc.save(temp_docx)
    html_content = convert(temp_docx)
    pdfkit.from_string(html_content, pdf_path)

# ---------------- DOCX TO PDF ----------------
# def convert_to_pdf(docx_path, pdf_path):
#     html_content = convert(temp_docx)
#     pdfkit.from_string(html_content, pdf_path)

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
    all_data = {}
    all_data.update(fetch_indices())
    all_data.update(fetch_top_gainers_losers())
    all_data.update(fetch_commodities_currency())
    all_data.update(fetch_commentary(all_data))
    all_data["REPORT_DATE"] = datetime.now().strftime("%d-%b-%Y")

    fill_template(TEMPLATE_PATH, OUTPUT_PDF, all_data)
    # convert_to_pdf(OUTPUT_DOCX, OUTPUT_PDF)
    send_email(OUTPUT_PDF)
