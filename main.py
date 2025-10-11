#!/usr/bin/env python3
import os
import sys
import logging
import json
from datetime import datetime
from docx import Document
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from jsonschema import validate, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Optional Secret Manager
try:
    from google.cloud import secretmanager
except Exception:
    secretmanager = None

# Gemini client (google-genai)
try:
    from google import genai
except Exception:
    genai = None

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

# Gemini key
def get_gemini_key():
    k = os.environ.get("GEMINI_API_KEY")
    if k:
        return k
    return get_secret("GEMINI_API_KEY")

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
    "GAINER_1_NAME","GAINER_1_PRICE","GAINER_1_CHANGE","GAINER_1_VOLUME",
    "GAINER_2_NAME","GAINER_2_PRICE","GAINER_2_CHANGE","GAINER_2_VOLUME",
    "LOSER_1_NAME","LOSER_1_PRICE","LOSER_1_CHANGE","LOSER_1_VOLUME",
    "LOSER_2_NAME","LOSER_2_PRICE","LOSER_2_CHANGE","LOSER_2_VOLUME",
    "INSTITUTIONAL_COMMENTARY","GLOBAL_MARKET_SUMMARY","COMMODITY_CURRENCY_COMMENTARY",
    "NIFTY_S1","NIFTY_S2","NIFTY_R1","NIFTY_R2",
    "BANK_NIFTY_S1","BANK_NIFTY_S2","BANK_NIFTY_R1","BANK_NIFTY_R2",
    "TECHNICAL_INDICATORS_COMMENTARY","CORPORATE_ANNOUNCEMENTS","ECONOMIC_DATA","REGULATORY_UPDATES","UPCOMING_EVENTS"
]

# ------------------- GEMINI CALL -------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8), retry=retry_if_exception_type(Exception))
def call_gemini_for_report():
    """Call Gemini (GenAI) to produce JSON report. Retries on exceptions."""
    if genai is None:
        raise RuntimeError("google-genai (genai) library not available in the environment.")
    api_key = get_gemini_key()
    if not api_key:
        raise RuntimeError("Missing GEMINI_API_KEY in env or Secret Manager.")

    # set env var so SDK can pick it up (some SDKs read env)
    os.environ.setdefault("GEMINI_API_KEY", api_key)

    # initialize client
    try:
        client = genai.Client()
    except Exception as e:
        logging.warning("Failed to init genai.Client() without params: %s. Attempting with env.", e)
        # try fallback init
        client = genai.Client()

    # Strict prompt: JSON only, follow keys. Keep it compact.
    prompt = f"""
You are a data assistant. Produce a JSON object ONLY (no surrounding text, no explanations, no markdown)
that contains market data for a daily market report. Use the date format DD-MMM-YYYY.
Return keys exactly as named (strings or numbers as appropriate). Use "NA" for unknown values.
Required keys: REPORT_DATE, EXECUTIVE_SUMMARY.
Include numeric values without currency symbols.
Only return a single JSON object.
Example: {{"REPORT_DATE":"11-Oct-2025","EXECUTIVE_SUMMARY":"Markets closed mixed.","NIFTY_CLOSING":22452.5}}
Keys expected: REPORT_DATE, EXECUTIVE_SUMMARY, NIFTY_CLOSING, NIFTY_CHANGE_POINTS, NIFTY_CHANGE_PERCENT, NIFTY_52W_HIGH, NIFTY_52W_LOW, TOP_SECTOR_1_NAME, TOP_SECTOR_1_CHANGE, TOP_SECTOR_1_REASON, BOTTOM_SECTOR_1_NAME, BOTTOM_SECTOR_1_CHANGE, BOTTOM_SECTOR_1_REASON, GAINER_1_NAME, GAINER_1_PRICE, GAINER_1_CHANGE, GAINER_1_VOLUME, GAINER_2_NAME, GAINER_2_PRICE, GAINER_2_CHANGE, GAINER_2_VOLUME, LOSER_1_NAME, LOSER_1_PRICE, LOSER_1_CHANGE, LOSER_1_VOLUME, LOSER_2_NAME, LOSER_2_PRICE, LOSER_2_CHANGE, LOSER_2_VOLUME, BRENT_PRICE, BRENT_CHANGE, GOLD_PRICE, GOLD_CHANGE, INR_USD_RATE, INR_USD_CHANGE
"""

    # Use model environment override or default
    model_name = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

    # Use the SDK call that many genai quickstarts show; handle different response shapes defensively
    try:
        resp = client.models.generate_content(model=model_name, contents=prompt)
    except Exception as e:
        # Try alternative API if SDK version differs
        try:
            resp = client.generate_text(model=model_name, input=prompt)
        except Exception as e2:
            raise RuntimeError(f"Gemini call failed: {e} / fallback failed: {e2}")

    # extract text robustly
    text = None
    try:
        text = getattr(resp, "text", None)
    except Exception:
        text = None

    if not text:
        # try nested structure
        try:
            text = resp.output[0].content[0].text
        except Exception:
            # fallback to str
            text = str(resp)

    text_str = (text or "").strip()

    # remove surrounding code fences if present
    if text_str.startswith("```"):
        parts = text_str.split("```")
        if len(parts) >= 2:
            text_str = parts[1].strip()

    # isolate JSON
    first = text_str.find("{")
    last = text_str.rfind("}")
    if first == -1 or last == -1:
        raise RuntimeError("Gemini response did not contain JSON object. Raw: " + text_str[:1000])
    json_str = text_str[first:last+1]

    try:
        data = json.loads(json_str)
    except Exception as e:
        raise RuntimeError(f"Failed to parse JSON from Gemini response: {e}. Raw JSON candidate: {json_str[:1000]}")

    # validate loosely (we don't strictly abort on validation failure to avoid endless retries)
    try:
        validate(instance=data, schema=REPORT_SCHEMA)
    except ValidationError as ve:
        logging.warning("Gemini output failed strict schema validation: %s. Proceeding with best-effort.", ve)

    return data

# ------------------- FETCH REPORT -------------------
def fetch_report_data():
    """Top-level fetch function: get data from Gemini and fill missing defaults."""
    try:
        logging.info("Requesting report from Gemini...")
        data = call_gemini_for_report()
    except Exception as e:
        logging.exception("Gemini fetch failed: %s", e)
        # minimal fallback so we still create a document and exit
        data = {"REPORT_DATE": datetime.now().strftime("%d-%b-%Y"), "EXECUTIVE_SUMMARY": "NA"}

    # ensure required keys
    if "REPORT_DATE" not in data:
        data["REPORT_DATE"] = datetime.now().strftime("%d-%b-%Y")
    if "EXECUTIVE_SUMMARY" not in data:
        data["EXECUTIVE_SUMMARY"] = data.get("EXECUTIVE_SUMMARY", "NA")

    # populate defaults to avoid missing placeholders in docx
    for k in DEFAULT_KEYS:
        if k not in data:
            data[k] = "NA"

    # ensure common gainer/loser keys exist (2 each)
    for i in (1, 2):
        for prefix in ("GAINER", "LOSER"):
            name = f"{prefix}_{i}_NAME"
            price = f"{prefix}_{i}_PRICE"
            change = f"{prefix}_{i}_CHANGE"
            volume = f"{prefix}_{i}_VOLUME"
            if name not in data: data[name] = "NA"
            if price not in data: data[price] = "NA"
            if change not in data: data[change] = "NA"
            if volume not in data: data[volume] = "NA"

    # ensure commodity/currency keys
    for k in ["BRENT_PRICE","BRENT_CHANGE","GOLD_PRICE","GOLD_CHANGE","INR_USD_RATE","INR_USD_CHANGE"]:
        if k not in data:
            data[k] = "NA"

    logging.info("Prepared report data with %d keys. Report date: %s", len(data.keys()), data.get("REPORT_DATE"))
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
