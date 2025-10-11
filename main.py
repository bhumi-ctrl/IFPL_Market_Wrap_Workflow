#!/usr/bin/env python3
"""
main.py
- Reads template.docx in working directory
- Detects placeholders {{KEY}}
- Calls Gemini (google-genai) to produce JSON values for those keys
- Fills docx (paragraphs + tables), saves output, emails to recipient
"""
import os
import re
import sys
import json
import logging
from datetime import datetime
from docx import Document
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Optional libraries (Secret Manager + Gemini)
try:
    from google.cloud import secretmanager
except Exception:
    secretmanager = None

try:
    from google import genai
except Exception:
    genai = None

# ---------------------- CONFIG ----------------------
TEMPLATE_FILE = os.environ.get("TEMPLATE_FILE", "template.docx")
OUT_DIR = os.environ.get("OUT_DIR", ".")
RECIPIENT_EMAIL = "bhumivedant.bv@gmail.com"  # user-specified
SENDER_EMAIL_ENV = "SENDER_EMAIL"
SENDER_PASSWORD_ENV = "SENDER_PASSWORD"
GEMINI_API_ENV = "GEMINI_API_KEY"
GEMINI_MODEL_ENV = "GEMINI_MODEL"
DEFAULT_MODEL = os.environ.get(GEMINI_MODEL_ENV, "gemini-2.5-flash")
# ---------------------- LOGGING ----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------- SECRET HELPERS ----------------------
def access_secret(secret_name: str) -> str | None:
    """Try Secret Manager if available and project env set."""
    if secretmanager is None:
        logging.debug("Secret Manager client not available.")
        return None
    project = os.environ.get("GCP_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        logging.debug("GCP project not set; skipping secretmanager for %s", secret_name)
        return None
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project}/secrets/{secret_name}/versions/latest"
    try:
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("utf-8")
    except Exception as e:
        logging.warning("access_secret(%s) failed: %s", secret_name, e)
        return None

def get_secret(name: str) -> str | None:
    v = os.environ.get(name)
    if v:
        return v
    return access_secret(name)

# ---------------------- DOCX PLACEHOLDER DISCOVERY ----------------------
PLACEHOLDER_RE = re.compile(r"\{\{\s*([A-Z0-9_]+)\s*\}\}")

def discover_placeholders(template_path: str) -> list:
    """Return unique placeholder keys found in paragraphs and table cells."""
    doc = Document(template_path)
    keys = set()
    # paragraphs
    for p in doc.paragraphs:
        for m in PLACEHOLDER_RE.findall(p.text):
            keys.add(m)
    # tables
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for p in cell.paragraphs:
                    for m in PLACEHOLDER_RE.findall(p.text):
                        keys.add(m)
    return sorted(keys)

# ---------------------- GEMINI CALL (with retries) ----------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=8),
       retry=retry_if_exception_type(Exception))
def call_gemini_json(keys: list, model: str, api_key: str) -> dict:
    """
    Ask Gemini to return only JSON with entries for the provided keys.
    Returns dict (may be missing keys).
    """
    if genai is None:
        raise RuntimeError("google-genai SDK not installed or available in runtime.")
    # ensure env contains key (some SDKs read env)
    os.environ.setdefault("GEMINI_API_KEY", api_key)

    client = genai.Client()

    # Build prompt: list keys, request types where obvious, require JSON only
    keys_list_text = ", ".join(keys)
    prompt = (
        "You are a data assistant. Return ONLY a JSON object (no explanations, no markdown, no backticks). "
        "The JSON must contain keys exactly as requested below. Use simple numbers (no currency symbols) for numeric fields, "
        "and short text for commentary. If you don't know a value, return \"NA\" (string).\n\n"
        f"Keys: {keys_list_text}\n\n"
        "Return something like: {\"REPORT_DATE\":\"11-Oct-2025\",\"EXECUTIVE_SUMMARY\":\"Markets...\",\"NIFTY_CLOSING\":22452.5, ...}\n"
        "Make REPORT_DATE format DD-MMM-YYYY. Keep responses compact and valid JSON.\n"
    )

    logging.info("Sending prompt to Gemini for %d keys", len(keys))
    # call SDK (handle possible variations in SDK)
    model_name = model or DEFAULT_MODEL
    try:
        resp = client.models.generate_content(model=model_name, contents=prompt)
    except Exception as e:
        # try older style fallback
        try:
            resp = client.generate_text(model=model_name, input=prompt)
        except Exception as e2:
            raise RuntimeError(f"Gemini call failed: {e} / fallback failed: {e2}")
    # extract text
    text = None
    try:
        text = getattr(resp, "text", None)
    except Exception:
        text = None
    if not text:
        try:
            text = resp.output[0].content[0].text
        except Exception:
            text = str(resp)
    text = (text or "").strip()
    # remove code fences if present
    if text.startswith("```"):
        parts = text.split("```")
        if len(parts) >= 2:
            text = parts[1].strip()
    # isolate JSON braces
    first = text.find("{")
    last = text.rfind("}")
    if first == -1 or last == -1:
        raise RuntimeError("Gemini did not return a JSON object. Raw output: " + text[:1000])
    json_str = text[first:last+1]
    try:
        data = json.loads(json_str)
    except Exception as e:
        raise RuntimeError(f"Failed to parse JSON from Gemini response: {e}. Candidate: {json_str[:1000]}")
    if not isinstance(data, dict):
        raise RuntimeError("Gemini returned JSON that is not an object.")
    logging.info("Gemini returned %d keys", len(data.keys()))
    return data

# ---------------------- DOCX FILLER ----------------------
def replace_in_paragraph(paragraph, data_dict):
    # Replace multiple occurrences safely
    text = paragraph.text
    for k, v in data_dict.items():
        placeholder = "{{" + k + "}}"
        if placeholder in text:
            text = text.replace(placeholder, str(v))
        # also handle whitespace-inside braces variants
        placeholder_ws = "{{ " + k + " }}"
        if placeholder_ws in text:
            text = text.replace(placeholder_ws, str(v))
    if text != paragraph.text:
        paragraph.text = text

def fill_docx(template_path: str, output_path: str, data: dict):
    doc = Document(template_path)
    # paragraphs
    for p in doc.paragraphs:
        replace_in_paragraph(p, data)
    # tables
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                for p in cell.paragraphs:
                    replace_in_paragraph(p, data)
    doc.save(output_path)
    logging.info("Saved filled document to %s", output_path)

# ---------------------- EMAIL SENDER ----------------------
def send_email(sender: str, password: str, recipient: str, subject: str, body: str, attachment_path: str):
    if not sender or not password:
        raise RuntimeError("Missing SMTP credentials: set SENDER_EMAIL and SENDER_PASSWORD as env or Secret Manager.")
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
    logging.info("Email sent successfully to %s", recipient)

# ---------------------- MAIN FLOW ----------------------
def main():
    logging.info("Starting template fill run.")
    if not os.path.exists(TEMPLATE_FILE):
        logging.error("Template file not found: %s", TEMPLATE_FILE)
        sys.exit(1)

    try:
        # 1) discover placeholders
        placeholders = discover_placeholders(TEMPLATE_FILE)
        if not placeholders:
            logging.warning("No placeholders found in template. Exiting.")
            sys.exit(1)
        logging.info("Discovered %d placeholders", len(placeholders))

        # 2) call Gemini to get values for all placeholders
        gemini_key = get_secret(GEMINI_API_ENV)
        if not gemini_key:
            raise RuntimeError("Missing GEMINI_API_KEY (env or Secret Manager).")
        response_dict = call_gemini_json(placeholders, model=os.environ.get(GEMINI_MODEL_ENV), api_key=gemini_key)

        # 3) coerce/normalize: ensure all placeholders present (fill NA)
        filled = {}
        for k in placeholders:
            v = response_dict.get(k)
            if v is None:
                # special-case REPORT_DATE default
                if k == "REPORT_DATE":
                    v = datetime.now().strftime("%d-%b-%Y")
                else:
                    v = "NA"
            # format floats/ints as plain numbers; keep strings as-is
            filled[k] = v

        # 4) write output docx
        out_name = f"Market_Report_{datetime.now().strftime('%Y%m%d')}.docx"
        out_path = os.path.join(OUT_DIR, out_name)
        fill_docx(TEMPLATE_FILE, out_path, filled)

        # 5) email to recipient
        sender = get_secret(SENDER_EMAIL_ENV)
        password = get_secret(SENDER_PASSWORD_ENV)
        subject = f"Daily Market Report - {filled.get('REPORT_DATE', '')}"
        body = "Please find attached the daily market report."
        send_email(sender, password, RECIPIENT_EMAIL, subject, body, out_path)

        logging.info("Run complete, exiting 0.")
        sys.exit(0)
    except Exception as e:
        logging.exception("Run failed: %s", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
