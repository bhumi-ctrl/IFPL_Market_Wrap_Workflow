import os
import io
import smtplib
import requests
import yfinance as yf
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
import google.auth
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import socket

# --- CONFIGURATION ---
TEMPLATE_DOC_ID = os.environ.get("GOOGLE_DOC_TEMPLATE_ID")
SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SENDER_EMAIL = os.environ.get("SENDER_EMAIL")
SENDER_PASSWORD = os.environ.get("SENDER_PASSWORD")  # use Gmail App Password
RECIPIENT_EMAILS = os.environ.get("RECIPIENT_EMAILS", "").split(",")
PROJECT = os.environ.get("GCP_PROJECT")

SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents"
]

# --- GCP AUTH HELPERS ---
def get_services():
    creds, _ = google.auth.default(scopes=SCOPES)
    docs = build("docs", "v1", credentials=creds)
    drive = build("drive", "v3", credentials=creds)
    return docs, drive

# --- GOOGLE DOC OPERATIONS ---
def copy_template(drive, template_id):
    try:
        name = f"Indian Market Wrap {datetime.utcnow().strftime('%Y-%m-%d')}"
        copied = drive.files().copy(fileId=template_id, body={"name": name}).execute()
        return copied["id"]
    except Exception as e:
        print(f"❌ Failed to copy template: {e}")
        return None

def replace_placeholders(docs, doc_id, replacements):
    if not doc_id:
        return
    requests_list = []
    for key, value in replacements.items():
        variants = [f"{{{{{key}}}}}", f"[{key}]", key]
        for v in variants:
            requests_list.append({
                "replaceAllText": {
                    "containsText": {"text": v, "matchCase": False},
                    "replaceText": value
                }
            })
    if requests_list:
        try:
            docs.documents().batchUpdate(documentId=doc_id, body={"requests": requests_list}).execute()
        except Exception as e:
            print(f"❌ Failed to replace placeholders: {e}")

def export_pdf(drive, doc_id):
    if not doc_id:
        return None
    try:
        mime = "application/pdf"
        request = drive.files().export_media(fileId=doc_id, mimeType=mime)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        fh.seek(0)
        return fh.read()
    except Exception as e:
        print(f"❌ Failed to export PDF: {e}")
        return None

# --- MARKET DATA FETCH ---
def fetch_market_data():
    out = {}
    try:
        nifty = yf.Ticker("^NSEI")
        sensex = yf.Ticker("^BSESN")
        now = datetime.now()
        out["DATE"] = now.strftime("%d-%b-%Y")

        for label, t in [("NIFTY", nifty), ("SENSEX", sensex)]:
            hist = t.history(period="1d", timeout=10)  # timeout added
            if not hist.empty:
                last_close = hist["Close"].iloc[-1]
                out[label] = f"{last_close:.2f}"
            else:
                out[label] = "N/A"
    except Exception as e:
        print(f"❌ Failed to fetch market data: {e}")
        out["DATE"] = datetime.now().strftime("%d-%b-%Y")
        out["NIFTY"] = out["SENSEX"] = "N/A"
    return out

# --- EMAIL SENDER ---
def send_email_with_pdf(pdf_bytes, subject, body):
    if not pdf_bytes:
        print("❌ No PDF to send, skipping email")
        return
    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = ", ".join(RECIPIENT_EMAILS)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    attachment = MIMEApplication(pdf_bytes, _subtype="pdf")
    attachment.add_header(
        "Content-Disposition",
        "attachment",
        filename=f"Indian_Market_Wrap_{datetime.now().strftime('%d-%b-%Y')}.pdf"
    )
    msg.attach(attachment)

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=30) as server:
            server.starttls()
            server.login(SENDER_EMAIL, SENDER_PASSWORD)
            server.send_message(msg)
        print(f"✅ Email sent to {RECIPIENT_EMAILS}")
    except (smtplib.SMTPException, socket.timeout) as e:
        print(f"❌ Email failed: {e}")

# --- MAIN RUN ---
def main_run():
    print("🟢 Job started")
    docs, drive = get_services()

    new_doc_id = copy_template(drive, TEMPLATE_DOC_ID)
    snapshot = fetch_market_data()
    replacements = {
        "DATE": snapshot.get("DATE", ""),
        "NIFTY": snapshot.get("NIFTY", ""),
        "SENSEX": snapshot.get("SENSEX", ""),
        "Executive Summary": f"Nifty closed at {snapshot.get('NIFTY')} and Sensex at {snapshot.get('SENSEX')}."
    }

    replace_placeholders(docs, new_doc_id, replacements)
    pdf_data = export_pdf(drive, new_doc_id)

    subject = f"Indian Market Daily Wrap — {snapshot.get('DATE')}"
    body = f"Attached is your daily market wrap for {snapshot.get('DATE')}."
    send_email_with_pdf(pdf_data, subject, body)

    # cleanup
    if new_doc_id:
        try:
            drive.files().delete(fileId=new_doc_id).execute()
        except Exception:
            pass
    print("✅ Job completed successfully")

if __name__ == "__main__":
    main_run()
