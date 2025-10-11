import requests
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import os

# Fetch live Indian market data from free sources
def fetch_market_data():
    # Example: Free Yahoo Finance API alternative (yfinance)
    import yfinance as yf

    nifty = yf.Ticker("^NSEI").history(period="1d")
    bank_nifty = yf.Ticker("^NSEBANK").history(period="1d")
    sensex = yf.Ticker("^BSESN").history(period="1d")

    # Commodities and currency (free APIs)
    brent = requests.get("https://www.quandl.com/api/v3/datasets/ODA/POILBRE_USD.json").json()
    gold = requests.get("https://www.quandl.com/api/v3/datasets/LBMA/GOLD.json").json()
    inr_usd = requests.get("https://api.exchangerate.host/latest?base=INR&symbols=USD").json()

    data = {
        "nifty": {
            "closing": round(nifty['Close'].iloc[-1], 2),
            "change_points": round(nifty['Close'].iloc[-1] - nifty['Open'].iloc[-1], 2),
            "change_percent": round((nifty['Close'].iloc[-1] - nifty['Open'].iloc[-1])/nifty['Open'].iloc[-1]*100, 2)
        },
        "bank_nifty": {
            "closing": round(bank_nifty['Close'].iloc[-1], 2),
            "change_points": round(bank_nifty['Close'].iloc[-1] - bank_nifty['Open'].iloc[-1], 2),
            "change_percent": round((bank_nifty['Close'].iloc[-1] - bank_nifty['Open'].iloc[-1])/bank_nifty['Open'].iloc[-1]*100, 2)
        },
        "sensex": {
            "closing": round(sensex['Close'].iloc[-1], 2),
            "change_points": round(sensex['Close'].iloc[-1] - sensex['Open'].iloc[-1], 2),
            "change_percent": round((sensex['Close'].iloc[-1] - sensex['Open'].iloc[-1])/sensex['Open'].iloc[-1]*100, 2)
        },
        "commodities": {
            "brent_crude": {
                "price": brent['dataset']['data'][-1][1],
                "change": round(brent['dataset']['data'][-1][1] - brent['dataset']['data'][-2][1], 2)
            },
            "gold": {
                "price": gold['dataset']['data'][-1][1],
                "change": round(gold['dataset']['data'][-1][1] - gold['dataset']['data'][-2][1], 2)
            }
        },
        "currencies": {
            "inr_usd": {
                "rate": inr_usd['rates']['USD'],
                "change": 0  # Could fetch historical for delta if needed
            }
        },
        "market_breadth": {
            "advances": "NA", "declines": "NA", "unchanged": "NA"
        },
        "top_gainers": [
            {"name": "TCS", "price": 0, "change_percent": 0, "volume": 0},
            {"name": "Infosys", "price": 0, "change_percent": 0, "volume": 0}
        ],
        "top_losers": [
            {"name": "JSW Steel", "price": 0, "change_percent": 0, "volume": 0},
            {"name": "Coal India", "price": 0, "change_percent": 0, "volume": 0}
        ],
        "institutional_activity": {
            "fii_equity_net": 0,
            "dii_equity_net": 0,
            "fii_debt_net": 0,
            "dii_debt_net": 0
        }
    }
    return data

# Generate Executive Summary using Gemini LLM
def generate_summary(data):
    gemini_api_key = os.getenv("GEMINI_API_KEY")
    prompt = f"""
    Generate a concise daily market wrap summary for India using this data:
    Nifty: {data['nifty']}
    Bank Nifty: {data['bank_nifty']}
    Sensex: {data['sensex']}
    Commodities: {data['commodities']}
    Currencies: {data['currencies']}
    Top Gainers: {data['top_gainers']}
    Top Losers: {data['top_losers']}
    Institutional Activity: {data['institutional_activity']}
    """
    response = requests.post(
        "https://api.gemini.com/v1/generate",
        headers={"Authorization": f"Bearer {gemini_api_key}"},
        json={"prompt": prompt, "max_tokens": 300}
    )
    return response.json().get("text", "Summary not available")

# Generate JSON template filled with current data
def generate_json_template(data, summary):
    report_date = datetime.now().strftime("%d-%b-%Y")
    json_template = {
        "REPORT_DATE": report_date,
        "EXECUTIVE_SUMMARY": summary,
        "NIFTY_CLOSING": data['nifty']['closing'],
        "NIFTY_CHANGE_POINTS": data['nifty']['change_points'],
        "NIFTY_CHANGE_PERCENT": data['nifty']['change_percent'],
        "SENSEX_CLOSING": data['sensex']['closing'],
        "SENSEX_CHANGE_POINTS": data['sensex']['change_points'],
        "SENSEX_CHANGE_PERCENT": data['sensex']['change_percent'],
        "BANK_NIFTY_CLOSING": data['bank_nifty']['closing'],
        "BANK_NIFTY_CHANGE_POINTS": data['bank_nifty']['change_points'],
        "BANK_NIFTY_CHANGE_PERCENT": data['bank_nifty']['change_percent'],
        "ADVANCES": data['market_breadth']['advances'],
        "DECLINES": data['market_breadth']['declines'],
        "UNCHANGED": data['market_breadth']['unchanged'],
        "FII_EQUITY_NET": data['institutional_activity']['fii_equity_net'],
        "DII_EQUITY_NET": data['institutional_activity']['dii_equity_net'],
        "GAINER_1_NAME": data['top_gainers'][0]['name'],
        "GAINER_1_CHANGE": data['top_gainers'][0]['change_percent'],
        "LOSER_1_NAME": data['top_losers'][0]['name'],
        "LOSER_1_CHANGE": data['top_losers'][0]['change_percent'],
        "GLOBAL_MARKET_SUMMARY": "Global markets update not available",
        "INR_USD_RATE": data['currencies']['inr_usd']['rate'],
        "INR_USD_CHANGE": data['currencies']['inr_usd']['change'],
        "BRENT_PRICE": data['commodities']['brent_crude']['price'],
        "BRENT_CHANGE": data['commodities']['brent_crude']['change'],
        "GOLD_PRICE": data['commodities']['gold']['price'],
        "GOLD_CHANGE": data['commodities']['gold']['change'],
    }
    return json_template

# Send email with the report
def send_email(report_json):
    sender_email = os.getenv("SENDER_EMAIL")
    sender_password = os.getenv("SENDER_PASSWORD")
    receiver_email = "bhumivedant.bv@gmail.com"

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = f"Indian Market Daily Wrap - {report_json['REPORT_DATE']}"

    body = json.dumps(report_json, indent=4)
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
        print("Email sent successfully.")
    except Exception as e:
        print(f"Failed to send email: {e}")

# Main workflow
def main():
    data = fetch_market_data()
    summary = generate_summary(data)
    report_json = generate_json_template(data, summary)
    send_email(report_json)

if __name__ == "__main__":
    main()
