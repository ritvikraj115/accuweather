import os
import requests
import pandas as pd
import time
import random
from fastapi import FastAPI
from bs4 import BeautifulSoup
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

# Load .env
print("🔄 Loading environment variables…")
load_dotenv()

app = FastAPI()

# Zoho Settings
ZOHO_CLIENT_ID     = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
ZOHO_OWNER         = os.getenv("ZOHO_OWNER")
ZOHO_APP           = os.getenv("ZOHO_APP")
ZOHO_FORM          = os.getenv("ZOHO_FORM")
PING_URL           = os.getenv("RENDER_PING_URL")

print(f"🐙 Zoho Client ID: {ZOHO_CLIENT_ID}")
print(f"📦 Zoho Owner/App/Form: {ZOHO_OWNER}/{ZOHO_APP}/{ZOHO_FORM}")
print(f"🔗 Ping URL: {PING_URL}")

LOCATION_CODE = "1-196832_1_al"
DAYS_TO_FETCH = 30

access_token = None

def refresh_zoho_token():
    global access_token
    print("🔑 refresh_zoho_token(): requesting new access token…")
    resp = requests.post(
        "https://accounts.zoho.in/oauth/v2/token",
        params={
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id": ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "grant_type": "refresh_token"
        }
    )
    print("📨 Zoho token response:", resp.status_code, resp.text[:200])
    resp.raise_for_status()
    data = resp.json()
    access_token = data["access_token"]
    print("✅ Got new Zoho access_token:", access_token[:20], "…")

def fetch_day_data(location_code: str, day: int) -> dict:
    print(f"🌦️  fetch_day_data(): day={day}")
    url = f"https://www.accuweather.com/en/in/rayanapadu/{location_code}/daily-weather-forecast/{location_code}?day={day}"
    print("→ GET", url)
    headers = {"User-Agent": "Mozilla/5.0", "Accept-Language": "en-US"}
    resp = requests.get(url, headers=headers, timeout=15)
    print("← Response:", resp.status_code)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    date_tag = soup.select_one(".subnav-pagination > div")
    date = date_tag.get_text(strip=True) if date_tag else ""
    print("  • Parsed date:", date)

    cards = soup.find_all("div", class_="half-day-card")
    print("  • Found half-day-card count:", len(cards))
    day_card = next((c for c in cards if c.select_one("h2") and "Day" in c.select_one("h2").get_text()), None)
    night_card = next((c for c in cards if c.select_one("h2") and "Night" in c.select_one("h2").get_text()), None)
    print("    – day_card:", bool(day_card), " night_card:", bool(night_card))

    def get_temp(card, label):
        if not card:
            print(f"    ! no {label} card")
            return ""
        t = card.select_one(".temperature")
        txt = t.get_text(strip=True).replace("°", "") if t else ""
        print(f"    • {label} temp:", txt)
        return txt

    def get_precip_chance(card):
        if not card:
            return ""
        for item in card.select(".panel-item"):
            label = item.find(text=True, recursive=False)
            if label and label.strip() == "Probability of Precipitation":
                val = item.select_one(".value")
                pct = val.get_text(strip=True).replace("%", "") if val else ""
                print("    • precip chance:", pct)
                return pct
        return ""

    def get_precip_amount(card):
        if not card:
            return ""
        for item in card.select(".panel-item"):
            label = item.find(text=True, recursive=False)
            if label and label.strip() == "Precipitation":
                val = item.select_one(".value")
                mm = val.get_text(strip=True).replace(" mm", "") if val else ""
                print("    • precip amount:", mm)
                return mm
        return ""

    return {
        "Date": date,
        "HighTemp": get_temp(day_card, "High"),
        "LowTemp": get_temp(night_card, "Low"),
        "PrecipChance_%": get_precip_chance(day_card),
        "PrecipAmount_mm": get_precip_amount(day_card)
    }

def upload_to_zoho(records):
    global access_token
    print("⬆️  upload_to_zoho(): uploading", len(records), "records")

    # 1. Create CSV file from records
    df = pd.DataFrame(records)
    csv_path = "forecast.csv"
    df.to_csv(csv_path, index=False)

    # 2. Upload file to Zoho Creator
    url = f"https://creator.zoho.in/api/v2/{ZOHO_OWNER}/{ZOHO_APP}/form/{ZOHO_FORM}"
    headers = {
        "Authorization": f"Zoho-oauthtoken {access_token}"
    }

    # 3. Prepare form data
    import json
    files = {
        "Name": (None,
            "Rayanadu"
        ),
        "Data": ("forecast.csv", open(csv_path, "rb"), "text/csv")
    }


    resp = requests.post(url, files=files, headers=headers)
    print("← Zoho upload response:", resp.status_code, resp.text[:200])

    if resp.status_code == 401:
        print("🔄 Token expired, refreshing and retrying upload")
        refresh_zoho_token()
        headers["Authorization"] = f"Zoho-oauthtoken {access_token}"
        resp = requests.post(url, files=files, headers=headers)
        print("← Retry response:", resp.status_code, resp.text[:200])

    resp.raise_for_status()
    print("✅ Uploaded to Zoho Creator")


def daily_job():
    print("📦 Running daily_job()")
    records = []
    for d in range(1, DAYS_TO_FETCH + 1):
        try:
            rec = fetch_day_data(LOCATION_CODE, d)
            if rec["Date"]:
                records.append(rec)
        except Exception as e:
            print(f"❌ Error fetching day {d}:", e)
        time.sleep(0.5 + random.random() * 0.5)
    print("▶️  All days fetched, uploading…")
    upload_to_zoho(records)

def keep_alive():
    if PING_URL:
        print("🔔 keep_alive(): pinging", PING_URL)
        try:
            r = requests.get(PING_URL)
            print("← keep_alive response:", r.status_code)
        except Exception as e:
            print("⚠️ keep_alive failed:", e)

#Schedule tasks (commented out for now)
scheduler = BackgroundScheduler()
scheduler.add_job(daily_job, 'cron', hour=12, minute=15, timezone='Asia/Kolkata')
scheduler.add_job(keep_alive, 'interval', minutes=7)
scheduler.add_job(refresh_zoho_token, 'interval', minutes=55)
scheduler.start()

# @app.on_event("startup")
# def startup_event():
#     print("🚀 Application startup: refreshing token and running daily job")
#     refresh_zoho_token()
#     daily_job()

@app.get("/ping")
def ping():
    print("🔍 /ping received")
    return {"status": "alive"}


