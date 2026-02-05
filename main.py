import os
import re
import time
from datetime import datetime

import pandas as pd
import numpy as np

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

# ---------------- CONFIG ----------------

CSV_FILE = "kandy_od_traffic.csv"

OD_PAIRS = [
    ("Peradeniya, Sri Lanka", "Kandy Municipal Town Hall, Sri Lanka"),
    ("Katugastota, Sri Lanka", "Kandy Railway Station, Sri Lanka"),
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

# ---------------- HELPERS ----------------

def ensure_csv_exists():
    """Guarantee CSV exists so Git commit step never fails."""
    if not os.path.exists(CSV_FILE):
        df = pd.DataFrame(columns=[
            "timestamp_utc",
            "origin",
            "destination",
            "eta_min",
            "distance_km",
            "avg_speed_kmh",
            "process_time_sec",
        ])
        df.to_csv(CSV_FILE, index=False)


def handle_google_consent(page):
    """
    Accept Google consent / GDPR popup if present.
    Must run immediately after page.goto().
    """
    try:
        for frame in page.frames:
            if "consent.google.com" in frame.url:
                btn = frame.locator("button:has-text('Accept all')")
                if btn.count() > 0:
                    btn.first.click()
                    page.wait_for_timeout(2000)
                    return
    except Exception:
        pass


def force_route_selection(page):
    """
    In headless mode, Google Maps sometimes does not auto-select a route.
    This forces the first route card to activate.
    """
    try:
        route_btn = page.locator("div[role='button']:has-text('min')")
        if route_btn.count() > 0:
            route_btn.first.click()
            page.wait_for_timeout(1500)
    except Exception:
        pass


def scrape_od_pair(page, origin, destination):
    start_time = time.time()

    url = f"https://www.google.com/maps/dir/{origin}/{destination}/"
    page.goto(url, timeout=90000, wait_until="domcontentloaded")

    handle_google_consent(page)

    # Wait for Maps UI shell
    page.wait_for_selector("div[role='main']", timeout=90000)

    force_route_selection(page)

    # Extract visible text (robust against UI changes)
    content = page.inner_text("div[role='main']")

    time_match = re.search(r"(\d+)\s*min", content)
    dist_match = re.search(r"([\d\.]+)\s*km", content)

    if not time_match or not dist_match:
        raise ValueError("Could not parse ETA or distance from page")

    eta_min = float(time_match.group(1))
    distance_km = float(dist_match.group(1))
    avg_speed_kmh = round(distance_km / (eta_min / 60), 2)

    process_time_sec = round(time.time() - start_time, 2)

    return {
        "timestamp_utc": datetime.utcnow().isoformat(),
        "origin": origin,
        "destination": destination,
        "eta_min": eta_min,
        "distance_km": distance_km,
        "avg_speed_kmh": avg_speed_kmh,
        "process_time_sec": process_time_sec,
    }

# ---------------- MAIN ----------------

def main():
    ensure_csv_exists()
    records = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            slow_mo=100,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )

        context = browser.new_context(
            user_agent=USER_AGENT,
            locale="en-US",
            timezone_id="Asia/Colombo",
            viewport={"width": 1280, "height": 800},
        )

        page = context.new_page()

        for origin, destination in OD_PAIRS:
            try:
                data = scrape_od_pair(page, origin, destination)
                records.append(data)
            except Exception as e:
                print(f"[ERROR] {origin} -> {destination}: {e}")

        browser.close()

    if not records:
        print("No records scraped.")
        return

    df_new = pd.DataFrame(records)
    df_existing = pd.read_csv(CSV_FILE)

    df_all = pd.concat([df_existing, df_new], ignore_index=True)
    df_all.to_csv(CSV_FILE, index=False)

    print(f"Appended {len(df_new)} records to {CSV_FILE}")

# ---------------- ENTRY ----------------

if __name__ == "__main__":
    main()
