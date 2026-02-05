import time
import csv
import os
from datetime import datetime

import pandas as pd
import numpy as np

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

OUTPUT_FILE = "kandy_od_traffic.csv"

# Example Origin–Destination pairs (URL encoded automatically by Google)
OD_PAIRS = [
    ("Peradeniya, Sri Lanka", "Kandy Municipal Town Hall, Sri Lanka"),
    ("Katugastota, Sri Lanka", "Kandy Railway Station, Sri Lanka"),
]

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)


def accept_google_consent(page):
    """
    Handles Google's consent / cookies popup if present.
    This is critical for headless CI environments.
    """
    try:
        page.wait_for_selector("button:has-text('Accept all')", timeout=5000)
        page.click("button:has-text('Accept all')")
        page.wait_for_timeout(2000)
    except PlaywrightTimeoutError:
        pass  # Consent dialog not shown


def scrape_od_pair(page, origin, destination):
    """
    Scrape ETA and distance from Google Maps Directions.
    """
    start_time = time.time()

    url = f"https://www.google.com/maps/dir/{origin}/{destination}/"
    page.goto(url, timeout=60000)

    accept_google_consent(page)

    # Wait for directions panel to fully load
    page.wait_for_selector("div#section-directions-trip-0", timeout=60000)

    # Time in traffic (e.g., "45 min")
    time_text = page.locator(
        "div#section-directions-trip-0 span.section-directions-trip-duration"
    ).inner_text()

    # Distance (e.g., "14.2 km")
    distance_text = page.locator(
        "div#section-directions-trip-0 div.section-directions-trip-distance"
    ).inner_text()

    # Parse numbers
    minutes = float(time_text.replace("min", "").strip())
    km = float(distance_text.replace("km", "").strip())

    avg_speed_kmh = round(km / (minutes / 60), 2)

    process_time_sec = round(time.time() - start_time, 2)

    return {
        "timestamp_utc": datetime.utcnow().isoformat(),
        "origin": origin,
        "destination": destination,
        "eta_min": minutes,
        "distance_km": km,
        "avg_speed_kmh": avg_speed_kmh,
        "process_time_sec": process_time_sec,
    }


def main():
    records = []

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )

        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 800},
            locale="en-US",
        )

        page = context.new_page()

        for origin, destination in OD_PAIRS:
            try:
                data = scrape_od_pair(page, origin, destination)
                records.append(data)
            except Exception as e:
                print(f"ERROR scraping {origin} -> {destination}: {e}")

        browser.close()

    if not records:
        print("No data collected — exiting.")
        return

    df = pd.DataFrame(records)

    if os.path.exists(OUTPUT_FILE):
        df_existing = pd.read_csv(OUTPUT_FILE)
        df = pd.concat([df_existing, df], ignore_index=True)

    df.to_csv(OUTPUT_FILE, index=False)
    print(f"Saved {len(records)} new records to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
