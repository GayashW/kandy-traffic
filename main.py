#!/usr/bin/env python3
"""
Kandy Traffic Monitor - Multi-modal ETA
Debug mode – limited segmentation, JSON output
"""

import asyncio
import json
import math
import time
import re
from pathlib import Path
from datetime import datetime

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ---------------- CONFIG ----------------

MAX_SEGMENTS_PER_ROUTE = 5   # 🔧 DEBUG LIMIT
THROTTLE_SEC = 2             # give time for Google Maps to load
MAX_RETRIES = 3

DATA_ROOT = Path("data/journeys")

ROUTES = [
    {"name": "Peradeniya-to-KMTT", "origin": (6.895575, 79.854851), "destination": (6.871813, 79.884564)},
    {"name": "Temple-to-Railway", "origin": (6.9271, 79.8612), "destination": (6.9619, 79.8823)},
]

# ---------------- LOGGING ----------------

def log(msg):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

# ---------------- GEO ----------------

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    d_phi = math.radians(lat2 - lat1)
    d_lambda = math.radians(lon2 - lon1)
    a = math.sin(d_phi / 2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(d_lambda / 2)**2
    return R * (2 * math.atan2(math.sqrt(a), math.sqrt(1 - a)))

def interpolate_segments(o, d, limit):
    segs = []
    for i in range(limit):
        segs.append((
            o[0] + (d[0] - o[0]) * i / limit,
            o[1] + (d[1] - o[1]) * i / limit,
            o[0] + (d[0] - o[0]) * (i + 1) / limit,
            o[1] + (d[1] - o[1]) * (i + 1) / limit,
        ))
    return segs

# ---------------- SCRAPER ----------------

async def scrape_segment(context, page, route, seg_idx, seg):
    url = f"https://www.google.com/maps/dir/{seg[0]},{seg[1]}/{seg[2]},{seg[3]}/"
    log(f"[GoogleMaps] 🌐 {url}")

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await page.goto(url, wait_until="networkidle", timeout=90000)
            # Wait for mode buttons to appear
            await page.wait_for_selector("button.m6Uuef", timeout=90000)
            await asyncio.sleep(2)  # ensure all buttons load

            result = {
                "segment_index": seg_idx,
                "origin": [seg[0], seg[1]],
                "destination": [seg[2], seg[3]],
                "distance_m": round(haversine(seg[0], seg[1], seg[2], seg[3]), 2),
                "status": "success",
                "eta_min": {},
                "avg_speed_kmh": {},
            }

            buttons = page.locator("button.m6Uuef")
            count = await buttons.count()

            for i in range(count):
                b = buttons.nth(i)
                mode = await b.get_attribute("data-tooltip")
                eta_text = await b.locator("div.Fl2iee.HNPWFe").inner_text()
                total_min = 0
                h = re.search(r"(\d+)\s*h", eta_text)
                m = re.search(r"(\d+)\s*min", eta_text)
                if h: total_min += int(h.group(1)) * 60
                if m: total_min += int(m.group(1))
                if total_min > 0:
                    result["eta_min"][mode.lower()] = total_min
                    result["avg_speed_kmh"][mode.lower()] = round((result["distance_m"] / 1000) / (total_min / 60), 2)
                log(f"[{mode}] ⏱ ETA={total_min} min | Speed={result['avg_speed_kmh'].get(mode.lower(), 0)} km/h")

            return result

        except PlaywrightTimeout as e:
            log(f"❌ Attempt {attempt} timeout. Retrying...")
            if attempt == MAX_RETRIES:
                return {"segment_index": seg_idx, "status": f"failed: timeout"}
            await asyncio.sleep(5)
            page = await context.new_page()  # reset page for retry

        except Exception as e:
            log(f"❌ Attempt {attempt} failed: {e}")
            if attempt == MAX_RETRIES:
                return {"segment_index": seg_idx, "status": f"failed: {str(e)[:50]}"}
            await asyncio.sleep(2)
            page = await context.new_page()

# ---------------- MAIN ----------------

async def main():
    now = datetime.utcnow()
    date_path = DATA_ROOT / now.strftime("%Y/%Y%m/%Y%m%d")
    date_path.mkdir(parents=True, exist_ok=True)
    outfile = date_path / f"{now.strftime('%Y%m%d.%H%M%S')}.json"

    all_results = {"timestamp_utc": now.isoformat(), "routes": {}}

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"]
        )
        context = await browser.new_context(locale="en-US")
        page = await context.new_page()

        for route in ROUTES:
            log(f"🚗 Route: {route['name']}")
            segments = interpolate_segments(route["origin"], route["destination"], MAX_SEGMENTS_PER_ROUTE)
            route_results = []

            for i, seg in enumerate(segments, 1):
                log(f"[{route['name']}] Segment {i}/{len(segments)}")
                res = await scrape_segment(context, page, route, i, seg)
                route_results.append(res)
                await asyncio.sleep(THROTTLE_SEC)

            all_results["routes"][route["name"]] = route_results

        await browser.close()

    outfile.write_text(json.dumps(all_results, indent=2))
    log(f"[Saved] {outfile} ({outfile.stat().st_size} B)")

if __name__ == "__main__":
    asyncio.run(main())
