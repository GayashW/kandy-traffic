#!/usr/bin/env python3
"""
Virtual Floating Car – Kandy Traffic Monitor
Splits roads into ≤5m segments, runs both directions, saves per-segment speed
"""

import asyncio
import csv
import math
import time
from datetime import datetime
from pathlib import Path

from playwright.async_api import async_playwright
from roads import ROADS  # import your roads.py with 5 roads

# ---------------- CONFIG ----------------

SEGMENT_FILE = Path("segments/kandy_segments.csv")
SEGMENT_FILE.parent.mkdir(exist_ok=True)

DATA_FILE = Path("kandy_segment_traffic.csv")

CSV_HEADERS = [
    "timestamp_utc",
    "segment_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "time_min",
    "distance_km",
    "avg_speed_kmh",
    "process_time_sec",
    "status",
]

MAX_RETRIES = 2

# ---------------- UTILITIES ----------------

def haversine(lat1, lng1, lat2, lng2):
    """Calculate distance in km between two points"""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def interpolate_points(lat1, lng1, lat2, lng2, max_dist_m=5):
    """Interpolate points between two coordinates for ≤ max_dist_m"""
    total_dist = haversine(lat1, lng1, lat2, lng2) * 1000  # m
    steps = max(1, int(total_dist / max_dist_m))
    points = [
        (lat1 + (lat2 - lat1) * i / steps, lng1 + (lng2 - lng1) * i / steps)
        for i in range(steps + 1)
    ]
    return points

# ---------------- SEGMENTS ----------------

def generate_segments():
    """Generate or load road segments (≤5m)"""
    if SEGMENT_FILE.exists():
        print(f"[Segments] Loaded existing segments ({SEGMENT_FILE})")
        segments = []
        with open(SEGMENT_FILE, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                segments.append(row)
        return segments

    print("[Segments] Creating new 5m segments...")
    segments = []
    segment_id = 1
    for road_name, points in ROADS.items():
        # forward direction
        for i in range(len(points) - 1):
            interp = interpolate_points(*points[i], *points[i + 1])
            for j in range(len(interp) - 1):
                segments.append({
                    "segment_id": str(segment_id),
                    "start_lat": interp[j][0],
                    "start_lng": interp[j][1],
                    "end_lat": interp[j+1][0],
                    "end_lng": interp[j+1][1],
                })
                segment_id += 1
        # reverse direction
        rev_points = list(reversed(points))
        for i in range(len(rev_points) - 1):
            interp = interpolate_points(*rev_points[i], *rev_points[i + 1])
            for j in range(len(interp) - 1):
                segments.append({
                    "segment_id": str(segment_id),
                    "start_lat": interp[j][0],
                    "start_lng": interp[j][1],
                    "end_lat": interp[j+1][0],
                    "end_lng": interp[j+1][1],
                })
                segment_id += 1

    SEGMENT_FILE.parent.mkdir(exist_ok=True)
    with open(SEGMENT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=segments[0].keys())
        writer.writeheader()
        writer.writerows(segments)

    print(f"[Segments] Saved {len(segments)} segments → {SEGMENT_FILE}")
    return segments

# ---------------- SCRAPER ----------------

async def scrape_segment(page, seg):
    start_time = time.time()
    result = {
        "timestamp_utc": datetime.utcnow().isoformat(),
        "segment_id": seg["segment_id"],
        "start_lat": seg["start_lat"],
        "start_lng": seg["start_lng"],
        "end_lat": seg["end_lat"],
        "end_lng": seg["end_lng"],
        "time_min": None,
        "distance_km": None,
        "avg_speed_kmh": None,
        "process_time_sec": 0,
        "status": "failed",
    }

    url = f"https://www.google.com/maps/dir/{seg['start_lat']},{seg['start_lng']}/{seg['end_lat']},{seg['end_lng']}/"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)

            content = await page.inner_text("div[role='main']")

            # extract time
            import re
            t_match = re.search(r"(\d+\s*h)?\s*(\d+\s*min)?", content)
            d_match = re.search(r"([\d.]+)\s*km", content)
            if t_match and d_match:
                h = int(re.search(r"(\d+)\s*h", t_match.group(0)).group(1)) if re.search(r"(\d+)\s*h", t_match.group(0)) else 0
                m = int(re.search(r"(\d+)\s*min", t_match.group(0)).group(1)) if re.search(r"(\d+)\s*min", t_match.group(0)) else 0
                total_min = h*60 + m
                result["time_min"] = total_min
                result["distance_km"] = float(d_match.group(1))
                if total_min > 0:
                    result["avg_speed_kmh"] = round(result["distance_km"] / (total_min / 60), 2)
                    result["status"] = "success"
            break
        except Exception as e:
            if attempt == MAX_RETRIES:
                result["status"] = f"error: {str(e)[:40]}"
            await page.wait_for_timeout(2000)

    result["process_time_sec"] = round(time.time() - start_time, 2)
    print(f"[Segment {seg['segment_id']}] {result['status']} ({result['process_time_sec']}s)")
    return result

# ---------------- MAIN ----------------

async def main():
    segments = generate_segments()
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width":1280,"height":800}, locale="en-US", timezone_id="Asia/Colombo"
        )
        page = await context.new_page()

        for idx, seg in enumerate(segments, 1):
            print(f"\n[{idx}/{len(segments)}] Processing segment {seg['segment_id']}")
            res = await scrape_segment(page, seg)
            results.append(res)

        await browser.close()

    # Append to CSV
    file_exists = DATA_FILE.exists()
    with open(DATA_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if not file_exists:
            writer.writeheader()
        for r in results:
            writer.writerow(r)

    print(f"\nSaved {len(results)} rows → {DATA_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
