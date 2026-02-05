import asyncio
import csv
import re
import time
from datetime import datetime
from pathlib import Path
from playwright.async_api import async_playwright

# ---------------- CONFIG ----------------
SEGMENT_FILE = Path("kandy_segments.csv")
DATA_DIR = Path("data/journeys")
DATA_DIR.mkdir(parents=True, exist_ok=True)

MAX_RETRIES = 2

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

# ---------------- SEGMENTS ----------------
def generate_segments():
    """
    Generate road segments if not exist.
    Here, just dummy example segments, normally you'd grid Kandy area or follow roads.
    """
    if SEGMENT_FILE.exists():
        print(f"[Segments] Loaded existing segments ({SEGMENT_FILE})")
        segments = []
        with open(SEGMENT_FILE, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                segments.append(row)
        return segments

    print("[Segments] Creating new segments...")
    segments = []
    segment_id = 1
    # Example: 4 segments, normally you'd split Kandy roads into 5m chunks
    coords = [
        ((6.980032, 79.875507), (6.943065, 79.878269)),
        ((6.943065, 79.878269), (6.895575, 79.854851)),
        ((6.943065, 79.878269), (6.910838, 79.887858)),
        ((6.943065, 79.878269), (6.931424, 79.842208)),
    ]
    for start, end in coords:
        segments.append({
            "segment_id": str(segment_id),
            "start_lat": start[0],
            "start_lng": start[1],
            "end_lat": end[0],
            "end_lng": end[1],
        })
        segment_id += 1

    # Save segments for future runs
    with open(SEGMENT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=segments[0].keys())
        writer.writeheader()
        writer.writerows(segments)
    print(f"[Segments] Saved {len(segments)} segments → {SEGMENT_FILE}")
    return segments

# ---------------- PARSERS ----------------
def parse_time_minutes(text: str):
    h = re.search(r"(\d+)\s*h", text)
    m = re.search(r"(\d+)\s*min", text)
    total = 0
    if h:
        total += int(h.group(1)) * 60
    if m:
        total += int(m.group(1))
    return total if total > 0 else None

def parse_distance_km(text: str):
    km = re.search(r"([\d.]+)\s*km", text)
    m = re.search(r"([\d.]+)\s*m\b", text)
    if km:
        return float(km.group(1))
    if m:
        return float(m.group(1)) / 1000
    return None

# ---------------- SCRAPER ----------------
async def scrape_segment(page, segment):
    start_time = time.time()
    url = f"https://www.google.com/maps/dir/{segment['start_lat']},{segment['start_lng']}/{segment['end_lat']},{segment['end_lng']}/"

    print(f"[GoogleMaps] 🌐 {url}")
    result = {
        "timestamp_utc": datetime.utcnow().isoformat(),
        **segment,
        "time_min": None,
        "distance_km": None,
        "avg_speed_kmh": None,
        "process_time_sec": 0,
        "status": "failed",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)
            content = await page.inner_text("div[role='main']")

            t_match = re.search(r"\d+\s*(?:h\s*)?\d*\s*min", content)
            d_match = re.search(r"[\d.]+\s*(?:km|m)\b", content)

            if t_match and d_match:
                result["time_min"] = parse_time_minutes(t_match.group(0))
                result["distance_km"] = parse_distance_km(d_match.group(0))
                if result["time_min"] and result["distance_km"]:
                    result["avg_speed_kmh"] = round(result["distance_km"] / (result["time_min"] / 60), 2)
                    result["status"] = "success"
                    break
        except Exception as e:
            print(f"  ❌ Attempt {attempt} failed: {e}")

    result["process_time_sec"] = round(time.time() - start_time, 2)
    print(f"[Segment] ID {segment['segment_id']} processed → {result['status']} ({result['process_time_sec']}s)")

    # Save JSON-like output for reference (optional)
    date_path = DATA_DIR / datetime.utcnow().strftime("%Y/%m%d/%Y%m%d.%H%M%S.json")
    date_path.parent.mkdir(parents=True, exist_ok=True)
    with open(date_path, "w", encoding="utf-8") as f:
        import json
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"[Journey] Wrote {date_path} ({date_path.stat().st_size} B)")

    return result

# ---------------- MAIN ----------------
async def main():
    segments = generate_segments()
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        for segment in segments:
            results.append(await scrape_segment(page, segment))
            await asyncio.sleep(1)  # optional pacing
        await browser.close()

    # Append results to CSV
    csv_file = Path("kandy_segment_speeds.csv")
    file_exists = csv_file.exists()
    with open(csv_file, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        if not file_exists:
            writer.writeheader()
        writer.writerows(results)
    print(f"\n[CSV] Saved {len(results)} rows → {csv_file}")

if __name__ == "__main__":
    asyncio.run(main())
