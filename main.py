import time
import pandas as pd
import os
from datetime import datetime
from playwright.sync_api import sync_playwright

ROUTES = [
    {"name": "Peradeniya_to_KMTT", "origin": "Peradeniya, Kandy", "dest": "KMTT, Kandy"},
    {"name": "Katugastota_to_KMTT", "origin": "Katugastota, Kandy", "dest": "KMTT, Kandy"},
    {"name": "KMTT_to_Getambe", "origin": "KMTT, Kandy", "dest": "Getambe, Kandy"}
]

def scrape_traffic():
    start_job = time.time()
    results = []
    
    with sync_playwright() as p:
        # User agent is critical to prevent Google from showing the 'Lite' map or consent popups
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for route in ROUTES:
            start_route = time.time()
            # Standard Directions URL
            url = f"https://www.google.com/maps/dir/{route['origin']}/{route['dest']}/"
            
            try:
                # Use a longer timeout and wait for load
                page.goto(url, wait_until="domcontentloaded", timeout=60000)
                
                # Handling the 'Consent' popup if it appears (common in some regions)
                if "consent.google.com" in page.url:
                    page.get_by_role("button", name="Accept all").click()
                
                # Wait for the specific duration element
                # Selector: Looks for the primary time estimate in the sidebar
                page.wait_for_selector('div[id="section-directions-trip-0"]', timeout=30000)
                
                duration_text = page.locator('div#section-directions-trip-0 >> text=/min|hr/').first.inner_text()
                distance_text = page.locator('div#section-directions-trip-0 >> text=/km/').first.inner_text()

                mins = int(''.join(filter(str.isdigit, duration_text.split('hr')[-1])))
                if 'hr' in duration_text:
                    mins += int(duration_text.split('hr')[0].strip()) * 60
                
                kms = float(''.join(c for c in distance_text if c.isdigit() or c == '.'))
                speed = round(kms / (mins / 60), 2)

                results.append({
                    "timestamp": timestamp,
                    "route": route['name'],
                    "eta_min": mins,
                    "dist_km": kms,
                    "speed_kmh": speed,
                    "proc_sec": round(time.time() - start_route, 2)
                })
                print(f"✅ {route['name']}: {mins}m")

            except Exception as e:
                print(f"❌ {route['name']} failed.")

        browser.close()
    
    if results:
        df = pd.DataFrame(results)
        file_path = 'kandy_od_traffic.csv'
        df.to_csv(file_path, mode='a', index=False, header=not os.path.exists(file_path))
    else:
        # Create empty file so Git doesn't fail
        open('kandy_od_traffic.csv', 'a').close()

if __name__ == "__main__":
    scrape_traffic()
