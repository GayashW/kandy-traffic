import time
import pandas as pd
import os
from datetime import datetime
from playwright.sync_api import sync_playwright

# ROUTE DEFINITIONS (Kandy Corridor)
ROUTES = [
    {"name": "Peradeniya_to_KMTT", "origin": "7.2694,80.5916", "dest": "7.2912,80.6318"},
    {"name": "Katugastota_to_KMTT", "origin": "7.3194,80.6272", "dest": "7.2912,80.6318"},
    {"name": "KMTT_to_Getambe", "origin": "7.2912,80.6318", "dest": "7.2764,80.6050"}
]

def scrape_traffic():
    start_job = time.time()
    results = []
    
    with sync_playwright() as p:
        # Launch browser with specific viewport for consistent UI rendering
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={'width': 1920, 'height': 1080})
        page = context.new_page()
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for route in ROUTES:
            start_route = time.time()
            # Direct URL for Google Maps Driving Directions
            url = f"https://www.google.com/maps/dir/{route['origin']}/{route['dest']}/am=t/data=!4m2!4m1!3e0"
            
            try:
                page.goto(url, wait_until="networkidle", timeout=60000)
                time.sleep(5) # Allow traffic layer to settle
                
                # Dynamic extraction of the first (fastest) route suggestion
                # Google stores duration and distance in specific divs; we target them by text patterns
                duration_text = page.locator('div#section-directions-trip-0 >> text=/min|hr/').first.inner_text()
                distance_text = page.locator('div#section-directions-trip-0 >> text=/km/').first.inner_text()

                # Parse Duration
                mins = 0
                if 'hr' in duration_text:
                    parts = duration_text.split('hr')
                    mins += int(parts[0].strip()) * 60
                    if 'min' in parts[1]:
                        mins += int(parts[1].replace('min', '').strip())
                else:
                    mins = int(''.join(filter(str.isdigit, duration_text)))

                # Parse Distance
                kms = float(''.join(c for c in distance_text if c.isdigit() or c == '.'))
                
                # Calculate Stats
                speed = round(kms / (mins / 60), 2)
                proc_time = round(time.time() - start_route, 2)

                results.append({
                    "timestamp": timestamp,
                    "route": route['name'],
                    "eta_min": mins,
                    "dist_km": kms,
                    "speed_kmh": speed,
                    "proc_sec": proc_time
                })
                print(f"✅ Extracted {route['name']}: {mins} mins")

            except Exception as e:
                print(f"❌ Failed {route['name']}: {str(e)}")

        browser.close()
    
    # Save Data
    if results:
        df = pd.DataFrame(results)
        file_path = 'kandy_od_traffic.csv'
        df.to_csv(file_path, mode='a', index=False, header=not os.path.exists(file_path))

if __name__ == "__main__":
    scrape_traffic()
