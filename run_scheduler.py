import time
import os
from datetime import datetime, timedelta
from scraper import NOCScraper
from config import NOC_USERNAME, NOC_PASSWORD, SCRAPE_INTERVAL_HOURS, STATION_OPS_URL
from database import init_db, get_session, get_metadata

def run_scheduled_scrape():
    print("Initializing Database...")
    init_db()
    
    # We'll use a loop that recreates the scraper periodically to avoid memory leaks 
    # or long-running browser issues, or just keep one open.
    # Let's try keeping one open and re-logging if needed.
    
    scraper = NOCScraper(headless=True)
    
    try:
        scraper.start()
        print(f"Attempting initial login for {NOC_USERNAME}...")
        if not scraper.login(NOC_USERNAME, NOC_PASSWORD):
            print("Failed to login. Please check your credentials in .env")
            return

        while True:
            # Refresh interval from metadata in case it changed in DB
            session = get_session()
            db_interval = get_metadata(session, "scrape_interval_hours")
            current_interval = int(db_interval) if db_interval else SCRAPE_INTERVAL_HOURS
            session.close()

            now = datetime.now()
            print(f"\n[{now.strftime('%H:%M:%S')}] Starting scheduled scrape sweep...")
            
            # Scrape range: Today and Tomorrow are most critical for crew changes
            today = now.replace(hour=0, minute=0, second=0, microsecond=0)
            tomorrow = today + timedelta(days=1)
            
            try:
                # Check if we are still logged in by visiting Station Ops
                scraper.page.goto(STATION_OPS_URL)
                scraper.page.wait_for_load_state("networkidle")
                
                if "Login" in scraper.page.title() or "Default.aspx" in scraper.page.url:
                    print("Session expired. Re-logging...")
                    scraper.login(NOC_USERNAME, NOC_PASSWORD)

                scraper.scrape_date_range(today, tomorrow)
                print(f"Sweep completed successfully.")
            except Exception as e:
                print(f"Error during scrape sweep: {e}")
                # Try to restart browser on next iteration if it's a "Target closed" error
                if "Target closed" in str(e) or "context has been closed" in str(e):
                    print("Browser context lost. Restarting scraper...")
                    try: scraper.stop()
                    except: pass
                    scraper = NOCScraper(headless=True)
                    scraper.start()
                    scraper.login(NOC_USERNAME, NOC_PASSWORD)

            print(f"Waiting {current_interval} hour(s) for next run...")
            # Sleep in 1-minute chunks to remain responsive to signals (though time.sleep is fine in simple scripts)
            for _ in range(current_interval * 60):
                time.sleep(60)
            
    except KeyboardInterrupt:
        print("\nScheduler stopped by user.")
    finally:
        try:
            scraper.stop()
        except:
            pass

if __name__ == "__main__":
    if not NOC_USERNAME or not NOC_PASSWORD:
        print("Error: NOC_USERNAME and NOC_PASSWORD must be set in .env file.")
    else:
        run_scheduled_scrape()
