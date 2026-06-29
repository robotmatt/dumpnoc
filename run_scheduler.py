import time
import os
from datetime import datetime, timedelta
from scraper import NOCScraper
from config import NOC_USERNAME, NOC_PASSWORD, SCRAPE_INTERVAL_HOURS, STATION_OPS_URL, SESSION_STATE_PATH
from database import init_db, get_session, get_metadata

def run_scheduled_scrape():
    print("Initializing Database...")
    init_db()
    
    session = get_session()
    auth_mode = get_metadata(session, "auth_mode", "legacy")
    session.close()
    
    scraper = NOCScraper(headless=True)
    
    try:
        scraper.start(auth_mode=auth_mode, storage_state_path=SESSION_STATE_PATH)
        print(f"Attempting initial login using {auth_mode.upper()} mode...")
        if not scraper.login(NOC_USERNAME, NOC_PASSWORD, auth_mode=auth_mode, storage_state_path=SESSION_STATE_PATH):
            print("Failed to login. Please check your credentials/session in Settings.")
            return

        while True:
            # Refresh interval and auth mode from metadata in case it changed in DB
            session = get_session()
            db_interval = get_metadata(session, "scrape_interval_hours")
            current_interval = int(db_interval) if db_interval else SCRAPE_INTERVAL_HOURS
            auth_mode = get_metadata(session, "auth_mode", "legacy")
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
                
                # Check if we've been redirected to a login page
                current_url = scraper.page.url
                current_title = scraper.page.title()
                if "StationOperations.aspx" not in current_url or "login" in current_url.lower() or "Login" in current_title or "Sign in" in current_title:
                    print("Session expired. Re-logging...")
                    if not scraper.login(NOC_USERNAME, NOC_PASSWORD, auth_mode=auth_mode, storage_state_path=SESSION_STATE_PATH):
                        print("Re-login failed. Scrape sweep skipped.")
                        # Wait for a bit before trying again
                        time.sleep(300)
                        continue

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
                    scraper.start(auth_mode=auth_mode, storage_state_path=SESSION_STATE_PATH)
                    scraper.login(NOC_USERNAME, NOC_PASSWORD, auth_mode=auth_mode, storage_state_path=SESSION_STATE_PATH)

            print(f"Waiting {current_interval} hour(s) for next run...")
            # Sleep in 1-minute chunks to remain responsive to signals
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
    init_db()
    session = get_session()
    auth_mode = get_metadata(session, "auth_mode", "legacy")
    session.close()

    if auth_mode == "legacy" and (not NOC_USERNAME or not NOC_PASSWORD):
        print("Error: NOC_USERNAME and NOC_PASSWORD must be set in .env file for legacy login.")
    elif auth_mode == "sso" and not os.path.exists(SESSION_STATE_PATH):
        print(f"Error: SSO session state file '{SESSION_STATE_PATH}' not found. Please log in once from the settings UI first.")
    else:
        run_scheduled_scrape()
