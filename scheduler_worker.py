
import time
import threading
from datetime import datetime, timedelta
from database import get_session, get_metadata, set_metadata
from scraper import NOCScraper
from config import SCRAPE_INTERVAL_HOURS, SCRAPE_DAYS, NOC_USERNAME, NOC_PASSWORD

def background_worker():
    """
    Background process that runs forever and scrapes data periodically.
    """
    print("[Background Scheduler] Worker started.")
    
    while True:
        try:
            session = get_session()
            
            # Fetch latest config from DB
            interval_str = get_metadata(session, "scrape_interval_hours")
            days_str = get_metadata(session, "scrape_days")
            last_sync_str = get_metadata(session, "last_successful_sync")
            
            interval = int(interval_str) if interval_str else SCRAPE_INTERVAL_HOURS
            num_days = int(days_str) if days_str else SCRAPE_DAYS
            
            should_scrape = False
            
            if not last_sync_str:
                should_scrape = True
            else:
                last_sync_dt = datetime.strptime(last_sync_str, '%Y-%m-%d %H:%M:%S')
                if datetime.now() >= last_sync_dt + timedelta(hours=interval):
                    should_scrape = True
            
            if should_scrape:
                print(f"[Background Scheduler] Starting scrape: Interval={interval}h, Days={num_days}")
                
                # We need credentials. If they aren't in config, we can't scrape automatically.
                # In this app, they are usually in config or provided by user in UI.
                # Background worker should only use what's stable.
                
                if NOC_USERNAME and NOC_PASSWORD:
                    scraper = NOCScraper(headless=True)
                    try:
                        scraper.start()
                        if scraper.login(NOC_USERNAME, NOC_PASSWORD):
                            # Scrape today + num_days past
                            today = datetime.now()
                            for i in range(num_days):
                                target_date = today - timedelta(days=i)
                                print(f"[Background Scheduler] Scraping {target_date.strftime('%Y-%m-%d')}...")
                                scraper.scrape_date(target_date)
                            
                            # Update last sync time
                            set_metadata(session, "last_successful_sync", datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                            print("[Background Scheduler] Scrape complete.")
                        else:
                            print("[Background Scheduler] Login failed.")
                    except Exception as e:
                        print(f"[Background Scheduler] Error during scrape: {e}")
                    finally:
                        scraper.stop()
                else:
                    print("[Background Scheduler] Missing credentials in config.py, skipping background scrape.")
            
            session.close()
            
        except Exception as e:
            print(f"[Background Scheduler] Worker loop error: {e}")
            
        # Sleep for a bit before checking again (e.g., 5 minutes)
        time.sleep(300)

def start_background_scheduler():
    """
    Starts the background worker thread if it's not already running.
    """
    # Use a global variable to track if thread is already started
    if not hasattr(start_background_scheduler, "_thread_started"):
        worker_thread = threading.Thread(target=background_worker, daemon=True)
        worker_thread.start()
        start_background_scheduler._thread_started = True
        print("[Background Scheduler] Thread initialized.")
