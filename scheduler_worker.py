
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

    should_scrape = False
    
    while True:
        try:
            session = get_session()
            
            # Fetch latest config from DB
            interval_str = get_metadata(session, "scrape_interval_hours")
            days_str = get_metadata(session, "scrape_days")
            last_sync_str = get_metadata(session, "last_successful_sync")
            
            interval = int(interval_str) if interval_str else SCRAPE_INTERVAL_HOURS
            num_days = int(days_str) if days_str else SCRAPE_DAYS
            
            if not last_sync_str:
                should_scrape = True
                next_scrape_dt = datetime.now()
            else:
                last_sync_dt = datetime.strptime(last_sync_str, '%Y-%m-%d %H:%M:%S')
                next_scrape_dt = last_sync_dt + timedelta(hours=interval)
                if datetime.now() >= next_scrape_dt:
                    should_scrape = True
            
            # Persist next scrape time for UI
            set_metadata(session, "next_scheduled_scrape", next_scrape_dt.strftime('%Y-%m-%d %H:%M:%S'))

            if should_scrape:
                print(f"[Background Scheduler] Starting scrape: Interval={interval}h, Days={num_days}")
                print(f"[Background Scheduler] Next run calculated for: {next_scrape_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                
                if NOC_USERNAME and NOC_PASSWORD:
                    scraper = NOCScraper(headless=True)
                    try:
                        scraper.start()
                        if scraper.login(NOC_USERNAME, NOC_PASSWORD):
                            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                            for i in range(num_days):
                                target_date = today + timedelta(days=i)
                                print(f"[Background Scheduler] Scraping {target_date.strftime('%Y-%m-%d')}...")
                                scraper.scrape_date(target_date)
                            
                            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            set_metadata(session, "last_successful_sync", now_str)
                            
                            # Recalculate next scrape for log
                            next_val = datetime.now() + timedelta(hours=interval)
                            set_metadata(session, "next_scheduled_scrape", next_val.strftime('%Y-%m-%d %H:%M:%S'))
                            
                            print(f"[Background Scheduler] Scrape complete. Next run at: {next_val.strftime('%H:%M:%S')}")
                            should_scrape = False
                        else:
                            print("[Background Scheduler] Login failed.")
                            should_scrape = False
                    except Exception as e:
                        print(f"[Background Scheduler] Error during scrape: {e}")
                    finally:
                        should_scrape = False
                        scraper.stop()
                else:
                    print("[Background Scheduler] Missing credentials in config.py, skipping background scrape.")
            else:
                # Console debug info
                time_to_wait = next_scrape_dt - datetime.now()
                print(f"[Background Scheduler] Idle. Next scrape in {time_to_wait.total_seconds()/60:.1f} minutes ({next_scrape_dt.strftime('%H:%M:%S')})")
            
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
