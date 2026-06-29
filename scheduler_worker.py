
import time
import threading
from datetime import datetime, timedelta
from database import get_session, get_metadata, set_metadata
from scraper import NOCScraper
from config import SCRAPE_INTERVAL_HOURS, SCRAPE_DAYS, NOC_USERNAME, NOC_PASSWORD, SESSION_STATE_PATH
import os
from tools.backup_db import create_db_backup

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
                # 0. Safety Catch: check if another scrape is already active
                if get_metadata(session, "is_scrape_in_progress") == "True":
                    print("[Background Scheduler] Scrape requested but another sync is already in progress. Skipping cycle.")
                    session.close()
                    time.sleep(60) # Try again sooner
                    continue

                print(f"[Background Scheduler] Starting scrape: Interval={interval}h, Days={num_days}")
                set_metadata(session, "is_scrape_in_progress", "True")
                print(f"[Background Scheduler] Next run calculated for: {next_scrape_dt.strftime('%Y-%m-%d %H:%M:%S')}")
                
                auth_mode = get_metadata(session, "auth_mode", "legacy")
                has_sso_session = auth_mode == "sso" and os.path.exists(SESSION_STATE_PATH)
                has_legacy_credentials = auth_mode == "legacy" and NOC_USERNAME and NOC_PASSWORD
                has_legacy_session = auth_mode == "legacy" and os.path.exists(SESSION_STATE_PATH)
                
                if has_sso_session or has_legacy_credentials or has_legacy_session:
                    # 1. Create a safety backup before any sync
                    print("[Background Scheduler] Creating safety backup...")
                    create_db_backup()
                    
                    scraper = NOCScraper(headless=True)
                    try:
                        scraper.start(auth_mode=auth_mode, storage_state_path=SESSION_STATE_PATH)
                        if scraper.login(username=NOC_USERNAME, password=NOC_PASSWORD, auth_mode=auth_mode, storage_state_path=SESSION_STATE_PATH):
                            # Clear last scrape error on successful login
                            set_metadata(session, "last_scrape_error", "")
                            
                            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                            
                            # Deep Sync Logic: once a day, look back 7 days
                            last_deep = get_metadata(session, "last_deep_sync_date")
                            current_date_str = today.strftime('%Y-%m-%d')
                            
                            start_offset = 0
                            if last_deep != current_date_str:
                                print("[Background Scheduler] Daily Deep Sync triggered: Looking back 7 days...")
                                start_offset = -7
                                set_metadata(session, "last_deep_sync_date", current_date_str)

                            for i in range(start_offset, num_days):
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
                            if auth_mode == "sso":
                                set_metadata(session, "last_scrape_error", "Microsoft SSO session has expired or is invalid. Please log in again in the Settings tab.")
                            else:
                                set_metadata(session, "last_scrape_error", "Legacy login failed. Please verify credentials in the Settings tab.")
                            should_scrape = False
                    except Exception as e:
                        print(f"[Background Scheduler] Error during scrape: {e}")
                        set_metadata(session, "last_scrape_error", f"Scrape error: {str(e)}")
                    finally:
                        should_scrape = False
                        set_metadata(session, "is_scrape_in_progress", "False")
                        scraper.stop()
                else:
                    if auth_mode == "sso":
                        print("[Background Scheduler] Missing active SSO session. Please log in interactively first.")
                        set_metadata(session, "last_scrape_error", "Missing Microsoft SSO session. Please log in once in the Settings tab.")
                    else:
                        print("[Background Scheduler] Missing legacy credentials or saved session, skipping background scrape.")
                        set_metadata(session, "last_scrape_error", "Missing legacy credentials. Please set them in the Settings tab or .env file.")
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
        # Reset Global Sync Lock on application startup
        try:
            session = get_session()
            set_metadata(session, "is_scrape_in_progress", "False")
            session.close()
            print("[Background Scheduler] Global sync lock reset.")
        except Exception as e:
            print(f"[Background Scheduler] Failed to reset sync lock: {e}")

        worker_thread = threading.Thread(target=background_worker, daemon=True)
        worker_thread.start()
        start_background_scheduler._thread_started = True
        print("[Background Scheduler] Thread initialized.")
