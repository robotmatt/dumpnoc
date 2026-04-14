import os
import asyncio
import json
from datetime import datetime, timedelta, timezone
import zoneinfo
import airportsdata

AIRPORTS_DATA = airportsdata.load('IATA')


# Fix for Playwright/asyncio on Windows (especially Python 3.8+)
if os.name == 'nt':
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    except AttributeError:
        # Fallback for environments where this isn't available
        pass

from playwright.sync_api import sync_playwright, TimeoutError
from bs4 import BeautifulSoup
from config import LOGIN_URL, STATION_OPS_URL
from database import get_session, Flight, CrewMember, flight_crew_association, DailySyncStatus

class NOCScraper:
    def __init__(self, headless=True):
        self.headless = headless
        self.playwright = None
        self.browser = None
        self.page = None
        self.session = get_session()

    def _get_utc_time(self, local_dt, airport_str):
        if not local_dt or not airport_str:
            return None
        # Extract IATA code (e.g., "TYS" from "TYS - KTYS - MCGHEE-TYSON")
        iata = airport_str.split(" - ")[0].strip()
        entry = AIRPORTS_DATA.get(iata)
        if not entry:
            return None
        tz_name = entry.get('tz')
        if not tz_name:
            return None
        try:
            tz = zoneinfo.ZoneInfo(tz_name)
            # local_dt is naive datetime from scraper
            local_with_tz = local_dt.replace(tzinfo=tz)
            # Convert to UTC and return as naive datetime
            return local_with_tz.astimezone(timezone.utc).replace(tzinfo=None)
        except Exception as e:
            # Silently fail if timezone is not found or invalid
            return None

    def start(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=self.headless)
        self.page = self.browser.new_page()

    def stop(self):
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
        self.session.close()

    def login(self, username, password):
        print(f"Navigating to {LOGIN_URL}...")
        self.page.goto(LOGIN_URL)
        
        # Determine selectors - usually id contains 'UserName' and 'Password' in ASP.NET
        # Fallback to standard inputs if specific IDs vary
        try:
            self.page.wait_for_selector("input[type='password']")
            
            # Fill Username
            # Try to find input that looks like username
            user_input = self.page.query_selector("input[name*='UserName']") or \
                         self.page.query_selector("input[id*='UserName']")
            if user_input:
                user_input.fill(username)
            else:
                print("Could not find username field by name/id, trying generic...")
                self.page.fill("input[type='text']", username) # Risky if multiple

            # Fill Password
            self.page.fill("input[type='password']", password)
            
            # Click Login
            # Look for submit button
            login_btn = self.page.query_selector("input[type='submit']") or \
                        self.page.query_selector("button:has-text('Login')")
            
            if login_btn:
                login_btn.click()
            else:
                self.page.press("input[type='password']", "Enter")

            self.page.wait_for_load_state("networkidle")
            
            # Check if login succeeded
            if "Login" in self.page.title() or self.page.query_selector("input[type='password']"):
                print("Login failed or still on login page.")
                return False
            
            print("Login successful.")
            return True

        except Exception as e:
            print(f"Error during login: {e}")
            return False

    def scrape_date_range(self, start_date, end_date):
        current_date = start_date
        while current_date <= end_date:
            print(f"Scraping data for {current_date.strftime('%Y-%m-%d')}...")
            success = self.scrape_date(current_date)
            if not success:
                print(f"Failed to scrape {current_date}")
            current_date += timedelta(days=1)
            
    def scrape_date(self, date_obj):
        # Navigate to Station Ops if not already there
        if "StationOperations.aspx" not in self.page.url:
            self.page.goto(STATION_OPS_URL)
            self.page.wait_for_load_state("networkidle")

        # 1. Interact with Date Picker (Only needed once if we stay on page)
        date_str = date_obj.strftime("%d%b%y").upper()
        print(f"Setting date to {date_str}...")
        
        try:
            # Clear existing value first
            self.page.click("#MasterMain_tbDate_DateFieldTextBox")
            self.page.fill("#MasterMain_tbDate_DateFieldTextBox", "")
            
            # Type slowly to trigger events
            self.page.type("#MasterMain_tbDate_DateFieldTextBox", date_str, delay=100)
            self.page.press("#MasterMain_tbDate_DateFieldTextBox", "Tab")
            
            # --- PASS 1: UTC ---
            # print("  [Pass 1] Switching to UTC...")
            # self.page.select_option("#MasterMain_TimeMode_DP_TimeModes", label="UTC")
            #  self.page.click("#MasterMain_btnSearch")
            #  self.page.wait_for_load_state("networkidle")
            #  self.page.wait_for_timeout(3000) # Safety
            
            # content_utc = self.page.content()
            # self.parse_and_save(content_utc, date_obj, mode="UTC")
            
            # --- PASS 2: Local ---
            print("Capturing in Local Time...")
            self.page.select_option("#MasterMain_TimeMode_DP_TimeModes", label="Local time")
            self.page.click("#MasterMain_btnSearch")
            self.page.wait_for_load_state("networkidle")
            self.page.wait_for_timeout(3000)
            
            content_local = self.page.content()
            seen_ids = self.parse_and_save(content_local, date_obj, mode="Local")
            
            # --- Pruning / Reconciliation ---
            # If the scrape was basically successful, remove anything in the DB for this 
            # station/date that we DIDN'T see in the current portal view.
            if seen_ids is not None:
                self._prune_missing_flights(date_obj, seen_ids)

            # Update Sync Status (Only once)
            self._update_sync_status(date_obj)
            return True
            
        except Exception as e:
            print(f"Error interacting with page controls: {e}")
            if "Target closed" in str(e): raise
            return False

    def _update_sync_status(self, date_obj):
        try:
            date_key = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
            sync_status = self.session.get(DailySyncStatus, date_key)
            if not sync_status:
                sync_status = DailySyncStatus(date=date_key)
                self.session.add(sync_status)
            
            sync_status.last_scraped_at = datetime.now()
            # Count flights logic? We can count them inside parse, or just query DB here
            count = self.session.query(Flight).filter(Flight.date >= date_key, Flight.date < date_key + timedelta(days=1)).count()
            sync_status.flights_found = count
            sync_status.status = "Success"
            self.session.commit()
            
            # Update Global Metadata
            from database import set_metadata
            now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            set_metadata(self.session, "last_successful_sync", now_str)
            
            # Sync to Firestore if enabled
            from config import ENABLE_CLOUD_SYNC
            from firestore_lib import is_cloud_sync_enabled
            if is_cloud_sync_enabled():
                from firestore_lib import upload_metadata
                upload_metadata("last_successful_sync", now_str)
                
                # Auto-upload the flights for this specific day
                from ingest_data import upload_flights_to_cloud
                # scraper's session is already open
                upload_flights_to_cloud(self.session, start_date=date_key, end_date=date_key)
            
            print(f"Sync status and global metadata updated for {date_key}")
        except Exception as e:
            print(f"Error updating sync status: {e}")

    def _get_or_create_crew(self, c_id, c_name):
        if not hasattr(self, '_crew_cache_by_id'):
            self._crew_cache_by_id = {}
            self._crew_cache_by_name = {}
            
            # Pre-load cache for massive speedup
            all_crew = self.session.query(CrewMember).all()
            for c in all_crew:
                if c.employee_id: self._crew_cache_by_id[c.employee_id] = c
                if c.name: self._crew_cache_by_name[c.name] = c
                
        crew = None
        if c_id and c_id in self._crew_cache_by_id:
            crew = self._crew_cache_by_id[c_id]
        elif c_name and c_name in self._crew_cache_by_name:
            crew = self._crew_cache_by_name[c_name]
            
        if not crew:
            crew = CrewMember(name=c_name, employee_id=c_id)
            self.session.add(crew)
            self.session.flush()
        else:
            changed = False
            if not crew.employee_id and c_id:
                crew.employee_id = c_id
                changed = True
            elif crew.name != c_name:
                crew.name = c_name
                changed = True
            if changed:
                self.session.flush()
                
        if c_id: self._crew_cache_by_id[c_id] = crew
        if c_name: self._crew_cache_by_name[c_name] = crew
        return crew

    def _prune_missing_flights(self, date_obj, seen_ids):
        """
        Removes flights from the DB that are associated with the current station 
        for date_obj but were not present in the seen_ids list.
        """
        try:
            date_key = date_obj.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # 1. Identify the current station from the page or the flights
            # Let's try to grab it from the typical Station Name field
            station_code = None
            try:
                # Wide set of potential selectors for Raido/NOC
                selectors = ["#MasterMain_tbStation_StationNameField", "#MasterMain_tbStation_StationFieldTextBox", "#MasterMain_lbStationName", ".StationHeader"]
                for sel in selectors:
                    station_el = self.page.query_selector(sel)
                    if station_el:
                        val = station_el.get_attribute("value") or station_el.inner_text()
                        if val:
                            station_code = val.split(" - ")[0].strip()
                            break
            except:
                pass
            
            # --- Inference Fallback ---
            # If we couldn't find it in the UI, but we have seen flights, we can infer it!
            # The station is the airport that appears in EVERY flight of the scrape.
            seen_flight_numbers = []
            if seen_ids:
                from collections import Counter
                airport_counts = Counter()
                # Query the objects we just saved to find the common station
                found_flights = self.session.query(Flight).filter(Flight.id.in_(seen_ids)).all()
                for f in found_flights:
                    if f.flight_number and f.flight_number not in seen_flight_numbers:
                        seen_flight_numbers.append(f.flight_number)
                    if f.departure_airport: airport_counts[f.departure_airport.split(" - ")[0].strip()] += 1
                    if f.arrival_airport: airport_counts[f.arrival_airport.split(" - ")[0].strip()] += 1
                
                if not station_code and airport_counts:
                    # The most common airport is almost certainly our base station
                    station_code, count = airport_counts.most_common(1)[0]
                    # Check if it appears in at least 50% of the flights (safety)
                    if count < (len(found_flights) // 2):
                        station_code = None # Ambiguous

            if not station_code:
                # We skip pruning for safety if we can't identify the station.
                # If seen_ids is empty AND we can't find station_code, we definitely can't prune.
                print("  [Prune] Could not detect current station in UI or via inference. Skipping reconciliation for safety.")
                return

            print(f"  [Prune] Reconciling flights on {date_key.date()}...")
            
            # 2. Query DB for all flights at this station on this day, 
            # PLUS any flight that shares a flight number with one we just saw.
            from sqlalchemy import or_
            db_flights = self.session.query(Flight).filter(
                Flight.date == date_key,
                or_(
                    Flight.departure_airport.like(f"{station_code}%"),
                    Flight.arrival_airport.like(f"{station_code}%"),
                    Flight.flight_number.in_(seen_flight_numbers) if seen_flight_numbers else False
                )
            ).all()
            
            to_delete = [f for f in db_flights if f.id not in seen_ids and f.status not in ("Canceled", "Flown")]
            
            if to_delete:
                print(f"  [Prune] Found {len(to_delete)} flights in DB no longer present in Ops. Purging...")
                for f in to_delete:
                    dep = f.departure_airport.split(" - ")[0] if f.departure_airport else "Unknown"
                    arr = f.arrival_airport.split(" - ")[0] if f.arrival_airport else "Unknown"
                    print(f"    -> Pruning {f.flight_number}: {dep} to {arr} (DB ID: {f.id})")
                    
                    # Cleanup associations
                    from database import flight_crew_association
                    self.session.execute(flight_crew_association.delete().where(flight_crew_association.c.flight_id == f.id))
                    self.session.delete(f)
                self.session.commit()
                print(f"  [Prune] Purged {len(to_delete)} flights.")
            else:
                print("  [Prune] Database is already in sync with Ops view.")
                
        except Exception as e:
            print(f"  [Prune] Error during reconciliation: {e}")
            self.session.rollback()

    def parse_and_save(self, html_content, date_obj, mode="Local"):
        soup = BeautifulSoup(html_content, 'html.parser')
        
        departure_panel = soup.find("div", id="MasterMain_panelUpper")
        arrival_panel = soup.find("div", id="MasterMain_panelLower")
        
        list_items_with_type = []
        if departure_panel:
            for it in departure_panel.find_all("div", class_="ListItem"):
                list_items_with_type.append((it, "Departure"))
        if arrival_panel:
            for it in arrival_panel.find_all("div", class_="ListItem"):
                list_items_with_type.append((it, "Arrival"))
                
        if not list_items_with_type:
            # Fallback
            for it in soup.find_all("div", class_="ListItem"):
                list_items_with_type.append((it, "Unknown"))
            
        print(f"  Parsing {len(list_items_with_type)} flights from Departures and Arrivals ({mode})...")
        
        # Track processed flights in this specific session to avoid double-parsing crew (Dep/Arr panels)
        processed_flights_in_session = set()
        seen_ids = []
        
        for item, panel_type in list_items_with_type:
            try:
                # 1. Flight Number
                header_table = item.find("div", class_="ItemHeader").find("table")
                if not header_table: continue
                
                header_cells = header_table.find_all("td")
                flight_number = header_cells[0].get_text(strip=True)
                
                # 2. Details (Move this up to get metadata before times)
                details_table = item.find("table", class_="ItemChildTableDetails")
                if not details_table: continue
                
                details = {}
                rows = details_table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True).rstrip(":")
                        val_cell = cells[1]
                        val_text = val_cell.get_text(strip=True)
                        details[key] = (val_text, val_cell)
                
                # --- DETERMINE THE TRUE FLIGHT DATE FIRST ---
                # Check for "Date" or "Oper Date" or anything containing "Date"
                flight_date = date_obj
                found_explicit_date = False
                for k, v in details.items():
                    if "DATE" in k.upper():
                        date_str = v[0]
                        try:
                            # Standard format: 07APR26 or 07APR
                            if len(date_str) > 5:
                                flight_date = datetime.strptime(date_str, "%d%b%y")
                            else:
                                # Infer year
                                flight_date = datetime.strptime(f"{date_str}{date_obj.year}", "%d%b%Y")
                            found_explicit_date = True
                            break
                        except:
                            continue
                
                # IMPORTANT: If the explicit date doesn't match date_obj, and we aren't 
                # within a logical +/-1 day window, it might be an error or background item.
                # However, many portals show 4/8 flights when viewing 4/7.
                # If it's a departure at 02:00 on 4/8, it shouldn't be in our 4/7 database record.
                if found_explicit_date and flight_date.date() != date_obj.date():
                    # For safety, if it's showing up on 07APR but the date is 08APR, it's an 08APR flight.
                    # This will automatically move it out of the 07APR view in the UI.
                    pass

                # 3. Parse specific fields using the newly determined flight_date as base
                std_str = details.get("STD", ["", None])[0] # e.g. "2159"
                sta_val = details.get("STA", ["", None])[0]
                atd_str = details.get("ATD", ["", None])[0]
                ata_val = details.get("ATA", ["", None])[0]
                
                # Check for Status via Background Colors (Case Insensitive)
                is_canceled = False
                is_flown_color = False
                header_div = item.find("div", class_="ItemHeader")
                header_style = header_div.get("style", "").upper() if header_div else ""
                
                if header_style:
                    # Canceled: Red (#FA0000, rgb(250, 0, 0))
                    if "#FA0000" in header_style or "RGB(250, 0, 0)" in header_style or "RGB(250,0,0)" in header_style:
                        is_canceled = True
                    # Flown: Brown (#4D2B09, rgb(77, 43, 9))
                    elif "#4D2B09" in header_style or "RGB(77, 43, 9)" in header_style or "RGB(77,43,9)" in header_style:
                        is_flown_color = True
                
                # --- Parse Times Sequentially using flight_date as reference ---
                parsed_std = self._parse_time(flight_date, std_str)
                
                def parse_relative(val_str, ref_time, fallback_date):
                    if not val_str or not val_str.isdigit() or len(val_str) != 4:
                        return None
                    h = int(val_str[:2])
                    m = int(val_str[2:])
                    # Base it on the reference time's date if available, otherwise fallback_date
                    base_dt = ref_time if ref_time else fallback_date
                    res = base_dt.replace(hour=h, minute=m, second=0, microsecond=0)
                    
                    # If we have a reference time and the new time is 'earlier' by a lot, 
                    # it means we crossed midnight.
                    # Heuristic: if it's > 6 hours 'earlier', it's probably the next day.
                    if ref_time and res < (ref_time - timedelta(hours=6)):
                        res += timedelta(days=1)
                    # Also handle if it's much later (more than 18 hours), might be previous day
                    # But usually ref is STD and we care about delays going forward.
                    return res

                parsed_atd = parse_relative(atd_str, parsed_std, flight_date)
                
                # Helper for Arrival Time Parsing (STA/ATA)
                def parse_arrival_complex(val_str, ref_time, fallback_date):
                    if not val_str: return None
                    val_clean = val_str.strip()
                    if " : " in val_clean:
                        try:
                            parts = val_clean.split(" : ")
                            if len(parts) == 2:
                                t_str, d_str = parts
                                # Handle partial dates or missing years
                                try:
                                    if len(d_str) > 5:
                                        dt_obj = datetime.strptime(d_str, "%d%b%y")
                                    else:
                                        dt_obj = datetime.strptime(f"{d_str}{fallback_date.year}", "%d%b%Y")
                                except:
                                    dt_obj = fallback_date
                                    
                                h = int(t_str[:2]); m = int(t_str[2:])
                                return dt_obj.replace(hour=h, minute=m, second=0, microsecond=0)
                        except: pass
                    
                    return parse_relative(val_clean, ref_time, fallback_date)

                parsed_sta = parse_arrival_complex(sta_val, parsed_std, flight_date)
                parsed_ata = parse_arrival_complex(ata_val, parsed_std, flight_date)

                # --- Extract Details Early for Matching ---
                tail_number = details.get("Registration", ["", None])[0]
                dep_apt = details.get("Departure", ["", None])[0]
                arr_apt = details.get("Arrival", ["", None])[0]
                pax_val = details.get("Pax", ["", None])[0]
                load_val = details.get("Load", ["", None])[0]
                notes_val = details.get("Notes", ["", None])[0]
                type_val = details.get("Type", ["", None])[0]
                ver_val = details.get("Version", ["", None])[0]
                if not isinstance(notes_val, str): notes_val = ""

                # Compute Status
                status_str = "Scheduled"
                if is_canceled:
                    status_str = "Canceled"
                elif is_flown_color:
                    status_str = "Flown"
                
                # --- DB Interaction & OOOI Extraction ---
                import re
                # We already have flight_date computed correctly now at the start of the loop

                actual_out = None
                actual_off = None
                actual_on = None
                actual_in = None
                
                if len(header_cells) > 2:
                    time_raw = header_cells[2].get_text(separator=' ', strip=True)
                    matches = re.findall(r'\d{4}', time_raw)
                    if panel_type == "Departure":
                        if len(matches) > 0: 
                            actual_out = parse_relative(matches[0], parsed_std, flight_date)
                        if len(matches) > 1:
                            actual_off = parse_relative(matches[1], parsed_std, flight_date)
                    elif panel_type == "Arrival":
                        if len(matches) > 0: 
                            # Use parsed_sta as reference if available, otherwise parsed_std
                            ref = parsed_sta if parsed_sta else parsed_std
                            actual_on = parse_relative(matches[0], ref, flight_date)
                        if len(matches) > 1:
                            ref = parsed_sta if parsed_sta else parsed_std
                            actual_in = parse_relative(matches[1], ref, flight_date)

                planned_block = None
                
                # --- UTC Conversion ---
                std_utc = self._get_utc_time(parsed_std, dep_apt)
                atd_utc = self._get_utc_time(parsed_atd, dep_apt)
                sta_utc = self._get_utc_time(parsed_sta, arr_apt)
                ata_utc = self._get_utc_time(parsed_ata, arr_apt)
                
                out_utc = self._get_utc_time(actual_out, dep_apt)
                off_utc = self._get_utc_time(actual_off, dep_apt)
                on_utc = self._get_utc_time(actual_on, arr_apt)
                in_utc = self._get_utc_time(actual_in, arr_apt)

                if std_utc and sta_utc:
                    planned_block = int((sta_utc - std_utc).total_seconds() // 60)
                
                # Modified Query: Match on Flight # AND Date AND Route (Dep/Arr)
                try:
                    query = self.session.query(Flight).filter(
                        Flight.flight_number == flight_number,
                        Flight.date == flight_date
                    )
                    
                    # identity: Flight # + Date + Dep + Arr
                    if dep_apt and arr_apt:
                        query = query.filter(
                            Flight.departure_airport == dep_apt,
                            Flight.arrival_airport == arr_apt
                        )
                    
                    # NOTE: We specifically DO NOT filter by tail_number here.
                    # If the registration changed (tail swap), we want to MATCH the existing record 
                    # for this flight number/route and UPDATE its tail number.
                    
                    matching_flights = query.all()
                    existing = matching_flights[0] if matching_flights else None
                    has_duplicate_warning = 1 if len(matching_flights) > 1 else 0
                    
                    was_new_flight = False
                    
                    if mode == "Local":
                        # Full Parse & Create
                        if not existing:
                            was_new_flight = True
                            flight = Flight(
                                flight_number=flight_number,
                                date=flight_date,
                                tail_number=tail_number,
                                scheduled_departure=parsed_std,
                                scheduled_arrival=parsed_sta,
                                actual_departure=parsed_atd,
                                actual_arrival=parsed_ata,
                                scheduled_departure_utc=std_utc,
                                scheduled_arrival_utc=sta_utc,
                                actual_departure_utc=atd_utc,
                                actual_arrival_utc=ata_utc,
                                actual_out=actual_out,
                                actual_off=actual_off,
                                actual_on=actual_on,
                                actual_in=actual_in,
                                actual_out_utc=out_utc,
                                actual_off_utc=off_utc,
                                actual_on_utc=on_utc,
                                actual_in_utc=in_utc,
                                planned_block_minutes=planned_block,
                                has_duplicate_warning=has_duplicate_warning,
                                departure_airport=dep_apt,
                                arrival_airport=arr_apt,
                                sta_raw=sta_val,
                                pax_data=pax_val,
                                load_data=load_val,
                                notes_data=str(notes_val),
                                aircraft_type=type_val,
                                version=ver_val,
                                status=status_str
                            )
                            self.session.add(flight)
                            self.session.flush()
                            existing = flight
                        
                        eff_out_utc = out_utc if out_utc else existing.actual_out_utc
                        eff_in_utc = in_utc if in_utc else existing.actual_in_utc
                        actual_block = None
                        if eff_out_utc and eff_in_utc:
                            actual_block = int((eff_in_utc - eff_out_utc).total_seconds() // 60)
                            
                        if not actual_out: actual_out = existing.actual_out
                        if not actual_off: actual_off = existing.actual_off
                        if not actual_on: actual_on = existing.actual_on
                        if not actual_in: actual_in = existing.actual_in
                        if not out_utc: out_utc = existing.actual_out_utc
                        if not off_utc: off_utc = existing.actual_off_utc
                        if not on_utc: on_utc = existing.actual_on_utc
                        if not in_utc: in_utc = existing.actual_in_utc
                        if has_duplicate_warning == 1: existing.has_duplicate_warning = 1
                        
                        changes = {}
                        fields_to_check = {
                            "Tail Number": ("tail_number", tail_number),
                            "Scheduled Departure": ("scheduled_departure", parsed_std),
                            "Scheduled Arrival": ("scheduled_arrival", parsed_sta),
                            "Actual Departure": ("actual_departure", parsed_atd),
                            "Actual Arrival": ("actual_arrival", parsed_ata),
                            "Actual Out": ("actual_out", actual_out),
                            "Actual Off": ("actual_off", actual_off),
                            "Actual On": ("actual_on", actual_on),
                            "Actual In": ("actual_in", actual_in),
                            "Scheduled Departure Zulu": ("scheduled_departure_utc", std_utc),
                            "Scheduled Arrival Zulu": ("scheduled_arrival_utc", sta_utc),
                            "Actual Departure Zulu": ("actual_departure_utc", atd_utc),
                            "Actual Arrival Zulu": ("actual_arrival_utc", ata_utc),
                            "Actual Out Zulu": ("actual_out_utc", out_utc),
                            "Actual Off Zulu": ("actual_off_utc", off_utc),
                            "Actual On Zulu": ("actual_on_utc", on_utc),
                            "Actual In Zulu": ("actual_in_utc", in_utc),
                            "Planned Block": ("planned_block_minutes", planned_block),
                            "Actual Block": ("actual_block_minutes", actual_block),
                            "Departure Airport": ("departure_airport", dep_apt),
                            "Arrival Airport": ("arrival_airport", arr_apt),
                            "Aircraft Type": ("aircraft_type", type_val),
                            "Version": ("version", ver_val),
                            "Status": ("status", status_str)
                        }
                        
                        for label, (attr, new_val) in fields_to_check.items():
                            old_val = getattr(existing, attr)
                            if old_val != new_val:
                                if (old_val is None and new_val == "") or (old_val == "" and new_val is None): continue
                                changes[label] = {"old": str(old_val) if old_val is not None else None, "new": str(new_val) if new_val is not None else None}
                                setattr(existing, attr, new_val)
                        
                        flight_key = (flight_number, flight_date)
                        if existing and existing.id not in seen_ids:
                             seen_ids.append(existing.id)
                             
                        if flight_key not in processed_flights_in_session:
                            processed_flights_in_session.add(flight_key)
                            
                            current_crew_list = []
                            from database import flight_crew_association
                            existing_crew_res = self.session.execute(flight_crew_association.select().where(flight_crew_association.c.flight_id == existing.id)).fetchall()
                            for ec in existing_crew_res:
                                cm = self.session.query(CrewMember).get(ec.crew_id)
                                current_crew_list.append({"id": cm.employee_id if cm else None, "name": cm.name if cm else "Unknown", "role": ec.role, "flags": ec.flags})
                            current_crew_list.sort(key=lambda x: (x['role'] or '', x['name'] or ''))
                            
                            new_crew_list = []
                            crew_data = details.get("Crew On Board", [None, None])
                            if crew_data[1]:
                                crew_lines = crew_data[1].get_text(separator="\n").split("\n")
                                for line in crew_lines:
                                    line = line.strip()
                                    if not line: continue
                                    parts = line.split(" - ", 1)
                                    if len(parts) < 2: continue
                                    role_code = parts[0].strip()
                                    rest = parts[1].strip()
                                    r_parts = rest.split(" ")
                                    e_id = r_parts[0]
                                    rest = rest[len(e_id)+1:].strip()
                                    flags = ""
                                    name = rest
                                    p_start = rest.find("(")
                                    p_end = rest.find(")")
                                    if p_start != -1 and p_end != -1 and p_end > p_start:
                                        flags = rest[p_start+1:p_end]
                                        name = rest[:p_start].strip()
                                    n_parts = name.split(" ")
                                    if n_parts and "@" in n_parts[-1]: name = " ".join(n_parts[:-1])
                                    new_crew_list.append({"id": e_id, "name": name, "role": role_code, "flags": flags})
                                    
                            def normalize_crew(c_list):
                                normalized = []
                                for c in c_list:
                                    n_clean = str(c.get("name") or "").strip()
                                    if "@" in n_clean: n_clean = n_clean.split("@")[0].strip()
                                    normalized.append({"id": str(c.get("id") or "").strip(), "name": n_clean.lower(), "role": str(c.get("role") or "").strip().upper(), "flags": str(c.get("flags") or "").strip()})
                                return sorted(normalized, key=lambda x: (x['role'], x['id'], x['name'], x['flags']))
                                
                            if json.dumps(normalize_crew(current_crew_list), sort_keys=True) != json.dumps(normalize_crew(new_crew_list), sort_keys=True):
                                if current_crew_list or new_crew_list: changes["Crew"] = {"old": current_crew_list, "new": new_crew_list}
                            
                            history_changes = {k: v for k, v in changes.items() if k != "Status"}
                            if history_changes and not was_new_flight:
                                try:
                                    summary = f"Changed: {', '.join(history_changes.keys())}"
                                    from database import FlightHistory
                                    hist = FlightHistory(flight_id=existing.id, timestamp=datetime.now(), changes_json=json.dumps(history_changes), description=summary)
                                    self.session.add(hist)
                                    print(f"  [History] {summary} for {flight_number}")
                                except Exception as e: print(f"Error logging history: {e}")
                                
                            self.session.execute(flight_crew_association.delete().where(flight_crew_association.c.flight_id == existing.id))
                            for c_dict in new_crew_list:
                                c_id, c_name, c_role, c_flags = c_dict["id"], c_dict["name"], c_dict["role"], c_dict["flags"]
                                crew = self._get_or_create_crew(c_id, c_name)
                                self.session.execute(flight_crew_association.insert().values(flight_id=existing.id, crew_id=crew.id, role=c_role, flags=c_flags))
                            self.session.flush()

                    elif mode == "UTC" and existing:
                        existing.scheduled_departure_utc = parsed_std
                        existing.scheduled_arrival_utc = parsed_sta
                    
                    self.session.commit() # Save progress for this specific flight leg
                    
                except Exception as e:
                    print(f"Error during database sync for item: {e}")
                    self.session.rollback()
            except Exception as e:
                print(f"Error parsing flight item structure: {e}")
                
        print(f"Data saved to database ({mode}).")
        return seen_ids

    def _parse_time(self, date_obj, time_str):
        if not time_str or len(time_str) != 4: return None
        try:
            h = int(time_str[:2])
            m = int(time_str[2:])
            return date_obj.replace(hour=h, minute=m, second=0, microsecond=0)
        except:
            return None
