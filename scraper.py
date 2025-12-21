import os
import asyncio
from datetime import datetime, timedelta

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
            print("  [Pass 1] Switching to UTC...")
            self.page.select_option("#MasterMain_TimeMode_DP_TimeModes", label="UTC")
            self.page.click("#MasterMain_btnSearch")
            self.page.wait_for_load_state("networkidle")
            self.page.wait_for_timeout(3000) # Safety
            
            content_utc = self.page.content()
            self.parse_and_save(content_utc, date_obj, mode="UTC")
            
            # --- PASS 2: Local ---
            print("  [Pass 2] Switching to Local Time...")
            self.page.select_option("#MasterMain_TimeMode_DP_TimeModes", label="Local time")
            self.page.click("#MasterMain_btnSearch")
            self.page.wait_for_load_state("networkidle")
            self.page.wait_for_timeout(3000)
            
            content_local = self.page.content()
            self.parse_and_save(content_local, date_obj, mode="Local")
            
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

    def parse_and_save(self, html_content, date_obj, mode="UTC"):
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # We only need Departures (MasterMain_panelUpper)
        # Avoids duplicate flights that appear in both Departures (today) and Arrivals (tomorrow)
        departure_panel = soup.find("div", id="MasterMain_panelUpper")
        if departure_panel:
            list_items = departure_panel.find_all("div", class_="ListItem")
        else:
            # Fallback to all list items if panel ID not found
            list_items = soup.find_all("div", class_="ListItem")
            
        print(f"  Parsing {len(list_items)} flights from Departures ({mode})...")
        
        for item in list_items:
            try:
                # 1. Flight Number
                header_table = item.find("div", class_="ItemHeader").find("table")
                if not header_table: continue
                
                header_cells = header_table.find_all("td")
                flight_number = header_cells[0].get_text(strip=True)
                
                # 2. Details
                details_table = item.find("table", class_="ItemChildTableDetails")
                if not details_table: continue
                
                details = {}
                rows = details_table.find_all("tr")
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True)
                        val_cell = cells[1]
                        val_text = val_cell.get_text(strip=True)
                        details[key] = (val_text, val_cell)
                
                # Parse specific fields
                std_str = details.get("STD", ["", None])[0] # e.g. "2159"
                sta_val = details.get("STA", ["", None])[0]
                
                # Parse Times
                parsed_std = self._parse_time(date_obj, std_str)
                parsed_sta = None
                if sta_val:
                    # Reuse STA logic
                    try:
                        sta_val_clean = sta_val.strip()
                        if " : " in sta_val_clean:
                            parts = sta_val_clean.split(" : ")
                            if len(parts) == 2:
                                t_str, d_str = parts
                                dt_obj = datetime.strptime(d_str, "%d%b%y")
                                h = int(t_str[:2]); m = int(t_str[2:])
                                parsed_sta = dt_obj.replace(hour=h, minute=m)
                        elif len(sta_val_clean) == 4 and sta_val_clean.isdigit():
                            h = int(sta_val_clean[:2]); m = int(sta_val_clean[2:])
                            parsed_sta = date_obj.replace(hour=h, minute=m)
                    except: pass

                # --- DB Interaction ---
                existing = self.session.query(Flight).filter_by(flight_number=flight_number, date=date_obj).first()
                
                if mode == "UTC":
                    # Full Parse & Create
                    tail_number = details.get("Registration", ["", None])[0]
                    dep_apt = details.get("Departure", ["", None])[0]
                    arr_apt = details.get("Arrival", ["", None])[0]
                    pax_val = details.get("Pax", ["", None])[0]
                    load_val = details.get("Load", ["", None])[0]
                    notes_val = details.get("Notes", ["", None])[0]
                    type_val = details.get("Type", ["", None])[0]
                    ver_val = details.get("Version", ["", None])[0]
                    if not isinstance(notes_val, str): notes_val = ""
                    
                    if not existing:
                        flight = Flight(
                            flight_number=flight_number,
                            date=date_obj,
                            tail_number=tail_number,
                            scheduled_departure=parsed_std,
                            scheduled_arrival=parsed_sta,
                            departure_airport=dep_apt,
                            arrival_airport=arr_apt,
                            sta_raw=sta_val,
                            pax_data=pax_val,
                            load_data=load_val,
                            notes_data=str(notes_val),
                            aircraft_type=type_val,
                            version=ver_val
                        )
                        self.session.add(flight)
                        self.session.flush()
                    else:
                        flight = existing
                        flight.scheduled_departure = parsed_std
                        flight.scheduled_arrival = parsed_sta
                        flight.sta_raw = sta_val
                        flight.pax_data = pax_val
                        flight.load_data = load_val
                        flight.notes_data = str(notes_val)
                        flight.aircraft_type = type_val
                        flight.version = ver_val
                        
                    # Crew Parsing (Only on UTC pass to avoid duplication)
                    # FIX: Clear existing crew first to avoid duplicates
                    # This is done by deleting existing associations for this flight
                    self.session.execute(flight_crew_association.delete().where(
                        flight_crew_association.c.flight_id == flight.id
                    ))
                    
                    crew_data = details.get("Crew On Board", [None, None])
                    if crew_data[1]:
                        crew_text_lines = crew_data[1].get_text(separator="\n").split("\n")
                        for line in crew_text_lines:
                            line = line.strip()
                            if not line: continue
                            parts = line.split(" - ", 1)
                            if len(parts) < 2: continue
                            role_code = parts[0].strip()
                            rest = parts[1].strip()
                            rest_parts = rest.split(" ")
                            emp_id = rest_parts[0]
                            rest = rest[len(emp_id)+1:].strip()
                            
                            flags = ""
                            name = rest
                            paren_start = rest.find("(")
                            paren_end = rest.find(")")
                            if paren_start != -1 and paren_end != -1 and paren_end > paren_start:
                                flags = rest[paren_start+1:paren_end]
                                name = rest[:paren_start].strip()
                            
                            name_parts = name.split(" ")
                            if name_parts and "@" in name_parts[-1]:
                                name = " ".join(name_parts[:-1])

                            crew = self.session.query(CrewMember).filter_by(employee_id=emp_id).first()
                            if not crew:
                                crew = CrewMember(name=name, employee_id=emp_id)
                                self.session.add(crew)
                                self.session.flush()
                            
                            # Add to Flight (since we cleared, we just append/insert info)
                            # But we need association flags.
                            # Standard append doesn't let us set flags easily, need to use association table insert
                            # Wait, Flight.crew_members.append(crew) creates a blank association row.
                            # We need to explicitly handle it.
                            
                            # Insert into association
                            ins = flight_crew_association.insert().values(
                                flight_id=flight.id, 
                                crew_id=crew.id, 
                                role=role_code,
                                flags=flags
                            )
                            self.session.execute(ins)

                elif mode == "Local":
                    # Update ONLY local times
                    if existing:
                        existing.scheduled_departure_local = parsed_std
                        existing.scheduled_arrival_local = parsed_sta
                        # We could parse actuals here too if available
                        
            except Exception as e:
                print(f"Error parsing flight item: {e}")
                
        print(f"Data saved to database ({mode}).")

    def _parse_time(self, date_obj, time_str):
        if not time_str or len(time_str) != 4: return None
        try:
            h = int(time_str[:2])
            m = int(time_str[2:])
            return date_obj.replace(hour=h, minute=m)
        except:
            return None
