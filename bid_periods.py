from datetime import datetime, date

def get_bid_period_date_range(year: int, month: int):
    """
    Returns the start and end dates (inclusive) for a given bid period month/year.
    
    Bid Period Definitions:
    Jan: Jan 1 - Jan 30
    Feb: Jan 31 - Mar 1 (Leap year makes Feb 31 days, otherwise 30)
    Mar: Mar 2 - Mar 31
    Apr: Apr 1 - Apr 30
    May: May 1 - May 31
    Jun: Jun 1 - Jun 30
    Jul: Jul 1 - Jul 31
    Aug: Aug 1 - Aug 31
    Sep: Sep 1 - Sep 30
    Oct: Oct 1 - Oct 31
    Nov: Nov 1 - Nov 30
    Dec: Dec 1 - Dec 31
    """
    
    if month == 1:  # January
        start_date = date(year, 1, 1)
        end_date = date(year, 1, 30)
    elif month == 2:  # February
        # Starts previous month (Jan 31)
        # Ends next month (Mar 1)
        start_date = date(year, 1, 31)
        end_date = date(year, 3, 1)
    elif month == 3:  # March
        # Starts Mar 2
        start_date = date(year, 3, 2)
        end_date = date(year, 3, 31)
    elif month == 4:  # April
        start_date = date(year, 4, 1)
        end_date = date(year, 4, 30)
    elif month == 5:  # May
        start_date = date(year, 5, 1)
        end_date = date(year, 5, 31)
    elif month == 6:  # June
        start_date = date(year, 6, 1)
        end_date = date(year, 6, 30)
    elif month == 7:  # July
        start_date = date(year, 7, 1)
        end_date = date(year, 7, 31)
    elif month == 8:  # August
        start_date = date(year, 8, 1)
        end_date = date(year, 8, 31)
    elif month == 9:  # September
        start_date = date(year, 9, 1)
        end_date = date(year, 9, 30)
    elif month == 10: # October
        start_date = date(year, 10, 1)
        end_date = date(year, 10, 31)
    elif month == 11: # November
        start_date = date(year, 11, 1)
        end_date = date(year, 11, 30)
    elif month == 12: # December
        start_date = date(year, 12, 1)
        end_date = date(year, 12, 31)
    else:
        raise ValueError(f"Invalid month: {month}")
        
    return start_date, end_date

def get_bid_period_from_date(target_date):
    """Returns the (year, month) tuple for the bid period containing the target_date."""
    # Ensure we're working with a date object
    if isinstance(target_date, datetime):
        d = target_date.date()
    else:
        d = target_date
        
    year = d.year
    
    # Check if target_date is in Jan (1-30)
    if date(year, 1, 1) <= d <= date(year, 1, 30):
        return year, 1
        
    # Check if target_date is in Feb bid period (Jan 31 - Mar 1)
    # Check bounds explicitly to avoid leap year complexity issues manually
    # Feb Bid Period Start: Jan 31
    if d == date(year, 1, 31):
        return year, 2
        
    # Feb Bid Period End: Mar 1
    if d == date(year, 3, 1):
        return year, 2
        
    # In between (Feb 1 - Feb 28/29)
    if d.month == 2:
        return year, 2
        
    # Check Mar (2-31)
    if date(year, 3, 2) <= d <= date(year, 3, 31):
        return year, 3
        
    # For others, it matches calendar month
    if 4 <= d.month <= 12:
        return year, d.month
        
    return year, d.month

def get_current_bid_period():
    """Returns the (year, month) of the bid period containing today."""
    return get_bid_period_from_date(date.today())
