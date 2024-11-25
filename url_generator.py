from datetime import datetime
import urllib.parse
from dateutil.relativedelta import relativedelta
from typing import List, Tuple

def generate_monthly_urls(base_url: str, start_date: str, end_date: str, elements_limit: int = 100) -> List[Tuple[str, int]]:
    """
    Generate URLs for each month between start_date and end_date.

    Args:
        base_url (str): Base URL with all parameters included.
        start_date (str): Start date in the format 'MM/YYYY'.
        end_date (str): End date in the format 'MM/YYYY'.
        elements_limit (int): Limit of elements per request.

    Returns:
        List[Tuple[str, int]]: List of tuples containing the URL and the elements limit.
    """
    # Parse start and end dates
    start = datetime.strptime(start_date, '%m/%Y')
    end = datetime.strptime(end_date, '%m/%Y')

    # French month names
    month_names_fr = {
        1: 'Janvier', 2: 'Février', 3: 'Mars', 4: 'Avril', 5: 'Mai',
        6: 'Juin', 7: 'Juillet', 8: 'Août', 9: 'Septembre',
        10: 'Octobre', 11: 'Novembre', 12: 'Décembre'
    }

    urls = []
    current = start

    while current <= end:
        # Format month and year into French format
        month = month_names_fr[current.month]
        year = str(current.year)
        date_fr = f"{month} {year}"

        # Parse the base URL
        parsed = urllib.parse.urlparse(base_url)
        query_params = urllib.parse.parse_qs(parsed.query)

        # Update query parameters with the current month and year
        query_params['minmonthyear'] = [date_fr]
        query_params['maxmonthyear'] = [date_fr]

        # Construct the new URL with updated query parameters
        new_query = urllib.parse.urlencode(query_params, doseq=True)
        new_url = urllib.parse.urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            parsed.fragment
        ))

        # Append the URL and elements limit as a tuple to the list
        urls.append((new_url, elements_limit))

        # Increment the date by one month
        current += relativedelta(months=1)

    return urls
