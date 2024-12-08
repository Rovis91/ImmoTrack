from datetime import datetime
import urllib.parse
from dateutil.relativedelta import relativedelta
from typing import List, Tuple
from enum import Enum

class SearchType(Enum):
    ALL_TYPES = ("Tous types de biens ensemble", "1%2C2%2C5%2C0%2C4")
    HOUSES_AND_APARTMENTS = ("Maisons et appartements ensemble", "1%2C2")
    HOUSES_OR_APARTMENTS = ("Maisons puis appartements séparément", ["1", "2"])
    THREE_CATEGORIES = ("Maisons, appartements et autres séparément", ["1", "2", "4%2C0%2C5"])

    def __init__(self, description, property_types):
        self.description = description
        self.property_types = property_types

class UrlGenerator:
    def __init__(self):
        self.month_names_fr = {
            1: 'Janvier', 2: 'Février', 3: 'Mars', 4: 'Avril', 5: 'Mai',
            6: 'Juin', 7: 'Juillet', 8: 'Août', 9: 'Septembre',
            10: 'Octobre', 11: 'Novembre', 12: 'Décembre'
        }

    def display_search_types(self) -> None:
        """Display available search types"""
        print("\nTypes de recherche disponibles:")
        for i, search_type in enumerate(SearchType, 1):
            print(f"{i}. {search_type.description}")

    def get_dates(self) -> Tuple[str, str]:
        """Get dates from user with defaults"""
        print("\nEntrez les dates (format: MM/YYYY) ou appuyez sur Enter pour les valeurs par défaut:")
        start_date = input("Date début [01/2014]: ").strip() or "01/2014"
        end_date = input("Date fin [06/2024]: ").strip() or "06/2024"
        return start_date, end_date

    def generate_urls(self, base_url: str, start_date: str, end_date: str, 
                     search_type: SearchType, elements_limit: int = 100) -> List[Tuple[str, int]]:
        """Generate URLs based on search type and date range"""
        parsed = urllib.parse.urlparse(base_url)
        base_params = urllib.parse.parse_qs(parsed.query)
        
        start = datetime.strptime(start_date, '%m/%Y')
        end = datetime.strptime(end_date, '%m/%Y')
        
        urls = []
        property_types = (
            search_type.property_types 
            if isinstance(search_type.property_types, list) 
            else [search_type.property_types]
        )
        
        for property_type in property_types:
            current = start
            while current <= end:
                month_fr = self.month_names_fr[current.month]
                date_fr = f"{month_fr} {current.year}"
                
                params = base_params.copy()
                params.update({
                    'propertytypes': [property_type],
                    'minmonthyear': [date_fr],
                    'maxmonthyear': [date_fr]
                })
                
                query = urllib.parse.urlencode(params, doseq=True)
                url = urllib.parse.urlunparse((
                    parsed.scheme,
                    parsed.netloc,
                    parsed.path,
                    parsed.params,
                    query,
                    parsed.fragment
                ))
                
                urls.append((url, elements_limit))
                current += relativedelta(months=1)
                
        return urls