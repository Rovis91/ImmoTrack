"""
URL generation for property scraping with configurable search parameters.
"""

from datetime import datetime
import urllib.parse
from dateutil.relativedelta import relativedelta
from typing import List, Tuple, Dict, Optional
from enum import Enum
from dataclasses import dataclass
import logging

logger = logging.getLogger(__name__)

class PropertyType(Enum):
    """Defines available property types with corresponding codes."""
    HOUSE = "1"
    APARTMENT = "2"
    LAND = "4"
    COMMERCIAL = "0"
    OTHER = "5"

class SearchType(Enum):
    """Predefined combinations of property types for searches."""
    ALL_TYPES = ("Tous types de biens ensemble", [
        PropertyType.HOUSE.value,
        PropertyType.APARTMENT.value,
        PropertyType.LAND.value,
        PropertyType.COMMERCIAL.value,
        PropertyType.OTHER.value
    ])
    ALL_TYPES_SPLIT = ("Tous types de biens séparés", [
        [PropertyType.HOUSE.value],  # Houses alone
        [PropertyType.APARTMENT.value],  # Apartments alone
        [PropertyType.LAND.value, PropertyType.COMMERCIAL.value, PropertyType.OTHER.value]  # Others grouped
    ])
    HOUSES_AND_APARTMENTS = ("Maisons et appartements ensemble", [
        PropertyType.HOUSE.value,
        PropertyType.APARTMENT.value
    ])
    HOUSES_ONLY = ("Maisons uniquement", [PropertyType.HOUSE.value])
    APARTMENTS_ONLY = ("Appartements uniquement", [PropertyType.APARTMENT.value])

    def __init__(self, description: str, property_types: List):
        self.description = description
        self.property_types = property_types
        # Flag to indicate if property types should be treated as separate groups
        self.is_split = isinstance(property_types[0], list) if property_types else False

@dataclass
class SearchParameters:
    """Holds the configurable parameters for URL generation."""
    min_price: int = 0
    max_price: int = 25_000_000
    min_surface: int = 0
    max_surface: int = 400
    min_rooms: int = 1
    max_rooms: int = 5
    min_land_surface: int = 0
    max_land_surface: int = 50_000
    location_center: str = "0.3293609303041194;46.575229268622195"
    zoom_level: float = 12.151412188068159

class UrlGenerator:
    """
    Generates URLs for property scraping using configurable search parameters.
    """

    def __init__(self):
        """Initialize the URL generator with French month names."""
        self.month_names_fr = {
            1: 'Janvier', 2: 'Février', 3: 'Mars', 4: 'Avril', 5: 'Mai',
            6: 'Juin', 7: 'Juillet', 8: 'Août', 9: 'Septembre',
            10: 'Octobre', 11: 'Novembre', 12: 'Décembre'
        }
        
    def _parse_date(self, date_str: str) -> datetime:
        """
        Parse a date string in MM/YYYY format.

        Args:
            date_str (str): Date string in MM/YYYY format.

        Returns:
            datetime: Parsed date object.

        Raises:
            ValueError: If the date format is invalid.
        """
        try:
            return datetime.strptime(date_str, '%m/%Y')
        except ValueError:
            raise ValueError(
                f"Invalid date format: {date_str}. Expected format: MM/YYYY"
            )

    def _validate_date_range(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> None:
        """
        Validate that the start date is before the end date.

        Args:
            start_date (datetime): Start date.
            end_date (datetime): End date.

        Raises:
            ValueError: If the start date is after the end date.
        """
        if start_date > end_date:
            raise ValueError("Start date must be before end date.")

    def generate_base_params(
        self,
        params: SearchParameters,
        property_types: List[str],
        date_fr: str
    ) -> Dict:
        """
        Generate the base query parameters for a single URL.

        Args:
            params (SearchParameters): The search parameters.
            property_types (List[str]): List of property type codes.
            date_fr (str): Date string in French format.

        Returns:
            Dict: Dictionary of query parameters for the URL.
        """
        return {
            'minprice': [str(params.min_price)],
            'maxprice': [str(params.max_price)],
            'minsurface': [str(params.min_surface)],
            'maxsurface': [str(params.max_surface)],
            'minrooms': [str(params.min_rooms)],
            'maxrooms': [str(params.max_rooms)],
            'minsurfaceland': [str(params.min_land_surface)],
            'maxsurfaceland': [str(params.max_land_surface)],
            'center': [params.location_center],
            'zoom': [str(params.zoom_level)],
            'propertytypes': property_types,  # Now accepts a list of types
            'minmonthyear': [date_fr],
            'maxmonthyear': [date_fr]
        }

    def generate_urls(
        self,
        base_url: str,
        start_date: str,
        end_date: str,
        search_type: SearchType,
        params: Optional[SearchParameters] = None,
        elements_limit: int = 100
    ) -> List[Tuple[str, int]]:
        """
        Generate a list of URLs for property scraping.

        Args:
            base_url (str): Base URL of the property website.
            start_date (str): Start date in MM/YYYY format.
            end_date (str): End date in MM/YYYY format.
            search_type (SearchType): Type of property search.
            params (Optional[SearchParameters]): Additional search parameters.
            elements_limit (int): Maximum number of elements to scrape per page.

        Returns:
            List[Tuple[str, int]]: A list of tuples, each containing a URL and its element limit.

        Raises:
            ValueError: If the date range or parameters are invalid.
        """
        # Parse and validate the date range
        start = self._parse_date(start_date)
        end = self._parse_date(end_date)
        self._validate_date_range(start, end)

        # Use default parameters if none are provided
        params = params or SearchParameters()

        # Parse the base URL
        parsed = urllib.parse.urlparse(base_url)
        urls = []

        # Handle property types based on whether they should be split or not
        property_type_groups = (
            search_type.property_types if search_type.is_split
            else [[t] for t in search_type.property_types]
        )

        # Generate URLs for each property type group and month
        for property_types in property_type_groups:
            current = start
            while current <= end:
                month_fr = self.month_names_fr[current.month]
                date_fr = f"{month_fr} {current.year}"

                # Generate query parameters with the group of property types
                query_params = self.generate_base_params(
                    params, property_types, date_fr
                )

                # Build the full URL
                query = urllib.parse.urlencode(query_params, doseq=True)
                url = urllib.parse.urlunparse((
                    parsed.scheme,
                    parsed.netloc,
                    parsed.path,
                    parsed.params,
                    query,
                    parsed.fragment
                ))

                urls.append((url, elements_limit))
                logger.debug(f"Generated URL for {date_fr} with property types {property_types}: {url}")

                current += relativedelta(months=1)

        logger.info(
            f"Generated {len(urls)} URLs for the period {start_date} to {end_date}."
        )
        return urls