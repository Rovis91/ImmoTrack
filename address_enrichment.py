import requests
import time
import logging
from abc import ABC, abstractmethod
from typing import Dict, Tuple, Optional, List
import pandas as pd
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class BaseAddressClient(ABC):
    """Base class for address-based API clients with rate limiting and retry logic."""
    
    def __init__(self, min_delay: float = 0.02, max_retries: int = 1, retry_delay: float = 1.0):
        """
        Initialize the base client.

        Args:
            min_delay: Minimum delay between requests in seconds
            max_retries: Maximum number of retry attempts for failed requests
            retry_delay: Delay between retry attempts in seconds
        """
        self.last_request_time = 0
        self.min_delay = min_delay
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def _wait_if_needed(self) -> None:
        """Ensure minimum delay between API requests."""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_delay:
            time.sleep(self.min_delay - time_since_last)
        self.last_request_time = time.time()

    def _make_request(self, url: str, params: Optional[Dict] = None) -> Optional[Dict]:
        """
        Make an API request with retry logic.

        Args:
            url: API endpoint URL
            params: Query parameters

        Returns:
            Optional[Dict]: API response data if successful, None otherwise
        """
        attempts = 0
        while attempts <= self.max_retries:
            try:
                self._wait_if_needed()
                response = requests.get(url, params=params)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as e:
                attempts += 1
                if attempts <= self.max_retries:
                    logging.warning(f"Request failed, attempt {attempts}/{self.max_retries}: {str(e)}")
                    time.sleep(self.retry_delay)
                else:
                    logging.error(f"All retry attempts failed for URL {url}: {str(e)}")
                    return None
            except Exception as e:
                logging.error(f"Unexpected error for URL {url}: {str(e)}")
                return None

    @abstractmethod
    def enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Abstract method to enrich a DataFrame with additional data.

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with enriched data
        """
        pass

class GeocodingClient(BaseAddressClient):
    """Client for the French government's geocoding API."""
    
    def __init__(self, min_delay: float = 0.02):
        """Initialize the geocoding client."""
        super().__init__(min_delay=min_delay)
        self.base_url = "https://api-adresse.data.gouv.fr/search/"

    def get_coordinates(self, address: str, city: str = "PARIS") -> Optional[Tuple[float, float]]:
        """
        Retrieve geographic coordinates for a given address.

        Args:
            address: Street address
            city: City name

        Returns:
            Optional tuple of (longitude, latitude)
        """
        query = f"{address} {city}"
        params = {
            "q": query,
            "limit": 1,
            "autocomplete": 0
        }

        response_data = self._make_request(self.base_url, params)
        
        if response_data and response_data.get("features"):
            coordinates = response_data["features"][0]["geometry"]["coordinates"]
            return coordinates[0], coordinates[1]  # longitude, latitude
        return None

    def enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add geographic coordinates to a DataFrame.

        Args:
            df: DataFrame with 'complete_address' and 'city_name' columns

        Returns:
            DataFrame with added 'longitude' and 'latitude' columns
        """
        required_columns = ['complete_address', 'city_name']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        df['longitude'] = None
        df['latitude'] = None

        total_addresses = len(df)
        success_count = 0

        for idx, row in df.iterrows():
            if idx % 10 == 0:
                logging.info(f"Geocoding progress: {idx}/{total_addresses}")

            try:
                coordinates = self.get_coordinates(
                    address=row['complete_address'],
                    city=row['city_name']
                )

                if coordinates:
                    df.at[idx, 'longitude'] = coordinates[0]
                    df.at[idx, 'latitude'] = coordinates[1]
                    success_count += 1

            except Exception as e:
                logging.error(f"Error geocoding row {idx}: {str(e)}")
                continue

        logging.info(f"Geocoding completed. {success_count}/{total_addresses} addresses successfully geocoded.")
        return df

class DPEClient(BaseAddressClient):
    """Client for the ADEME DPE API."""
    
    def __init__(self, min_delay: float = 0.05):  # 50ms for rate limit of 100/5s
        """Initialize the DPE client."""
        super().__init__(min_delay=min_delay)
        self.base_url = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-france/lines"

    def get_dpe_data(self, address: str, city: str = "PARIS") -> Optional[Dict]:
        """
        Retrieve DPE data for a given address.

        Args:
            address: Street address
            city: City name

        Returns:
            Optional[Dict]: Most recent DPE data if found and valid
        """
        query = f"{address} {city}"
        params = {
            "q": query,
            "size": 10  # Get multiple results to find best match
        }

        response_data = self._make_request(self.base_url, params)
        
        if not response_data or not response_data.get("results"):
            return None

        # Filter for high confidence matches and sort by date
        valid_matches = [
            result for result in response_data["results"]
            if result.get("geo_score", 0) > 0.8
        ]

        if not valid_matches:
            return None

        # Return most recent DPE
        return max(
            valid_matches,
            key=lambda x: x.get("date_etablissement_dpe", "1900-01-01")
        )

    def enrich_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add DPE data to a DataFrame.

        Args:
            df: DataFrame with 'complete_address' and 'city_name' columns

        Returns:
            DataFrame with added DPE columns
        """
        required_columns = ['complete_address', 'city_name']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Initialize DPE columns
        dpe_columns = [
            'dpe_energy_class', 'dpe_ges_class', 'dpe_energy_value',
            'dpe_ges_value', 'dpe_surface', 'dpe_date', 'dpe_construction_year',
            'dpe_type', 'building_type'
        ]
        
        for col in dpe_columns:
            df[col] = None

        total_properties = len(df)
        success_count = 0

        for idx, row in df.iterrows():
            if idx % 10 == 0:
                logging.info(f"DPE lookup progress: {idx}/{total_properties}")

            try:
                dpe_data = self.get_dpe_data(
                    address=row['complete_address'],
                    city=row['city_name']
                )

                if dpe_data:
                    df.at[idx, 'dpe_energy_class'] = dpe_data.get('classe_consommation_energie')
                    df.at[idx, 'dpe_ges_class'] = dpe_data.get('classe_estimation_ges')
                    df.at[idx, 'dpe_energy_value'] = dpe_data.get('consommation_energie')
                    df.at[idx, 'dpe_ges_value'] = dpe_data.get('estimation_ges')
                    df.at[idx, 'dpe_surface'] = dpe_data.get('surface_thermique_lot')
                    df.at[idx, 'dpe_date'] = dpe_data.get('date_etablissement_dpe')
                    df.at[idx, 'dpe_construction_year'] = dpe_data.get('annee_construction')
                    df.at[idx, 'dpe_type'] = dpe_data.get('tr001_modele_dpe_type_libelle')
                    df.at[idx, 'building_type'] = dpe_data.get('tr002_type_batiment_description')
                    success_count += 1

            except Exception as e:
                logging.error(f"Error fetching DPE data for row {idx}: {str(e)}")
                continue

        logging.info(f"DPE lookup completed. {success_count}/{total_properties} properties enriched with DPE data.")
        return df

def enrich_address_data(df: pd.DataFrame, 
                       include_geocoding: bool = True, 
                       include_dpe: bool = True) -> pd.DataFrame:
    """
    Enrich address data with geocoding and DPE information.

    Args:
        df: Input DataFrame with address information
        include_geocoding: Whether to include geocoding enrichment
        include_dpe: Whether to include DPE data enrichment

    Returns:
        Enriched DataFrame
    """
    enriched_df = df.copy()

    if include_geocoding:
        geocoder = GeocodingClient()
        enriched_df = geocoder.enrich_dataframe(enriched_df)

    if include_dpe:
        dpe_client = DPEClient()
        enriched_df = dpe_client.enrich_dataframe(enriched_df)

    return enriched_df
