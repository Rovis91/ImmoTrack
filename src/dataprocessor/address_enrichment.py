"""
Address enrichment module for adding geocoding and DPE data to properties.
"""
import re 
import time
import logging
import requests
import difflib
from geopy.distance import geodesic
from typing import Dict, Tuple, Optional, List
import pandas as pd
from .processor_base import ProcessorBase

logger = logging.getLogger(__name__)

class AddressEnrichment(ProcessorBase):
    """Enriches property data with geocoding and DPE information."""

    def __init__(self):
        """Initialize address enrichment with API endpoints and rate limiting."""
        self.geocoding_url = "https://api-adresse.data.gouv.fr/search/"
        self.dpe_url = "https://data.ademe.fr/data-fair/api/v1/datasets/dpe-france/lines"
        
        # Rate limiting settings
        self.last_request_time = 0
        self.min_delay = 0.1  # 100ms between requests
        self.max_retries = 5
        self.retry_delay = 1.0
        self.max_delay = 32.0  # Maximum backoff delay

    def _calculate_distance(self, coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
        """
        Calculate the geodesic distance between two coordinate pairs.
        
        Args:
            coord1: Tuple of (latitude, longitude) for the first location.
            coord2: Tuple of (latitude, longitude) for the second location.

        Returns:
            Distance in meters.
        """
        return geodesic(coord1, coord2).meters

    def _wait_if_needed(self) -> None:
        """Ensure minimum delay between API requests."""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)
        self.last_request_time = time.time()

    def _make_request(self, url: str, params: Dict) -> Optional[Dict]:
        """
        Make API request with retry logic and rate limiting.
        
        Args:
            url: API endpoint URL
            params: Query parameters
            
        Returns:
            Optional[Dict]: API response data if successful, None otherwise
        """
        attempts = 0
        while attempts < self.max_retries:
            try:
                self._wait_if_needed()
                
                headers = {
                    'Accept': 'application/json',
                    'User-Agent': 'PropertyDataProcessor/1.0'
                }

                response = requests.get(
                    url, 
                    params=params,
                    headers=headers,
                    timeout=30
                )

                if response.status_code == 200:
                    return response.json()
                    
                elif response.status_code == 429:  # Too Many Requests
                    retry_after = int(response.headers.get('Retry-After', self.retry_delay))
                    logger.warning(f"Rate limit exceeded. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    
                elif response.status_code >= 500:  # Server errors
                    delay = min(self.retry_delay * (2 ** attempts), self.max_delay)
                    logger.warning(f"Server error {response.status_code}. Retrying in {delay}s...")
                    time.sleep(delay)
                    
                else:
                    logger.error(f"Request failed with status {response.status_code}: {response.text}")
                    return None

            except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
                delay = min(self.retry_delay * (2 ** attempts), self.max_delay)
                logger.warning(f"Request error: {str(e)}. Retrying in {delay}s... ({attempts + 1}/{self.max_retries})")
                time.sleep(delay)
                
            attempts += 1

        logger.error(f"Failed to get response from {url} after {self.max_retries} attempts")
        return None

    def _get_coordinates(self, address: str, city: str) -> Optional[Tuple[float, float]]:
        """
        Get geographic coordinates for an address.
        
        Args:
            address: Street address
            city: City name
            
        Returns:
            Optional tuple of (longitude, latitude)
        """
        params = {
            "q": f"{address} {city}",
            "limit": 1,
            "type": "housenumber"
        }

        data = self._make_request(self.geocoding_url, params)
        if data and data.get("features"):
            coords = data["features"][0]["geometry"]["coordinates"]
            return coords[0], coords[1]  # longitude, latitude
        return None

    def _get_dpe_data(self, address: str, city: str, coordinates: Optional[Tuple[float, float]]) -> Optional[Dict]:
        """
        Fetch DPE data for a given address using ADEME API.
        """
        try:
            logger.debug(f"Searching DPE for: Address='{address}', City='{city}'")

            # Build simple query parameters like our successful curl test
            params = {
                "select": "geo_adresse,geo_score,date_etablissement_dpe,classe_consommation_energie,classe_estimation_ges,consommation_energie,estimation_ges,tr002_type_batiment_description,latitude,longitude",
                "q": f"{address} {city}",  # Search full address including city
                "sort": "-geo_score,-date_etablissement_dpe",
                "size": 10
            }

            # Add geo distance filter if coordinates available
            if coordinates:
                params["geo_distance"] = f"{coordinates[0]}:{coordinates[1]}:100"

            logger.debug(f"DPE API query parameters: {params}")
            
            data = self._make_request(self.dpe_url, params)
            
            if not data or not data.get("results"):
                logger.debug("No results in API response")
                return None

            # Log each potential match
            for result in data["results"]:
                logger.debug(
                    f"Found match: "
                    f"address='{result.get('geo_adresse')}', "
                    f"score={result.get('geo_score')}, "
                    f"type={result.get('tr002_type_batiment_description')}"
                )
                
                # Check geo_score quality
                if result.get("geo_score", 0) < 0.8:
                    continue
                    
                # Verify building type
                building_type = result.get("tr002_type_batiment_description")
                if not building_type or building_type not in [
                    "Maison Individuelle",
                    "Logement",
                    "Bâtiment collectif à usage principal d'habitation"
                ]:
                    continue

                return result

            return None

        except Exception as e:
            logger.error(f"Error fetching DPE data for {address}: {str(e)}")
            logger.debug("Error details:", exc_info=True)
            return None
      
    def process(self, input_path: str, output_path: str) -> bool:
        """
        Process property data file with address enrichment.
        
        Args:
            input_path: Path to input CSV file
            output_path: Path to save enriched CSV file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            logger.info(f"Starting address enrichment process...")
            logger.info(f"Loading data from {input_path}")
            
            # Load input data
            df = self.load_csv(input_path)
            if df is None:
                return False

            # Validate required columns
            required = ['complete_address', 'city_name']
            if any(col not in df.columns for col in required):
                logger.error(f"Missing required columns: {required}")
                return False

            # Initialize new columns
            new_columns = {
                # Geocoding columns
                'longitude': None, 
                'latitude': None,
                # DPE columns
                'dpe_energy_class': None,
                'dpe_energy_value': None,
                'dpe_ges_class': None, 
                'dpe_ges_value': None,
                'building_type': None,
                'dpe_date': None
            }
            
            for col, default in new_columns.items():
                if col not in df.columns:
                    df[col] = default

            total_properties = len(df)
            logger.info(f"Starting enrichment for {total_properties} properties...")
            
            # Process each property
            geocoding_success = 0
            dpe_success = 0

            for idx, row in df.iterrows():
                if idx % 5 == 0:
                    logger.info(f"Processing property {idx+1}/{total_properties}")

                try:
                    # Step 1: Geocoding
                    coords = self._get_coordinates(row['complete_address'], row['city_name'])
                    if coords:
                        df.at[idx, 'longitude'], df.at[idx, 'latitude'] = coords
                        geocoding_success += 1
                        logger.debug(f"Geocoding successful for {row['complete_address']}")
                    
                    # Step 2: DPE Data
                    dpe_data = self._get_dpe_data(
                        address=row['complete_address'],
                        city=row['city_name'],
                        coordinates=coords
                    )

                    if dpe_data:
                        # Map DPE fields to DataFrame columns
                        field_mapping = {
                            'classe_consommation_energie': 'dpe_energy_class',
                            'consommation_energie': 'dpe_energy_value',
                            'classe_estimation_ges': 'dpe_ges_class',
                            'estimation_ges': 'dpe_ges_value',
                            'tr002_type_batiment_description': 'building_type',
                            'date_etablissement_dpe': 'dpe_date'
                        }

                        for api_field, df_column in field_mapping.items():
                            df.at[idx, df_column] = dpe_data.get(api_field)
                        
                        dpe_success += 1
                        logger.debug(
                            f"DPE data found for {row['complete_address']}: "
                            f"Energy={dpe_data.get('classe_consommation_energie')}, "
                            f"GES={dpe_data.get('classe_estimation_ges')}"
                        )

                    # Add delay between properties
                    time.sleep(0.5)  # 500ms delay

                except Exception as e:
                    logger.error(f"Error processing property {idx}: {str(e)}")
                    continue

            # Calculate and log success rates
            geocoding_rate = (geocoding_success / total_properties) * 100
            dpe_rate = (dpe_success / total_properties) * 100
            
            logger.info("Enrichment completed:")
            logger.info(f"- Geocoding success: {geocoding_success}/{total_properties} ({geocoding_rate:.1f}%)")
            logger.info(f"- DPE data success: {dpe_success}/{total_properties} ({dpe_rate:.1f}%)")

            # Save enriched data
            success = self.save_csv(df, output_path)
            if success:
                logger.info(f"Successfully saved enriched data to {output_path}")
            
            return success

        except Exception as e:
            logger.error(f"Enrichment failed: {str(e)}")
            logger.debug("Error details:", exc_info=True)
            return False