"""
Address enrichment module for adding geocoding and DPE data to properties.
Production-ready implementation with error handling and logging.
"""
import logging
from pathlib import Path
import time
from typing import Dict, Optional
import pandas as pd
import requests
import re

logger = logging.getLogger(__name__)

class APIHandler:
    """Handles API requests with rate limiting."""
    
    def __init__(self, min_delay: float = 0.1):
        self.last_request_time = 0
        self.min_delay = min_delay
        
    def make_request(self, url: str, params: Dict) -> Optional[Dict]:
        """Make API request with rate limiting."""
        current_time = time.time()
        if (elapsed := current_time - self.last_request_time) < self.min_delay:
            time.sleep(self.min_delay - elapsed)
            
        try:
            response = requests.get(
                url,
                params=params,
                headers={'Accept': 'application/json'},
                timeout=10  # Reduced timeout for production
            )
            self.last_request_time = time.time()
            
            if response.status_code == 200:
                return response.json()
            return None
            
        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            return None

def validate_address_match(input_address: str, dpe_address: str) -> bool:
    """Validate if DPE address matches input address."""
    def clean_address(addr: str) -> str:
        addr = addr.lower()
        for word in ['rue', 'avenue', 'boulevard', 'place', 'chemin', 'impasse']:
            addr = addr.replace(word, '')
        addr = re.sub(r'[^\w\s]', '', addr)
        return ' '.join(addr.split())

    # Extract house number and street name
    number_pattern = re.compile(r'^\s*(\d+[a-zA-Z]?)\s+(.+)$')
    
    input_match = number_pattern.match(clean_address(input_address))
    dpe_match = number_pattern.match(clean_address(dpe_address))
    
    if not (input_match and dpe_match):
        return False
        
    input_number, input_street = input_match.groups()
    dpe_number, dpe_street = dpe_match.groups()
    
    return input_number == dpe_number and (input_street in dpe_street or dpe_street in input_street)

class AddressEnrichment:
    """Enriches property data with geocoding and DPE information."""
    
    # Active DPE endpoints (removed failing endpoint)
    DPE_ENDPOINTS = [
        "dpe-france/lines",
        "dpe-v2-logements-existants/lines",
        "dpe-v2-logements-neufs/lines", 
        "audit-opendata/lines",
        "dpe-v2-tertiaire-2/lines"
    ]
    
    def __init__(self):
        self.api = APIHandler()
        self.geocoding_url = "https://api-adresse.data.gouv.fr/search/"
        self.dpe_base_url = "https://data.ademe.fr/data-fair/api/v1/datasets/"

    def _get_geocoding(self, address: str, city: str) -> Optional[Dict]:
        """Get geocoding data for an address."""
        data = self.api.make_request(
            self.geocoding_url,
            {"q": f"{address} {city}", "limit": 1}
        )
        
        if not data or not data.get("features"):
            return None
            
        feature = data["features"][0]
        coords = feature["geometry"]["coordinates"]
        props = feature["properties"]
        
        return {
            'longitude': coords[0],
            'latitude': coords[1],
            'zipcode': props.get("postcode"),
            'insee_code': props.get("citycode"),
            'region': props.get("context")
        }

    def _get_dpe_data(self, formatted_address: str) -> Optional[Dict]:
        """Query multiple DPE APIs and validate address matches."""
        params = {"q": formatted_address}
        
        for endpoint in self.DPE_ENDPOINTS:
            url = f"{self.dpe_base_url}{endpoint}"
            
            if data := self.api.make_request(url, params):
                if results := data.get("results", []):
                    for dpe_data in results:
                        if dpe_address := dpe_data.get('geo_adresse'):
                            if validate_address_match(formatted_address, dpe_address):
                                logger.info(f"Found matching DPE: {dpe_address}")
                                return dpe_data
                                
        return None

    def process(self, input_path: str, output_path: str) -> bool:
        """Process property data file with address enrichment."""
        try:
            logger.info(f"Starting processing of {input_path}")
            df = pd.read_csv(input_path)
            
            if not all(col in df.columns for col in ['complete_address', 'city_name']):
                logger.error("Missing required columns")
                return False

            logger.info(f"Processing {len(df)} properties")
            geocoding_success = 0
            dpe_success = 0

            for idx, row in df.iterrows():
                try:
                    if idx % 100 == 0:
                        logger.info(f"Progress: {idx}/{len(df)}")

                    if geo_data := self._get_geocoding(row['complete_address'], row['city_name']):
                        geocoding_success += 1
                        for key, value in geo_data.items():
                            df.at[idx, key] = value
                        
                        formatted_address = f"{row['complete_address']} {geo_data['zipcode']} {row['city_name']}"
                        
                        if dpe_data := self._get_dpe_data(formatted_address):
                            dpe_success += 1
                            for key, value in dpe_data.items():
                                df.at[idx, f'dpe_{key}'] = value
                                
                except Exception as e:
                    logger.error(f"Error processing row {idx}: {str(e)}")
                    continue

            success_rate = {
                'total_properties': len(df),
                'geocoding_success': geocoding_success,
                'geocoding_rate': f"{(geocoding_success/len(df))*100:.1f}%",
                'dpe_success': dpe_success,
                'dpe_rate': f"{(dpe_success/len(df))*100:.1f}%"
            }
            
            logger.info(f"Processing results: {success_rate}")

            df.to_csv(output_path, index=False)
            logger.info(f"Results saved to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            return False