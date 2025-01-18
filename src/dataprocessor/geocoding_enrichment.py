"""
Geocoding service for enriching property addresses with geographic data.
Uses the French government API bulk CSV geocoding endpoint.
"""

import logging
import os
import io
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
import requests

from .processor_base import ProcessorBase

logger = logging.getLogger(__name__)

@dataclass
class GeocodingResult:
    """Container for geocoding API results."""
    longitude: float
    latitude: float
    zipcode: str
    insee_code: str
    region: str

class GeocodingService(ProcessorBase):
    """Service for geocoding addresses using French government API."""
    
    API_URL = "https://api-adresse.data.gouv.fr/search/csv/"
    REQUIRED_COLUMNS = {'complete_address', 'city_name'}
    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB in bytes
    
    # Column mapping from API response to our format
    COLUMN_MAPPING = {
        'latitude': 'latitude',
        'longitude': 'longitude',
        'result_postcode': 'zipcode',
        'result_citycode': 'insee_code',
        'result_context': 'region'
    }
    
    def __init__(self) -> None:
        """Initialize geocoding service."""
        pass
    
    def process(self, input_path: str, output_path: str, **kwargs) -> bool:
        """Process CSV file using bulk geocoding API."""
        try:
            logger.info(f"Starting bulk geocoding process for {input_path}")
            
            # Check file size
            file_size = os.path.getsize(input_path)
            if file_size > self.MAX_FILE_SIZE:
                logger.error(
                    f"Input file size ({file_size} bytes) exceeds maximum "
                    f"allowed size ({self.MAX_FILE_SIZE} bytes)"
                )
                return False
            
            # Read and validate input
            df = self.load_csv(input_path)
            if df is None or not self._validate_input(df):
                return False
            
            # Prepare temporary file for API
            temp_input = Path(input_path).parent / 'temp_geocoding_input.csv'
            df[['complete_address', 'city_name']].to_csv(temp_input, index=False)
            
            try:
                # Make API request
                with open(temp_input, 'rb') as f:
                    response = requests.post(
                        self.API_URL,
                        files={'data': f},
                        data={
                            'columns': ['complete_address', 'city_name']
                        }
                    )
                
                if response.status_code != 200:
                    logger.error(f"API error: {response.status_code}")
                    return False
                
                # Read API response into DataFrame
                api_df = pd.read_csv(io.StringIO(response.content.decode('utf-8')))
                
                # Create enriched DataFrame with original data
                result_df = df.copy()
                
                # Add geocoding columns
                for api_col, our_col in self.COLUMN_MAPPING.items():
                    if api_col in api_df.columns:
                        result_df[our_col] = api_df[api_col]
                    else:
                        logger.warning(f"Missing column in API response: {api_col}")
                        result_df[our_col] = None
                
                # Save enriched data
                success = self.save_csv(result_df, output_path)
                if success:
                    logger.info(
                        f"Geocoding completed successfully. "
                        f"Processed {len(df)} addresses"
                    )
                return success
                
            except Exception as e:
                logger.error(f"API request failed: {str(e)}")
                return False
                
            finally:
                # Cleanup temporary file
                if temp_input.exists():
                    temp_input.unlink()
            
        except Exception as e:
            logger.error(f"Failed to process file: {str(e)}")
            return False
    
    def _validate_input(self, df: pd.DataFrame) -> bool:
        """Validate input DataFrame structure."""
        if df is None:
            return False
            
        # Check required columns
        missing_cols = self.REQUIRED_COLUMNS - set(df.columns)
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False
        
        # Check if DataFrame is empty
        if df.empty:
            logger.error("Input DataFrame is empty")
            return False
        
        return True

def main():
    """Test function to simulate processor pipeline call."""
    import json
    
    # Load config
    with open('config/scraping_config.json', 'r') as f:
        config = json.load(f)
    
    # Setup paths
    input_path = "data/processed/parsed.csv"
    output_path = str(Path(config['dataprocessor_output_dir']) / 'enriched.csv')
    
    # Run service
    service = GeocodingService()
    success = service.process(input_path, output_path)
    
    if not success:
        logger.error("Geocoding process failed")
        
if __name__ == "__main__":
    main()