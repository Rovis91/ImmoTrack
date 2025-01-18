"""
Orchestrator for the complete data processing pipeline.
Manages the flow between parsing, geocoding, DPE enrichment and pricing.
"""
import json
import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Any, Optional


logger = logging.getLogger(__name__)


class ProcessorBase:
    """Base class for all data processors with common data handling."""
    
    @staticmethod
    def load_json(json_path: str) -> Optional[Dict]:
        """
        Load JSON file and handle nested data structures.
        
        Args:
            json_path: Path to JSON file
            
        Returns:
            Dict containing parsed JSON if successful, None if failed
        """
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load JSON file {json_path}: {e}")
            return None
    
    @staticmethod
    def save_json(data: Dict, output_path: str) -> bool:
        """
        Save data structure to JSON file.
        
        Args:
            data: Data structure to save
            output_path: Output file path
            
        Returns:
            bool: True if successful, False if failed
        """
        try:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Successfully saved JSON to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save JSON file {output_path}: {e}")
            return False
            
    @staticmethod
    def load_csv(csv_path: str) -> Optional[pd.DataFrame]:
        """
        Load CSV file into DataFrame with error handling.
        
        Args:
            csv_path: Path to CSV file
            
        Returns:
            DataFrame if successful, None if failed
        """
        try:
            return pd.read_csv(csv_path)
        except Exception as e:
            logger.error(f"Failed to load CSV file {csv_path}: {e}")
            return None
            
    @staticmethod
    def save_csv(df: pd.DataFrame, output_path: str, index: bool = False) -> bool:
        """
        Save DataFrame to CSV with proper directory handling.
        
        Args:
            df: DataFrame to save
            output_path: Output file path
            index: Whether to include index
            
        Returns:
            bool: True if successful, False if failed
        """
        try:
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=index)
            logger.info(f"Successfully saved CSV to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to save CSV file {output_path}: {e}")
            return False
    
    def process(self, input_path: str, output_path: str, **kwargs) -> bool:
        """
        Abstract method to be implemented by child classes.
        
        Args:
            input_path: Input file path
            output_path: Output file path
            **kwargs: Additional arguments
            
        Returns:
            bool: True if successful, False if failed
        """
        raise NotImplementedError("Process method must be implemented by child class")

class DataProcessor:
    """Orchestrator for the complete data processing pipeline."""
    
    def __init__(self, config: Dict[str, Any]):
        """Initialize data processor with configuration.
        
        Args:
            config: Dictionary containing processing configuration
                Required keys:
                - reference_prices_path: Path to reference prices CSV
                - output_dir: Base directory for outputs
                Optional keys:
                - keep_intermediate: Whether to keep intermediate files
        """
        self.config = config
        self.output_dir = Path(config['output_dir'])
        from .data_parser import DataParser
        from .geocoding_enrichment import GeocodingService
        from .dpe_enrichment import DPEService
        from .price_estimator import PriceEstimator
        # Initialize processors
        self.parser = DataParser()
        self.geocoder = GeocodingService()
        self.dpe_service = DPEService()
        self.estimator = PriceEstimator(config['reference_prices_path'])
        
        # Ensure output directory exists
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _get_file_path(self, stage: str, extension: str = 'csv') -> Path:
        """Generate standardized file paths for each stage."""
        return self.output_dir / f"{stage}.{extension}"

    async def process(self, input_json: str) -> bool:
        """Run complete processing pipeline.
        
        Args:
            input_json: Path to input JSON file from scraper
            
        Returns:
            bool: True if all steps successful, False otherwise
        """
        try:
            # Define stage file paths
            parsed_path = self._get_file_path('parsed')
            geocoded_path = self._get_file_path('geocoded')
            enriched_path = self._get_file_path('enriched')
            final_path = self._get_file_path('final')
            
            logger.info(f"Starting processing pipeline for {input_json}")
            
            # Step 1: Parse raw data
            logger.info("Starting parsing step...")
            if not self.parser.process(input_json, str(parsed_path)):
                logger.error("Parsing step failed")
                return False
            
            # Step 2: Add geocoding data
            logger.info("Starting geocoding step...")
            if not self.geocoder.process(str(parsed_path), str(geocoded_path)):
                logger.error("Geocoding step failed")
                return False
            
            # Step 3: Add DPE data
            logger.info("Starting DPE enrichment step...")
            if not await self.dpe_service.process(str(geocoded_path), str(enriched_path)):
                logger.error("DPE enrichment step failed")
                return False
            
            # Step 4: Add price estimates
            logger.info("Starting price estimation step...")
            if not await self.estimator.process(str(enriched_path), str(final_path)):
                logger.error("Price estimation step failed")
                return False
            
            logger.info("Processing pipeline completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Processing pipeline failed: {e}")
            logger.debug("Error details:", exc_info=True)
            return False