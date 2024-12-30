"""
Configuration management for the scraper module.
Handles both environment variables and JSON configuration files.
"""

import os
import json
from typing import Dict, Optional
from dataclasses import dataclass
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


@dataclass
class ScrapingConfig:
    """Configuration settings specific to the scraping process."""
    max_retries: int = 3  # Maximum number of retry attempts for failed tasks
    retry_delay: int = 5000  # Delay between retries in milliseconds
    elements_limit: int = 100  # Maximum number of elements to scrape per page
    output_dir: str = 'data/raw'  # Directory to save scraping output

@dataclass
class WebsiteSelectors:
    """CSS selectors for extracting property data from the website."""
    property_list: str = 'div.hidden.md\\:block.overflow-y-auto.flex-grow.children-hover\\:bg-gray-50'
    property_item: str = 'div.border-b.border-b-gray-100 > div.text-sm.relative.font-sans'
    address: str = 'p.text-gray-700.font-bold.truncate'
    price: str = 'p.text-primary-500.font-bold.whitespace-nowrap'
    details: str = 'div.flex.gap-4.text-gray-600'

@dataclass
class MeilleursAgentsSelectors:
    """CSS selectors specific to MeilleursAgents price data."""
    container: str = 'div.prices-summary__prices--container'
    apartment_price: str = 'div.prices-summary__apartment-prices .prices-summary__price-range .big-number'
    house_price: str = 'div.prices-summary__house-prices .prices-summary__price-range .big-number'

class Config:
    """
    Manages configuration for the scraper, combining environment variables and JSON files.
    """

    def __init__(
        self,
        config_path: Optional[str] = None,
        env_file: Optional[str] = None
    ):
        """
        Initialize configuration from files and environment.
        
        Args:
            config_path: Path to JSON configuration file
            env_file: Path to .env file
        """
        # Load environment variables if .env file provided
        if env_file:
            from dotenv import load_dotenv
            load_dotenv(env_file)
            
        # Initialize base configuration
        self.proxy = False  # Default proxy setting
        self.scraping = ScrapingConfig()
        self.selectors = WebsiteSelectors()
        
        # Load from JSON if provided
        if config_path:
            self._load_config(config_path)
            
        # Ensure output directory exists
        self._ensure_output_dir()

    def _load_config(self, config_path: str) -> None:
        """
        Load configuration from JSON file.
        
        Args:
            config_path: Path to configuration file
        """
        try:
            with open(config_path, 'r') as f:
                config_data = json.load(f)
                
            # Load base settings
            self.proxy = config_data.get('proxy', False)
                
            # Update scraping config
            if 'scraping' in config_data:
                for key, value in config_data['scraping'].items():
                    if hasattr(self.scraping, key):
                        setattr(self.scraping, key, value)
                        
            # Update selectors
            if 'selectors' in config_data:
                for key, value in config_data['selectors'].items():
                    if hasattr(self.selectors, key):
                        setattr(self.selectors, key, value)
                        
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            logger.info("Using default configuration")

    def _ensure_output_dir(self) -> None:
        """
        Ensure the output directory exists, creating it if necessary.
        """
        Path(self.scraping.output_dir).mkdir(parents=True, exist_ok=True)

    def generate_output_path(self, prefix: str = "scraped_data") -> Path:
        """
        Generate a unique output file path with a timestamp.

        Args:
            prefix (str): Prefix for the output filename.

        Returns:
            Path: The generated file path.
        """
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return Path(self.scraping.output_dir) / f"{prefix}_{timestamp}.json"

    def to_dict(self) -> Dict:
        """
        Convert the current configuration to a dictionary.

        Returns:
            Dict: A dictionary representation of the configuration.
        """
        from dataclasses import asdict
        return {
            'browser': asdict(self.browser),
            'scraping': asdict(self.scraping),
            'selectors': asdict(self.selectors)
        }
