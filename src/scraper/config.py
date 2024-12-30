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
class BrowserConfig:
    """Configuration settings specific to browser behavior."""
    headless: bool = True
    default_timeout: int = 30000  # Timeout for browser actions in milliseconds
    navigation_timeout: int = 60000  # Timeout for navigation actions in milliseconds
    viewport_width: int = 1920  # Width of the browser viewport
    viewport_height: int = 1080  # Height of the browser viewport
    user_agent: str = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

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

    def __init__(self, config_path: Optional[str] = None, env_file: Optional[str] = None):
        """
        Initialize the configuration class.

        Args:
            config_path (Optional[str]): Path to the JSON configuration file.
            env_file (Optional[str]): Path to a .env file containing environment variables.
        """
        # Load environment variables from .env file if provided
        if env_file:
            from dotenv import load_dotenv
            load_dotenv(env_file)

        # Default configurations
        self.browser = BrowserConfig()
        self.scraping = ScrapingConfig()
        self.selectors = WebsiteSelectors()

        # Override defaults with JSON file if provided
        if config_path:
            self._load_config(config_path)

        # Ensure the output directory exists
        self._ensure_output_dir()

    def _load_config(self, config_path: str) -> None:
        """
        Load configuration values from a JSON file.

        Args:
            config_path (str): Path to the JSON configuration file.
        """
        try:
            with open(config_path, 'r') as f:
                config_data = json.load(f)

            # Update browser configurations
            if 'browser' in config_data:
                for key, value in config_data['browser'].items():
                    if hasattr(self.browser, key):
                        setattr(self.browser, key, value)

            # Update scraping configurations
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
            logger.error(f"Error loading configuration from {config_path}: {str(e)}")
            logger.info("Using default configuration.")

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
