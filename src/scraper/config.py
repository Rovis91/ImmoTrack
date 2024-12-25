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
    """Browser-specific configuration settings."""
    headless: bool = True
    default_timeout: int = 30000  # 30 seconds
    navigation_timeout: int = 60000  # 60 seconds
    viewport_width: int = 1920
    viewport_height: int = 1080
    user_agent: str = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

@dataclass
class ScrapingConfig:
    """Scraping-specific configuration settings."""
    max_retries: int = 3
    retry_delay: int = 5000  # 5 seconds
    elements_limit: int = 100
    output_dir: str = 'data/raw'

@dataclass
class WebsiteSelectors:
    """CSS selectors for property elements."""
    property_list: str = 'div.hidden.md\\:block.overflow-y-auto.flex-grow.children-hover\\:bg-gray-50'
    property_item: str = 'div.border-b.border-b-gray-100 > div.text-sm.relative.font-sans'
    address: str = 'p.text-gray-700.font-bold.truncate'
    price: str = 'p.text-primary-500.font-bold.whitespace-nowrap'
    details: str = 'div.flex.gap-4.text-gray-600'

@dataclass
class MeilleursAgentsSelectors:
    """CSS selectors for MeilleursAgents price elements."""
    container: str = 'div.prices-summary__prices--container'
    apartment_price: str = 'div.prices-summary__apartment-prices .prices-summary__price-range .big-number'
    house_price: str = 'div.prices-summary__house-prices .prices-summary__price-range .big-number'

class Config:
    """
    Main configuration class that loads and manages all scraper settings.
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
            
        # Initialize with default values
        self.browser = BrowserConfig()
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
                
            # Update browser config
            if 'browser' in config_data:
                for key, value in config_data['browser'].items():
                    if hasattr(self.browser, key):
                        setattr(self.browser, key, value)
                        
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
        """Create output directory if it doesn't exist."""
        Path(self.scraping.output_dir).mkdir(parents=True, exist_ok=True)
        
    def generate_output_path(self, prefix: str = "scraped_data") -> Path:
        """
        Generate timestamped output file path.
        
        Args:
            prefix: Prefix for the output filename
            
        Returns:
            Path object for the output file
        """
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return Path(self.scraping.output_dir) / f"{prefix}_{timestamp}.json"
    
    def to_dict(self) -> Dict:
        """Convert configuration to dictionary for logging."""
        from dataclasses import asdict
        return {
            'browser': asdict(self.browser),
            'scraping': asdict(self.scraping),
            'selectors': asdict(self.selectors)
        }