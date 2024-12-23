# src/scraper/config.py
import os
from typing import Dict
from dotenv import load_dotenv
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

class ScraperConfig:
    """Configuration for the scraper module."""
    
    # Browser configuration
    BROWSER_ENDPOINT = os.getenv('BROWSER_ENDPOINT', 'wss://brd-customer-hl_bc9ff225-zone-scraping_browser1:djwsvbhv3d0h@brd.superproxy.io:9222')
    
    # Timeouts (in milliseconds)
    DEFAULT_TIMEOUT = 30000  # 30 seconds
    NAVIGATION_TIMEOUT = 60000  # 60 seconds
    
    # Retry configuration
    MAX_RETRIES = 3
    RETRY_DELAY = 5000  # 5 seconds
    
    # Selectors
    SELECTORS = {
        'property_list': 'div.hidden.md\\:block.overflow-y-auto.flex-grow.children-hover\\:bg-gray-50.children-hover\\:shadow-lg.transition.duration-300',
        'property_item': 'div.border-b.border-b-gray-100 > div.text-sm.relative.font-sans'
    }
    
    # Output configuration
    OUTPUT_DIR = 'scraped_data'
    
    @classmethod
    def get_output_filename(cls, batch_id: str) -> str:
        """Generate unique output filename for scraped data."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{cls.OUTPUT_DIR}/scraping_results_{batch_id}_{timestamp}.json"
    
    @classmethod
    def ensure_output_dir(cls) -> None:
        """Ensure output directory exists."""
        os.makedirs(cls.OUTPUT_DIR, exist_ok=True)
    
    @classmethod
    def to_dict(cls) -> Dict:
        """Convert config to dictionary for logging."""
        return {
            'browser_endpoint': cls.BROWSER_ENDPOINT,
            'default_timeout': cls.DEFAULT_TIMEOUT,
            'navigation_timeout': cls.NAVIGATION_TIMEOUT,
            'max_retries': cls.MAX_RETRIES,
            'retry_delay': cls.RETRY_DELAY,
            'output_dir': cls.OUTPUT_DIR
        }