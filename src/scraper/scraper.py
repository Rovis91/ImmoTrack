import json
import asyncio
import logging
from typing import List, Dict, Optional
from datetime import datetime
from pathlib import Path
from .browser import BrowserManager
from .config import ScraperConfig

logger = logging.getLogger(__name__)

class PropertyScraper:
    """Main scraper class handling the scraping process."""
    
    def __init__(self, urls: List[str], output_file: Optional[str] = None):
        """
        Initialize scraper with list of URLs.
        
        Args:
            urls: List of URLs to scrape
            output_file: Optional output file path. If not provided, will generate one.
        """
        self.urls = urls
        self.output_file = output_file or ScraperConfig.get_output_filename(
            batch_id=datetime.now().strftime("%Y%m%d")
        )
        
    async def _init_output_file(self) -> None:
        """Initialize output file with URL structure."""
        ScraperConfig.ensure_output_dir()
        
        initial_data = {
            'scraping_started': None,
            'scraping_completed': None,
            'results': [
                {
                    'url': url,
                    'timestamp': None,
                    'properties_count': 0,
                    'properties': []
                }
                for url in self.urls
            ]
        }
        
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(initial_data, f, indent=2)
            
        logger.info(f"Initialized output file: {self.output_file}")
        
    def _load_progress(self) -> tuple[List[str], Dict]:
        """Load progress from existing file."""
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Get URLs that haven't been processed yet
            pending_urls = [
                result['url'] for result in data['results']
                if result['timestamp'] is None
            ]
            
            return pending_urls, data
            
        except FileNotFoundError:
            logger.info("No existing progress file found, starting fresh")
            return self.urls, None
            
    def _save_progress(self, data: Dict) -> None:
        """Save progress to file."""
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
            
    async def _process_url(self, url: str, retry_count: int = 0) -> Optional[Dict]:
        """
        Process a single URL with retry logic.
        
        Args:
            url: URL to process
            retry_count: Current retry attempt
            
        Returns:
            Dictionary with scraped data or None if all retries failed
        """
        try:
            async with BrowserManager() as browser:
                properties = await browser.get_properties(url)
                
                return {
                    'url': url,
                    'timestamp': datetime.now().isoformat(),
                    'properties_count': len(properties),
                    'properties': properties
                }
                
        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")
            
            if retry_count < ScraperConfig.MAX_RETRIES:
                logger.info(f"Retrying URL {url} (attempt {retry_count + 1}/{ScraperConfig.MAX_RETRIES})")
                await asyncio.sleep(ScraperConfig.RETRY_DELAY / 1000)  # Convert to seconds
                return await self._process_url(url, retry_count + 1)
            else:
                logger.error(f"All retry attempts failed for URL: {url}")
                return None
                
    async def run(self) -> None:
        """Run the scraping process."""
        # Initialize or load progress
        if not Path(self.output_file).exists():
            await self._init_output_file()
            
        pending_urls, data = self._load_progress()
        
        if not data:
            data = {
                'scraping_started': datetime.now().isoformat(),
                'scraping_completed': None,
                'results': []
            }
            
        logger.info(f"Starting scraping process for {len(pending_urls)} URLs")
        
        # Process URLs sequentially
        for url in pending_urls:
            result = await self._process_url(url)
            if result:
                # Update or add result
                url_exists = False
                for existing in data['results']:
                    if existing['url'] == url:
                        existing.update(result)
                        url_exists = True
                        break
                        
                if not url_exists:
                    data['results'].append(result)
                    
                # Save progress after each successful URL
                self._save_progress(data)
                
            else:
                logger.error(f"Failed to process URL: {url}")
                
        # Mark scraping as completed
        data['scraping_completed'] = datetime.now().isoformat()
        self._save_progress(data)
        
        logger.info("Scraping process completed")