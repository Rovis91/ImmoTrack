"""
Main scraper module coordinating the property scraping process.
"""

import json
import asyncio
import logging
from typing import List, Dict, Optional, Tuple
from datetime import datetime
from pathlib import Path
from .browser import BrowserManager
from .config import Config, ScraperState
from .url_generator import UrlGenerator, SearchType, SearchParameters

logger = logging.getLogger(__name__)

class PropertyScraper:
    """
    Main scraper class handling the end-to-end scraping process.
    
    Attributes:
        config (Config): Scraping configuration
        urls (List[Tuple[str, int]]): List of URLs to scrape with their element limits
        state (ScraperState): Current state of the scraper
        output_file (Path): Path to output file
    """
    
    def __init__(
        self,
        config: Config,
        base_url: str,
        start_date: str,
        end_date: str,
        search_type: SearchType,
        search_params: Optional[SearchParameters] = None,
        output_file: Optional[str] = None
    ):
        """
        Initialize scraper with configuration and search parameters.
        
        Args:
            config: Configuration object
            base_url: Base URL for property search
            start_date: Start date in MM/YYYY format
            end_date: End date in MM/YYYY format
            search_type: Type of property search
            search_params: Optional search parameters
            output_file: Optional output file path
        """
        self.config = config
        self.state = ScraperState.READY
        
        # Generate URLs
        url_generator = UrlGenerator()
        self.urls = url_generator.generate_urls(
            base_url=base_url,
            start_date=start_date,
            end_date=end_date,
            search_type=search_type,
            params=search_params,
            elements_limit=config.scraping.elements_limit
        )
        
        # Setup output file
        if output_file:
            self.output_file = Path(output_file)
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.output_file = Path(config.scraping.output_dir) / f"scraping_{timestamp}.json"
            
    async def _init_output_file(self) -> None:
        """Initialize output file with scraping metadata and URL structure."""
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        initial_data = {
            'scraping_started': None,
            'scraping_completed': None,
            'config': self.config.to_dict(),
            'results': [
                {
                    'url': url,
                    'elements_limit': limit,
                    'timestamp': None,
                    'retry_count': 0,
                    'properties_count': 0,
                    'properties': []
                }
                for url, limit in self.urls
            ]
        }
        
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(initial_data, f, indent=2)
            
        logger.info(f"Initialized output file: {self.output_file}")
        
    def _load_progress(self) -> Tuple[List[Tuple[str, int]], Dict]:
        """
        Load progress from existing file.
        
        Returns:
            Tuple containing:
              - List of pending URLs with their limits
              - Existing data dictionary
        """
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            # Get URLs that haven't been processed or failed
            pending_urls = [
                (result['url'], result.get('elements_limit', 100))
                for result in data['results']
                if result['timestamp'] is None or (
                    result['retry_count'] < self.config.scraping.max_retries and
                    result['properties_count'] == 0
                )
            ]
            
            return pending_urls, data
            
        except FileNotFoundError:
            logger.info("No existing progress file found, starting fresh")
            return self.urls, None
            
    def _save_progress(self, data: Dict) -> None:
        """
        Save progress to file.
        
        Args:
            data: Current scraping data
        """
        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
            
    async def _process_url(
        self,
        browser: BrowserManager,
        url: str,
        elements_limit: int,
        retry_count: int = 0
    ) -> Optional[Dict]:
        """
        Process a single URL with retry logic.
        
        Args:
            browser: Browser manager instance
            url: URL to process
            elements_limit: Maximum number of elements to scrape
            retry_count: Current retry attempt
            
        Returns:
            Dictionary with scraped data or None if all retries failed
        """
        try:
            properties = await browser.get_properties(url)
            
            return {
                'url': url,
                'elements_limit': elements_limit,
                'timestamp': datetime.now().isoformat(),
                'retry_count': retry_count,
                'properties_count': len(properties),
                'properties': properties
            }
            
        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")
            
            if retry_count < self.config.scraping.max_retries:
                retry_count += 1
                logger.info(
                    f"Retrying URL {url} (attempt {retry_count}/{self.config.scraping.max_retries})"
                )
                await asyncio.sleep(self.config.scraping.retry_delay / 1000)
                return await self._process_url(browser, url, elements_limit, retry_count)
            else:
                logger.error(f"All retry attempts failed for URL: {url}")
                return None
                
    async def run(self) -> Optional[Path]:
        """
        Run the scraping process.
        
        Returns:
            Path to output file if successful, None otherwise
        """
        try:
            # Initialize state and output
            self.state = ScraperState.RUNNING
            if not self.output_file.exists():
                await self._init_output_file()
                
            pending_urls, data = self._load_progress()
            
            if not data:
                data = {
                    'scraping_started': datetime.now().isoformat(),
                    'scraping_completed': None,
                    'config': self.config.to_dict(),
                    'results': []
                }
                
            logger.info(f"Starting scraping process for {len(pending_urls)} URLs")
            
            # Initialize browser
            async with BrowserManager(self.config) as browser:
                # Process URLs sequentially
                for url, limit in pending_urls:
                    result = await self._process_url(browser, url, limit)
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
            
            self.state = ScraperState.COMPLETED
            logger.info("Scraping process completed successfully")
            return self.output_file
            
        except Exception as e:
            self.state = ScraperState.ERROR
            logger.error(f"Scraping process failed: {str(e)}")
            return None