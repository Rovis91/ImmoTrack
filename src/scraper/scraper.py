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
    Coordinates the end-to-end property scraping process.

    Attributes:
        config (Config): Scraper configuration object.
        urls (List[Tuple[str, int]]): List of URLs to scrape with corresponding element limits.
        state (ScraperState): Current state of the scraper process.
        output_file (Path): Path to the output JSON file.
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
        Initialize the scraper with configuration and search parameters.

        Args:
            config (Config): Scraper configuration object.
            base_url (str): Base URL for property search.
            start_date (str): Start date for scraping in MM/YYYY format.
            end_date (str): End date for scraping in MM/YYYY format.
            search_type (SearchType): Type of property search (e.g., apartments, houses).
            search_params (Optional[SearchParameters]): Additional search parameters.
            output_file (Optional[str]): Path to save the output JSON file.
        """
        self.config = config
        self.state = ScraperState.READY

        # Generate URLs based on parameters
        url_generator = UrlGenerator()
        self.urls = url_generator.generate_urls(
            base_url=base_url,
            start_date=start_date,
            end_date=end_date,
            search_type=search_type,
            params=search_params,
            elements_limit=config.scraping.elements_limit
        )

        # Configure output file
        if output_file:
            self.output_file = Path(output_file)
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            self.output_file = Path(config.scraping.output_dir) / f"scraping_{timestamp}.json"

    async def _init_output_file(self) -> None:
        """
        Initialize the output file with metadata and structure.
        """
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

    def _load_progress(self) -> Tuple[List[Tuple[str, int]], Optional[Dict]]:
        """
        Load scraping progress from the existing output file.

        Returns:
            Tuple[List[Tuple[str, int]], Optional[Dict]]:
                - List of pending URLs with their element limits.
                - Existing progress data as a dictionary, or None if no file exists.
        """
        try:
            with open(self.output_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

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
            logger.info("No existing progress file found, starting fresh.")
            return self.urls, None

    def _save_progress(self, data: Dict) -> None:
        """
        Save the current progress to the output file.

        Args:
            data (Dict): Dictionary containing current scraping progress.
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
        Scrape a single URL with retry logic.

        Args:
            browser (BrowserManager): The browser manager instance.
            url (str): URL to scrape.
            elements_limit (int): Maximum number of elements to scrape.
            retry_count (int): Current retry attempt count.

        Returns:
            Optional[Dict]: Dictionary containing scraped data, or None if retries fail.
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
                logger.info(f"Retrying URL {url} (attempt {retry_count}/{self.config.scraping.max_retries})")
                await asyncio.sleep(self.config.scraping.retry_delay / 1000)
                return await self._process_url(browser, url, elements_limit, retry_count)
            else:
                logger.error(f"All retry attempts failed for URL: {url}")
                return None

    async def run(self) -> Optional[Path]:
        """
        Execute the full scraping process.

        Returns:
            Optional[Path]: Path to the completed output file, or None if the process fails.
        """
        try:
            # Set the scraper state to RUNNING and initialize the output file
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

            # Open the browser and process URLs sequentially
            async with BrowserManager(self.config) as browser:
                for url, limit in pending_urls:
                    result = await self._process_url(browser, url, limit)
                    if result:
                        # Update progress with the scraped result
                        for existing in data['results']:
                            if existing['url'] == url:
                                existing.update(result)
                                break
                        else:
                            data['results'].append(result)

                        # Save progress after processing each URL
                        self._save_progress(data)
                    else:
                        logger.error(f"Failed to process URL: {url}")

            # Mark the scraping as completed
            data['scraping_completed'] = datetime.now().isoformat()
            self._save_progress(data)

            self.state = ScraperState.COMPLETED
            logger.info("Scraping process completed successfully.")
            return self.output_file

        except Exception as e:
            self.state = ScraperState.ERROR
            logger.error(f"Scraping process failed: {str(e)}")
            return None
