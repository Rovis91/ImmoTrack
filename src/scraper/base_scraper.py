"""
Base scraper that can use either Browse AI or manual scraping strategy.
"""

from pathlib import Path
from datetime import datetime
from typing import Optional, List
from enum import Enum
import logging
import json
from .config import Config
from .url_generator import SearchType, UrlGenerator
from .browser import BrowserManager

logger = logging.getLogger(__name__)

class ScraperType(Enum):
    """Defines available scraping strategies."""
    MANUAL = "manual"
    BROWSE_AI = "browse_ai"

class ScraperState(str, Enum):
    """Represents the current state of the scraper."""
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    ERROR = "error"


class Scraper:
    def __init__(
        self,
        base_url: str,
        scraper_type: ScraperType = ScraperType.MANUAL,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        search_type: Optional[SearchType] = None,
        output_file: Optional[str] = None
    ):
        """
        Initialize the scraper with base configuration and parameters.

        Args:
            base_url (str): The base URL for the scraper.
            scraper_type (ScraperType): The scraping strategy to use.
            start_date (Optional[str]): Start date for scraping in MM/YYYY format.
            end_date (Optional[str]): End date for scraping in MM/YYYY format.
            search_type (Optional[SearchType]): Type of properties to search.
            output_file (Optional[str]): Path to save scraping output.
        """
        self.base_url = base_url
        self.scraper_type = scraper_type
        self.config = Config()

        current_date = datetime.now()
        self.start_date = start_date or "01/2014"
        self.end_date = end_date or current_date.strftime("%m/%Y")
        self.search_type = search_type or SearchType.ALL_TYPES

        if output_file:
            self.output_file = Path(output_file)
        else:
            timestamp = current_date.strftime("%Y%m%d_%H%M%S")
            self.output_file = Path(self.config.scraping.output_dir) / f"scraping_{timestamp}.json"

    async def _init_output_file(self, urls: list) -> None:
        """
        Initialize the output file with metadata.

        Args:
            urls (list): List of URLs to scrape.
        """
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

        initial_data = {
            'scraping_started': datetime.now().isoformat(),
            'scraping_completed': None,
            'results': [{'url': url, 'timestamp': None, 'properties': []} for url in urls]
        }

        with open(self.output_file, 'w', encoding='utf-8') as f:
            json.dump(initial_data, f, indent=2)

    async def run(self) -> Optional[Path]:
        """
        Execute the scraping process based on the configured strategy.

        Returns:
            Optional[Path]: Path to the output file if successful, None otherwise.
        """
        try:
            # Generate URLs for scraping
            url_generator = UrlGenerator()
            urls = url_generator.generate_urls(
                self.base_url,
                self.start_date,
                self.end_date,
                self.search_type
            )

            if self.scraper_type == ScraperType.MANUAL:
                results = []

                # Ensure the output directory exists
                self.output_file.parent.mkdir(parents=True, exist_ok=True)

                async with BrowserManager(self.config) as browser:
                    for url, limit in urls:
                        try:
                            # Fetch HTML content for properties
                            properties_html = await browser.get_properties(url)

                            # Append scraping results
                            results.append({
                                'url': url,
                                'timestamp': int(datetime.now().timestamp()),
                                'count': len(properties_html),
                                'properties': [{'html': html} for html in properties_html]
                            })
                        except Exception as e:
                            logger.error(f"Error processing URL {url}: {str(e)}")
                            continue

                    # Save results to the output file
                    output_data = {
                        'scraping_completed': int(datetime.now().timestamp()),
                        'results': results
                    }

                    with open(self.output_file, 'w', encoding='utf-8') as f:
                        json.dump(output_data, f, indent=2)

                    return self.output_file

            else:
                logger.error("Browse AI scraping is not yet implemented")
                return None

        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}")
            return None

    def _generate_output_path(self, prefix: str = "scraped_data") -> Path:
        """
        Generate a timestamped path for the output file.

        Args:
            prefix (str): Prefix for the output filename.

        Returns:
            Path: Generated path for the output file.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.output_dir / f"{prefix}_{timestamp}.json"

    def _update_state(self, new_state: ScraperState) -> None:
        """
        Update the scraper's state and log the change.

        Args:
            new_state (ScraperState): The new state to set.
        """
        old_state = self.state
        self.state = new_state
        self.logger.info(f"State changed: {old_state.value} -> {new_state.value}")

    async def _browse_ai_scrape(self, urls: List[str]) -> Optional[Path]:
        """
        Perform scraping using the Browse AI strategy.

        Args:
            urls (List[str]): List of URLs to scrape.

        Returns:
            Optional[Path]: Path to the output file if successful, None otherwise.
        """
        try:
            if not self.browse_ai_config:
                raise ValueError("Browse AI configuration is required")

            from .browse_ai_scraper import BrowseAIClient

            client = BrowseAIClient(
                api_key=self.browse_ai_config.get('api_key'),
                robot_id=self.browse_ai_config.get('robot_id')
            )

            bulk_run_id = await client.create_bulk_run(urls)
            if not bulk_run_id:
                raise RuntimeError("Failed to create bulk run")

            results = await client.wait_for_bulk_run(bulk_run_id)
            if not results:
                raise RuntimeError("Failed to retrieve bulk run results")

            output_path = self._generate_output_path("browse_ai_data")
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2)

            return output_path

        except Exception as e:
            self.logger.error(f"Browse AI scraping failed: {str(e)}")
            return None

    async def _manual_scrape(self, urls: List[str]) -> Optional[Path]:
        """
        Perform scraping using the manual browser strategy.

        Args:
            urls (List[str]): List of URLs to scrape.

        Returns:
            Optional[Path]: Path to the output file if successful, None otherwise.
        """
        try:
            if not self.browser_config:
                raise ValueError("Browser configuration is required")

            from .browser import BrowserManager

            properties = []
            async with BrowserManager(self.browser_config) as browser:
                for url in urls:
                    page_properties = await browser.get_properties(url)
                    if page_properties:
                        properties.extend(page_properties)

            if not properties:
                raise ValueError("No properties found")

            output_path = self._generate_output_path("manual_data")
            results = {
                "scraping_date": datetime.now().isoformat(),
                "total_properties": len(properties),
                "properties": properties
            }

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2)

            return output_path

        except Exception as e:
            self.logger.error(f"Manual scraping failed: {str(e)}")
            return None

    async def scrape(self, urls: List[str]) -> Optional[Path]:
        """
        Execute the scraping process with the selected strategy.

        Args:
            urls (List[str]): List of URLs to scrape.

        Returns:
            Optional[Path]: Path to the output file if successful, None otherwise.
        """
        self._update_state(ScraperState.RUNNING)

        try:
            if self.scraper_type == ScraperType.BROWSE_AI:
                result = await self._browse_ai_scrape(urls)
            else:
                result = await self._manual_scrape(urls)

            if result:
                self._update_state(ScraperState.COMPLETED)
            else:
                self._update_state(ScraperState.ERROR)

            return result

        except Exception as e:
            self.logger.error(f"Scraping failed: {str(e)}")
            self._update_state(ScraperState.ERROR)
            return None
