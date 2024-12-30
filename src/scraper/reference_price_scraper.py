"""
Specialized scraper for fetching reference prices from MeilleursAgents website.
"""

from typing import Optional, Dict
from datetime import datetime
import logging
from bs4 import BeautifulSoup

from .base_scraper import Scraper, ScraperType
from .browser import BrowserManager
from .config import Config, MeilleursAgentsSelectors

logger = logging.getLogger(__name__)

class ReferencePriceScraper(Scraper):
    """
    A scraper for extracting current market prices from MeilleursAgents.
    Extends the base Scraper class with specific logic for price data.
    """

    def __init__(self) -> None:
        """
        Initialize the ReferencePriceScraper with MeilleursAgents-specific settings.
        """
        super().__init__(
            base_url="https://www.meilleursagents.com/prix-immobilier",
            scraper_type=ScraperType.MANUAL
        )
        self.config.selectors = MeilleursAgentsSelectors()

    def _clean_price_text(self, text: str) -> float:
        """
        Clean and convert raw price text into a float value.

        Args:
            text (str): Raw price text extracted from HTML.

        Returns:
            float: Cleaned and converted price value.
        """
        return float(
            text.strip()
            .replace('€', '')
            .replace(' ', '')
            .strip()
        )

    def _parse_prices(self, html: str) -> Optional[Dict[str, float]]:
        """
        Extract apartment and house prices from HTML content.

        Args:
            html (str): Raw HTML content from the webpage.

        Returns:
            Optional[Dict[str, float]]: A dictionary containing:
                - apartment_price: Average price per m² for apartments.
                - house_price: Average price per m² for houses.
                - timestamp: ISO 8601 timestamp of the parsing operation.
            Returns None if parsing fails or required elements are missing.
        """
        try:
            soup = BeautifulSoup(html, 'lxml')
            
            container = soup.select_one(self.config.selectors.container)
            logger.debug(f"Found price container: {container is not None}")
            
            apartment_element = soup.select_one(self.config.selectors.apartment_price)
            house_element = soup.select_one(self.config.selectors.house_price)
            
            logger.debug(f"Found price elements - Apartment: {apartment_element is not None}, House: {house_element is not None}")

            if apartment_element and house_element:
                apartment_price = self._clean_price_text(apartment_element.text)
                house_price = self._clean_price_text(house_element.text)
                
                logger.debug(f"Parsed prices - Apartment: {apartment_price}€, House: {house_price}€")
                
                return {
                    'apartment_price': apartment_price,
                    'house_price': house_price,
                    'timestamp': datetime.now().isoformat()
                }

            logger.warning("Missing price elements")
            logger.debug(f"HTML preview: {html[:500]}...")
            return None

        except Exception as e:
            logger.error(f"Price parsing failed: {e}")
            logger.debug("Error details:", exc_info=True)
            return None

    async def get_city_prices(self, city: str, zipcode: str) -> Optional[Dict[str, float]]:
        """
        Fetch the current market prices for a given city.

        Args:
            city (str): The name of the city.
            zipcode (str): The postal code of the city.

        Returns:
            Optional[Dict[str, float]]: A dictionary containing:
                - apartment_price: Average price per m² for apartments.
                - house_price: Average price per m² for houses.
                - timestamp: ISO 8601 timestamp of the operation.
            Returns None if the fetch or parsing fails.
        """
        url = f"{self.base_url}/{city.lower()}-{zipcode}/"
        logger.info(f"Fetching prices for {city} ({zipcode})")

        async with BrowserManager(self.config) as browser:
            try:
                html = await browser.get_page_content(url)
                return self._parse_prices(html) if html else None
            except Exception as e:
                logger.error(f"Failed to fetch prices for {city}: {e}")
                return None