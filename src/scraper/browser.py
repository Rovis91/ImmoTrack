"""
Browser management using Playwright for property scraping.
"""

import asyncio
from typing import Optional, List, Dict, Any
import logging
from playwright.async_api import async_playwright, Browser, Page, Playwright
from .config import Config

logger = logging.getLogger(__name__)

class BrowserManager:
    """
    Manages browser instance and page interactions for scraping.
    
    Attributes:
        config: Configuration object with browser settings
    """
    
    def __init__(self, config: Config):
        """
        Initialize browser manager.
        
        Args:
            config: Configuration object containing browser settings
        """
        self.config = config
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._page: Optional[Page] = None
        self._context = None
        
    async def _initialize_playwright(self) -> None:
        """Initialize Playwright instance if not already initialized."""
        if not self._playwright:
            logger.debug("Initializing Playwright")
            self._playwright = await async_playwright().start()
            
    async def connect(self) -> None:
        """
        Initialize browser session with configured settings.
        
        Raises:
            RuntimeError: If Playwright initialization fails
        """
        try:
            await self._initialize_playwright()
            
            if not self._playwright:
                raise RuntimeError("Failed to initialize Playwright")
            
            logger.info("Launching browser...")
            
            # Launch browser with configured settings
            self._browser = await self._playwright.chromium.launch(
                headless=self.config.browser.headless
            )
            
            # Create context with custom settings
            self._context = await self._browser.new_context(
                user_agent=self.config.browser.user_agent,
                viewport={
                    'width': self.config.browser.viewport_width,
                    'height': self.config.browser.viewport_height
                }
            )
            
            # Create new page with configured timeouts
            self._page = await self._context.new_page()
            self._page.set_default_timeout(self.config.browser.default_timeout)
            self._page.set_default_navigation_timeout(self.config.browser.navigation_timeout)
            
            logger.info("Browser launched successfully")
            
        except Exception as e:
            logger.error(f"Failed to launch browser: {str(e)}")
            await self.close()
            raise
            
    async def close(self) -> None:
        """Clean up browser resources."""
        try:
            if self._page:
                await self._page.close()
                self._page = None
                
            if self._context:
                await self._context.close()
                self._context = None
                
            if self._browser:
                await self._browser.close()
                self._browser = None
                
            if self._playwright:
                await self._playwright.stop()
                self._playwright = None
                
            logger.info("Browser resources cleaned up")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            
    async def _extract_property_data(self, element) -> Optional[Dict[str, Any]]:
        """
        Extract structured data from a property element.
        
        Args:
            element: Playwright element representing a property
            
        Returns:
            Dictionary with extracted property data or None if extraction fails
        """
        try:
            selectors = self.config.selectors
            data = {}
            
            # Extract address and city
            address_element = await element.query_selector(selectors.address)
            if address_element:
                address_text = await address_element.text_content()
                if ' - ' in address_text:
                    address, city = address_text.rsplit(' - ', 1)
                    data['complete_address'] = address.strip()
                    data['city_name'] = city.strip()
                else:
                    data['complete_address'] = address_text.strip()
                    data['city_name'] = ""
            
            # Extract price
            price_element = await element.query_selector(selectors.price)
            if price_element:
                price_text = await price_element.text_content()
                price = ''.join(c for c in price_text if c.isdigit())
                data['price'] = int(price) if price else None
            
            # Extract details (rooms, surface)
            details_element = await element.query_selector(selectors.details)
            if details_element:
                details_text = await details_element.text_content()
                # Add details parsing logic here
            
            # Store original HTML
            data['html'] = await element.inner_html()
            
            return data
            
        except Exception as e:
            logger.error(f"Error extracting property data: {str(e)}")
            return None
            
    async def get_properties(self, url: str, retry_count: int = 0) -> List[Dict[str, Any]]:
        """
        Fetch and parse properties from a given URL.
        
        Args:
            url: URL to scrape
            retry_count: Current retry attempt number
            
        Returns:
            List of dictionaries containing property data
            
        Raises:
            RuntimeError: If browser is not initialized
        """
        if not self._page:
            raise RuntimeError("Browser not initialized. Call connect() first.")
            
        properties = []
        
        try:
            # Navigate to page
            logger.info(f"Navigating to {url}")
            await self._page.goto(url, wait_until='networkidle')
            await asyncio.sleep(5)  # Let dynamic content load
            
            # Wait for property list
            property_list = await self._page.wait_for_selector(
                self.config.selectors.property_list,
                timeout=self.config.browser.default_timeout
            )
            
            if not property_list:
                logger.warning("Property list selector not found")
                return properties
            
            # Get all property elements
            property_elements = await self._page.query_selector_all(
                self.config.selectors.property_item
            )
            
            # Extract data from each property
            for element in property_elements:
                data = await self._extract_property_data(element)
                if data:
                    properties.append(data)
            
            # Log results
            properties_count = len(properties)
            if properties_count == self.config.scraping.elements_limit:
                logger.info(f"Found maximum number of properties ({properties_count}) for URL: {url}")
            elif properties_count > 0:
                logger.info(f"Found {properties_count} properties for URL: {url}")
            else:
                logger.warning(f"No properties found for URL: {url}")
                
            return properties
            
        except Exception as e:
            logger.error(f"Error fetching properties from {url}: {str(e)}")
            
            # Retry logic
            if retry_count < self.config.scraping.max_retries:
                logger.info(f"Retrying ({retry_count + 1}/{self.config.scraping.max_retries})")
                await asyncio.sleep(self.config.scraping.retry_delay / 1000)  # Convert to seconds
                return await self.get_properties(url, retry_count + 1)
                
            raise
            
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()