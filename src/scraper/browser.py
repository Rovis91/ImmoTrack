import asyncio
from typing import Optional, List
import logging
from playwright.async_api import async_playwright, Browser, Page, Playwright
from .config import ScraperConfig

logger = logging.getLogger(__name__)

class BrowserManager:
    """Manages browser instance and page interactions."""
    
    def __init__(self):
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._page: Optional[Page] = None
        self._context = None
        
    async def _initialize_playwright(self) -> None:
        """Initialize Playwright instance."""
        if not self._playwright:
            logger.debug("Initializing Playwright")
            self._playwright = await async_playwright().start()
            
    async def connect(self) -> None:
        """Connect to Bright Data's browser endpoint."""
        try:
            await self._initialize_playwright()
            
            if not self._playwright:
                raise RuntimeError("Failed to initialize Playwright")
            
            logger.info("Connecting to browser endpoint...")
            
            # Configuration pour ignorer robots.txt
            self._browser = await self._playwright.chromium.connect_over_cdp(
                endpoint_url=ScraperConfig.BROWSER_ENDPOINT,
                timeout=ScraperConfig.DEFAULT_TIMEOUT,
            )
            
            # Créer un nouveau contexte avec les options personnalisées
            self._context = await self._browser.new_context(
                ignore_https_errors=True,
                bypass_csp=True,
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            # Activer l'interception des requêtes pour ignorer robots.txt
            await self._context.route("**/*", lambda route: route.continue_())
            
            # Créer une nouvelle page
            self._page = await self._context.new_page()
            
            # Configurer les timeouts
            self._page.set_default_timeout(ScraperConfig.DEFAULT_TIMEOUT)
            self._page.set_default_navigation_timeout(ScraperConfig.NAVIGATION_TIMEOUT)
            
            logger.info("Successfully connected to browser endpoint")
            
        except Exception as e:
            logger.error(f"Failed to connect to browser: {str(e)}")
            await self.close()
            raise
            
    async def close(self) -> None:
        """Close all browser resources in correct order."""
        try:
            if self._page:
                logger.debug("Closing page")
                await self._page.close()
                self._page = None
                
            if self._context:
                logger.debug("Closing context")
                await self._context.close()
                self._context = None
                
            if self._browser:
                logger.debug("Closing browser")
                await self._browser.close()
                self._browser = None
                
            if self._playwright:
                logger.debug("Stopping Playwright")
                await self._playwright.stop()
                self._playwright = None
                
            logger.info("Browser resources cleaned up")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")
            
    async def get_properties(self, url: str) -> List[str]:
        """
        Fetch properties from a given URL.
        
        Args:
            url: URL to scrape
            
        Returns:
            List of HTML strings for each property found
        """
        if not self._page:
            raise RuntimeError("Browser not initialized. Call connect() first.")
            
        properties = []
        
        try:
            # Navigate to page
            logger.info(f"Navigating to {url}")
            await self._page.goto(url, wait_until='networkidle')
            
            # Wait for property list to load
            logger.debug("Waiting for property list")
            property_list = await self._page.wait_for_selector(
                ScraperConfig.SELECTORS['property_list'],
                timeout=ScraperConfig.DEFAULT_TIMEOUT
            )
            
            if not property_list:
                logger.warning("Property list selector not found")
                return properties
            
            # Get all property elements
            property_elements = await self._page.query_selector_all(
                ScraperConfig.SELECTORS['property_item']
            )
            
            # Extract HTML for each property
            for element in property_elements:
                try:
                    html = await element.inner_html()
                    if html:
                        properties.append(html)
                except Exception as e:
                    logger.error(f"Error extracting property HTML: {str(e)}")
                    continue
            
            properties_count = len(properties)
            if properties_count == 100:
                logger.info(f"Found maximum number of properties (100) for URL: {url}")
            elif properties_count > 0:
                logger.warning(f"Found {properties_count} properties (less than 100) for URL: {url}")
            else:
                logger.warning(f"No properties found for URL: {url}")
                
            return properties
            
        except Exception as e:
            logger.error(f"Error fetching properties from {url}: {str(e)}")
            raise
            
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()