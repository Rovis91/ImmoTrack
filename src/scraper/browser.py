"""
Browser management using Playwright for property scraping.
"""

import asyncio
from typing import Optional, List
import logging
from playwright.async_api import async_playwright, Browser, Page, Playwright
from .config import Config

logger = logging.getLogger(__name__)

class BrowserManager:
    """Manages browser instance and page interactions for scraping."""
    
    def __init__(self, config: Config):
        """Initialize browser manager."""
        self.config = config
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
        """Initialize browser session."""
        try:
            await self._initialize_playwright()
            
            if not self._playwright:
                raise RuntimeError("Failed to initialize Playwright")
            
            logger.info("Launching browser...")
            
            self._browser = await self._playwright.chromium.launch(
                headless=self.config.browser.headless
            )
            
            self._context = await self._browser.new_context(
                user_agent=self.config.browser.user_agent,
                viewport={
                    'width': self.config.browser.viewport_width,
                    'height': self.config.browser.viewport_height
                }
            )
            
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
            
    async def get_properties(self, url: str, retry_count: int = 0) -> List[str]:
        """
        Fetch property HTML elements from a given URL.
        
        Args:
            url: URL to scrape
            retry_count: Current retry attempt number
            
        Returns:
            List of HTML strings for each property
        """
        if not self._page:
            raise RuntimeError("Browser not initialized. Call connect() first.")
            
        html_elements = []
        
        try:
            logger.info(f"Navigating to {url}")
            await self._page.goto(url, wait_until='networkidle')
            await asyncio.sleep(5)
            
            property_list = await self._page.wait_for_selector(
                self.config.selectors.property_list,
                timeout=self.config.browser.default_timeout
            )
            
            if not property_list:
                logger.warning("Property list selector not found")
                return html_elements
            
            property_elements = await self._page.query_selector_all(
                self.config.selectors.property_item
            )
            
            for element in property_elements:
                html = await element.inner_html()
                if html:
                    html_elements.append(html)
            
            count = len(html_elements)
            logger.info(f"Found {count} properties for URL: {url}")
            return html_elements
            
        except Exception as e:
            logger.error(f"Error fetching properties from {url}: {str(e)}")
            
            if retry_count < self.config.scraping.max_retries:
                logger.info(f"Retrying ({retry_count + 1}/{self.config.scraping.max_retries})")
                await asyncio.sleep(self.config.scraping.retry_delay / 1000)
                return await self.get_properties(url, retry_count + 1)
            
            raise
            
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()