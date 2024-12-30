"""
Browser management using Playwright for property scraping.
"""

import asyncio
from typing import Optional, List
import logging
import os
from playwright.async_api import async_playwright, Browser, Page, Playwright
from .config import Config

logger = logging.getLogger(__name__)

class BrowserManager:
    """Manages browser instance and page interactions for scraping."""


    DEFAULT_TIMEOUT = 30000
    DEFAULT_RETRY_COUNT = 3
    DEFAULT_RETRY_DELAY = 5000
    DEFAULT_SELECTORS = {
        'property_list': 'div.hidden.md\\:block.overflow-y-auto.flex-grow.children-hover\\:bg-gray-50',
        'property_item': 'div.border-b.border-b-gray-100 > div.text-sm.relative.font-sans'
    }
    
    def __init__(self, config: Config):
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
            
    async def _connect_with_proxy(self) -> None:
        """Connect to browser using Bright Data proxy."""
        try:
            if not self.config.browser.proxy_endpoint:
                self.config.browser.proxy_endpoint = os.getenv('BROWSER_ENDPOINT')
                
            if not self.config.browser.proxy_endpoint:
                raise ValueError("Proxy endpoint not configured")
                
            logger.info("Connecting to Bright Data proxy...")
            self._browser = await self._playwright.chromium.connect_over_cdp(
                self.config.browser.proxy_endpoint
            )
            
            # Create new context with configured viewport and user agent
            self._context = await self._browser.new_context(
                viewport={
                    'width': self.config.browser.viewport_width,
                    'height': self.config.browser.viewport_height
                },
                user_agent=self.config.browser.user_agent
            )
            
            logger.info("Successfully connected to proxy")
            
        except Exception as e:
            logger.error(f"Failed to connect to proxy: {str(e)}")
            raise
            
    async def _connect_direct(self) -> None:
        """Connect to browser directly without proxy."""
        try:
            logger.info("Launching browser without proxy...")
            
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
            
            logger.info("Browser launched successfully")
            
        except Exception as e:
            logger.error(f"Failed to launch browser: {str(e)}")
            raise
            
    async def connect(self) -> None:
        """Initialize browser session with or without proxy based on configuration."""
        try:
            await self._initialize_playwright()
            
            if not self._playwright:
                raise RuntimeError("Failed to initialize Playwright")
            
            # Check if proxy is enabled in config
            if hasattr(self.config, 'proxy') and self.config.proxy:
                # Use Bright Data proxy
                proxy_endpoint = os.getenv('BROWSER_ENDPOINT')
                if not proxy_endpoint:
                    raise ValueError("BROWSER_ENDPOINT not set in environment variables")
                    
                logger.info("Connecting to Bright Data proxy...")
                self._browser = await self._playwright.chromium.connect_over_cdp(proxy_endpoint)
            else:
                # Direct connection
                logger.info("Launching browser without proxy...")
                self._browser = await self._playwright.chromium.launch(headless=True)
            
            # Create context with standard settings
            self._context = await self._browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )
            
            # Create page with standard timeouts
            self._page = await self._context.new_page()
            self._page.set_default_timeout(30000)
            self._page.set_default_navigation_timeout(60000)
            
            logger.info("Browser session initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize browser session: {str(e)}")
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
            await asyncio.sleep(5)  # Base wait for content loading
            
            # Utilise des sélecteurs CSS directement
            property_list = await self._page.wait_for_selector(
                'div.hidden.md\\:block.overflow-y-auto.flex-grow.children-hover\\:bg-gray-50',
                timeout=30000  # Utilise une valeur par défaut directement
            )
            
            if not property_list:
                logger.warning("Property list selector not found")
                return html_elements
            
            property_elements = await self._page.query_selector_all(
                'div.border-b.border-b-gray-100 > div.text-sm.relative.font-sans'
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
            
            if retry_count < 3:  # Utilise une valeur par défaut pour max_retries
                logger.info(f"Retrying ({retry_count + 1}/3)")
                await asyncio.sleep(5)  # Utilise une valeur par défaut pour retry_delay
                return await self.get_properties(url, retry_count + 1)
            
            raise
        
    async def get_page_content(self, url: str) -> Optional[str]:
        """
        Get full page HTML content with smart waiting strategy.
        
        Args:
            url: Target URL to fetch content from
            
        Returns:
            Optional[str]: HTML content if successful, None otherwise
        """
        try:
            await self._page.goto(
                url, 
                wait_until='networkidle',
                timeout=self.config.browser.navigation_timeout
            )
            await asyncio.sleep(5)

            content = await self._page.content()
            logger.debug(f"Fetched content length: {len(content)}")

            if len(content) < 1000:
                logger.warning("Content seems too short, might be incomplete")
            elif "prices-summary" not in content:
                logger.warning("Expected content markers not found in page")
                
            return content
        
        except Exception as e:
            logger.error(f"Failed to get page content: {str(e)}")
            return None
        
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()