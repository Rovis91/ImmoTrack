# test.py

import asyncio
import logging
from src.scraper.base_scraper import Scraper

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.immo-data.fr/explorateur/transaction/recherche?minprice=0&maxprice=25000000&minpricesquaremeter=0&maxpricesquaremeter=40000&propertytypes=0%2C1%2C2%2C4%2C5&minmonthyear=Janvier%202014&maxmonthyear=Juin%202024&nbrooms=1%2C2%2C3%2C4%2C5&minsurface=0&maxsurface=400&minsurfaceland=0&maxsurfaceland=50000&center=2.3431957478042023%3B48.85910487750468&zoom=13.042327120629595"

async def test_manual_scraping():
    """Test manual scraping for January 2014."""
    try:
        scraper = Scraper(
            base_url=BASE_URL,
            start_date="01/2014",
            end_date="01/2014"
        )
        logger.info("Starting scraping for January 2014...")
        return await scraper.run()
        
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return None

if __name__ == "__main__":
    asyncio.run(test_manual_scraping())