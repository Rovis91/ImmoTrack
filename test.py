# test.py

import asyncio
import pandas as pd
import logging
from src.scraper.reference_price_scraper import ReferencePriceScraper
from src.dataprocessor.price_estimator import PriceEstimator

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def test_reference_price_scraping():
    """Test reference price scraping for a missing city."""
    try:
        estimator = PriceEstimator(
            reference_prices_path="data/reference_prices.csv"
        )
        
        logger.info("Testing price update for POITIERS...")
        await estimator._fetch_city_prices('POITIERS', '86000')
        
        new_references = pd.read_csv("data/reference_prices.csv")
        poitiers_data = new_references[new_references['city_name'] == 'POITIERS']
        
        if not poitiers_data.empty:
            logger.info("Successfully added reference prices for POITIERS:")
            logger.info(f"Apartment price: {poitiers_data[poitiers_data['property_type'] == 'Appartement']['price_per_m2'].iloc[0]}")
            logger.info(f"House price: {poitiers_data[poitiers_data['property_type'] == 'Maison']['price_per_m2'].iloc[0]}")
            return True
        else:
            logger.error("Failed to add reference prices")
            return False
            
    except Exception as e:
        logger.error(f"Test failed: {str(e)}")
        return False

# Test plus ciblé juste pour le scraper
async def test_scraper_only():
    """Test just the reference price scraper."""
    try:
        scraper = ReferencePriceScraper()
        prices = await scraper.get_city_prices('POITIERS', '86000')
        
        if prices:
            logger.info("Successfully scraped prices:")
            logger.info(f"Apartment price: {prices['apartment_price']}")
            logger.info(f"House price: {prices['house_price']}")
            return True
        return False
        
    except Exception as e:
        logger.error(f"Scraper test failed: {str(e)}")
        return False

if __name__ == "__main__":
    # Test uniquement le scraper d'abord
    logger.info("Testing reference price scraper...")
    asyncio.run(test_scraper_only())
    
    # Puis test complet avec l'estimateur
    logger.info("\nTesting full price estimation process...")
    asyncio.run(test_reference_price_scraping())