# src/cli/command_handlers.py

from pathlib import Path
from typing import Dict, Optional
from src.scraper.base_scraper import Scraper
from src.scraper.url_generator import SearchType

async def start_scraping(config: Dict) -> Optional[Path]:
    """
    Start scraping process with provided configuration.
    
    Args:
        config: Dictionary containing scraping configuration
            Required keys:
            - base_url: Base URL for scraping
            Optional keys:
            - start_date: Start date (MM/YYYY)
            - end_date: End date (MM/YYYY)
            - output_file: Output file path
    
    Returns:
        Path to results file if successful, None otherwise
    """
    try:
        scraper = Scraper(
            base_url=config["base_url"],
            start_date=config.get("start_date"),
            end_date=config.get("end_date"),
            search_type=SearchType[config.get("search_type", "ALL_TYPES")],
            output_file=config.get("output_file")
        )
        
        return await scraper.run()
        
    except KeyError as e:
        raise ValueError(f"Missing required configuration: {str(e)}")
    except Exception as e:
        raise Exception(f"Scraping failed: {str(e)}")