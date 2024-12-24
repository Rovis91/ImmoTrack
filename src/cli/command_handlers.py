from pathlib import Path
from typing import Dict, Optional
import asyncio
from src.scraper.base_scraper import Scraper
from src.scraper.url_generator import SearchType
from src.dataprocessor.processor_base import DataProcessor


def resolve_path(base_dir: str, file_path: str) -> str:
    """
    Resolve a file path relative to a base directory.
    """
    return str(Path(base_dir).joinpath(file_path).resolve())


async def start_scraping(config: Dict) -> Optional[Path]:
    """
    Start scraping process with provided configuration.
    """
    try:
        # Resolve output file path
        config["output_scraper"] = resolve_path("data/raw", config["output_scraper"])

        scraper = Scraper(
            base_url=config["base_url"],
            start_date=config["start_date"],
            end_date=config["end_date"],
            search_type=SearchType[config["search_type"]],
            output_file=config["output_scraper"]
        )
        return await scraper.run()
    except KeyError as e:
        raise ValueError(f"Missing required configuration: {str(e)}")
    except Exception as e:
        raise Exception(f"Scraping failed: {str(e)}")
    
async def start_scraping(config: Dict) -> Optional[Path]:
    """
    Start scraping process with provided configuration.
    """
    try:
        scraper = Scraper(
            base_url=config["base_url"],
            start_date=config["start_date"],
            end_date=config["end_date"],
            search_type=SearchType[config["search_type"]],
            output_file=config["output_scraper"]
        )
        return await scraper.run()
    except KeyError as e:
        raise ValueError(f"Missing required configuration: {str(e)}")
    except Exception as e:
        raise Exception(f"Scraping failed: {str(e)}")

def start_parser(config: Dict) -> bool:
    """
    Start the parsing process with the provided configuration.
    """
    try:
        # Map dataprocessor_output_dir to output_dir for DataProcessor compatibility
        config["output_dir"] = config["dataprocessor_output_dir"]

        # Resolve input file and reference price paths
        config["parser_input"] = resolve_path("data/raw", config["parser_input"])
        config["reference_prices_path"] = resolve_path("data/raw", config["reference_prices_path"])

        processor = DataProcessor(config)
        return processor.process(config["parser_input"])
    except Exception as e:
        raise Exception(f"Parsing failed: {str(e)}")


def start_full_process(config: Dict) -> bool:
    """
    Run the full process: scrape and parse.
    """
    try:
        # Map dataprocessor_output_dir to output_dir for DataProcessor compatibility
        config["output_dir"] = config["dataprocessor_output_dir"]

        # Resolve paths
        config["output_scraper"] = resolve_path("data/raw", config["output_scraper"])
        config["reference_prices_path"] = resolve_path("data/raw", config["reference_prices_path"])

        # Step 1: Scraping
        scraper_output = asyncio.run(start_scraping(config))
        if not scraper_output:
            raise Exception("Scraping step failed.")
        
        # Step 2: Parsing
        processor = DataProcessor(config)
        return processor.process(scraper_output)
    except Exception as e:
        raise Exception(f"Full process failed: {str(e)}")