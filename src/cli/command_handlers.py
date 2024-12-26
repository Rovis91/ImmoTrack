from pathlib import Path
from typing import Dict, Optional
import pandas as pd
import logging
from src.scraper.base_scraper import Scraper
from src.scraper.url_generator import SearchType
from src.dataprocessor.processor_base import DataProcessor
from src.utils.storage_manager import PropertyDataManager, DataFormat

logger = logging.getLogger(__name__)

def verify_paths(config: Dict) -> None:
    """
    Verify that all required paths exist and are accessible.
    
    Args:
        config: Configuration dictionary containing file paths
        
    Raises:
        FileNotFoundError: If a required file is missing
        PermissionError: If a file/directory is not accessible
    """
    required_paths = {
        'parser_input': 'Input file',
        'reference_prices_path': 'Reference prices file'
    }
    
    for key, description in required_paths.items():
        if key in config:
            path = Path(config[key])
            if not path.exists():
                raise FileNotFoundError(f"{description} not found: {path}")
            if not path.is_file():
                raise ValueError(f"{description} is not a file: {path}")

def prepare_processor_config(config: Dict) -> Dict:
    """
    Prepare configuration for the DataProcessor by mapping keys correctly.
    
    Args:
        config: Original configuration dictionary
        
    Returns:
        Dict: Processed configuration with correct keys
    """
    processor_config = config.copy()
    
    # Map dataprocessor_output_dir to output_dir
    if 'dataprocessor_output_dir' in processor_config:
        processor_config['output_dir'] = processor_config['dataprocessor_output_dir']
    
    # Ensure all required keys are present
    required_keys = ['output_dir', 'reference_prices_path']
    missing_keys = [key for key in required_keys if key not in processor_config]
    
    if missing_keys:
        raise ValueError(f"Missing required configuration keys: {', '.join(missing_keys)}")
    
    return processor_config

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
        logger.error(f"Scraping failed: {str(e)}", exc_info=True)
        raise

async def start_parser(config: Dict) -> bool:
    """
    Start the parsing process with the provided configuration.
    """
    try:
        # Verify all required paths exist
        verify_paths(config)
        
        # Prepare processor configuration
        processor_config = prepare_processor_config(config)
        
        # Initialize processor with correct configuration
        processor = DataProcessor(processor_config)
        
        # Run processing (now with await)
        success = await processor.process(config["parser_input"])
        
        if not success:
            logger.error("Processing failed")
            return False
            
        return True
        
    except Exception as e:
        logger.error(f"Parsing failed: {str(e)}", exc_info=True)
        raise

async def start_full_process(config: Dict) -> bool:
    """
    Run the full process: scrape and parse.
    """
    try:
        # Step 1: Scraping
        scraper_output = await start_scraping(config)
        if not scraper_output:
            raise Exception("Scraping step failed.")
            
        # Update config with scraping output
        config["parser_input"] = str(scraper_output)
        
        # Step 2: Parsing
        if not start_parser(config):
            raise Exception("Parsing step failed.")
            
        return True
        
    except Exception as e:
        logger.error(f"Full process failed: {str(e)}", exc_info=True)
        raise

def init_storage_manager(config: Dict) -> PropertyDataManager:
    """Initialize and return storage manager instance."""
    try:
        manager = PropertyDataManager(
            main_file=config["storage_file"],
            invalid_file=config["invalid_file"],
            log_file=config["log_file"]
        )
        return manager
    except Exception as e:
        logger.error(f"Failed to initialize storage manager: {str(e)}")
        raise

def get_storage_summary(manager: PropertyDataManager) -> Dict:
    """Get summary of stored data."""
    try:
        return manager.get_summary()
    except Exception as e:
        logger.error(f"Failed to get storage summary: {str(e)}")
        raise

def process_data_file(manager: PropertyDataManager, file_path: str) -> Dict:
    """Process data file for import or update."""
    try:
        # Load CSV with all columns as strings initially
        df = pd.read_csv(file_path, dtype=str)
        logger.info(f"Loaded file {file_path} with {len(df)} rows")
        
        print("\nInput Data Info:")
        print(f"Total rows: {len(df)}")
        print(f"Columns: {df.columns.tolist()}")
        
        return manager.add_data(df)
        
    except Exception as e:
        logger.error(f"Failed to process data file: {str(e)}")
        raise

def export_data(manager: PropertyDataManager, query: str, output_path: str) -> bool:
    """Export filtered data to CSV file."""
    try:
        result = manager.query_data(query)
        result.to_csv(output_path, index=False)
        logger.info(f"Exported {len(result)} rows to {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to export data: {str(e)}")
        raise

def delete_data(manager: PropertyDataManager, condition: str) -> Dict:
    """Delete entries matching condition."""
    try:
        return manager.delete_data(condition)
    except Exception as e:
        logger.error(f"Failed to delete data: {str(e)}")
        raise
