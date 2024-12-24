"""
Test script for DataProcessor execution.
"""

import logging
from pathlib import Path
from src.dataprocessor.processor_base import DataProcessor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def test_data_processor():
    """Test the complete DataProcessor pipeline."""
    try:
        # Configuration for the processor
        config = {
            "reference_prices_path": "data/reference_prices.csv",
            "output_dir": "data/test_output",
            "keep_intermediate": True
        }

        # Input file
        input_json = "data/raw/scraping_20241224_105611.json"

        # Check if the input file exists
        if not Path(input_json).exists():
            logger.error(f"Input file not found: {input_json}")
            return False

        # Initialize the processor
        processor = DataProcessor(config)

        # Execute the processing pipeline
        if processor.process(input_json):
            logger.info("DataProcessor pipeline executed successfully.")
            return True
        else:
            logger.error("DataProcessor pipeline failed.")
            return False

    except Exception as e:
        logger.error(f"Error during DataProcessor test: {str(e)}")
        logger.debug("Error details:", exc_info=True)
        return False

if __name__ == "__main__":
    test_data_processor()
