import json
import pandas as pd
from datetime import datetime
import logging
from browse_ai_client import BrowseAIClient
from url_generator import generate_monthly_urls
from data_parser import parse_browse_ai_response

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def save_raw_data(data: dict, filename: str = None) -> str:
    """
    Save raw Browse AI data to a JSON file.

    Args:
        data (dict): Raw data from Browse AI.
        filename (str, optional): Name of the file to save. Defaults to a timestamped name.

    Returns:
        str: Path to the saved file.
    """
    if filename is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"raw_data_{timestamp}.json"
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    
    logging.info("Raw data saved to file: %s", filename)
    return filename

def run_full_process():
    """
    Execute the full process from URL generation to data parsing and saving.
    """
    base_url = ("https://www.immo-data.fr/explorateur/transaction/recherche"
                "?minprice=0&maxprice=25000000&minpricesquaremeter=0"
                "&maxpricesquaremeter=40000&propertytypes=1%2C2%2C5"
                "&minmonthyear=D%C3%A9cembre%202014&maxmonthyear=D%C3%A9cembre%202014"
                "&nbrooms=1%2C2%2C3%2C4%2C5&minsurface=0&maxsurface=400"
                "&minsurfaceland=0&maxsurfaceland=50000"
                "&center=2.3540979812628393%3B48.845467949508645&zoom=15.130634442433555")
    
    # Get dates from the user
    print("\nEnter the start and end dates (format: MM/YYYY):")
    start_date = input("Start date: ")
    end_date = input("End date: ")
    
    try:
        # Generate URLs
        logging.info("Generating URLs...")
        urls = generate_monthly_urls(base_url, start_date, end_date)
        url_list = [url for url, _ in urls]
        
        # Initialize Browse AI client
        client = BrowseAIClient()
        logging.info("Starting scraping for %d URLs...", len(url_list))
        bulk_run_id = client.create_bulk_run(url_list)
        
        # Wait and fetch results
        logging.info("Waiting for results...")
        results = client.wait_for_bulk_run(bulk_run_id)
        
        # Save raw data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        raw_data_file = save_raw_data(results, f"browse_ai_data_{timestamp}.json")
        logging.info("Raw data saved to file: %s", raw_data_file)
        
        # Parse and save processed data
        process_raw_data(results, timestamp)
        
    except Exception as e:
        logging.error("An error occurred during the process: %s", e)

def parse_from_file():
    """
    Parse data from an existing JSON file.
    """
    print("\nEnter the name of the JSON file to parse (must be in the same folder):")
    filename = input("File name: ")
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        process_raw_data(data, timestamp)
        
    except FileNotFoundError:
        logging.error("File not found: %s", filename)
    except json.JSONDecodeError:
        logging.error("Invalid JSON file: %s", filename)
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)

def process_raw_data(data: dict, timestamp: str):
    """
    Process raw data, parse it, and save to a CSV file.

    Args:
        data (dict): Raw data from Browse AI.
        timestamp (str): Timestamp for file naming.
    """
    logging.info("Processing raw data...")
    properties = parse_browse_ai_response(data)
    
    # Create DataFrame and save
    df = pd.DataFrame(properties)
    output_file = f"parsed_data_{timestamp}.csv"
    df.to_csv(output_file, index=False)
    logging.info("Parsed data saved to file: %s", output_file)
    
    # Display basic statistics
    print("\nStatistics:")
    print(f"Number of properties: {len(properties)}")
    print(f"Available columns: {', '.join(df.columns)}")

def display_menu() -> str:
    """
    Display the main menu and get the user's choice.

    Returns:
        str: The user's choice.
    """
    print("\n=== IMMO DATA SCRAPER ===")
    print("1. Execute full process (Browse AI + Parsing)")
    print("2. Parse data from an existing JSON file")
    print("3. Quit")
    return input("\nChoose an option (1-3): ")

def main():
    """
    Main function to run the application.
    """
    while True:
        choice = display_menu()
        
        if choice == "1":
            logging.info("Starting full process...")
            run_full_process()
        
        elif choice == "2":
            logging.info("Entering file parsing mode...")
            parse_from_file()
        
        elif choice == "3":
            logging.info("Exiting program. Goodbye!")
            break
        
        else:
            logging.warning("Invalid option. Please try again.")
        
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Program interrupted by user.")
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)
