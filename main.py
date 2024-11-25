import json
import pandas as pd
from datetime import datetime
import logging
import numpy as np
import os
from typing import Optional
from browse_ai_client import BrowseAIClient
from url_generator import generate_monthly_urls
from data_parser import parse_browse_ai_response
from geocoding import add_coordinates_to_df
from price_estimator import AdaptivePriceEstimator

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
    
    # Create DataFrame
    df = pd.DataFrame(properties)
    
    # Add geographic coordinates
    logging.info("Adding geographic coordinates...")
    df = add_coordinates_to_df(df)
    
    # Save to CSV
    output_file = f"parsed_data_{timestamp}.csv"
    df.to_csv(output_file, index=False)
    logging.info("Parsed data saved to file: %s", output_file)
    
    # Display statistics
    print("\nStatistics:")
    print(f"Number of properties: {len(properties)}")
    print(f"Successfully geocoded: {len(df[df['longitude'].notna()])}")
    print(f"Available columns: {', '.join(df.columns)}")

def estimate_prices_from_file(data_file: Optional[str] = None) -> None:
    """
    Estimate prices for properties in a CSV file and save the results to a new enriched file.

    Args:
        data_file (str, optional): Path to the input CSV file containing property data. 
                                   If not provided, the user will be prompted to enter it.

    Raises:
        Exception: If an error occurs during the process.
    """
    reference_file = "reference_prices.csv"

    try:
        # Prompt for input file if not provided
        if not data_file:
            print("\nEnter the name of the CSV file with property data:")
            data_file = input("File name: ")

        # Generate output filename with a timestamp
        base_name = os.path.splitext(data_file)[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{base_name}_enriched_{timestamp}.csv"

        # Load property data
        logging.info("Loading property data from file: %s", data_file)
        properties_df = pd.read_csv(data_file)

        # Filter out rows with "Biens Multiples"
        properties_df = properties_df[properties_df['property_type'] != 'Biens Multiples'].copy()
        logging.info("Processing %d properties (excluding 'Biens Multiples')...", len(properties_df))

        # Initialize the adaptive price estimator
        estimator = AdaptivePriceEstimator(reference_file)

        # Add columns for calculated metrics
        properties_df['actual_price_per_m2'] = properties_df['price'] / properties_df['surface_area']
        properties_df['reference_price_per_m2'] = None
        properties_df['estimated_price_per_m2'] = None
        properties_df['estimation_confidence'] = None
        properties_df['comparable_count'] = 0

        # Process each property in the DataFrame
        total_properties = len(properties_df)
        for idx, row in properties_df.iterrows():
            if idx % 10 == 0:
                logging.info("Progress: %d/%d properties processed", idx, total_properties)

            # Convert row data to dictionary
            property_data = row.to_dict()

            # Retrieve reference price
            ref_key = (property_data['city_name'], property_data['property_type'])
            ref_price = estimator.reference_prices.get(ref_key)
            if not ref_price:
                ref_price = estimator.city_averages.get(property_data['city_name'])
            properties_df.at[idx, 'reference_price_per_m2'] = ref_price

            # Estimate price for the property
            estimated_price, details = estimator.estimate_price(property_data, properties_df)

            if estimated_price is not None:
                properties_df.at[idx, 'estimated_price_per_m2'] = estimated_price / property_data['surface_area']
                if isinstance(details, dict):
                    properties_df.at[idx, 'estimation_confidence'] = details.get("confidence_score")
                    properties_df.at[idx, 'comparable_count'] = details.get("comparable_count")

        # Save the enriched results to a new CSV file
        properties_df.to_csv(output_file, index=False)
        logging.info("Enriched data successfully saved to: %s", output_file)

        # Display summary statistics
        print(f"\nResults saved to: {output_file}")
        print(f"Total properties processed: {total_properties}")

        print("\nPrice per m² Statistics:")
        stats = properties_df[['actual_price_per_m2', 'reference_price_per_m2', 'estimated_price_per_m2']].describe()
        print(stats)

        # Calculate and display Mean Absolute Percentage Error (MAPE)
        mape = np.mean(
            np.abs(
                (properties_df['actual_price_per_m2'] - properties_df['estimated_price_per_m2']) /
                properties_df['actual_price_per_m2']
            )
        ) * 100
        print(f"\nMean Absolute Percentage Error: {mape:.1f}%")

    except Exception as e:
        logging.error("An error occurred during price estimation: %s", e)
        raise

def display_menu() -> str:
    """
    Display the main menu and get the user's choice.

    Returns:
        str: The user's choice.
    """
    print("\n=== IMMO DATA SCRAPER ===")
    print("1. Execute full process (Browse AI + Parsing)")
    print("2. Parse data from an existing JSON file")
    print("3. Estimate prices from CSV file")
    print("4. Quit")
    return input("\nChoose an option (1-4): ")

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
            logging.info("Starting price estimation...")
            estimate_prices_from_file()
        
        elif choice == "4":
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
