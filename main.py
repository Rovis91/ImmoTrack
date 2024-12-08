import json
import pandas as pd
from datetime import datetime
import logging
import numpy as np
import os
from typing import Optional
from browse_ai_client import BrowseAIClient
from data_parser import parse_browse_ai_response
from geocoding import add_coordinates_to_df
from price_estimator import GrowthPriceEstimator
from url_generator import UrlGenerator, SearchType

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
    """Execute the full process from URL generation to data parsing and saving."""
    base_url = ('https://www.immo-data.fr/explorateur/transaction/recherche?minprice=0&maxprice=25000000&minpricesquaremeter=0&maxpricesquaremeter=40000&propertytypes=4%2C0%2C5%2C2%2C1&minmonthyear=Mai%202024&maxmonthyear=Juin%202024&nbrooms=1%2C2%2C3%2C4%2C5&minsurface=0&maxsurface=400&minsurfaceland=0&maxsurfaceland=50000&center=3.178897646468954%3B50.68687158514737&zoom=13.059885316477079')
    
    # Initialize URL generator
    url_generator = UrlGenerator()
    
    # Display search types and get user choice
    url_generator.display_search_types()
    while True:
        try:
            choice = int(input("\nChoisissez un type de recherche (1-4): "))
            if 1 <= choice <= 4:
                search_type = list(SearchType)[choice-1]
                break
            print("Choix invalide. Veuillez choisir un nombre entre 1 et 4.")
        except ValueError:
            print("Veuillez entrer un nombre valide.")
    
    # Get dates
    start_date, end_date = url_generator.get_dates()
    
    try:
        # Generate URLs
        logging.info("Generating URLs...")
        urls = url_generator.generate_urls(base_url, start_date, end_date, search_type)
        url_list = [url for url, _ in urls]
        
        logging.info(f"Generated {len(url_list)} URLs for {search_type.description}")
        
        # Initialize Browse AI client
        client = BrowseAIClient()
        logging.info("Starting scraping for %d URLs...", len(url_list))
        bulk_run_id = client.create_bulk_run(url_list)
        
        # Wait for completion and fetch all results
        logging.info("Waiting for results...")
        results = client.fetch_recent_results(
            hours_back=1,  # Regarder seulement la dernière heure
            check_interval=30  # Vérifier toutes les 30 secondes
        )
        
        if not results:
            raise ValueError("No results retrieved")
            
        # Traiter les résultats
        for result in results:
            timestamp = datetime.fromtimestamp(result["createdAt"]/1000).strftime("%Y%m%d_%H%M%S")
            
            # Parse and save processed data
            process_raw_data({"robotTasks": {"items": [result]}}, timestamp)
        
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
    reference_file = "reference_prices.csv"

    try:
        if not data_file:
            data_file = input("\nEnter the name of the CSV file with property data: ")

        base_name = os.path.splitext(data_file)[0]
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{base_name}_enriched_{timestamp}.csv"

        # Load and filter data
        properties_df = pd.read_csv(data_file)
        properties_df = properties_df[properties_df['property_type'] != 'Biens Multiples'].copy()
        logging.info("Processing %d properties...", len(properties_df))

        # Initialize estimator and calculate growth rates
        estimator = GrowthPriceEstimator(reference_file)
        estimator.calculate_growth_rates(properties_df)

        # Prepare result columns
        properties_df['actual_price_per_m2'] = properties_df['price'] / properties_df['surface_area']
        properties_df['current_reference_price_per_m2'] = None
        properties_df['estimated_current_price_per_m2'] = None
        properties_df['cumulative_growth'] = None

        total_properties = len(properties_df)
        for idx, row in properties_df.iterrows():
            if idx % 10 == 0:
                logging.info("Progress: %d/%d properties processed", idx, total_properties)

            try:
                estimated_price, details = estimator.estimate_price(row.to_dict())

                if estimated_price is not None and isinstance(details, dict):
                    properties_df.at[idx, 'estimated_current_price_per_m2'] = details['projected_price_m2']
                    properties_df.at[idx, 'current_reference_price_per_m2'] = details['current_reference_price']
                    
                    # Calculate cumulative growth
                    initial = details['initial_price_m2']
                    final = details['projected_price_m2']
                    if initial and final:
                        cumulative_growth = ((final / initial) - 1) * 100
                        properties_df.at[idx, 'cumulative_growth'] = cumulative_growth

            except Exception as e:
                logging.warning(f"Failed to estimate price for property {idx}: {str(e)}")
                continue

        # Create yearly means summary
        yearly_means_data = []
        for (city, prop_type), data in estimator.growth_rates.items():
            row = {
                'city': city,
                'property_type': prop_type
            }
            # Add means for each year
            row.update(data['yearly_means'])
            yearly_means_data.append(row)
            
        yearly_means_df = pd.DataFrame(yearly_means_data)

        # Save results
        properties_df.to_csv(output_file, index=False)
        yearly_means_df.to_csv(f"{base_name}_yearly_means_{timestamp}.csv", index=False)
        
        logging.info("Enriched data saved to: %s", output_file)

        # Print summary
        print(f"\nResults saved to: {output_file}")
        print(f"Total properties processed: {total_properties}")
        print(f"Successful estimations: {len(properties_df.dropna(subset=['estimated_current_price_per_m2']))}")
        
        if len(properties_df) > 0:
            print("\nPrice per m² Statistics:")
            stats_df = properties_df[[
                'actual_price_per_m2', 
                'current_reference_price_per_m2',
                'estimated_current_price_per_m2',
                'cumulative_growth'
            ]].describe()
            print(stats_df)

            print("\nYearly Means Summary:")
            print(yearly_means_df)

    except Exception as e:
        logging.error("Error during price estimation: %s", e)
        raise

def fetch_recent_manually():
    """
    Manually fetch and process recent Browse AI results.
    Accumulates all results before processing.
    """
    try:
        # Get hours to look back
        print("\nHow many hours back do you want to look?")
        hours = int(input("Hours (default 24): ") or "24")
        
        # Initialize Browse AI client
        client = BrowseAIClient()
        
        # Fetch results
        logging.info(f"Fetching results from the last {hours} hours...")
        results = client.fetch_recent_results(
            hours_back=hours,
            check_interval=30
        )
        
        if not results:
            print("No results found in the specified time period.")
            return
            
        # Display summary
        print(f"\nFound {len(results)} successful tasks.")
        print("\nDo you want to process these results? (y/n)")
        choice = input().lower()
        
        if choice == 'y':
            # Combine all results into one structure
            combined_data = {
                "robotTasks": {
                    "items": results
                }
            }
            
            # Process combined results
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            logging.info("Processing all results together...")
            process_raw_data(combined_data, timestamp)
            
            print("\nAll results have been processed together.")
        else:
            print("\nResults are saved in browse_ai_data directory but not processed.")
            
    except ValueError as e:
        logging.error("Invalid input: %s", e)
    except Exception as e:
        logging.error("An error occurred: %s", e)

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
    print("4. Fetch recent Browse AI results")
    print("5. Quit")
    return input("\nChoose an option (1-5): ")

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
            logging.info("Starting manual fetch of recent results...")
            fetch_recent_manually()
        
        elif choice == "5":
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
