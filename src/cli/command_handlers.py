import json
import logging
import os
import pandas as pd
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from ..scraper.browse_ai_scraper import BrowseAIClient
from ..dataprocessor.data_parser import parse_browse_ai_response
from ..dataprocessor.address_enrichment import enrich_address_data
from ..dataprocessor.price_estimator import GrowthPriceEstimator
from ..scraper.url_generator import UrlGenerator, SearchType
from ..scraper.scraper import PropertyScraper

class CommandHandler:
    """Handles all CLI commands for the application."""

    @staticmethod
    def get_scraping_config() -> Tuple[str, SearchType, str, str]:
        """Get scraping configuration from user input."""
        base_url = input("\nEnter the base URL for scraping: ")
        
        url_generator = UrlGenerator()
        url_generator.display_search_types()
        
        while True:
            try:
                choice = int(input("\nChoose a search type (1-4): "))
                if 1 <= choice <= 4:
                    search_type = list(SearchType)[choice-1]
                    break
                print("Invalid choice. Please choose a number between 1 and 4.")
            except ValueError:
                print("Please enter a valid number.")
        
        start_date, end_date = url_generator.get_dates()
        return base_url, search_type, start_date, end_date

    @staticmethod
    def get_scraping_method() -> str:
        """Get scraping method choice from user."""
        print("\nChoose scraping method:")
        print("1. Browse AI")
        print("2. Manual Scraper")
        while True:
            choice = input("Enter choice (1-2): ").strip()
            if choice in ['1', '2']:
                return choice
            print("Invalid choice. Please enter 1 or 2.")

    @staticmethod
    def process_raw_data(data: Dict[str, Any], timestamp: str) -> None:
        """
        Process raw data from scraping results.
        
        Args:
            data: Raw scraping data
            timestamp: Timestamp for file naming
        """
        logging.info("Processing raw data...")
        properties = parse_browse_ai_response(data)
        
        # Create DataFrame
        df = pd.DataFrame(properties)
        
        # Add geographic coordinates
        logging.info("Adding geographic coordinates...")
        df = enrich_address_data(df)
        
        # Save to CSV
        output_file = f"parsed_data_{timestamp}.csv"
        df.to_csv(output_file, index=False)
        logging.info("Parsed data saved to file: %s", output_file)
        
        # Display statistics
        print("\nStatistics:")
        print(f"Number of properties: {len(properties)}")
        print(f"Successfully geocoded: {len(df[df['longitude'].notna()])}")
        print(f"Available columns: {', '.join(df.columns)}")
        
        # Update reference prices
        CommandHandler._update_reference_prices(df)

    @staticmethod
    def _update_reference_prices(df: pd.DataFrame) -> None:
        """Update reference prices with new cities."""
        valid_cities = set(df['city_name'].dropna().unique()) - {''}
        reference_file = "reference_prices.csv"
        
        try:
            existing_ref = pd.read_csv(reference_file)
            existing_cities = set(existing_ref['city_name'])
        except FileNotFoundError:
            existing_ref = pd.DataFrame(columns=['city_name', 'property_type', 'price_per_m2'])
            existing_cities = set()
            logging.warning(f"Reference file {reference_file} not found. Creating new file.")
        
        new_cities = valid_cities - existing_cities
        
        if new_cities:
            print("\nAdding new cities to reference_prices.csv:")
            for city in sorted(new_cities):
                print(f"- {city}")
            
            new_entries = []
            for city in new_cities:
                new_entries.extend([
                    {'city_name': city, 'property_type': 'Appartement', 'price_per_m2': ''},
                    {'city_name': city, 'property_type': 'Maison', 'price_per_m2': ''}
                ])
            
            updated_ref = pd.concat([
                existing_ref,
                pd.DataFrame(new_entries)
            ], ignore_index=True)
            
            updated_ref.to_csv(reference_file, index=False)
            logging.info(f"Updated {reference_file} with {len(new_cities)} new cities")
        else:
            print("\nNo new cities to add to reference file.")

    @staticmethod
    def handle_full_process() -> None:
        """Handle the full scraping process."""
        try:
            # Get configuration
            base_url, search_type, start_date, end_date = CommandHandler.get_scraping_config()
            scraping_method = CommandHandler.get_scraping_method()
            
            # Generate URLs
            url_generator = UrlGenerator()
            urls = url_generator.generate_urls(base_url, start_date, end_date, search_type)
            url_list = [url for url, _ in urls]
            
            logging.info(f"Generated {len(url_list)} URLs for {search_type.description}")
            
            if scraping_method == '1':
                # Browse AI method
                client = BrowseAIClient()
                logging.info("Starting Browse AI scraping...")
                
                bulk_run_id = client.create_bulk_run(url_list)
                logging.info(f"Bulk run created with ID: {bulk_run_id}")
                
                full_results = client.wait_for_bulk_run(bulk_run_id)
                if not full_results or 'robotTasks' not in full_results:
                    raise ValueError("No valid results retrieved")
                
                successful_tasks = [
                    task for task in full_results["robotTasks"]["items"] 
                    if task["status"] == "successful"
                ]
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                CommandHandler.process_raw_data({
                    "robotTasks": {"items": successful_tasks}
                }, timestamp)
                
            else:
                # Manual scraping method
                scraper = PropertyScraper(url_list)
                logging.info("Starting manual scraping...")
                scraper.run()
                
        except Exception as e:
            logging.error("An error occurred during the process: %s", e)
            raise

    @staticmethod
    def handle_parse_file() -> None:
        """Parse data from an existing JSON file."""
        try:
            print("\nEnter the name of the JSON file to parse (must be in the same folder):")
            filename = input("File name: ")
            
            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            CommandHandler.process_raw_data(data, timestamp)
            
        except FileNotFoundError:
            logging.error("File not found: %s", filename)
            raise
        except json.JSONDecodeError:
            logging.error("Invalid JSON file: %s", filename)
            raise
        except Exception as e:
            logging.error("An unexpected error occurred: %s", e)
            raise

    @staticmethod
    def handle_estimate_prices() -> None:
        """Estimate prices from CSV file."""
        try:
            data_file = input("\nEnter the name of the CSV file with property data: ")
            reference_file = "reference_prices.csv"

            base_name = os.path.splitext(data_file)[0]
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"{base_name}_enriched_{timestamp}.csv"

            # Load and process data
            properties_df = pd.read_csv(data_file)
            properties_df = properties_df[properties_df['property_type'] != 'Biens Multiples'].copy()
            
            # Initialize estimator
            estimator = GrowthPriceEstimator(reference_file)
            estimator.calculate_growth_rates(properties_df)
            
            # Process properties
            CommandHandler._process_price_estimates(properties_df, estimator, base_name, output_file, timestamp)
            
        except Exception as e:
            logging.error("Error during price estimation: %s", e)
            raise

    @staticmethod
    def _process_price_estimates(
        properties_df: pd.DataFrame,
        estimator: GrowthPriceEstimator,
        base_name: str,
        output_file: str,
        timestamp: str
    ) -> None:
        """Process price estimates for properties."""
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
                    
                    if details['initial_price_m2'] and details['projected_price_m2']:
                        cumulative_growth = ((details['projected_price_m2'] / details['initial_price_m2']) - 1) * 100
                        properties_df.at[idx, 'cumulative_growth'] = cumulative_growth

            except Exception as e:
                logging.warning(f"Failed to estimate price for property {idx}: {str(e)}")

        # Save results and generate summaries
        CommandHandler._save_estimation_results(properties_df, estimator, base_name, output_file, timestamp)

    @staticmethod
    def _save_estimation_results(
        properties_df: pd.DataFrame,
        estimator: GrowthPriceEstimator,
        base_name: str,
        output_file: str,
        timestamp: str
    ) -> None:
        """Save estimation results and generate summaries."""
        # Create yearly means summary
        yearly_means_data = []
        for (city, prop_type), data in estimator.growth_rates.items():
            row = {
                'city': city,
                'property_type': prop_type,
                **data['yearly_means']
            }
            yearly_means_data.append(row)
            
        yearly_means_df = pd.DataFrame(yearly_means_data)

        # Save results
        properties_df.to_csv(output_file, index=False)
        yearly_means_df.to_csv(f"{base_name}_yearly_means_{timestamp}.csv", index=False)
        
        # Print summary
        print(f"\nResults saved to: {output_file}")
        print(f"Total properties processed: {len(properties_df)}")
        print(f"Successful estimations: {len(properties_df.dropna(subset=['estimated_current_price_per_m2']))}")
        
        if len(properties_df) > 0:
            print("\nPrice per m² Statistics:")
            stats_columns = [
                'actual_price_per_m2', 
                'current_reference_price_per_m2',
                'estimated_current_price_per_m2',
                'cumulative_growth'
            ]
            print(properties_df[stats_columns].describe())
            print("\nYearly Means Summary:")
            print(yearly_means_df)

    @staticmethod
    def handle_fetch_recent() -> None:
        """Fetch and process recent Browse AI results."""
        try:
            print("\nHow many hours back do you want to look?")
            hours = int(input("Hours (default 24): ") or "24")
            
            client = BrowseAIClient()
            results = client.fetch_recent_results(
                hours_back=hours,
                check_interval=30
            )
            
            if not results:
                print("No results found in the specified time period.")
                return
                
            print(f"\nFound {len(results)} successful tasks.")
            if input("\nDo you want to process these results? (y/n): ").lower() == 'y':
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                CommandHandler.process_raw_data({
                    "robotTasks": {"items": results}
                }, timestamp)
                print("\nAll results have been processed together.")
            else:
                print("\nResults are saved in browse_ai_data directory but not processed.")
                
        except ValueError as e:
            logging.error("Invalid input: %s", e)
            raise
        except Exception as e:
            logging.error("An error occurred: %s", e)
            raise