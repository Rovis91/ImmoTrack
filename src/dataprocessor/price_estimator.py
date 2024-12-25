"""
Price estimation module for calculating current market values based on historical data.
Uses reference prices and historical data to estimate current property values.
"""

from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Set, Tuple, List
from pathlib import Path
import logging
import pandas as pd

from .processor_base import ProcessorBase
from src.scraper.reference_price_scraper import ReferencePriceScraper

logger = logging.getLogger(__name__)


class PriceEstimator(ProcessorBase):
    """
    Estimates current market values using historical growth rates.
    
    This class handles:
    - Reference price management with automatic updates for missing cities
    - Historical growth rate calculations
    - Current market value estimations
    
    Attributes:
        reference_prices_path (str): Path to CSV containing reference prices
        price_scraper (ReferencePriceScraper): Scraper for fetching missing prices
        growth_rates (Dict): Calculated growth rates per city and property type
        current_prices (Dict): Current reference prices per city and property type
    """

    def __init__(
        self,
        reference_prices_path: str,
        price_scraper: Optional[ReferencePriceScraper] = None
    ) -> None:
        """
        Initialize PriceEstimator with reference data and optional scraper.

        Args:
            reference_prices_path: Path to CSV with current market prices
            price_scraper: Optional custom scraper instance for price fetching
        """
        self.reference_prices_path = reference_prices_path
        self.price_scraper = price_scraper or ReferencePriceScraper()
        self.growth_rates: Dict[Tuple[str, str], Dict] = {}
        self.current_prices: Optional[Dict[Tuple[str, str], float]] = None

    # --- Reference Prices Management ---
    
    def _load_reference_prices(self) -> bool:
        """
        Load current market reference prices per city and property type.
        
        Returns:
            bool: True if prices were successfully loaded
        """
        try:
            logger.info("Loading reference prices...")
            df = self.load_csv(self.reference_prices_path)
            if df is None:
                logger.error("Failed to load reference prices")
                return False
                
            self.current_prices = {
                (row['city_name'], row['property_type']): row['price_per_m2'] 
                for _, row in df.iterrows()
            }
            
            logger.info(f"Loaded {len(self.current_prices)} reference prices")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load reference prices: {e}")
            return False

    async def _fetch_city_prices(
        self, 
        city: str, 
        zipcode: str
    ) -> Optional[Dict[str, float]]:
        """
        Fetch and save reference prices for a single city.
        
        Args:
            city: City name
            zipcode: Postal code
            
        Returns:
            Optional[Dict[str, float]]: Dictionary with apartment and house prices if successful
        """
        try:
            logger.info(f"Fetching prices for {city} ({zipcode})")
            prices = await self.price_scraper.get_city_prices(city, zipcode)
            
            if not prices:
                logger.warning(f"Could not fetch prices for {city}")
                return None

            # Create DataFrame for new prices
            new_data = pd.DataFrame({
                'city_name': [city, city],
                'zipcode': [zipcode, zipcode],
                'property_type': ['Appartement', 'Maison'],
                'price_per_m2': [
                    prices['apartment_price'],
                    prices['house_price']
                ]
            })
            
            # Save to CSV
            if Path(self.reference_prices_path).exists():
                new_data.to_csv(
                    self.reference_prices_path,
                    mode='a',
                    header=False,
                    index=False
                )
            else:
                new_data.to_csv(
                    self.reference_prices_path,
                    index=False
                )

            logger.info(f"Added reference prices for {city}")
            return prices

        except Exception as e:
            logger.error(f"Failed to fetch prices for {city}: {e}")
            return None

    async def _update_missing_references(
        self,
        df: pd.DataFrame
    ) -> Set[Tuple[str, str]]:
        """
        Update reference prices for all missing cities in the dataset.
        
        Args:
            df: DataFrame containing property data
            
        Returns:
            Set[Tuple[str, str]]: Set of city_keys that could not be updated
        """
        # Find missing cities
        missing_cities = set()
        for idx, row in df.iterrows():
            city_key = (row['city_name'], row['property_type'])
            if city_key not in self.current_prices:
                missing_cities.add((row['city_name'], row['zipcode']))

        if not missing_cities:
            return set()

        # Update missing cities
        logger.info(f"Found {len(missing_cities)} missing cities")
        failed_cities = set()
        
        for city, zipcode in missing_cities:
            prices = await self._fetch_city_prices(city, zipcode)
            if prices:
                self.current_prices[(city, 'Appartement')] = prices['apartment_price']
                self.current_prices[(city, 'Maison')] = prices['house_price']
            else:
                failed_cities.add((city, zipcode))

        return failed_cities

    # --- Growth Rate Calculation ---

    def _calculate_yearly_growth(
        self,
        yearly_means: Dict[int, float]
    ) -> Dict[int, Dict[str, float]]:
        """
        Calculate year-over-year growth rates from mean prices.
        
        Args:
            yearly_means: Dictionary of year -> mean price
            
        Returns:
            Dictionary of year -> growth rate information
        """
        years = sorted(yearly_means.keys())
        yearly_growth = {}
        
        for i in range(len(years)-1):
            year = years[i]
            next_year = years[i+1]
            growth_rate = (yearly_means[next_year] / yearly_means[year]) - 1
            yearly_growth[year] = {
                'growth_rate': growth_rate,
                'from_price': yearly_means[year],
                'to_price': yearly_means[next_year]
            }
            
        return yearly_growth

    def _calculate_growth_rates(self, df: pd.DataFrame) -> bool:
        """
        Calculate historical growth rates from property data.
        
        Args:
            df: DataFrame containing historical property data
            
        Returns:
            bool: True if growth rates were successfully calculated
        """
        try:
            logger.info("Calculating historical growth rates...")
            
            # Prepare data
            df['year'] = pd.to_datetime(df['mutation_date'], format='%d/%m/%Y').dt.year
            df['price_per_m2'] = df['price'] / df['surface_area']
            
            # Calculate rates per city and property type
            for city in df['city_name'].unique():
                for prop_type in ['Appartement', 'Maison']:
                    mask = (df['city_name'] == city) & (df['property_type'] == prop_type)
                    city_data = df[mask]
                    
                    if city_data.empty:
                        logger.debug(f"No data for {city} - {prop_type}")
                        continue
                    
                    # Calculate yearly means
                    yearly_means = city_data.groupby('year')['price_per_m2'].mean().to_dict()
                    
                    # Add current reference price
                    current_price = self.current_prices.get((city, prop_type))
                    if current_price:
                        yearly_means[2024] = current_price
                    
                    # Calculate growth rates
                    yearly_growth = self._calculate_yearly_growth(yearly_means)
                    
                    self.growth_rates[(city, prop_type)] = {
                        'yearly_means': yearly_means,
                        'yearly_growth': yearly_growth
                    }
            
            logger.info(f"Calculated growth rates for {len(self.growth_rates)} city-property combinations")
            return True
            
        except Exception as e:
            logger.error(f"Failed to calculate growth rates: {e}")
            return False

    # --- Price Estimation ---

    def _estimate_property_price(self, row: pd.Series) -> pd.Series:
        """
        Estimate current market value for a single property.
        Keeps all rows but marks estimation status.
        """
        try:
            city = row['city_name']
            prop_type = row['property_type']
            key = (city, prop_type)
            
            # Initialize estimation columns
            row['estimated_price'] = None
            row['initial_price_m2'] = row['price'] / row['surface_area']
            row['final_price_m2'] = None
            row['total_growth_rate'] = None
            row['estimation_status'] = 'NO_REFERENCE'  # Default status
            
            if key not in self.current_prices:
                logger.debug(f"No reference price for {city} - {prop_type}")
                return row
                
            # Get sale date and year
            sale_date = datetime.strptime(row['mutation_date'], '%d/%m/%Y')
            sale_year = sale_date.year
            
            # Handle current year properties
            if sale_year >= 2024:
                row['estimated_price'] = row['price']
                row['final_price_m2'] = row['initial_price_m2']
                row['total_growth_rate'] = 0
                row['estimation_status'] = 'CURRENT_YEAR'
                return row

            if key not in self.growth_rates:
                row['estimation_status'] = 'NO_GROWTH_RATE'
                return row

            # Calculate price evolution
            growth_data = self.growth_rates[key]
            yearly_growth = growth_data['yearly_growth']
            current_price = row['initial_price_m2']
            
            for year in range(sale_year, 2024):
                if year in yearly_growth:
                    growth_info = yearly_growth[year]
                    current_price *= (1 + growth_info['growth_rate'])
            
            # Store results
            row['final_price_m2'] = round(current_price)
            row['estimated_price'] = round(current_price * row['surface_area'])
            row['total_growth_rate'] = round(
                ((current_price / row['initial_price_m2']) - 1) * 100
            )
            row['initial_price_m2'] = round(row['initial_price_m2'])
            row['estimation_status'] = 'SUCCESS'
            
            return row
            
        except Exception as e:
            logger.error(f"Failed to estimate price for property: {e}")
            row['estimation_status'] = 'ERROR'
            return row
        
    async def process(self, input_path: str, output_path: str) -> bool:
        """
        Process property data and estimate current market values.
        
        Args:
            input_path: Path to input CSV file
            output_path: Path for output CSV file
            
        Returns:
            bool: True if processing was successful
        """
        try:
            logger.info(f"Starting price estimation for {input_path}")
            
            # Load reference prices
            if not self._load_reference_prices():
                return False

            # Load and validate input data
            df = self.load_csv(input_path)
            if df is None:
                return False

            required_columns = [
                'city_name', 'zipcode', 'property_type',
                'price', 'surface_area', 'mutation_date'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing required columns: {missing_columns}")
                return False

            # Update missing reference prices
            failed_cities = await self._update_missing_references(df)
            if failed_cities:
                logger.warning(
                    f"Could not update prices for {len(failed_cities)} cities"
                )

            # Calculate growth rates
            if not self._calculate_growth_rates(df):
                return False

            # Process all rows
            processed_rows = []
            for idx, row in df.iterrows():
                processed_row = row.copy()
                processed_row = self._estimate_property_price(processed_row)
                processed_rows.append(processed_row)

            # Create final DataFrame (keeping all rows)
            result_df = pd.DataFrame(processed_rows)

            # Log statistics with more detail
            total_properties = len(result_df)
            estimated_count = len(result_df[result_df['estimation_status'] == 'SUCCESS'])
            no_reference = len(result_df[result_df['estimation_status'] == 'NO_REFERENCE'])
            current_year = len(result_df[result_df['estimation_status'] == 'CURRENT_YEAR'])
            errors = len(result_df[result_df['estimation_status'] == 'ERROR'])

            logger.info(
                f"Processed {total_properties} properties:\n"
                f"- {estimated_count} successfully estimated\n"
                f"- {no_reference} missing reference prices\n"
                f"- {current_year} current year properties\n"
                f"- {errors} estimation errors"
            )

            # Save all rows
            return self.save_csv(result_df, output_path, index=False)

        except Exception as e:
            logger.error(f"Price estimation failed: {e}")
            logger.debug("Error details:", exc_info=True)
            return False