# src/dataprocessor/price_estimator.py

"""
Price estimation module for calculating current market values based on historical data.
"""
import logging
import json
from datetime import datetime
from typing import Dict, Optional
import pandas as pd
from .processor_base import ProcessorBase

logger = logging.getLogger(__name__)

class PriceEstimator(ProcessorBase):
    """Estimates current market values using historical growth rates."""

    def __init__(self, reference_prices_path: str):
        """
        Initialize with reference prices data.
        
        Args:
            reference_prices_path: Path to CSV with current market prices per city
        """
        self.reference_prices_path = reference_prices_path
        self.growth_rates = {}
        self.current_prices = None

    def _load_reference_prices(self) -> bool:
        """Load current market reference prices per city and property type."""
        try:
            logger.info("Loading reference prices...")
            df = self.load_csv(self.reference_prices_path)
            if df is None:
                logger.error("Failed to load reference prices. File is empty or invalid.")
                return False
                
            self.current_prices = {
                (row['city_name'], row['property_type']): row['price_per_m2'] 
                for _, row in df.iterrows()
            }
            logger.info(f"Loaded reference prices for {len(self.current_prices)} city-property combinations.")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load reference prices: {e}")
            return False

    def _calculate_growth_rates(self, df: pd.DataFrame) -> bool:
        """Calculate growth rates from historical data."""
        try:
            logger.info("Calculating growth rates from historical data...")
            
            # Add year and price per m²
            df['year'] = pd.to_datetime(df['mutation_date'], format='%d/%m/%Y').dt.year
            df['price_per_m2'] = df['price'] / df['surface_area']
            
            for city in df['city_name'].unique():
                for prop_type in ['Appartement', 'Maison']:
                    mask = (df['city_name'] == city) & (df['property_type'] == prop_type)
                    city_data = df[mask]
                    
                    if city_data.empty:
                        logger.info(f"No data for {city} - {prop_type}, skipping...")
                        continue
                    
                    yearly_means = city_data.groupby('year')['price_per_m2'].mean().to_dict()
                    
                    # Add current reference price if available
                    current_price = self.current_prices.get((city, prop_type))
                    if current_price:
                        yearly_means[2024] = current_price
                    
                    # Calculate year-over-year growth rates
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
                    
                    self.growth_rates[(city, prop_type)] = {
                        'yearly_means': yearly_means,
                        'yearly_growth': yearly_growth
                    }
                    
            logger.info(f"Calculated growth rates for {len(self.growth_rates)} city-property combinations.")
            return True
            
        except Exception as e:
            logger.error(f"Failed to calculate growth rates: {e}")
            return False

    def _estimate_property_price(self, row: pd.Series) -> pd.Series:
        """
        Estimate current market value for a single property.
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
            
            if key not in self.growth_rates:
                return row
            
            sale_date = datetime.strptime(row['mutation_date'], '%d/%m/%Y')
            sale_year = sale_date.year
            
            if sale_year >= 2024:
                row['estimated_price'] = row['price']
                row['final_price_m2'] = row['initial_price_m2']
                row['total_growth_rate'] = 0
                return row

            growth_data = self.growth_rates[key]
            yearly_growth = growth_data['yearly_growth']
            
            # Calculate price evolution
            current_price = row['initial_price_m2']
            
            for year in range(sale_year, 2024):
                if year in yearly_growth:
                    growth_info = yearly_growth[year]
                    current_price *= (1 + growth_info['growth_rate'])
            
            # Store results in columns
            row['final_price_m2'] = round(current_price)
            row['estimated_price'] = round(current_price * row['surface_area'])
            row['total_growth_rate'] = round(((current_price / row['initial_price_m2']) - 1) * 100)
            row['initial_price_m2'] = round(row['initial_price_m2'])
            
            return row
            
        except Exception as e:
            logger.error(f"Failed to estimate price for property: {e}")
            return row

    def process(self, input_path: str, output_path: str) -> bool:
        """
        Process property data file and add price estimates.
        """
        try:
            logger.info(f"Starting price estimation for file: {input_path}")
            
            if not self._load_reference_prices():
                logger.error("Failed to load reference prices.")
                return False

            df = self.load_csv(input_path)
            if df is None:
                logger.error("Input file is empty or invalid.")
                return False

            required_columns = ['city_name', 'property_type', 'price', 'surface_area', 'mutation_date']
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(f"Missing required columns in input data: {missing_columns}")
                return False

            if not self._calculate_growth_rates(df):
                logger.error("Failed to calculate growth rates.")
                return False

            df = df.apply(self._estimate_property_price, axis=1)
            total_properties = len(df)

            # Drop unwanted columns before saving
            columns_to_exclude = ['estimation_status', 'applied_rates_json']
            df = df.drop(columns=[col for col in columns_to_exclude if col in df.columns])

            # Round numeric columns to integers
            numeric_columns = ['price_per_m2', 'estimated_price', 'initial_price_m2', 'final_price_m2', 'total_growth_rate']
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: int(round(x)) if pd.notnull(x) else '')

            successful_estimates = df[df['estimated_price'].notnull()].shape[0]
            logger.info(f"Estimated prices for {successful_estimates}/{total_properties} properties.")

            return self.save_csv(df, output_path, index=False)
            
        except Exception as e:
            logger.error(f"Price estimation failed: {e}")
            return False