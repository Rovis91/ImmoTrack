import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Optional, Tuple
import logging

class GrowthPriceEstimator:
    def __init__(self, reference_prices_path: str):
        """
        Initialize with current market reference prices (2024 prices).
        
        Args:
            reference_prices_path: Path to CSV with current market prices per city
        """
        self.current_prices = self._load_reference_prices(reference_prices_path)
        self.growth_rates = {}

    def _load_reference_prices(self, path: str) -> Dict:
        """Load current market reference prices per city and property type."""
        df = pd.read_csv(path)
        return {(row['city_name'], row['property_type']): row['price_per_m2'] 
                for _, row in df.iterrows()}

    def calculate_growth_rates(self, historical_data: pd.DataFrame) -> None:
        """
        Calculate yearly means and growth rates between consecutive years.
        """
        # Convert dates to years and calculate price per m²
        historical_data['year'] = pd.to_datetime(
            historical_data['mutation_date'], 
            format='%d/%m/%Y'
        ).dt.year
        historical_data['price_per_m2'] = historical_data['price'] / historical_data['surface_area']
        
        growth_rates = {}
        
        # Calculate for each city and property type
        for city in historical_data['city_name'].unique():
            for prop_type in ['Appartement', 'Maison']:
                # Filter data
                mask = (historical_data['city_name'] == city) & \
                       (historical_data['property_type'] == prop_type)
                city_data = historical_data[mask]
                
                if len(city_data) > 0:
                    # Calculate mean price per m² for each year
                    yearly_means = city_data.groupby('year')['price_per_m2'].mean().to_dict()
                    
                    # Get current reference price (2024)
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
                    
                    growth_rates[(city, prop_type)] = {
                        'yearly_means': yearly_means,
                        'yearly_growth': yearly_growth
                    }
        
        self.growth_rates = growth_rates

    def estimate_price(self, property_data: Dict) -> Tuple[Optional[float], Dict]:
        """
        Estimate current (2024) market value for a property using yearly growth rates.
        """
        city = property_data['city_name']
        prop_type = property_data['property_type']
        key = (city, prop_type)
        
        if key not in self.growth_rates:
            return None, {"error": f"No growth data for {city} - {prop_type}"}
        
        sale_date = datetime.strptime(property_data['mutation_date'], '%d/%m/%Y')
        sale_year = sale_date.year
        initial_price_m2 = property_data['price'] / property_data['surface_area']
        
        growth_data = self.growth_rates[key]
        yearly_means = growth_data['yearly_means']
        yearly_growth = growth_data['yearly_growth']
        
        # Project price using successive yearly growth rates
        current_price = initial_price_m2
        applied_rates = []
        
        for year in range(sale_year, 2024):
            if year in yearly_growth:
                growth_info = yearly_growth[year]
                current_price *= (1 + growth_info['growth_rate'])
                applied_rates.append({
                    'year': year,
                    'rate': growth_info['growth_rate'],
                    'price': current_price
                })
        
        return current_price * property_data['surface_area'], {
            "method": "yearly_growth",
            "initial_price_m2": initial_price_m2,
            "projected_price_m2": current_price,
            "yearly_means": yearly_means,
            "applied_rates": applied_rates,
            "current_reference_price": yearly_means[2024]
        }