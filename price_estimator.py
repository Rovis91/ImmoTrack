import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from scipy.spatial.distance import cdist
import logging


class AdaptivePriceEstimator:
    """
    An adaptive price estimator for properties based on reference prices and comparable properties.
    """

    def __init__(
        self,
        reference_prices_path: str,
        min_references: int = 5,
        max_references: int = 30,
        initial_radius_km: float = 1,
        max_radius_km: float = 5,
        radius_step_km: float = 1
    ):
        """
        Initialize the price estimator with reference prices and configuration parameters.

        Args:
            reference_prices_path (str): Path to the reference prices CSV file.
            min_references (int): Minimum number of comparable properties.
            max_references (int): Maximum number of comparable properties.
            initial_radius_km (float): Initial search radius in kilometers.
            max_radius_km (float): Maximum search radius in kilometers.
            radius_step_km (float): Step size for increasing the search radius.
        """
        self.params = {
            "min_references": min_references,
            "max_references": max_references,
            "initial_radius_km": initial_radius_km,
            "max_radius_km": max_radius_km,
            "radius_step_km": radius_step_km
        }
        self.reference_prices, self.city_averages = self._load_reference_prices(reference_prices_path)

    def _load_reference_prices(self, path: str) -> Tuple[Dict, Dict]:
        """
        Load reference prices and calculate city averages.

        Args:
            path (str): Path to the CSV file.

        Returns:
            Tuple[Dict, Dict]: A dictionary of reference prices and city averages.
        """
        try:
            df = pd.read_csv(path)
            reference_prices = {}
            city_averages = {}

            for _, row in df.iterrows():
                reference_prices[(row['city_name'], row['property_type'])] = row['price_per_m2']

            for city in df['city_name'].unique():
                city_data = df[df['city_name'] == city]
                city_averages[city] = city_data['price_per_m2'].mean()

            return reference_prices, city_averages
        except Exception as e:
            logging.error("Error loading reference prices: %s", e)
            raise

    @staticmethod
    def _calculate_temporal_score(date_str: str) -> float:
        """
        Calculate a temporal score with exponential decay based on the date.

        Args:
            date_str (str): Date string in 'dd/mm/yyyy' format.

        Returns:
            float: Temporal score between 0 and 1.
        """
        current_date = datetime.now()
        sale_date = datetime.strptime(date_str, '%d/%m/%Y')
        months_diff = (current_date - sale_date).days / 30.44
        return np.exp(-months_diff / 12)

    def _calculate_distance_score(self, distance_km: float) -> float:
        """
        Calculate a distance score based on proximity.

        Args:
            distance_km (float): Distance in kilometers.

        Returns:
            float: Distance score between 0 and 1.
        """
        return max(0, 1 - (distance_km / self.params['max_radius_km']))

    @staticmethod
    def _calculate_dispersion_score(prices_per_m2: np.ndarray) -> float:
        """
        Calculate a dispersion score based on price variability.

        Args:
            prices_per_m2 (np.ndarray): Array of price per square meter values.

        Returns:
            float: Dispersion score between 0 and 1.
        """
        if len(prices_per_m2) < 2:
            return 1.0
        cv = np.std(prices_per_m2) / np.mean(prices_per_m2)
        return max(0, 1 - cv)

    def _find_comparable_properties(
        self,
        target_property: Dict,
        historical_data: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Find comparable properties for the target property using adaptive radius.

        Args:
            target_property (Dict): Dictionary containing target property details.
            historical_data (pd.DataFrame): Historical property data.

        Returns:
            pd.DataFrame: DataFrame of comparable properties.
        """
        filtered_data = historical_data[
            (historical_data['property_type'] == target_property['property_type']) &
            (pd.to_datetime(historical_data['mutation_date'], format='%d/%m/%Y') >
             datetime.now() - pd.DateOffset(years=2))
        ].copy()

        if filtered_data.empty:
            return pd.DataFrame()

        target_coords = np.array([[target_property['latitude'], target_property['longitude']]])
        property_coords = filtered_data[['latitude', 'longitude']].values
        distances_km = cdist(target_coords, property_coords, metric='euclidean')[0] * 111
        filtered_data['distance_km'] = distances_km

        radius = self.params['initial_radius_km']
        comparable_properties = pd.DataFrame()

        while radius <= self.params['max_radius_km']:
            comparable_properties = filtered_data[filtered_data['distance_km'] <= radius].sort_values('distance_km')
            if len(comparable_properties) >= self.params['min_references']:
                break
            radius += self.params['radius_step_km']

        return comparable_properties.head(self.params['max_references'])

    def estimate_price(
        self,
        target_property: Dict,
        historical_data: pd.DataFrame
    ) -> Tuple[Optional[float], Dict]:
        """
        Estimate the price of a property with a confidence score.

        Args:
            target_property (Dict): Dictionary containing target property details.
            historical_data (pd.DataFrame): Historical property data.

        Returns:
            Tuple[Optional[float], Dict]: Estimated price and details dictionary.
        """
        ref_key = (target_property['city_name'], target_property['property_type'])
        reference_price_m2 = self.reference_prices.get(ref_key)

        if not reference_price_m2:
            city_average = self.city_averages.get(target_property['city_name'])
            if city_average:
                logging.info("Using city average price for %s", ref_key)
                return city_average * target_property['surface_area'], {
                    "method": "city_average",
                    "price_per_m2": city_average,
                    "property_type": target_property['property_type']
                }
            logging.warning("No reference price found for city %s", target_property['city_name'])
            return None, {"error": "No reference price available"}

        comparables = self._find_comparable_properties(target_property, historical_data)
        if comparables.empty:
            return reference_price_m2 * target_property['surface_area'], {
                "warning": "No comparable properties found",
                "method": "reference_only"
            }

        comparables['temporal_score'] = comparables['mutation_date'].apply(self._calculate_temporal_score)
        comparables['distance_score'] = comparables['distance_km'].apply(self._calculate_distance_score)
        prices_per_m2 = comparables['price'] / comparables['surface_area']
        dispersion_score = self._calculate_dispersion_score(prices_per_m2.values)
        comparables['overall_score'] = (
            comparables['temporal_score'] + comparables['distance_score'] + dispersion_score
        ) / 3

        city_confidence_score = comparables['overall_score'].mean()
        weighted_price_m2 = (prices_per_m2 * comparables['overall_score']).sum() / comparables['overall_score'].sum()
        calculated_price = weighted_price_m2 * target_property['surface_area']
        reference_price = reference_price_m2 * target_property['surface_area']
        final_price = (city_confidence_score * calculated_price + (1 - city_confidence_score) * reference_price)

        details = {
            "final_price": final_price,
            "confidence_score": city_confidence_score,
            "comparable_count": len(comparables),
            "reference_price": reference_price,
            "calculated_price": calculated_price,
            "comparables": comparables[[
                'complete_address', 'price', 'surface_area', 'distance_km',
                'temporal_score', 'overall_score'
            ]].to_dict('records')
        }

        return final_price, details
