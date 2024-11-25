import requests
import time
import logging
from typing import Dict, Tuple, Optional
import pandas as pd


class GeocodingClient:
    """
    A client to interact with the French government's geocoding API.
    """

    def __init__(self):
        """
        Initialize the GeocodingClient with the API base URL and rate-limiting settings.
        """
        self.base_url = "https://api-adresse.data.gouv.fr/search/"
        self.last_request_time = 0
        self.min_delay = 0.02  # 20ms delay between requests

    def _wait_if_needed(self):
        """
        Ensure the minimum delay between API requests is respected.
        """
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_delay:
            time.sleep(self.min_delay - time_since_last)
        self.last_request_time = time.time()

    def get_coordinates(self, address: str, city: str = "PARIS") -> Optional[Tuple[float, float]]:
        """
        Retrieve geographic coordinates for a given address and city.

        Args:
            address (str): The address to geocode.
            city (str): The city associated with the address (default: "PARIS").

        Returns:
            Optional[Tuple[float, float]]: A tuple (longitude, latitude) if successful, otherwise None.
        """
        self._wait_if_needed()

        query = f"{address} {city}"
        params = {
            "q": query,
            "limit": 1,
            "autocomplete": 0,
        }

        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            data = response.json()

            if data.get("features"):
                coordinates = data["features"][0]["geometry"]["coordinates"]
                return coordinates[0], coordinates[1]  # longitude, latitude

        except requests.RequestException as e:
            logging.error("Error making request for address '%s': %s", query, str(e))
        except KeyError as e:
            logging.error("Unexpected response format for address '%s': %s", query, str(e))
        except Exception as e:
            logging.error("Error processing geocoding for address '%s': %s", query, str(e))

        return None


def add_coordinates_to_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add geographic coordinates (longitude and latitude) to a DataFrame.

    Args:
        df (pd.DataFrame): The DataFrame containing addresses to geocode.

    Returns:
        pd.DataFrame: The DataFrame with added longitude and latitude columns.

    Raises:
        ValueError: If required columns are missing.
    """
    logging.info("Starting the geocoding process...")

    # Check for required columns
    required_columns = ['complete_address', 'city_name']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logging.error("Missing required columns: %s", missing_columns)
        logging.error("Available columns: %s", df.columns.tolist())
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Initialize new columns for coordinates
    df['longitude'] = None
    df['latitude'] = None

    geocoder = GeocodingClient()
    total_addresses = len(df)
    success_count = 0

    # Iterate over DataFrame rows
    for idx, row in df.iterrows():
        if idx % 10 == 0:  # Log progress every 10 rows
            logging.info("Geocoding progress: %d/%d addresses", idx, total_addresses)

        try:
            coordinates = geocoder.get_coordinates(
                address=row['complete_address'],
                city=row['city_name']
            )

            if coordinates:
                df.at[idx, 'longitude'] = coordinates[0]
                df.at[idx, 'latitude'] = coordinates[1]
                success_count += 1

        except Exception as e:
            logging.error("Error geocoding row %d: %s", idx, str(e))
            continue

    logging.info("Geocoding completed. %d/%d addresses successfully geocoded.", success_count, total_addresses)
    return df
