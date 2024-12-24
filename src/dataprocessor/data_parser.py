"""
Parser module for extracting structured data from HTML property elements.
"""
import json
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Optional
from bs4 import BeautifulSoup
from .processor_base import ProcessorBase

logger = logging.getLogger(__name__)

class DataParser(ProcessorBase):
    """Extracts structured data from raw HTML property elements."""

    def _parse_property(self, html_content: str) -> Optional[Dict]:
        """
        Parse a single property's HTML content.
        
        Args:
            html_content: Raw HTML string
            
        Returns:
            Dict of parsed property data if successful, None otherwise
        """
        try:
            soup = BeautifulSoup(html_content, 'lxml')
            property_data = {}

            # Address
            address_tag = soup.find('p', class_='text-gray-700 font-bold truncate')
            if address_tag:
                address = address_tag.text.strip()
                if ' - ' in address:
                    parts = address.rsplit(' - ', 1)
                    property_data["complete_address"] = parts[0].strip()
                    property_data["city_name"] = parts[1].strip()
                else:
                    property_data["complete_address"] = address
                    property_data["city_name"] = ""

            # Property type
            type_tag = soup.find('p', class_='flex items-center text-sm text-gray-400')
            property_data["property_type"] = type_tag.span.text.strip() if type_tag and type_tag.span else ""

            # Price
            price_tag = soup.find('p', class_='text-primary-500 font-bold whitespace-nowrap')
            if price_tag and price_tag.span:
                price_text = price_tag.span.text.replace('€', '').replace(' ', '').replace('.', '')
                property_data["price"] = int(price_text) if price_text.isdigit() else 0
            else:
                property_data["price"] = 0

            # Number of rooms
            rooms_tag = (
                soup.find('svg', class_='fa-objects-column')
                .find_next('span', class_='font-semibold') 
                if soup.find('svg', class_='fa-objects-column') else None
            )
            property_data["rooms"] = int(rooms_tag.text.strip()) if rooms_tag else None

            # Surface area
            surface_tag = (
                soup.find('svg', class_='fa-ruler-combined')
                .find_next('span', class_='font-semibold') 
                if soup.find('svg', class_='fa-ruler-combined') else None
            )
            if surface_tag:
                surface_text = surface_tag.text.replace(',', '.').strip()
                property_data["surface_area"] = float(surface_text) if surface_text.replace('.', '').isdigit() else None
            else:
                property_data["surface_area"] = None

            # Mutation date
            date_tag = soup.find('time')
            if date_tag and date_tag.get('datetime'):
                try:
                    timestamp = int(date_tag['datetime']) // 1000
                    mutation_date = datetime.fromtimestamp(timestamp)
                    property_data["mutation_date"] = mutation_date.strftime('%d/%m/%Y')
                except (ValueError, TypeError) as e:
                    logger.warning(f"Failed to parse mutation date: {e}")
                    property_data["mutation_date"] = None
            else:
                property_data["mutation_date"] = None

            # Analysis link
            analysis_link = soup.find('a', attrs={
                'class': lambda x: x and all(c in x.split() for c in ['whitespace-nowrap', 'border', 'bg-primary-500'])
            })
            if analysis_link and analysis_link.get('href'):
                href = analysis_link.get('href')
                property_data["analysis_url"] = f"https://www.immo-data.fr{href}" if href.startswith('/') else href
            else:
                logger.warning(f"Analysis link not found in HTML")

            return property_data

        except Exception as e:
            logger.error(f"Error parsing property: {e}")
            return None

    def process(self, input_path: str, output_path: str) -> bool:
        """
        Process raw scraping data and save parsed results.
        """
        try:
            # First load the raw JSON file properly
            with open(input_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)

            # Parse all properties
            parsed_properties = []
            total_count = 0

            # Iterate through results structure
            for result in raw_data.get("results", []):
                for property_data in result.get("properties", []):
                    total_count += 1
                    if html_content := property_data.get("html"):
                        if parsed_prop := self._parse_property(html_content):
                            parsed_properties.append(parsed_prop)
                            
            if not parsed_properties:
                logger.error("No properties were successfully parsed")
                return False

            # Convert parsed data to DataFrame
            parsed_df = pd.DataFrame(parsed_properties)
            
            # Log parsing results
            success_rate = (len(parsed_properties) / total_count * 100) if total_count > 0 else 0
            logger.info(f"Parsed {len(parsed_properties)}/{total_count} properties ({success_rate:.1f}%)")
            
            # Save using parent class method
            return self.save_csv(parsed_df, output_path)

        except Exception as e:
            logger.error(f"Parsing failed: {str(e)}")
            logger.debug("Error details:", exc_info=True)
            return False