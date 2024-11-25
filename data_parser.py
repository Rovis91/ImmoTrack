import logging
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Dict, List

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def parse_property(element_data: Dict) -> Dict:
    """
    Parse property data from an element returned by Browse AI.

    Args:
        element_data (Dict): Dictionary containing the property data to parse.

    Returns:
        Dict: Parsed property with structured information.

    Raises:
        ValueError: If a parsing issue occurs.
    """
    try:
        property_data = {}
        html_string = element_data.get('element', '')
        soup = BeautifulSoup(html_string, 'lxml')

        # Address
        address_tag = soup.find('p', class_='text-gray-700 font-bold truncate')
        address = address_tag.text.strip() if address_tag else ""
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
                timestamp = int(date_tag['datetime']) // 1000  # Browse AI timestamps are in milliseconds
                mutation_date = datetime.fromtimestamp(timestamp)
                property_data["mutation_date"] = mutation_date.strftime('%d/%m/%Y')
            except (ValueError, TypeError) as e:
                logging.warning("Failed to parse mutation date: %s", e)
                property_data["mutation_date"] = None
        else:
            property_data["mutation_date"] = None

        return property_data
    
    except Exception as e:
        logging.error("Error parsing property. Element data: %s", element_data)
        logging.error("Exception details: %s", e)
        raise

def parse_browse_ai_response(browse_ai_data: Dict) -> List[Dict]:
    """
    Parse the full Browse AI response to extract all properties.

    Args:
        browse_ai_data (Dict): Full JSON response from Browse AI.

    Returns:
        List[Dict]: List of parsed properties.

    Raises:
        ValueError: If a parsing issue occurs for any property.
    """
    properties = []
    
    # Iterate through tasks in the bulk run
    for task in browse_ai_data.get("robotTasks", {}).get("items", []):
        if task.get("status") == "successful":
            # Extract elements from the captured list
            elements = task.get("capturedLists", {}).get("elements", [])
            
            # Parse each element
            for element in elements:
                try:
                    parsed_property = parse_property(element)
                    properties.append(parsed_property)
                except Exception as e:
                    logging.error("Error parsing a property: %s", e)
                    continue
    
    return properties
