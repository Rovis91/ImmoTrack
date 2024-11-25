import os
import time
import logging
import requests
from dotenv import load_dotenv
from typing import List, Dict

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class BrowseAIClient:
    """
    A client to interact with the Browse AI API for creating and managing bulk scraping runs.
    """
    def __init__(self):
        """
        Initialize the BrowseAIClient with the API key and robot ID from environment variables.
        """
        self.api_key = os.getenv('BROWSE_AI_API_KEY')
        if not self.api_key:
            logging.error("BROWSE_AI_API_KEY is not set in environment variables.")
            raise ValueError("API key is required.")
        
        self.robot_id = os.getenv('ROBOT_ID')
        if not self.robot_id:
            logging.error("ROBOT_ID is not set in environment variables.")
            raise ValueError("Robot ID is required.")
        
        self.base_url = "https://api.browse.ai/v2/robots"
        self.headers = {"Authorization": f"Bearer {self.api_key}"}

    def create_bulk_run(self, urls: List[str], elements_limit: int = 100) -> str:
        """
        Creates a bulk scraping run with a list of URLs and a limit of elements per request.

        Args:
            urls (List[str]): List of URLs to scrape.
            elements_limit (int): Maximum number of elements to scrape per URL.

        Returns:
            str: The ID of the created bulk run.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        logging.info("Creating bulk run with %d URLs and an elements limit of %d.", len(urls), elements_limit)
        
        payload = {
            "title": "Immo Data Scraping",
            "inputParameters": [
                {
                    "originUrl": url,
                    "elements_limit": elements_limit
                }
                for url in urls
            ]
        }

        try:
            response = requests.post(
                f"{self.base_url}/{self.robot_id}/bulk-runs",
                json=payload,
                headers=self.headers
            )
            response.raise_for_status()
            bulk_run_id = response.json()["result"]["bulkRun"]["id"]
            logging.info("Bulk run created successfully with ID: %s", bulk_run_id)
            return bulk_run_id
        except requests.RequestException as e:
            logging.error("Failed to create bulk run: %s", e)
            raise

    def get_bulk_run_status(self, bulk_run_id: str) -> Dict:
        """
        Retrieves the status of a bulk run.

        Args:
            bulk_run_id (str): The ID of the bulk run to check.

        Returns:
            Dict: The status of the bulk run.

        Raises:
            requests.HTTPError: If the API request fails.
        """
        logging.info("Fetching status for bulk run ID: %s", bulk_run_id)
        
        try:
            response = requests.get(
                f"{self.base_url}/{self.robot_id}/bulk-runs/{bulk_run_id}",
                headers=self.headers
            )
            response.raise_for_status()
            return response.json()["result"]
        except requests.RequestException as e:
            logging.error("Failed to fetch bulk run status: %s", e)
            raise

    def wait_for_bulk_run(self, bulk_run_id: str, check_interval: int = 60) -> Dict:
        """
        Waits for a bulk run to complete by periodically checking its status.

        Args:
            bulk_run_id (str): The ID of the bulk run to monitor.
            check_interval (int): Time in seconds between status checks.

        Returns:
            Dict: The final status of the bulk run.

        Raises:
            requests.HTTPError: If the API request fails during status checks.
        """
        logging.info("Waiting for bulk run ID: %s to complete.", bulk_run_id)
        
        while True:
            try:
                status = self.get_bulk_run_status(bulk_run_id)
                if all(task.get("status") == "successful" for task in status["robotTasks"]["items"]):
                    logging.info("Bulk run completed successfully.")
                    return status
                logging.info("Bulk run not complete. Checking again in %d seconds.", check_interval)
                time.sleep(check_interval)
            except Exception as e:
                logging.error("Error while waiting for bulk run completion: %s", e)
                raise
