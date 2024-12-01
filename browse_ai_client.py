import os
import time
import logging
import requests
import json
from datetime import datetime, timedelta
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
    
    def fetch_recent_results(
        self,
        hours_back: int = 24,
        output_dir: str = "browse_ai_data",
        check_interval: int = 60
    ) -> List[Dict]:
        """
        Fetch and save results from recent Browse AI searches with pagination.
        
        Args:
            hours_back: Number of hours to look back
            output_dir: Directory to save raw data
            check_interval: Interval between status checks in seconds
            
        Returns:
            List[Dict]: List of all successful tasks
        """
        os.makedirs(output_dir, exist_ok=True)
        start_time = int((datetime.now() - timedelta(hours=hours_back)).timestamp() * 1000)
        
        try:
            # Get initial bulk runs list
            logging.info(f"Fetching bulk runs from the last {hours_back} hours...")
            all_bulk_runs = []
            page = 1
            
            while True:
                response = requests.get(
                    f"{self.base_url}/{self.robot_id}/bulk-runs",
                    headers=self.headers,
                    params={"page": str(page)}
                )
                response.raise_for_status()
                data = response.json()["result"]
                
                # Filter bulk runs by time
                bulk_runs = [
                    run for run in data["items"] 
                    if run["createdAt"] >= start_time
                ]
                all_bulk_runs.extend(bulk_runs)
                
                if not data.get("hasMore", False):
                    break
                page += 1
            
            if not all_bulk_runs:
                logging.info("No recent bulk runs found")
                return []
            
            # Process each bulk run with pagination
            all_results = []
            for bulk_run in all_bulk_runs:
                bulk_run_id = bulk_run["id"]
                timestamp = datetime.fromtimestamp(bulk_run["createdAt"]/1000)
                logging.info(f"Processing bulk run {bulk_run_id} from {timestamp}")
                
                # Wait until all tasks are complete
                while True:
                    current_page = 1
                    bulk_run_tasks = []
                    
                    # Get all pages of tasks for this bulk run
                    while True:
                        response = requests.get(
                            f"{self.base_url}/{self.robot_id}/bulk-runs/{bulk_run_id}",
                            headers=self.headers,
                            params={"page": str(current_page)}
                        )
                        response.raise_for_status()
                        run_data = response.json()["result"]
                        
                        tasks = run_data["robotTasks"]["items"]
                        bulk_run_tasks.extend(tasks)
                        
                        if not run_data["robotTasks"].get("hasMore", False):
                            break
                        current_page += 1
                    
                    # Check if all tasks are complete
                    pending_tasks = [
                        task for task in bulk_run_tasks 
                        if task["status"] not in ["successful", "failed"]
                    ]
                    
                    if not pending_tasks:
                        # Save complete bulk run data
                        output_file = os.path.join(
                            output_dir,
                            f"browse_ai_data_{bulk_run_id}_{timestamp:%Y%m%d_%H%M%S}.json"
                        )
                        
                        complete_data = {
                            "bulkRun": bulk_run,
                            "robotTasks": {
                                "totalCount": len(bulk_run_tasks),
                                "items": bulk_run_tasks
                            }
                        }
                        
                        with open(output_file, 'w', encoding='utf-8') as f:
                            json.dump(complete_data, f, indent=2)
                        
                        logging.info(f"Saved data to {output_file}")
                        
                        # Add successful tasks to results
                        successful_tasks = [
                            task for task in bulk_run_tasks 
                            if task["status"] == "successful"
                        ]
                        all_results.extend(successful_tasks)
                        break
                    
                    logging.info(f"Waiting for {len(pending_tasks)} tasks to complete...")
                    time.sleep(check_interval)
            
            logging.info(f"Retrieved {len(all_results)} successful tasks from {len(all_bulk_runs)} bulk runs")
            return all_results
        
        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed: {e}")
            raise
        except Exception as e:
            logging.error(f"An error occurred: {e}")
            raise