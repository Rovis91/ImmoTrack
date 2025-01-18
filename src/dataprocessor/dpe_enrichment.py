"""
DPE (Energy Performance) enrichment service with concurrent processing.
Handles mass DPE data retrieval using ADEME API with proxy rotation.
"""

import logging
import os
import re
import time
import json
import asyncio
import aiohttp
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd
import random
from dotenv import load_dotenv

from .processor_base import ProcessorBase

logger = logging.getLogger(__name__)

@dataclass
class DPEEndpoint:
    """Configuration for DPE API endpoint with time validity range.
    
    Attributes:
        name: Endpoint path name
        start_date: Start date of validity
        end_date: End date of validity
        priority: Processing priority (lower = higher)
    """
    name: str
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    priority: int = 0

    def is_applicable(self, property_date: date) -> bool:
        """Check if endpoint is applicable for a given date."""
        if self.start_date and property_date < self.start_date:
            return False
        if self.end_date and property_date > self.end_date:
            return False
        return True

class ProxyManager:
    """Manages Bright Data proxy connections with concurrent request support."""

    def __init__(self) -> None:
        """Initialize proxy manager with environment credentials."""
        load_dotenv()
        
        self._username = os.getenv('BRIGHT_DATA_GOV_USERNAME')
        self._password = os.getenv('BRIGHT_DATA_GOV_PASSWORD')
        self._port = int(os.getenv('BRIGHT_DATA_PORT', '22225'))
        
        if not all([self._username, self._password]):
            raise ValueError("Bright Data credentials not found in environment")
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._proxy_timeout = aiohttp.ClientTimeout(total=5)

    async def __aenter__(self) -> 'ProxyManager':
        """Set up async context with session initialization."""
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Clean up resources on context exit."""
        if self._session:
            await self._session.close()
            self._session = None

    def _get_proxy_url(self) -> str:
        """Generate rotating proxy URL."""
        session_id = str(random.random())
        return (
            f'http://{self._username}-country-fr-session-{session_id}:'
            f'{self._password}@brd.superproxy.io:{self._port}'
        )

    async def make_request(self, url: str, params: Dict) -> Optional[Dict]:
        """Make async request through proxy with automatic rotation."""
        if not self._session:
            raise RuntimeError("ProxyManager must be used as context manager")

        try:
            async with self._session.get(
                url,
                params=params,
                proxy=self._get_proxy_url(),
                timeout=self._proxy_timeout,
                headers={'Accept': 'application/json'}
            ) as response:
                if response.status == 200:
                    return await response.json()
                return None

        except Exception as e:
            logger.debug(f"Request failed: {str(e)}")
            return None

class DPEService(ProcessorBase):
    """Service for enriching addresses with DPE data using concurrent processing."""

    ENDPOINTS = [
        DPEEndpoint("dpe-france/lines", end_date=date(2021, 6, 1), priority=1),
        DPEEndpoint("dpe-v2-logements-existants/lines", start_date=date(2021, 6, 1), priority=2),
        DPEEndpoint("dpe-v2-logements-neufs/lines", start_date=date(2021, 6, 1), priority=3),
        DPEEndpoint("audit-opendata/lines", start_date=date(2023, 1, 1), priority=4)
    ]

    BASE_URL = "https://data.ademe.fr/data-fair/api/v1/datasets/"
    MAX_CONCURRENT_REQUESTS = 500
    SAVE_BATCH_SIZE = 100
    MAX_RETRIES = 3
    IGNORE_FIELDS = {
        'tr001_modele_dpe_type_libelle', 'geopoint', 'latitude', 'i',
        'geo_adresse', 'rand', 'code_insee_commune_actualise',
        'version_methode_dpe', 'nom_methode_dpe', 'tv016_departement_code',
        'longitude', 'id', 'year', 'price_per_m2', 'initial_price_m2',
        'estimation_status', 'zipcode'
    }

    def __init__(self) -> None:
        """Initialize service with concurrent processing controls."""
        self.proxy_manager = ProxyManager()
        self._semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_REQUESTS)
        self._results_queue = asyncio.Queue()
        self._save_lock = asyncio.Lock()
        self._processed_count = 0
        self._total_count = 0
        self._found_dpe_count = 0
        self._last_progress_time = time.time()

    async def process(self, input_path: str, output_path: str, **kwargs) -> bool:
        """Process CSV file with concurrent DPE lookups."""
        try:
            df = self.load_csv(input_path)
            if df is None:
                return False

            start_idx = self._get_last_processed_index(df)
            self._total_count = len(df)
            self._processed_count = start_idx + 1

            logger.info(f"Processing {self._total_count - start_idx - 1} addresses")

            async with self.proxy_manager:
                saver_task = asyncio.create_task(self._save_results(df, output_path))
                
                tasks = []
                for idx in range(start_idx + 1, len(df)):
                    tasks.append(self._process_address(idx, df.iloc[idx]))
                    
                    if len(tasks) >= self.MAX_CONCURRENT_REQUESTS:
                        await asyncio.gather(*tasks)
                        tasks = []

                if tasks:
                    await asyncio.gather(*tasks)

                await self._results_queue.put(None)
                await saver_task

            return True

        except Exception as e:
            logger.error(f"Processing failed: {str(e)}")
            return False

    def _get_last_processed_index(self, df: pd.DataFrame) -> int:
        """Find index of last row with DPE data."""
        dpe_columns = [col for col in df.columns if col.startswith('dpe_')]
        if not dpe_columns:
            return -1

        for idx in range(len(df) - 1, -1, -1):
            if any(pd.notna(df.iloc[idx][col]) for col in dpe_columns):
                return idx
        return -1

    def _get_applicable_endpoints(self, property_date: date) -> List[DPEEndpoint]:
        """Get endpoints applicable for given date, sorted by priority."""
        applicable = [
            endpoint for endpoint in self.ENDPOINTS 
            if endpoint.is_applicable(property_date)
        ]
        return sorted(applicable, key=lambda x: x.priority)

    def _validate_address_match(self, input_addr: str, dpe_addr: str, city_name: str) -> bool:
        """
        Validate if DPE address matches input address with strict number matching
        and flexible street name comparison.
        """
        def clean_text(text: str) -> str:
            text = text.lower()
            replacements = {
                'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
                'à': 'a', 'â': 'a', 'ä': 'a',
                'ì': 'i', 'î': 'i', 'ï': 'i',
                'ò': 'o', 'ó': 'o', 'ô': 'o', 'ö': 'o',
                'ù': 'u', 'û': 'u', 'ü': 'u',
                'ç': 'c',
                '-': '', "'": '', '.': '',
                ',': '', '_': '', ' ': ''
            }
            for old, new in replacements.items():
                text = text.replace(old, new)
            
            return ''.join(c for c in text if c.isalnum())

        def remove_common_words(text: str) -> str:
            common_words = {'de', 'du', 'des', 'le', 'la', 'les', 'l', 'et', 'en', 'sur'}
            words = {clean_text(w) for w in text.split()}
            return ''.join(w for w in words if w not in common_words)

        # Clean and normalize each part
        clean_input_addr = clean_text(input_addr)
        clean_dpe_addr = clean_text(dpe_addr)
        clean_city = clean_text(city_name)

        # Verify city presence in DPE address
        if clean_city not in clean_dpe_addr:
            return False

        # Get first continuous number from both addresses
        input_number = ''.join(c for c in clean_input_addr if c.isdigit())[:3]
        dpe_number = ''.join(c for c in clean_dpe_addr if c.isdigit())[:3]

        if not input_number or not dpe_number:
            return False

        if input_number != dpe_number:
            return False

        # Remove numbers and clean street names
        input_street = remove_common_words(''.join(c for c in clean_input_addr if not c.isdigit()))
        dpe_street = remove_common_words(''.join(c for c in clean_dpe_addr if not c.isdigit()))

        # Check if all input street characters are in DPE street
        return input_street in dpe_street
    
    async def _process_address(self, idx: int, row: pd.Series) -> None:
        """Process single address with concurrency control."""
        async with self._semaphore:
            try:
                property_date = datetime.strptime(
                    row['mutation_date'], 
                    '%d/%m/%Y'
                ).date()

                endpoints = self._get_applicable_endpoints(property_date)
                if dpe_data := await self._get_dpe_data(
                    row['complete_address'],
                    property_date,
                    row['city_name'],
                    endpoints
                ):
                    await self._results_queue.put((idx, dpe_data))
                    self._found_dpe_count += 1

                self._processed_count += 1
                self._show_progress()

            except Exception as e:
                logger.debug(f"Error processing address {idx}: {str(e)}")

    async def _save_results(self, df: pd.DataFrame, output_path: str) -> None:
        """Save results in batches to reduce I/O."""
        pending_saves = 0

        while True:
            result = await self._results_queue.get()
            if result is None:
                if pending_saves > 0:
                    self.save_csv(df, output_path)
                break

            idx, dpe_data = result
            for key, value in dpe_data.items():
                df.loc[idx, f"dpe_{key}"] = value

            pending_saves += 1
            if pending_saves >= self.SAVE_BATCH_SIZE:
                self.save_csv(df, output_path)
                pending_saves = 0

            self._results_queue.task_done()

    async def _get_dpe_data(
        self, 
        address: str, 
        property_date: date,
        city_name: str,
        endpoints: List[DPEEndpoint]
    ) -> Optional[Dict]:
        """Get DPE data from applicable endpoints."""
        retries = 0
        while retries < self.MAX_RETRIES:
            for endpoint in endpoints:
                try:
                    params = {"q": address}
                    url = f"{self.BASE_URL}{endpoint.name}"

                    if response := await self.proxy_manager.make_request(url, params):
                        if results := response.get("results", []):
                            for dpe_data in results:
                                if dpe_address := dpe_data.get('geo_adresse'):
                                    if self._validate_address_match(
                                        address, 
                                        dpe_address,
                                        city_name
                                    ):
                                        return {
                                            k: v for k, v in dpe_data.items()
                                            if k not in self.IGNORE_FIELDS 
                                            and v is not None
                                        }

                except Exception as e:
                    logger.debug(f"Error fetching DPE data: {str(e)}")
                    continue

            retries += 1

        return None

    def _show_progress(self) -> None:
        """Display progress with completion percentage."""
        if self._processed_count % 100 == 0:
            percentage = (self._processed_count / self._total_count) * 100
            found_percentage = (
                self._found_dpe_count / self._processed_count * 100
                if self._processed_count > 0 else 0
            )
            
            logger.info(
                f"Progress: {self._processed_count}/{self._total_count} "
                f"({percentage:.1f}%) - Found DPE: {self._found_dpe_count} "
                f"({found_percentage:.1f}%)"
            )
