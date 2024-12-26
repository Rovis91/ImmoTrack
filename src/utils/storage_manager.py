from dataclasses import dataclass
from typing import Dict, List, Set, Tuple, Optional
from enum import Enum
import pandas as pd
import logging
import uuid
from datetime import datetime
from pathlib import Path

class ColumnAction(Enum):
    """Possible actions for column handling."""
    RENAME = "rename"
    KEEP = "keep"
    REMOVE = "remove"

class DataFormat:
    """Manages column specifications and data formatting rules."""

    # Required columns in final format
    REQUIRED_COLUMNS = {"address", "city", "sale_date"}
    
    # Column mapping specifications
    COLUMN_SPECS = {
        # Location data
        "complete_address": ("address", "string", "rename"),
        "city_name": ("city", "string", "rename"),
        "zipcode": ("zipcode", "integer", "keep"),
        "insee_code": ("insee_code", "string", "keep"),
        "region": ("region", "string", "keep"),
        "longitude": ("longitude", "float", "keep"),
        "latitude": ("latitude", "float", "keep"),
        
        # Property details
        "property_type": ("type", "string", "rename"),
        "price": ("price", "integer", "keep"),
        "rooms": ("rooms", "integer", "keep"),
        "surface_area": ("surface", "integer", "rename"),
        "mutation_date": ("sale_date", "date", "rename"),
        "analysis_url": ("analysis_url", "string", "keep"),
        
        # DPE data
        "dpe_classe_consommation_energie": ("dpe_energy_class", "string", "rename"),
        "dpe_annee_construction": ("construction_year", "integer", "rename"),
        "dpe_surface_thermique_lot": ("thermal_surface", "integer", "rename"),
        "dpe_tr002_type_batiment_description": ("building_type", "string", "rename"),
        "dpe_estimation_ges": ("dpe_ges_estimate", "float", "rename"),
        "dpe_geo_score": ("dpe_geo_score", "float", "keep"),
        "dpe_classe_estimation_ges": ("dpe_ges_class", "string", "rename"),
        "dpe_consommation_energie": ("energy_consumption", "float", "rename"),
        "dpe_date_etablissement_dpe": ("dpe_date", "date", "rename"),
        "dpe__score": ("dpe_score", "float", "rename"),
        # Estimation data
        "estimated_price": ("estimated_price", "integer", "keep"),
        "final_price_m2": ("final_price_per_m2", "integer", "rename"),
        "total_growth_rate": ("growth_rate", "float1", "rename"),  # float1 = 1 decimal
    }

    # Columns to drop (not included in final dataset)
    DROP_COLUMNS = {
        "dpe_tr001_modele_dpe_type_libelle", "dpe__geopoint", "dpe_latitude",
        "dpe__i", "dpe_geo_adresse", "dpe__rand", "dpe_code_insee_commune_actualise",
        "dpe_version_methode_dpe", "dpe_nom_methode_dpe", "dpe_tv016_departement_code",
        "dpe_longitude", "dpe__id", "year", "price_per_m2", "initial_price_m2",
        "estimation_status","zipcode"
    }

    FINAL_COLUMN_ORDER = [
        # Core Property Info (Required)
        "uuid",
        "address",
        "city",
        "sale_date",
        
        # Location Details
        "postal_code",
        "insee_code", 
        "region",
        "latitude",
        "longitude",
        
        # Property Characteristics
        "type",
        "surface",
        "rooms",
        "price",
        "analysis_url",
        
        # Energy Performance (DPE)
        "dpe_energy_class",
        "dpe_ges_class",
        "energy_consumption",
        "dpe_ges_estimate",
        "dpe_score",
        "dpe_geo_score",
        "construction_year",
        "thermal_surface",
        "building_type",
        "dpe_date",
        
        # Price Analysis
        "estimated_price",
        "final_price_per_m2",
        "growth_rate",
        
        # Metadata
        "last_modified"
    ]

    @classmethod
    def get_rename_mapping(cls) -> Dict[str, str]:
        """Get column rename mapping."""
        return {
            old_name: spec[0] 
            for old_name, spec in cls.COLUMN_SPECS.items() 
            if spec[2] == "rename"
        }

    @classmethod
    def get_dtypes(cls) -> Dict[str, str]:
        """Get data types for columns."""
        return {
            spec[0]: spec[1]
            for spec in cls.COLUMN_SPECS.values()
        }

    @classmethod
    def get_final_columns(cls) -> Set[str]:
        """Get set of columns in final format."""
        return {
            spec[0] for spec in cls.COLUMN_SPECS.values()
        } | {"uuid", "last_modified"}

class PropertyDataManager:
    """Manages property data with standardization and storage capabilities."""
    
    def __init__(self, main_file: str, invalid_file: str, log_file: str):
        """Initialize manager with file paths and setup logging."""
        self.main_file = Path(main_file)
        self.invalid_file = Path(invalid_file)
        self.data = pd.DataFrame()
        
        # Setup logging
        self.logger = logging.getLogger("PropertyDataManager")
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler(log_file, mode='a')
        handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
        self.logger.addHandler(handler)
        
        # Load data if exists
        self.load_data()

    def load_data(self) -> None:
        """Load and format data from main CSV file."""
        try:
            if self.main_file.exists():
                self.data = pd.read_csv(self.main_file)
                self.logger.info(f"Loaded {len(self.data)} rows from {self.main_file}")
            else:
                self.data = pd.DataFrame()
                self.logger.warning(f"No existing data file at {self.main_file}")
        except Exception as e:
            self.logger.error(f"Failed to load data: {str(e)}")
            raise RuntimeError(f"Failed to load data: {str(e)}")

    def save_data(self) -> None:
        """Save current data to main CSV file."""
        try:
            self.main_file.parent.mkdir(parents=True, exist_ok=True)
            self.data.to_csv(self.main_file, index=False)
            self.logger.info(f"Saved {len(self.data)} rows to {self.main_file}")
        except Exception as e:
            self.logger.error(f"Failed to save data: {str(e)}")
            raise RuntimeError(f"Failed to save data: {str(e)}")
         
    def add_data(self, new_data: pd.DataFrame) -> Dict:
        """
        Add new data to the main dataset with proper column handling and ordering.
        
        Args:
            new_data: DataFrame containing new property data
            
        Returns:
            Dict with statistics about the operation
        """
        try:
            self.logger.info(f"Starting to process {len(new_data)} new rows")
            df = new_data.copy()
            
            # Step 1: Drop unwanted columns
            columns_to_drop = [col for col in DataFormat.DROP_COLUMNS if col in df.columns]
            df = df.drop(columns=columns_to_drop, errors='ignore')
            self.logger.info(f"Dropped {len(columns_to_drop)} unwanted columns")
            
            # Step 2: Verify required columns before renaming
            rename_mapping = DataFormat.get_rename_mapping()
            missing_required = {
                old_col for old_col, new_col in rename_mapping.items()
                if old_col not in df.columns and new_col in DataFormat.REQUIRED_COLUMNS
            }
            if missing_required:
                raise ValueError(f"Missing required columns: {missing_required}")
            
            # Step 3: Rename columns
            df = df.rename(columns=rename_mapping)
            self.logger.info("Renamed columns according to specification")
            
            # Step 4: Add metadata columns
            current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if 'uuid' not in df.columns:
                df['uuid'] = [str(uuid.uuid4()) for _ in range(len(df))]
            df['last_modified'] = current_timestamp
            
            # Step 5: Ensure all final columns exist with correct order
            for column in DataFormat.FINAL_COLUMN_ORDER:
                if column not in df.columns:
                    df[column] = None
                    self.logger.info(f"Added missing column: {column}")
            
            # Step 6: Reorder columns to match final format
            df = df[DataFormat.FINAL_COLUMN_ORDER]
            self.logger.info("Reordered columns to match final format")
            
            # Step 7: Handle updates vs new data
            if not self.data.empty:
                # First ensure main data has same structure
                for col in DataFormat.FINAL_COLUMN_ORDER:
                    if col not in self.data.columns:
                        self.data[col] = None
                self.data = self.data[DataFormat.FINAL_COLUMN_ORDER]
                
                # Identify updates using UUID or required columns
                if 'uuid' in df.columns:
                    update_mask = self.data['uuid'].isin(df['uuid'])
                else:
                    update_mask = (
                        (self.data['address'].isin(df['address'])) &
                        (self.data['city'].isin(df['city'])) &
                        (self.data['sale_date'].isin(df['sale_date']))
                    )
                
                # Process updates and additions
                to_update = df[df['uuid'].isin(self.data.loc[update_mask, 'uuid'])]
                to_add = df[~df['uuid'].isin(self.data.loc[update_mask, 'uuid'])]
                
                # Update existing records
                for _, row in to_update.iterrows():
                    self.data.loc[self.data['uuid'] == row['uuid']] = row
                
                # Append new records
                self.data = pd.concat([self.data, to_add], ignore_index=True)
                
                update_count = len(to_update)
                add_count = len(to_add)
                
            else:
                # Initialize data with new records
                self.data = df
                update_count = 0
                add_count = len(df)
            
            # Save updated dataset
            self.save_data()
            
            result = {
                "total_processed": len(df),
                "updated": update_count,
                "added": add_count,
                "final_column_count": len(self.data.columns),
                "timestamp": current_timestamp
            }
            
            self.logger.info(f"Operation completed: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to process data: {str(e)}")
            raise RuntimeError(f"Failed to process data: {str(e)}")
            
    def _process_update(self, update_df: pd.DataFrame) -> Dict:
        updated = 0
        failed = 0
        
        for _, row in update_df.iterrows():
            mask = self.data['uuid'] == row['uuid']
            if any(mask):
                self.data.loc[mask] = row
                self.data.loc[mask, 'last_modified'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                updated += 1
            else:
                failed += 1
                
        if updated:
            self.save_data()
            
        return {"updated": updated, "failed": failed}

    def query_data(self, query: str) -> pd.DataFrame:
        """Query current data using pandas query syntax."""
        try:
            return self.data.query(query)
        except Exception as e:
            self.logger.error(f"Query failed: {str(e)}")
            raise RuntimeError(f"Query failed: {str(e)}")

    def get_summary(self) -> Dict:
        """Get summary statistics of current data."""
        try:
            if self.data.empty:
                return {
                    "total_entries": 0,
                    "date_range": None,
                    "cities": [],
                    "storage_size": 0
                }
            
            storage_size = self.main_file.stat().st_size / (1024 * 1024)  # MB
            
            return {
                "total_entries": len(self.data),
                "date_range": (
                    self.data['sale_date'].min(),
                    self.data['sale_date'].max()
                ),
                "cities": sorted(self.data['city'].unique().tolist()),
                "storage_size": round(storage_size, 2)
            }
            
        except Exception as e:
            self.logger.error(f"Failed to generate summary: {str(e)}")
            raise RuntimeError(f"Failed to generate summary: {str(e)}")
    
    def delete_data(self, condition: str) -> Dict:
        """
        Delete entries matching condition.
        
        Args:
            condition: Query string (e.g., "city == 'PARIS' and price > 200000")
        
        Returns:
            Dict with deletion statistics
        """
        try:
            original_count = len(self.data)
            self.data = self.data.query(f"not ({condition})").reset_index(drop=True)
            deleted_count = original_count - len(self.data)
            
            if deleted_count > 0:
                self.save_data()
                
            return {
                "original_count": original_count,
                "deleted_count": deleted_count,
                "remaining_count": len(self.data)
            }
            
        except Exception as e:
            self.logger.error(f"Delete operation failed: {str(e)}")
            raise RuntimeError(f"Delete operation failed: {str(e)}")

    def build_query(self, conditions: Dict) -> str:
        """
        Build query string from condition dictionary.
        
        Args:
            conditions: Dict with conditions like:
                {
                    'city': ['PARIS', 'LYON'],
                    'price_range': (200000, 500000),
                    'date_range': ('01/01/2023', '31/12/2023')
                }
        
        Returns:
            Query string for pandas
        """
        query_parts = []
        
        if cities := conditions.get('city'):
            city_list = [f"'{city}'" for city in cities]
            query_parts.append(f"city in [{', '.join(city_list)}]")
            
        if price_range := conditions.get('price_range'):
            min_price, max_price = price_range
            query_parts.append(f"price >= {min_price} and price <= {max_price}")
            
        if date_range := conditions.get('date_range'):
            start_date, end_date = date_range
            query_parts.append(f"sale_date >= '{start_date}' and sale_date <= '{end_date}'")
        
        return ' and '.join(query_parts) if query_parts else ''