import pandas as pd
import logging
import uuid
from datetime import datetime
from pathlib import Path

class PropertyDataManager:
    def __init__(self, main_file: str, invalid_file: str, log_file: str, column_mapping: dict = None):
        """
        Initializes the Property Data Manager.

        Args:
            main_file (str): Path to the main CSV file storing property data.
            invalid_file (str): Path to store invalid entries.
            log_file (str): Path for storing logs of changes.
            column_mapping (dict): Optional mapping of input column names to standard names.
        """
        self.main_file = Path(main_file)
        self.invalid_file = Path(invalid_file)
        self.log_file = Path(log_file)
        self.data = pd.DataFrame()  # Stores valid data
        self.invalid_data = pd.DataFrame()  # Stores invalid data
        self.logger = self._setup_logger()
        self.column_mapping = column_mapping or {}

    def _setup_logger(self):
        """Sets up the logger for tracking changes."""
        logger = logging.getLogger("PropertyDataManager")
        logger.setLevel(logging.INFO)
        handler = logging.FileHandler(self.log_file)
        formatter = logging.Formatter("%(asctime)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def load_data(self):
        """Loads the main dataset from the CSV file."""
        if self.main_file.exists():
            self.data = pd.read_csv(self.main_file)
        else:
            self.data = pd.DataFrame()

    def save_data(self):
        """Saves the valid dataset back to the main CSV file."""
        self.data.to_csv(self.main_file, index=False)

    def save_invalid_data(self):
        """Saves invalid data entries to the specified file."""
        if not self.invalid_data.empty:
            self.invalid_data.to_csv(self.invalid_file, index=False)

    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Renames columns in a DataFrame based on the column mapping.

        Args:
            df (pd.DataFrame): The DataFrame to rename columns.

        Returns:
            pd.DataFrame: The DataFrame with renamed columns.
        """
        return df.rename(columns=self.column_mapping, inplace=False)

    def _is_duplicate(self, row: pd.Series) -> bool:
        """
        Checks if a row already exists in the dataset based on `address`, `city`, and `sale_date`.

        Args:
            row (pd.Series): The row to check for duplicates.

        Returns:
            bool: True if the row is a duplicate, False otherwise.
        """
        return not self.data[(self.data["address"] == row["address"]) &
                             (self.data["city"] == row["city"]) &
                             (self.data["sale_date"] == row["sale_date"])].empty

    def import_data(self, file_path: str):
        """
        Imports data from a CSV file into the main dataset.

        Args:
            file_path (str): Path to the CSV file to import.
        """
        new_data = pd.read_csv(file_path)

        # Rename columns if mapping is provided
        if self.column_mapping:
            new_data = self.rename_columns(new_data)

        valid_rows = []
        invalid_rows = []

        for _, row in new_data.iterrows():
            if self._is_duplicate(row):
                self.logger.info(f"Duplicate row skipped: {row.to_dict()}")
            elif pd.isna(row["address"]) or pd.isna(row["city"]) or pd.isna(row["sale_date"]):
                row["validation_error"] = "Mandatory fields are missing"
                invalid_rows.append(row)
            else:
                row["uuid"] = str(uuid.uuid4())
                row["last_modified"] = datetime.now().isoformat()
                valid_rows.append(row)

        self.invalid_data = pd.DataFrame(invalid_rows)
        if not valid_rows:
            self.logger.info("No valid rows to add.")
            return

        self.data = pd.concat([self.data, pd.DataFrame(valid_rows)], ignore_index=True)
        self.logger.info(f"Imported {len(valid_rows)} valid rows from {file_path}.")
        self.save_data()
        self.save_invalid_data()

    def add_data(self, new_data: pd.DataFrame):
        """
        Adds new data to the dataset, automatically renaming and validating rows.

        Args:
            new_data (pd.DataFrame): The new data to add.
        """
        if self.column_mapping:
            new_data = self.rename_columns(new_data)

        validated_data = self.validate_data(new_data)
        self.data = pd.concat([self.data, validated_data], ignore_index=True)

        self.logger.info(f"Added {len(validated_data)} rows.")
        self.save_data()
        self.save_invalid_data()

    def validate_data(self, new_data: pd.DataFrame):
        """
        Validates new data and separates valid rows from invalid rows.

        Args:
            new_data (pd.DataFrame): The DataFrame to validate.

        Returns:
            pd.DataFrame: Validated rows.
        """
        valid_rows = []
        invalid_rows = []

        for _, row in new_data.iterrows():
            if pd.isna(row.get("address")) or pd.isna(row.get("city")) or pd.isna(row.get("sale_date")):
                row["validation_error"] = "Mandatory fields are missing"
                invalid_rows.append(row)
            else:
                row["uuid"] = str(uuid.uuid4())
                row["last_modified"] = datetime.now().isoformat()
                valid_rows.append(row)

        self.invalid_data = pd.DataFrame(invalid_rows)
        return pd.DataFrame(valid_rows)

    def delete_data(self, condition: str):
        """
        Deletes rows based on a condition.

        Args:
            condition (str): The condition to evaluate for deletion.
        """
        original_count = len(self.data)
        self.data = self.data.query(f"not ({condition})").reset_index(drop=True)
        removed_count = original_count - len(self.data)
        self.logger.info(f"Deleted {removed_count} rows where {condition}.")
        self.save_data()

    def update_data(self, condition: str, updates: dict):
        """
        Updates specific columns in rows matching a condition.

        Args:
            condition (str): The condition to match rows.
            updates (dict): A dictionary of column updates.
        """
        rows_to_update = self.data.query(condition)
        for col, value in updates.items():
            self.data.loc[rows_to_update.index, col] = value
        self.data.loc[rows_to_update.index, "last_modified"] = datetime.now().isoformat()
        self.logger.info(f"Updated {len(rows_to_update)} rows where {condition}.")
        self.save_data()

    def export_data(self, output_path: str, condition: str):
        """
        Exports a subset of data matching a condition to a new CSV file.

        Args:
            output_path (str): Path to save the exported data.
            condition (str): The condition to filter rows.
        """
        filtered_data = self.data.query(condition)
        filtered_data.to_csv(output_path, index=False)
        self.logger.info(f"Exported {len(filtered_data)} rows to {output_path}.")
