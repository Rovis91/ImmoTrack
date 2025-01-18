import json
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd

from .email_service import EmailService

logger = logging.getLogger(__name__)

class CustomerEmailService:
    def __init__(self, customers_dir: Path):
        self.customers_dir = Path(customers_dir)
        self.email_service = EmailService()

    def load_customer_config(self, customer_id: str) -> Dict:
        customer_dir = self.customers_dir / customer_id
        config_path = customer_dir / 'config.json'
        
        if not config_path.exists():
            raise ValueError(f"Customer config not found: {config_path}")
            
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        required_fields = ['first_name', 'last_name', 'email', 'status']
        missing_fields = [field for field in required_fields if not config.get(field)]
        if missing_fields:
            raise ValueError(f"Missing required fields in config: {missing_fields}")
            
        if config['status'] != 'active':
            raise ValueError(f"Customer {customer_id} is not active")
            
        return config

    def get_customer_properties(self, customer_id: str, config: Dict) -> List[Dict]:
        customer_dir = self.customers_dir / customer_id
        db_path = customer_dir / 'properties.csv'
        
        if not db_path.exists():
            raise ValueError(f"Customer database not found: {db_path}")
            
        # Read CSV with NA values handled properly
        df = pd.read_csv(db_path, keep_default_na=False, na_values=['nan'])
        
        # Add validation_pending column if it doesn't exist
        if 'validation_pending' not in df.columns:
            df['validation_pending'] = ''
        
        # Convert and filter dates
        df['sale_date'] = pd.to_datetime(df['sale_date'], format='%d/%m/%Y')
        date_mask = (df['sale_date'].dt.year >= 2017) & (df['sale_date'].dt.year <= 2019)
        df = df[date_mask]
        
        # Filter unsent and not pending properties
        df = df[df['sent'].eq('') & df['validation_pending'].eq('')]
        
        # Apply customer preferences
        if config.get('code_insee'):
            df = df[df['insee_code'].isin(config['code_insee'])]
            
        if config.get('property_types'):
            df = df[df['type'].isin(config['property_types'])]
        
        # Convert date back to string format
        df['sale_date'] = df['sale_date'].dt.strftime('%d/%m/%Y')
        
        addresses_per_report = config.get('addresses_per_report', 10)
        df = df.head(addresses_per_report)
        
        return df.to_dict('records')

    def _get_pending_properties(self, customer_id: str) -> List[Dict]:
        customer_dir = self.customers_dir / customer_id
        db_path = customer_dir / 'properties.csv'
        
        if not db_path.exists():
            raise ValueError(f"Customer database not found: {db_path}")
            
        df = pd.read_csv(db_path, keep_default_na=False, na_values=['nan'])
        
        # Check for string 'True' instead of boolean True
        pending_df = df[df['validation_pending'] == 'True']
        
        return pending_df.to_dict('records')

    def send_for_validation(self, customer_id: str) -> bool:
        try:
            config = self.load_customer_config(customer_id)
            properties = self.get_customer_properties(customer_id, config)
            
            if not properties:
                logger.warning(f"No properties to send for customer {customer_id}")
                return False
                
            user_data = {
                'first_name': config['first_name'],
                'last_name': config['last_name'],
                'email': config['email'],
                'company_name': config.get('company_name', '')
            }
            
            if self.email_service.send_for_validation(user_data, properties):
                self._mark_properties_pending(customer_id, properties)
                logger.info(f"Properties sent for validation for customer {customer_id}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to send for validation for customer {customer_id}: {str(e)}")
            return False

    def confirm_validation(self, customer_id: str) -> bool:
        try:
            config = self.load_customer_config(customer_id)
            properties = self._get_pending_properties(customer_id)
            
            if not properties:
                logger.warning(f"No pending properties found for customer {customer_id}")
                return False
                
            user_data = {
                'first_name': config['first_name'],
                'last_name': config['last_name'],
                'email': config['email'],
                'company_name': config.get('company_name', '')
            }
            
            if self.email_service.send_to_customer(user_data, properties):
                self._mark_properties_sent(customer_id, properties)
                logger.info(f"Validation confirmed for customer {customer_id}")
                return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to confirm validation for customer {customer_id}: {str(e)}")
            return False

    def cancel_validation(self, customer_id: str) -> bool:
        try:
            customer_dir = self.customers_dir / customer_id
            db_path = customer_dir / 'properties.csv'
            
            df = pd.read_csv(db_path, keep_default_na=False, na_values=['nan'])
            df.loc[df['validation_pending'].eq(True), 'validation_pending'] = ''
            df.to_csv(db_path, index=False, na_rep='')
            
            logger.info(f"Validation cancelled for customer {customer_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to cancel validation for customer {customer_id}: {str(e)}")
            return False
        
    def _mark_properties_pending(self, customer_id: str, properties: List[Dict]) -> None:
        customer_dir = self.customers_dir / customer_id
        db_path = customer_dir / 'properties.csv'
        
        try:
            # Read the CSV
            df = pd.read_csv(db_path, keep_default_na=False, na_values=['nan'])
            
            # Get property IDs
            property_ids = [prop['uuid'] for prop in properties]
            
            # Add validation_pending column if it doesn't exist
            if 'validation_pending' not in df.columns:
                df['validation_pending'] = ''
                
            # Mark properties as pending using string 'True' instead of boolean True
            df.loc[df['uuid'].isin(property_ids), 'validation_pending'] = 'True'
            
            # Save back to CSV
            df.to_csv(db_path, index=False, na_rep='')
            
            logger.info(f"Marked {len(property_ids)} properties as pending validation")
            
        except Exception as e:
            logger.error(f"Error marking properties as pending: {str(e)}")
            raise

    def _mark_properties_sent(self, customer_id: str, properties: List[Dict]) -> None:
        customer_dir = self.customers_dir / customer_id
        db_path = customer_dir / 'properties.csv'
        
        df = pd.read_csv(db_path, keep_default_na=False, na_values=['nan'])
        property_ids = [prop['uuid'] for prop in properties]
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        df.loc[df['uuid'].isin(property_ids), 'sent'] = current_date
        df.loc[df['uuid'].isin(property_ids), 'validation_pending'] = ''
        
        df.to_csv(db_path, index=False, na_rep='')
        logger.info(f"Marked {len(property_ids)} properties as sent")

    def list_customers(self) -> List[Dict]:
        customers = []
        
        for customer_dir in self.customers_dir.iterdir():
            if customer_dir.is_dir():
                config_path = customer_dir / 'config.json'
                if config_path.exists():
                    try:
                        with open(config_path, 'r', encoding='utf-8') as f:
                            config = json.load(f)
                            customers.append({
                                'id': customer_dir.name,
                                'name': f"{config['first_name']} {config['last_name']}",
                                'email': config['email'],
                                'status': config['status']
                            })
                    except Exception as e:
                        logger.error(f"Error loading config for {customer_dir.name}: {str(e)}")
                        
        return customers