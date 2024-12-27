import json
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd

from .email_service import EmailService

logger = logging.getLogger(__name__)

class CustomerEmailService:
    """Service to handle customer-specific email operations."""

    def __init__(self, customers_dir: Path, email_service: Optional[EmailService] = None):
        """
        Initialize CustomerEmailService.
        
        Args:
            customers_dir: Path to customers directory
            email_service: Optional EmailService instance (creates new one if not provided)
        """
        self.customers_dir = Path(customers_dir)
        self.email_service = email_service or EmailService()

    def load_customer_config(self, customer_id: str) -> Dict:
        """
        Load and validate customer configuration.
        
        Args:
            customer_id: Customer directory name
            
        Returns:
            Dict containing customer configuration
            
        Raises:
            ValueError: If config is invalid or customer is inactive
        """
        customer_dir = self.customers_dir / customer_id
        config_path = customer_dir / 'config.json'
        
        if not config_path.exists():
            raise ValueError(f"Customer config not found: {config_path}")
            
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
            
        # Validate required fields
        required_fields = ['first_name', 'last_name', 'email', 'status']
        missing_fields = [field for field in required_fields if not config.get(field)]
        if missing_fields:
            raise ValueError(f"Missing required fields in config: {missing_fields}")
            
        # Check status
        if config['status'] != 'active':
            raise ValueError(f"Customer {customer_id} is not active")
            
        return config

    def get_customer_properties(self, customer_id: str, config: Dict) -> List[Dict]:
        """
        Load and filter properties based on customer preferences.
        
        Args:
            customer_id: Customer directory name
            config: Customer configuration dictionary
            
        Returns:
            List of filtered property dictionaries
        """
        customer_dir = self.customers_dir / customer_id
        db_path = customer_dir / 'properties.csv'
        
        if not db_path.exists():
            raise ValueError(f"Customer database not found: {db_path}")
            
        # Load properties
        df = pd.read_csv(db_path)
        
        # Filter unsent properties
        df = df[df['sent'].isna()]
        
        # Apply customer preferences
        if config.get('cities'):
            df = df[df['city'].isin(config['cities'])]
            
        if config.get('property_types'):
            df = df[df['type'].isin(config['property_types'])]
            
        # Limit to requested number
        addresses_per_report = config.get('addresses_per_report', 10)
        df = df.head(addresses_per_report)
        
        return df.to_dict('records')

    def update_sent_status(self, customer_id: str, properties: List[Dict]) -> None:
        """
        Update sent status for properties in customer database.
        
        Args:
            customer_id: Customer directory name
            properties: List of properties that were sent
        """
        customer_dir = self.customers_dir / customer_id
        db_path = customer_dir / 'properties.csv'
        
        # Load database
        df = pd.read_csv(db_path)
        
        # Get IDs of sent properties
        sent_ids = [prop['uuid'] for prop in properties]
        
        # Update sent date
        current_date = datetime.now().strftime('%Y-%m-%d')
        df.loc[df['uuid'].isin(sent_ids), 'sent'] = current_date
        
        # Save back to CSV
        df.to_csv(db_path, index=False)
        logger.info(f"Updated sent status for {len(sent_ids)} properties")

    def send_customer_report(self, customer_id: str) -> bool:
        """
        Send property report to customer.
        
        Args:
            customer_id: Customer directory name
            
        Returns:
            bool: True if email was sent successfully
        """
        try:
            # Load customer config
            config = self.load_customer_config(customer_id)
            
            # Get properties
            properties = self.get_customer_properties(customer_id, config)
            
            if not properties:
                logger.warning(f"No properties to send for customer {customer_id}")
                return False
                
            # Prepare user data for email
            user_data = {
                'first_name': config['first_name'],
                'last_name': config['last_name'],
                'email': config['email'],
                'company_name': config.get('company_name', '')
            }
            
            # Send email
            success = self.email_service.send_monthly_report(user_data, properties)
            
            if success:
                # Update sent status
                self.update_sent_status(customer_id, properties)
                logger.info(f"Successfully sent report to customer {customer_id}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to send report to customer {customer_id}: {str(e)}")
            return False

    def list_customers(self) -> List[Dict]:
        """
        List all customers with their basic information.
        
        Returns:
            List of customer information dictionaries
        """
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