import os
import smtplib
import logging
from email.mime.text import MIMEText
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader, select_autoescape

logger = logging.getLogger(__name__)

class EmailService:
    """Service to handle email formatting and sending."""
    
    def __init__(self):
        """
        Initialize EmailService with validation email address.
        
        Args:
            validation_email: Email address for validation
        """
        # Load environment variables
        load_dotenv()
        
        # SMTP Configuration
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.hostinger.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '465'))
        self.smtp_email = os.getenv('SMTP_EMAIL', 'trackimmo@directivai.com')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        self.validation_email = os.getenv('VALIDATION_EMAIL')
        self.logo_url = os.getenv('LOGO_URL')
        
        # Setup Jinja2 environment
        template_dir = Path(__file__).parent
        self.jinja_env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=select_autoescape(['html', 'xml'])
        )
        
        self.jinja_env.filters['format_price'] = self._format_price
        self.template = self.jinja_env.get_template('monthly_report.html')

    def _format_price(self, value: float) -> str:
        """Format price with thousand separators."""
        try:
            return f"{int(value):,}".replace(',', ' ') + " €"
        except (ValueError, TypeError):
            return "N/A"

    def _send_email(self, recipient: str, subject: str, html_content: str) -> bool:
        """
        Internal method to send email via SMTP.
        
        Args:
            recipient: Recipient email address
            subject: Email subject
            html_content: HTML content of email
            
        Returns:
            bool: True if sent successfully
        """
        try:
            message = MIMEText(html_content, "html")
            message["Subject"] = subject
            message["From"] = f"TrackImmo <{self.smtp_email}>"
            message["To"] = recipient
            
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                server.login(self.smtp_email, self.smtp_password)
                server.send_message(message)
                
            logger.info(f"Email sent successfully to {recipient}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email: {str(e)}")
            return False

    def send_for_validation(self, user_data: Dict[str, str], properties_data: List[Dict]) -> bool:
        """
        Send properties to validation email address.
        
        Args:
            user_data: Customer information
            properties_data: List of properties to validate
            
        Returns:
            bool: True if validation email sent successfully
        """
        try:
            # Generate HTML content
            html_content = self.template.render(
                user_data=user_data,
                properties_data=properties_data,
                logo_url=self.logo_url,
                is_validation=True  # Template can show validation banner
            )
            
            # Create validation-specific subject
            subject = f"[VALIDATION] Rapport Immo {user_data['last_name']} - {datetime.now().strftime('%B %Y')}"
            
            return self._send_email(self.validation_email, subject, html_content)
            
        except Exception as e:
            logger.error(f"Error preparing validation email: {str(e)}")
            return False

    def send_to_customer(self, user_data: Dict[str, str], properties_data: List[Dict]) -> bool:
        """
        Send validated properties to customer.
        
        Args:
            user_data: Customer information
            properties_data: List of validated properties
            
        Returns:
            bool: True if customer email sent successfully
        """
        try:
            # Generate fresh HTML content
            html_content = self.template.render(
                user_data=user_data,
                properties_data=properties_data,
                logo_url=self.logo_url,
                is_validation=False
            )
            
            subject = f"Rapport Immo - {datetime.now().strftime('%B %Y')}"
            
            return self._send_email(user_data['email'], subject, html_content)
            
        except Exception as e:
            logger.error(f"Error preparing customer email: {str(e)}")
            return False

class CustomerEmailService:
    """Service to handle customer-specific email operations."""

    def __init__(self, customers_dir: Path):
        """
        Initialize CustomerEmailService.
        
        Args:
            customers_dir: Path to customers directory
            validation_email: Email address for validation
        """
        self.customers_dir = Path(customers_dir)
        self.email_service = EmailService()

    def send_for_validation(self, customer_id: str) -> bool:
        """
        Select properties and send for validation.
        
        Args:
            customer_id: Customer directory name
            
        Returns:
            bool: True if validation email sent successfully
        """
        try:
            # Load customer config
            config = self.load_customer_config(customer_id)
            
            # Get and filter properties
            properties = self.get_customer_properties(customer_id, config)
            
            if not properties:
                logger.warning(f"No properties to validate for customer {customer_id}")
                return False
            
            # Mark properties as pending validation
            self._mark_properties_pending(customer_id, properties)
            
            # Send validation email
            return self.email_service.send_for_validation(config, properties)
            
        except Exception as e:
            logger.error(f"Failed to send validation for customer {customer_id}: {str(e)}")
            return False

    def confirm_validation(self, customer_id: str) -> bool:
        """
        Send validated properties to customer.
        
        Args:
            customer_id: Customer directory name
            
        Returns:
            bool: True if customer email sent successfully
        """
        try:
            # Load customer config
            config = self.load_customer_config(customer_id)
            
            # Get pending validation properties
            properties = self._get_pending_properties(customer_id)
            
            if not properties:
                logger.warning(f"No pending properties found for customer {customer_id}")
                return False
            
            # Send to customer
            if self.email_service.send_to_customer(config, properties):
                # Update sent status
                self._mark_properties_sent(customer_id, properties)
                return True
            return False
            
        except Exception as e:
            logger.error(f"Failed to confirm validation for customer {customer_id}: {str(e)}")
            return False

    def cancel_validation(self, customer_id: str) -> bool:
        """
        Reset validation status of pending properties.
        
        Args:
            customer_id: Customer directory name
            
        Returns:
            bool: True if cancelled successfully
        """
        try:
            df = self._load_customer_db(customer_id)
            
            # Reset validation status
            df.loc[df['validation_pending'] == True, 'validation_pending'] = False
            
            # Save changes
            self._save_customer_db(customer_id, df)
            logger.info(f"Cancelled validation for customer {customer_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to cancel validation for customer {customer_id}: {str(e)}")
            return False

    def _mark_properties_pending(self, customer_id: str, properties: List[Dict]) -> None:
        """Mark properties as pending validation in customer database."""
        df = self._load_customer_db(customer_id)
        property_ids = [p['uuid'] for p in properties]
        df.loc[df['uuid'].isin(property_ids), 'validation_pending'] = True
        self._save_customer_db(customer_id, df)

    def _mark_properties_sent(self, customer_id: str, properties: List[Dict]) -> None:
        """Mark properties as sent in customer database."""
        df = self._load_customer_db(customer_id)
        property_ids = [p['uuid'] for p in properties]
        current_date = datetime.now().strftime('%Y-%m-%d')
        df.loc[df['uuid'].isin(property_ids), 'sent'] = current_date
        df.loc[df['uuid'].isin(property_ids), 'validation_pending'] = False
        self._save_customer_db(customer_id, df)

    def _get_pending_properties(self, customer_id: str) -> List[Dict]:
        """Get properties pending validation for customer."""
        df = self._load_customer_db(customer_id)
        pending_df = df[df['validation_pending'] == True]
        return pending_df.to_dict('records')

    def _load_customer_db(self, customer_id: str) -> pd.DataFrame:
        """Load customer's property database."""
        db_path = self.customers_dir / customer_id / 'properties.csv'
        return pd.read_csv(db_path)

    def _save_customer_db(self, customer_id: str, df: pd.DataFrame) -> None:
        """Save customer's property database."""
        db_path = self.customers_dir / customer_id / 'properties.csv'
        df.to_csv(db_path, index=False)