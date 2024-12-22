import os
import smtplib
import logging
from email.mime.text import MIMEText
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader, select_autoescape

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

class EmailService:
    """Service to handle email formatting and sending."""
    
    def __init__(self, test_mode: bool = False):
        """
        Initialize EmailService with SMTP configuration.
        
        Args:
            test_mode (bool): If True, sends to test email address only
        """
        # Load environment variables
        load_dotenv()
        
        # SMTP Configuration
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.hostinger.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '465'))
        self.smtp_email = os.getenv('SMTP_EMAIL', 'trackimmo@directivai.com')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        self.test_email = os.getenv('TEST_EMAIL', 'antoine.rtc3@gmail.com')
        self.logo_url = os.getenv('LOGO_URL', 'https://assets.zyrosite.com/cdn-cgi/image/format=auto,w=134,h=141,fit=crop/YanzOKzpyqC1QgL3/untitled-design-1---copie-m6LwgLQyjZsgMRrq.png')
        
        self.test_mode = test_mode
        
        # Setup Jinja2 environment
        template_dir = Path(__file__).parent
        self.jinja_env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=select_autoescape(['html', 'xml'])
        )
        
        # Add custom filters
        self.jinja_env.filters['format_price'] = self._format_price
        
        # Load template
        self.template = self.jinja_env.get_template('monthly_report.html')

    def _format_price(self, value: float) -> str:
        """Format price with thousand separators."""
        try:
            return f"{int(value):,}".replace(',', ' ') + " €"
        except (ValueError, TypeError):
            return "N/A"

    def send_monthly_report(
        self,
        user_data: Dict[str, str],
        properties_data: List[Dict]
    ) -> bool:
        """
        Send monthly report email to user.
        
        Args:
            user_data: Dictionary containing user information (first_name, last_name, email)
            properties_data: List of property dictionaries
            
        Returns:
            bool: True if email was sent successfully, False otherwise
        """
        try:
            # Prepare email content
            html_content = self.template.render(
                user_data=user_data,
                properties_data=properties_data,
                logo_url=self.logo_url
            )
            
            # Create email message
            message = MIMEText(html_content, "html")
            message["Subject"] = f"Rapport Immo - {datetime.now().strftime('%B %Y')}"
            message["From"] = f"TrackImmo <{self.smtp_email}>"
            
            # Determine recipient
            recipient = self.test_email if self.test_mode else user_data.get('email')
            if not recipient:
                logger.error("No recipient email provided")
                return False
                
            message["To"] = recipient
            
            # Send email
            try:
                with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                    logger.info(f"Connecting to SMTP server: {self.smtp_server}")
                    server.login(self.smtp_email, self.smtp_password)
                    server.send_message(message)
                    
                logger.info(f"Email sent successfully to {recipient}")
                return True
                
            except Exception as e:
                logger.error(f"Failed to send email: {str(e)}")
                return False
                
        except Exception as e:
            logger.error(f"Error preparing email: {str(e)}")
            return False

    def _validate_smtp_config(self) -> bool:
        """Validate SMTP configuration."""
        if not all([self.smtp_server, self.smtp_port, self.smtp_email, self.smtp_password]):
            logger.error("Missing SMTP configuration")
            return False
        return True

    def test_connection(self) -> bool:
        """Test SMTP connection."""
        try:
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                server.login(self.smtp_email, self.smtp_password)
                logger.info("SMTP connection test successful")
                return True
        except Exception as e:
            logger.error(f"SMTP connection test failed: {str(e)}")
            return False