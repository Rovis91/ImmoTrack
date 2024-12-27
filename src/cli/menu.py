import json
from pathlib import Path
from typing import Optional, Dict
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.table import Table
from .command_handlers import (
    start_scraping, start_parser, start_full_process,
    init_storage_manager, get_storage_summary, process_data_file,
    export_data, delete_data
)
from src.email.email_service import EmailService
from src.email.customer_service import CustomerEmailService
console = Console()

def resolve_path(file_path: str, base_dir: Optional[str] = None) -> Path:
    """
    Resolve a file path, making it absolute if it isn't already.
    
    Args:
        file_path: Path to resolve
        base_dir: Optional base directory to resolve relative paths against
        
    Returns:
        Path: Resolved absolute path
    """
    path = Path(file_path)
    
    # If path is absolute, return it
    if path.is_absolute():
        return path
        
    # If base_dir provided, resolve against it
    if base_dir:
        return Path(base_dir).resolve() / path
        
    # Otherwise resolve against current working directory
    return path.resolve()

class Menu:
    def __init__(self):
        self.console = Console()
        self.project_root = Path(__file__).parent.parent.parent.resolve()
        self.customers_dir = self.project_root / "customers"
        self.email_service = EmailService()
        self.customer_email_service = CustomerEmailService(self.customers_dir, self.email_service)
        
    def _display_header(self):
        self.console.print(Panel.fit(
            "[bold blue]TrackImmo Scraper[/bold blue]\n"
            "[dim]Property data collection and processing tool[/dim]"
        ))
        
    def _load_config(self, config_path: str) -> Optional[Dict]:
        try:
            config_file = resolve_path(config_path, self.project_root)
            
            if not config_file.exists():
                raise FileNotFoundError(f"Config file not found: {config_file}")
                
            with open(config_file, 'r') as f:
                config = json.load(f)
                
            # Update required keys
            required_keys = [
                "base_url", "start_date", "end_date", "search_type",
                "output_scraper", "dataprocessor_output_dir", "reference_prices_path",
                "storage_file", "invalid_file", "log_file"  # New storage keys
            ]
            
            missing_keys = [key for key in required_keys if key not in config]
            if missing_keys:
                raise ValueError(f"Missing required configuration keys: {', '.join(missing_keys)}")
                
            # Update path resolution
            path_keys = [
                'output_scraper', 'dataprocessor_output_dir', 'reference_prices_path',
                'storage_file', 'invalid_file', 'log_file'  # New storage paths
            ]
            for key in path_keys:
                if key in config:
                    config[key] = str(resolve_path(config[key], self.project_root))
                    
            return config
            
        except Exception as e:
            self.console.print(f"[red]Error loading config: {str(e)}[/red]")
            return None

    def _display_storage_menu(self, config: Dict):
        """Handle storage management menu."""
        while True:
            self.console.print("\n[yellow]Storage Management:[/yellow]")
            table = Table(show_header=False, box=None)
            table.add_row("[1]", "View data summary")
            table.add_row("[2]", "Import/Update data")
            table.add_row("[3]", "Export data")
            table.add_row("[4]", "Delete data")
            table.add_row("[b]", "Back to main menu")
            self.console.print(table)
            
            choice = Prompt.ask("\nChoose an action", choices=["1", "2", "3", "4", "b"])
            
            if choice == "b":
                break
                
            try:
                manager = init_storage_manager(config)
                
                if choice == "1":
                    self._handle_summary(manager)
                elif choice == "2":
                    self._handle_import_update(manager)
                elif choice == "3":
                    self._handle_export(manager)
                elif choice == "4":
                    self._handle_delete(manager)
                    
            except Exception as e:
                self.console.print(f"[red]Error: {str(e)}[/red]")

    def _handle_summary(self, manager):
        """Display data summary."""
        summary = get_storage_summary(manager)
        
        self.console.print("\n[bold]Storage Summary:[/bold]")
        self.console.print(f"Total entries: {summary['total_entries']}")
        if summary['date_range']:
            self.console.print(f"Date range: {summary['date_range'][0]} to {summary['date_range'][1]}")
        self.console.print(f"Cities covered: {len(summary['cities'])}")
        self.console.print(f"Storage size: {summary['storage_size']} MB")

    def _display_customer_menu(self):
        """Handle customer email operations menu."""
        while True:
            self.console.print("\n[yellow]Customer Email Operations:[/yellow]")
            table = Table(show_header=False, box=None)
            table.add_row("[1]", "List customers")
            table.add_row("[2]", "Send customer report")
            table.add_row("[3]", "View customer status")
            table.add_row("[b]", "Back to main menu")
            self.console.print(table)
            
            choice = Prompt.ask("\nChoose an action", choices=["1", "2", "3", "b"])
            
            if choice == "b":
                break
                
            try:
                if choice == "1":
                    self._handle_list_customers()
                elif choice == "2":
                    self._handle_send_customer_report()
                elif choice == "3":
                    self._handle_customer_status()
                    
            except Exception as e:
                self.console.print(f"[red]Error: {str(e)}[/red]")

    def _handle_list_customers(self):
        """Display list of available customers."""
        customers = self.customer_email_service.list_customers()
        
        if not customers:
            self.console.print("[yellow]No customers found[/yellow]")
            return
            
        table = Table(title="Available Customers")
        table.add_column("ID")
        table.add_column("Name")
        table.add_column("Email")
        table.add_column("Status")
        
        for customer in customers:
            status_color = "green" if customer['status'] == 'active' else "red"
            table.add_row(
                customer['id'],
                customer['name'],
                customer['email'],
                f"[{status_color}]{customer['status']}[/{status_color}]"
            )
            
        self.console.print(table)

    def _handle_send_customer_report(self):
        """Handle sending report to a specific customer."""
        # First list customers
        self._handle_list_customers()
        
        # Get customer selection
        customer_id = Prompt.ask("\nEnter customer ID")
        
        try:
            # Confirm action
            if Confirm.ask(f"Send report to customer {customer_id}?"):
                if self.customer_email_service.send_customer_report(customer_id):
                    self.console.print("[green]Report sent successfully[/green]")
                else:
                    self.console.print("[red]Failed to send report[/red]")
        except ValueError as e:
            self.console.print(f"[red]Error: {str(e)}[/red]")

    def _handle_customer_status(self):
        """Display customer status and sending history."""
        # First list customers
        self._handle_list_customers()
        
        # Get customer selection
        customer_id = Prompt.ask("\nEnter customer ID")
        
        try:
            config = self.customer_email_service.load_customer_config(customer_id)
            
            self.console.print(f"\n[bold]Customer: {config['first_name']} {config['last_name']}[/bold]")
            self.console.print(f"Email: {config['email']}")
            self.console.print(f"Status: {config['status']}")
            self.console.print(f"Subscription started: {config.get('subscription_start_date', 'N/A')}")
            
            # Show preferences
            self.console.print("\n[bold]Preferences:[/bold]")
            self.console.print(f"Cities: {', '.join(config.get('cities', []))}")
            self.console.print(f"Property Types: {', '.join(config.get('property_types', []))}")
            self.console.print(f"Addresses per report: {config.get('addresses_per_report', 'N/A')}")
            
        except ValueError as e:
            self.console.print(f"[red]Error: {str(e)}[/red]")

    def _handle_import_update(self, manager):
        """Handle data import or update."""
        file_path = Prompt.ask("Enter path to CSV file")
        file_path = resolve_path(file_path, self.project_root)
        
        if not file_path.exists():
            self.console.print(f"[red]File not found: {file_path}[/red]")
            return
            
        if Confirm.ask(f"Process file: {file_path}?"):
            stats = process_data_file(manager, str(file_path))
            self.console.print("[green]Operation completed:[/green]")
            for key, value in stats.items():
                self.console.print(f"{key}: {value}")

    def _handle_export(self, manager):
        """Handle data export with query."""
        self.console.print("\n[bold]Query Examples:[/bold]")
        self.console.print("- city == 'PARIS' and price > 200000")
        self.console.print("- sale_date >= '01/01/2023' and rooms > 2")
        self.console.print("- city in ['PARIS', 'LYON'] and surface > 50")
        
        query = Prompt.ask("\nEnter query condition")
        output_path = Prompt.ask("Enter output file path")
        output_path = resolve_path(output_path, self.project_root)
        
        if export_data(manager, query, str(output_path)):
            self.console.print("[green]Data exported successfully[/green]")

    def _handle_delete(self, manager):
        """Handle data deletion."""
        self.console.print("\n[bold]Delete condition examples:[/bold]")
        self.console.print("- city == 'PARIS'")
        self.console.print("- price < 100000")
        
        condition = Prompt.ask("\nEnter deletion condition")
        
        if Confirm.ask(f"Delete entries matching: {condition}?", default=False):
            stats = delete_data(manager, condition)
            self.console.print("[green]Deletion completed:[/green]")
            for key, value in stats.items():
                self.console.print(f"{key}: {value}")
                                        
    async def start(self):
        """Main menu loop."""
        while True:
            self._display_header()
            
            self.console.print("\n[yellow]Available Actions:[/yellow]")
            table = Table(show_header=False, box=None)
            table.add_row("[1]", "Run scraper only")
            table.add_row("[2]", "Run parser only")
            table.add_row("[3]", "Run full process (scrape + parse)")
            table.add_row("[4]", "Storage Management")
            table.add_row("[5]", "Customer Operations")
            table.add_row("[q]", "Quit")
            self.console.print(table)
            
            choice = Prompt.ask("\nChoose an action", choices=["1", "2", "3", "4", "5", "q"])
            
            if choice == "q":
                break

            config = self._load_config("config/scraping_config.json")
            if not config:
                continue

            try:
                if choice == "1":
                    if Confirm.ask("Start scraping with loaded config?"):
                        result = await start_scraping(config)
                        if result:
                            self.console.print(f"[green]Scraping completed. Results saved to: {result}[/green]")
                        else:
                            self.console.print("[red]Scraping failed[/red]")

                elif choice == "2":
                    input_file = Prompt.ask(
                        "Enter input file for parsing",
                        default=config["output_scraper"]
                    )
                    
                    input_path = resolve_path(input_file, self.project_root)
                    
                    if not input_path.exists():
                        self.console.print(f"[red]Input file not found: {input_path}[/red]")
                        continue
                        
                    config["parser_input"] = str(input_path)
                    
                    if Confirm.ask("Start parsing with loaded config?"):
                        if await start_parser(config):  # Ajout du await ici
                            self.console.print(f"[green]Parsing completed. Results saved to: {config['dataprocessor_output_dir']}[/green]")
                        else:
                            self.console.print("[red]Parsing failed[/red]")

                elif choice == "3":
                    if Confirm.ask("Start full process with loaded config?"):
                        if await start_full_process(config):
                            self.console.print(f"[green]Full process completed. Results saved to: {config['dataprocessor_output_dir']}[/green]")
                        else:
                            self.console.print("[red]Full process failed[/red]")
                
                elif choice == "4":
                    self._display_storage_menu(config)
                
                elif choice == "5":
                    self._display_customer_menu()
                            
            except Exception as e:
                self.console.print(f"[red]Error: {str(e)}[/red]")
                
            self.console.print("\nPress Enter to continue...")
            input()