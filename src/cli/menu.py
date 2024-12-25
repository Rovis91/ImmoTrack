# menu.py

import json
from pathlib import Path
from typing import Optional, Dict
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.table import Table
from .command_handlers import start_scraping, start_parser, start_full_process

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
        # Get the project root directory (parent of src)
        self.project_root = Path(__file__).parent.parent.parent.resolve()
        
    def _display_header(self):
        self.console.print(Panel.fit(
            "[bold blue]TrackImmo Scraper[/bold blue]\n"
            "[dim]Property data collection and processing tool[/dim]"
        ))
        
    def _load_config(self, config_path: str) -> Optional[Dict]:
        try:
            # Resolve config path relative to project root if not absolute
            config_file = resolve_path(config_path, self.project_root)
            
            if not config_file.exists():
                raise FileNotFoundError(f"Config file not found: {config_file}")
                
            with open(config_file, 'r') as f:
                config = json.load(f)
                
            # Validate required keys
            required_keys = [
                "base_url",
                "start_date",
                "end_date",
                "search_type",
                "output_scraper",
                "dataprocessor_output_dir",
                "reference_prices_path"
            ]
            
            missing_keys = [key for key in required_keys if key not in config]
            if missing_keys:
                raise ValueError(f"Missing required configuration keys: {', '.join(missing_keys)}")
                
            # Resolve paths in config relative to project root
            path_keys = ['output_scraper', 'dataprocessor_output_dir', 'reference_prices_path']
            for key in path_keys:
                if key in config:
                    config[key] = str(resolve_path(config[key], self.project_root))
                    
            return config
            
        except Exception as e:
            self.console.print(f"[red]Error loading config: {str(e)}[/red]")
            return None
            
    async def start(self):
        while True:
            self._display_header()
            
            # Display menu options
            self.console.print("\n[yellow]Available Actions:[/yellow]")
            table = Table(show_header=False, box=None)
            table.add_row("[1]", "Run scraper only")
            table.add_row("[2]", "Run parser only")
            table.add_row("[3]", "Run full process (scrape + parse)")
            table.add_row("[q]", "Quit")
            self.console.print(table)
            
            choice = Prompt.ask("\nChoose an action", choices=["1", "2", "3", "q"])
            
            if choice == "q":
                break
                
            # Get config file path
            default_config = "config/scraping_config.json"
            config_path = Prompt.ask(
                "Enter config file path",
                default=default_config
            )
            
            # Load and validate config
            config = self._load_config(config_path)
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
                            
            except Exception as e:
                self.console.print(f"[red]Error: {str(e)}[/red]")
                
            self.console.print("\nPress Enter to continue...")
            input()