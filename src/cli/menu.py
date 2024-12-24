import json
from pathlib import Path
from typing import Optional, Dict
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.table import Table
from .command_handlers import start_scraping, start_parser, start_full_process

console = Console()


def resolve_path(base_dir: str, file_path: str) -> str:
    """
    Resolve a file path relative to a base directory.
    If the file_path is already absolute or starts with the base_dir, it is returned as-is.
    """
    base_path = Path(base_dir).resolve()
    full_path = Path(file_path).resolve()

    # If the file_path is already within the base_dir, return it as-is
    if str(full_path).startswith(str(base_path)):
        return str(full_path)

    # Otherwise, join base_dir and file_path
    return str(base_path.joinpath(file_path).resolve())


class Menu:
    def __init__(self):
        self.console = Console()
        
    def _display_header(self):
        self.console.print(Panel.fit(
            "[bold blue]TrackImmo Scraper[/bold blue]\n"
            "[dim]Property data collection and processing tool[/dim]"
        ))
        
    def _load_config(self, config_path: str) -> Optional[Dict]:
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
                # Validate required keys
                required_keys = ["base_url", "start_date", "end_date", "search_type", "output_scraper", "dataprocessor_output_dir"]
                for key in required_keys:
                    if key not in config:
                        raise ValueError(f"Missing required configuration key: {key}")
                return config
        except Exception as e:
            self.console.print(f"[red]Error loading config: {str(e)}[/red]")
            return None
            
    async def start(self):
        while True:
            self._display_header()
            
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
            
            config_path = Prompt.ask(
                "Enter config file path",
                default="config/scraping_config.json"
            )
            config = self._load_config(config_path)
            if not config:
                continue

            if choice == "1":
                confirm = Confirm.ask("Start scraping with loaded config?")
                if confirm:
                    try:
                        result = await start_scraping(config)
                        if result:
                            self.console.print(f"[green]Scraping completed. Results saved to: {result}[/green]")
                        else:
                            self.console.print("[red]Scraping failed[/red]")
                    except Exception as e:
                        self.console.print(f"[red]Error during scraping: {str(e)}[/red]")

            elif choice == "2":
                input_file = Prompt.ask(
                    "Enter input file for parsing",
                    default=config["output_scraper"]
                )
                # Resolve paths relative to the correct base directory
                config["parser_input"] = resolve_path("data/raw", input_file)
                config["output_dir"] = config["dataprocessor_output_dir"]

                # Use reference_prices_path as-is
                config["reference_prices_path"] = config["reference_prices_path"]

                confirm = Confirm.ask("Start parsing with loaded config?")
                if confirm:
                    try:
                        if start_parser(config):
                            console.print(f"[green]Parsing completed. Results saved to: {config['dataprocessor_output_dir']}[/green]")
                        else:
                            console.print("[red]Parsing failed[/red]")
                    except Exception as e:
                        console.print(f"[red]Error during parsing: {str(e)}[/red]")

            elif choice == "3":
                confirm = Confirm.ask("Start full process (scrape + parse) with loaded config?")
                if confirm:
                    try:
                        # Use await since start_full_process is now asynchronous
                        if await start_full_process(config):
                            console.print(f"[green]Full process completed. Results saved to: {config['dataprocessor_output_dir']}[/green]")
                        else:
                            console.print("[red]Full process failed[/red]")
                    except Exception as e:
                        console.print(f"[red]Error during full process: {str(e)}[/red]")

                
            self.console.print("\nPress Enter to continue...")
            input()
