import json
from pathlib import Path
from typing import Optional, Dict
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt, Confirm
from rich.table import Table
from .command_handlers import start_scraping

console = Console()

class Menu:
    def __init__(self):
        self.console = Console()
        
    def _display_header(self):
        self.console.print(Panel.fit(
            "[bold blue]TrackImmo Scraper[/bold blue]\n"
            "[dim]Property data collection tool[/dim]"
        ))
        
    def _load_config(self, config_path: str) -> Optional[Dict]:
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.console.print(f"[red]Error loading config: {str(e)}[/red]")
            return None
            
    async def start(self):
        while True:
            self._display_header()
            
            self.console.print("\n[yellow]Available Actions:[/yellow]")
            table = Table(show_header=False, box=None)
            table.add_row("[1]", "Start scraping with config")
            table.add_row("[q]", "Quit")
            self.console.print(table)
            
            choice = Prompt.ask("\nChoose an action", choices=["1", "q"])
            
            if choice == "q":
                break
                
            if choice == "1":
                config_path = Prompt.ask(
                    "Enter config file path",
                    default="config/scraping_config.json"
                )
                
                config = self._load_config(config_path)
                if config:
                    confirm = Confirm.ask("Start scraping with loaded config?")
                    if confirm:
                        try:
                            result = await start_scraping(config)
                            if result:
                                self.console.print(
                                    f"[green]Scraping completed. Results saved to: {result}[/green]"
                                )
                            else:
                                self.console.print("[red]Scraping failed[/red]")
                        except Exception as e:
                            self.console.print(f"[red]Error during scraping: {str(e)}[/red]")
                
            self.console.print("\nPress Enter to continue...")
            input()