from enum import Enum
import logging
from typing import Callable, Dict

class MenuOption(Enum):
    FULL_PROCESS = "1"
    PARSE_FILE = "2"
    ESTIMATE_PRICES = "3"
    FETCH_RECENT = "4"
    QUIT = "5"

class Menu:
    def __init__(self):
        self.handlers: Dict[MenuOption, Callable] = {}

    def register_handler(self, option: MenuOption, handler: Callable) -> None:
        """Register a handler function for a menu option."""
        self.handlers[option] = handler

    def display(self) -> str:
        """Display the main menu and get user's choice."""
        print("\n=== IMMO DATA SCRAPER ===")
        print("1. Execute full process")
        print("2. Parse data from an existing JSON file")
        print("3. Estimate prices from CSV file")
        print("4. Fetch recent Browse AI results")
        print("5. Quit")
        return input("\nChoose an option (1-5): ")

    def handle_choice(self, choice: str) -> bool:
        """
        Handle the user's menu choice.
        
        Returns:
            bool: False if the program should quit, True otherwise.
        """
        try:
            option = MenuOption(choice)
            if option == MenuOption.QUIT:
                logging.info("Exiting program. Goodbye!")
                return False
                
            if option in self.handlers:
                self.handlers[option]()
                input("\nPress Enter to continue...")
                return True
                
        except ValueError:
            logging.warning("Invalid option. Please try again.")
            return True
            
        return True