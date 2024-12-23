import logging
from src.cli import Menu, MenuOption, CommandHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    """Main function to run the application."""
    menu = Menu()
    
    # Register command handlers
    menu.register_handler(MenuOption.FULL_PROCESS, CommandHandler.handle_full_process)
    menu.register_handler(MenuOption.PARSE_FILE, CommandHandler.handle_parse_file)
    menu.register_handler(MenuOption.ESTIMATE_PRICES, CommandHandler.handle_estimate_prices)
    menu.register_handler(MenuOption.FETCH_RECENT, CommandHandler.handle_fetch_recent)
    
    while True:
        choice = menu.display()
        if not menu.handle_choice(choice):
            break

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Program interrupted by user.")
    except Exception as e:
        logging.error("An unexpected error occurred: %s", e)