# main.py

import asyncio
import logging
from pathlib import Path
from src.cli.menu import Menu

def setup_logging():
    """Configure logging for the application."""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_dir / "app.log"),
            logging.StreamHandler()
        ]
    )

def main():
    setup_logging()
    menu = Menu()
    asyncio.run(menu.start())

if __name__ == "__main__":
    main()