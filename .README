# TrackImmo

A Python-based real estate data analysis tool that processes and analyzes publicly available property transaction data from French government open data sources. The tool aggregates data from various public APIs and websites that repurpose this information, providing insights into property market trends.

## Data Sources

- Public property transaction data (DVF - Demande de Valeur Foncière)
- Government energy performance database (DPE)
- National address database (Base Adresse Nationale)
- Public property price references

## Features

- **Data Collection**
  - Automated collection of public property transaction records
  - Integration with government open data APIs
  - Historical data aggregation

- **Data Processing**
  - Address enrichment using national address database
  - Energy performance (DPE) data integration
  - Price analysis based on historical public records
  - Standardized data storage

- **Analysis & Reporting**
  - Historical price trend analysis
  - Monthly report generation with property insights
  - Customizable data queries

## Project Structure

``` txt
├── config/                 # Configuration files
├── data/                   # Data storage
├── src/                   # Source code
│   ├── cli/              # Command-line interface
│   ├── dataprocessor/    # Data processing modules
│   ├── email/            # Email reporting
│   ├── scraper/         # Data collection modules
│   └── utils/            # Utility functions
└── main.py               # Application entry point
```

## Requirements

- Python 3.8+
- pandas
- BeautifulSoup4
- requests
- Rich (for CLI interface)

## Installation

1.Clone the repository:

```bash
git clone https://github.com/yourusername/trackimmo.git
cd trackimmo
```

2.Create and activate virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3.Install dependencies:

```bash
pip install -r requirements.txt
```

4.Configure the application:

- Copy `.env.example` to `.env`
- Update configuration in `config/scraping_config.json`

## Usage

Start the application:

```bash
python main.py
```

Available operations:

- Data collection
- Data processing
- Full analysis process
- Data management and export

## Development

### Code Standards

- Type hints required
- Docstrings for functions
- Error handling included
- Logging implemented
- Test coverage expected

### Testing

```bash
python -m pytest
```

## Contributing

1. Fork the repository
2. Create your feature branch
3. Commit your changes
4. Push to the branch
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
