# test.py
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
from src.scraper.browser import BrowserManager
from src.scraper.config import ScraperConfig
from src.scraper.scraper import PropertyScraper

# Configuration du logging avec plus de détails
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - [%(name)s] - %(message)s"
)

logger = logging.getLogger("immo_scraper_test")

async def test_single_url():
    """
    Test le scraping sur une seule URL avec validation des données.
    """
    test_url = (
        "https://www.immo-data.fr/explorateur/transaction/recherche"
        "?minprice=0&maxprice=25000000"
        "&minpricesquaremeter=0&maxpricesquaremeter=40000"
        "&propertytypes=0%2C1%2C2%2C4%2C5"
        "&minmonthyear=Janvier%202014&maxmonthyear=Juin%202024"
        "&nbrooms=1%2C2%2C3%2C4%2C5"
        "&minsurface=0&maxsurface=400"
        "&minsurfaceland=0&maxsurfaceland=50000"
        "&center=2.3431957478042023%3B48.85910487750468"
        "&zoom=13.042327120629595"
    )
    
    logger.info("🚀 Démarrage du test de scraping")
    logger.debug(f"URL de test: {test_url}")
    
    # Création du dossier de test pour les résultats
    test_output_dir = Path("test_results")
    test_output_dir.mkdir(exist_ok=True)
    
    # Nom de fichier unique pour ce test
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = test_output_dir / f"test_scraping_{timestamp}.json"
    
    try:
        # Test du scraper
        scraper = PropertyScraper(urls=[test_url], output_file=str(output_file))
        await scraper.run()
        
        # Vérification des résultats
        if output_file.exists():
            with open(output_file, 'r', encoding='utf-8') as f:
                results = json.load(f)
            
            # Analyse des résultats
            if results.get('results'):
                for result in results['results']:
                    properties_count = result.get('properties_count', 0)
                    logger.info(f"✓ Trouvé {properties_count} propriétés pour l'URL")
                    
                    if properties_count > 0:
                        logger.info("✓ Données extraites avec succès")
                    else:
                        logger.warning("⚠️ Aucune propriété trouvée")
                        
                logger.info(f"✓ Résultats sauvegardés dans: {output_file}")
            else:
                logger.error("❌ Aucun résultat dans le fichier de sortie")
                
        else:
            logger.error("❌ Fichier de résultats non trouvé")
            
    except Exception as e:
        logger.error(f"❌ Erreur pendant le test: {str(e)}")
        raise
        
async def test_browser_direct():
    """
    Test direct du BrowserManager pour vérifier l'extraction des propriétés.
    """
    logger.info("🔍 Test direct du BrowserManager")
    
    test_url = (
        "https://www.immo-data.fr/explorateur/transaction/recherche"
        "?minprice=0&maxprice=25000000"
        "&propertytypes=1"  # Seulement les appartements pour ce test
        "&minmonthyear=Janvier%202024&maxmonthyear=Janvier%202024"
        "&center=2.3431957478042023%3B48.85910487750468"
        "&zoom=13.042327120629595"
    )
    
    async with BrowserManager() as browser:
        try:
            properties = await browser.get_properties(test_url)
            
            if properties:
                logger.info(f"✓ Extrait {len(properties)} propriétés")
                
                # Sauvegarde d'un exemple
                example_file = Path("test_results") / "example_property.html"
                if properties:
                    with open(example_file, 'w', encoding='utf-8') as f:
                        f.write(properties[0])
                    logger.info(f"✓ Exemple sauvegardé dans: {example_file}")
                    
            else:
                logger.warning("⚠️ Aucune propriété trouvée")
                
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'extraction: {str(e)}")
            raise

async def main():
    """
    Fonction principale exécutant tous les tests.
    """
    try:
        logger.info("=== Démarrage des tests ===")
        
        # Test de la connexion directe au navigateur
        logger.info("\n1️⃣ Test du BrowserManager")
        await test_browser_direct()
        
        # Test du scraper complet
        logger.info("\n2️⃣ Test du scraper complet")
        await test_single_url()
        
        logger.info("\n✨ Tous les tests sont terminés avec succès")
        
    except KeyboardInterrupt:
        logger.info("\n⚠️ Tests interrompus par l'utilisateur")
    except Exception as e:
        logger.error(f"\n❌ Erreur fatale pendant les tests: {str(e)}")
        raise
    finally:
        logger.info("=== Fin des tests ===")

if __name__ == "__main__":
    # Création du dossier de test
    Path("test_results").mkdir(exist_ok=True)
    
    # Lancement des tests
    asyncio.run(main())