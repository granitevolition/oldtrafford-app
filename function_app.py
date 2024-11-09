
import azure.functions as func
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import pandas as pd
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import os
from azure.storage.blob import BlobServiceClient
import json
from io import StringIO

app = func.FunctionApp()

class AzureScraper:
    def __init__(self):
        """Initialize the Azure-compatible scraper"""
        self.url = "https://play.pakakumi.com/"
        self.setup_logging()
        self.setup_blob_storage()
        self.setup_driver()

    def setup_logging(self):
        """Configure logging"""
        self.logger = logging.getLogger('azure.func.AzureScraper')

    def setup_blob_storage(self):
        """Setup Azure Blob Storage connection"""
        connect_str = os.getenv('AzureWebJobsStorage')
        self.blob_service_client = BlobServiceClient.from_connection_string(connect_str)
        self.container_name = "scraper-data"
        
        # Create container if it doesn't exist
        try:
            self.blob_service_client.create_container(self.container_name)
        except Exception as e:
            self.logger.info(f"Container already exists or error: {str(e)}")

    def setup_driver(self):
        """Setup Chrome driver with Azure-compatible options"""
        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/90.0.4430.85 Safari/537.36"
        )
        
        # Setup Chrome driver with webdriver_manager
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 20)

    def get_current_values(self):
        """Get the current values from the website"""
        try:
            if self.driver.current_url != self.url:
                self.logger.info(f"Navigating to {self.url}")
                self.driver.get(self.url)

            multiplier_element = self.wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "a.css-19toqs6"))
            )
            multiplier = float(multiplier_element.text.replace('x', '').strip())

            playing_element = self.wait.until(
                EC.visibility_of_element_located(
                    (By.XPATH, '//span[contains(text(), "Playing")]/following-sibling::strong')
                )
            )
            playing = int(playing_element.text.strip())

            online_element = self.wait.until(
                EC.visibility_of_element_located(
                    (By.XPATH, '//span[contains(text(), "Online")]/following-sibling::strong')
                )
            )
            online = int(online_element.text.strip())

            timestamp = datetime.now()

            return multiplier, online, playing, timestamp
        except Exception as e:
            self.logger.error(f"Error getting values: {str(e)}")
            return None, None, None, None

    def save_to_blob(self, data):
        """Save data to Azure Blob Storage"""
        try:
            # Create CSV in memory
            df = pd.DataFrame([data])
            csv_string = df.to_csv(index=False)
            
            # Generate blob name with timestamp
            blob_name = f"multiplier_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            
            # Upload to blob storage
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            blob_client.upload_blob(csv_string)
            
            self.logger.info(f"Data saved to blob: {blob_name}")
        except Exception as e:
            self.logger.error(f"Error saving to blob: {str(e)}")

    def cleanup(self):
        """Cleanup resources"""
        try:
            self.driver.quit()
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

@app.function_name(name="ScraperTrigger")
@app.schedule(schedule="*/5 * * * *", arg_name="timer", run_on_startup=True)
def scraper_trigger(timer: func.TimerRequest) -> None:
    """Azure Function timer trigger to run the scraper every 5 minutes"""
    logging.info('Scraper function triggered')
    
    scraper = None
    try:
        scraper = AzureScraper()
        multiplier, online, playing, timestamp = scraper.get_current_values()
        
        if all(v is not None for v in [multiplier, online, playing, timestamp]):
            data = {
                'timestamp': timestamp.isoformat(),
                'multiplier': multiplier,
                'online': online,
                'playing': playing
            }
            scraper.save_to_blob(data)
            logging.info(f"Successfully scraped and saved data: {json.dumps(data)}")
        else:
            logging.error("Failed to get valid values from the website")
            
    except Exception as e:
        logging.error(f"Error in scraper function: {str(e)}")
    finally:
        if scraper:
            scraper.cleanup()