
import azure.functions as func
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import pandas as pd
from selenium.webdriver.chrome.options import Options
import os
from azure.storage.blob import BlobServiceClient
import json
import traceback
import chromedriver_autoinstaller
from functools import wraps
import time

def retry_on_exception(retries=3, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for i in range(retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if i == retries - 1:  # Last attempt
                        raise
                    logging.warning(f"Attempt {i+1} failed: {str(e)}. Retrying...")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

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
        logging.basicConfig(level=logging.INFO)

    def setup_blob_storage(self):
        """Setup Azure Blob Storage connection"""
        try:
            connect_str = os.getenv('AzureWebJobsStorage')
            if not connect_str:
                raise ValueError("No storage connection string found!")
            
            self.logger.info("Initializing blob storage client")
            self.blob_service_client = BlobServiceClient.from_connection_string(connect_str)
            self.container_name = "scraper-data"
            
            try:
                container_client = self.blob_service_client.get_container_client(self.container_name)
                container_client.get_container_properties()
                self.logger.info("Container exists")
            except Exception as e:
                self.logger.info(f"Creating new container: {str(e)}")
                self.blob_service_client.create_container(self.container_name)
        except Exception as e:
            self.logger.error(f"Error in blob storage setup: {str(e)}", exc_info=True)
            raise

    def setup_driver(self):
        """Setup Chrome driver with Azure-compatible options"""
        try:
            self.logger.info("Setting up Chrome driver...")
            chromedriver_autoinstaller.install()
            
            chrome_options = Options()
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--window-size=1920,1080')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--dns-prefetch-disable')
            chrome_options.binary_location = "/usr/bin/chromium-browser"
            chrome_options.add_argument(
                "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/90.0.4430.85 Safari/537.36"
            )
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.wait = WebDriverWait(self.driver, 20)
            self.logger.info("Chrome driver setup successful")
        except Exception as e:
            self.logger.error(f"Failed to setup Chrome driver: {str(e)}", exc_info=True)
            raise

    @retry_on_exception(retries=3, delay=2)
    def get_current_values(self):
        """Get the current values from the website"""
        try:
            if self.driver.current_url != self.url:
                self.logger.info(f"Navigating to {self.url}")
                self.driver.get(self.url)
                time.sleep(5)  # Wait for page to load

            self.logger.info("Getting multiplier...")
            multiplier_element = self.wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, "a.css-19toqs6"))
            )
            multiplier = float(multiplier_element.text.replace('x', '').strip())
            self.logger.info(f"Multiplier: {multiplier}")

            self.logger.info("Getting playing count...")
            playing_element = self.wait.until(
                EC.visibility_of_element_located(
                    (By.XPATH, '//span[contains(text(), "Playing")]/following-sibling::strong')
                )
            )
            playing = int(playing_element.text.strip())
            self.logger.info(f"Playing count: {playing}")

            self.logger.info("Getting online count...")
            online_element = self.wait.until(
                EC.visibility_of_element_located(
                    (By.XPATH, '//span[contains(text(), "Online")]/following-sibling::strong')
                )
            )
            online = int(online_element.text.strip())
            self.logger.info(f"Online count: {online}")

            timestamp = datetime.now()
            return multiplier, online, playing, timestamp
        except Exception as e:
            self.logger.error(f"Error getting values: {str(e)}", exc_info=True)
            return None, None, None, None

    def save_to_blob(self, data):
        """Save data to Azure Blob Storage"""
        try:
            self.logger.info("Creating DataFrame...")
            df = pd.DataFrame([data])
            csv_string = df.to_csv(index=False)
            
            blob_name = f"multiplier_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            self.logger.info(f"Uploading to blob: {blob_name}")
            
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, 
                blob=blob_name
            )
            blob_client.upload_blob(csv_string)
            
            self.logger.info(f"Successfully saved data to blob: {blob_name}")
        except Exception as e:
            self.logger.error(f"Error saving to blob: {str(e)}", exc_info=True)
            raise

    def cleanup(self):
        """Cleanup resources"""
        try:
            self.driver.quit()
            self.logger.info("Chrome driver cleaned up")
        except Exception as e:
            self.logger.error(f"Error during cleanup: {str(e)}")

@app.function_name(name="ScraperTrigger")
@app.schedule(schedule="*/5 * * * *", arg_name="timer", run_on_startup=True)
def scraper_trigger(timer: func.TimerRequest) -> None:
    """Azure Function timer trigger to run the scraper every 5 minutes"""
    logging.info('================ SCRAPER FUNCTION STARTED ================')
    logging.info(f'Function triggered at: {datetime.now().isoformat()}')
    
    # Check environment
    logging.info('Checking environment variables...')
    storage_connection = os.getenv('AzureWebJobsStorage')
    logging.info(f'Storage connection string exists: {bool(storage_connection)}')
    
    scraper = None
    try:
        logging.info('Initializing scraper...')
        scraper = AzureScraper()
        logging.info('Scraper initialized successfully')
        
        logging.info('Getting current values...')
        multiplier, online, playing, timestamp = scraper.get_current_values()
        
        if all(v is not None for v in [multiplier, online, playing, timestamp]):
            data = {
                'timestamp': timestamp.isoformat(),
                'multiplier': multiplier,
                'online': online,
                'playing': playing
            }
            logging.info(f'Got values successfully: {json.dumps(data)}')
            
            logging.info('Saving to blob storage...')
            scraper.save_to_blob(data)
            logging.info('Successfully saved to blob storage')
        else:
            logging.error("Failed to get valid values from the website")
            
    except Exception as e:
        logging.error(f"Error in scraper function: {str(e)}")
        logging.error(f"Traceback: {traceback.format_exc()}")
    finally:
        if scraper:
            logging.info('Cleaning up scraper...')
            scraper.cleanup()
            
    logging.info('================ SCRAPER FUNCTION COMPLETED ================')

