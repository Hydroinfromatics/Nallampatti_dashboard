import requests
import os
import logging
from datetime import datetime, timedelta
import pandas as pd
import time
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self, api_url: str):
        self.api_url = api_url.rstrip('/')  # Remove trailing slash if present
        self.credentials = {
            "username": os.getenv('API_USERNAME', 'Kamlesh123'),
            "password": os.getenv('API_PASSWORD', '1234567')
        }
        self.headers = {"Content-Type": "application/json"}
        self.token = None
        self.token_expiry = None
        self.last_sensor_update = None
        self.SENSOR_UPDATE_INTERVAL = timedelta(minutes=10)

    def _generate_token(self) -> bool:
        """Generate new authentication token"""
        try:
            # Log the full URL being used
            token_url = f"{self.api_url}/get_token"
            logger.info(f"Attempting to generate token at: {token_url}")
            
            response = requests.post(
                token_url,
                json=self.credentials,
                headers=self.headers,
                timeout=10
            )
            
            logger.info(f"Token generation response status: {response.status_code}")
            
            if response.status_code == 200:
                self.token = response.json().get("token")
                self.token_expiry = datetime.now()
                logger.info("Token generated successfully")
                return True
            else:
                logger.error(f"Token generation failed: {response.status_code}")
                logger.error(f"Response content: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Token generation error: {str(e)}")
            return False

    def should_fetch_data(self) -> bool:
        """Check if enough time has passed since last sensor update"""
        if not self.last_sensor_update:
            return True
        
        time_since_update = datetime.now() - self.last_sensor_update
        return time_since_update >= self.SENSOR_UPDATE_INTERVAL

    def get_data(self) -> Optional[pd.DataFrame]:
        """Fetch data from API with retry mechanism"""
        if not self.should_fetch_data():
            logger.info("Skipping fetch - not enough time elapsed since last sensor update")
            return None

        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                if not self.token or (datetime.now() - self.token_expiry).seconds > 3600:
                    if not self._generate_token():
                        logger.error("Failed to generate token")
                        time.sleep(retry_delay)
                        continue

                headers = {
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json"
                }

                # Log the full URL being used
                data_url = f"{self.api_url}/nallampatti_data"
                logger.info(f"Attempting to fetch data from: {data_url}")
                logger.info(f"Using headers: {headers}")

                response = requests.get(
                    data_url,
                    headers=headers,
                    timeout=10
                )

                logger.info(f"Data fetch response status: {response.status_code}")

                if response.status_code == 200:
                    data = response.json()
                    df = pd.DataFrame(data)
                    
                    # Check if we have new data
                    if self.last_sensor_update and not df.empty:
                        latest_timestamp = pd.to_datetime(df['timestamp']).max()
                        if latest_timestamp <= self.last_sensor_update:
                            logger.info("No new sensor data available")
                            return None
                    
                    self.last_sensor_update = datetime.now()
                    logger.info(f"Successfully fetched {len(df)} records")
                    return df
                    
                elif response.status_code == 401:
                    logger.warning("Token expired, retrying...")
                    self.token = None
                    time.sleep(retry_delay)
                    continue
                elif response.status_code == 404:
                    logger.error(f"API endpoint not found: {data_url}")
                    logger.error(f"Response content: {response.text}")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Data fetch failed with status {response.status_code}")
                    logger.error(f"Response content: {response.text}")
                    time.sleep(retry_delay)

            except requests.exceptions.RequestException as e:
                logger.error(f"Request error (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
            except Exception as e:
                logger.error(f"Unexpected error (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)

        return None

    def test_connection(self) -> bool:
        """Test API connection and authentication"""
        try:
            logger.info(f"Testing connection to API at: {self.api_url}")
            
            # Test base URL
            response = requests.get(self.api_url, timeout=5)
            logger.info(f"Base URL test response: {response.status_code}")
            
            # Test token generation
            if self._generate_token():
                logger.info("Token generation test successful")
                
                # Test data endpoint
                data_response = requests.get(
                    f"{self.api_url}/nallampatti_data",
                    headers={"Authorization": f"Bearer {self.token}"},
                    timeout=5
                )
                logger.info(f"Data endpoint test response: {data_response.status_code}")
                
                return True
            return False
            
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False
