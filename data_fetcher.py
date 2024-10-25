import requests
import os
import logging
from datetime import datetime
import pandas as pd
import time
from typing import Optional, Dict, Any

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self, api_url: str):
        self.api_url = api_url
        self.credentials = {
            "username": os.getenv('API_USERNAME', 'default_user'),
            "password": os.getenv('API_PASSWORD', 'default_pass')
        }
        self.headers = {"Content-Type": "application/json"}
        self.token = None
        self.token_expiry = None

    def _generate_token(self) -> bool:
        """Generate new authentication token"""
        try:
            response = requests.post(
                f"{self.api_url}/get_token",
                json=self.credentials,
                headers=self.headers,
                timeout=10
            )
            if response.status_code == 200:
                self.token = response.json().get("token")
                self.token_expiry = datetime.now()
                return True
            logger.error(f"Token generation failed: {response.status_code}")
            return False
        except Exception as e:
            logger.error(f"Token generation error: {str(e)}")
            return False

    def get_data(self) -> Optional[pd.DataFrame]:
        """Fetch data from API with retry mechanism"""
        max_retries = 3
        retry_delay = 5

        for attempt in range(max_retries):
            try:
                if not self.token or (datetime.now() - self.token_expiry).seconds > 3600:
                    if not self._generate_token():
                        continue

                headers = {
                    "Authorization": f"Bearer {self.token}",
                    "Content-Type": "application/json"
                }

                response = requests.get(
                    f"{self.api_url}/data",
                    headers=headers,
                    timeout=10
                )

                if response.status_code == 200:
                    data = response.json()
                    return pd.DataFrame(data)
                elif response.status_code == 401:
                    logger.warning("Token expired, retrying...")
                    self.token = None
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(f"Data fetch failed: {response.status_code}")
                    return None

            except Exception as e:
                logger.error(f"Data fetch error (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)

        return None