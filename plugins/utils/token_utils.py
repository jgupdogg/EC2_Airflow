# plugins/utils/token_utils.py
import time
import json
import logging
import pandas as pd
import requests
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

def fetch_token_metadata_from_api(token_addresses):
    """
    Fetch token metadata from Birdeye API for multiple tokens.
    Returns a dictionary mapping addresses to their metadata.
    """
    # Get API key from Airflow connection
    try:
        connection = BaseHook.get_connection('fmp_api')
        extra_config = json.loads(connection.extra) if connection.extra else {}
        api_key = extra_config.get('apikey', '')
    except Exception as e:
        logger.error(f"Error retrieving API key: {e}")
        api_key = ''
    
    api_base_url = "https://public-api.birdeye.so/defi/v3/token/meta-data/single"
    headers = {
        "accept": "application/json",
        "x-chain": "solana"
    }
    
    if api_key:
        headers["x-api-key"] = api_key
    
    results = {}
    for i, address in enumerate(token_addresses):
        # Rate limiting
        if i > 0 and i % 5 == 0:
            time.sleep(2)
            
        try:
            params = {"address": address}
            response = requests.get(api_base_url, headers=headers, params=params, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and "data" in data:
                    results[address] = {
                        "ADDRESS": data["data"].get("address"),
                        "SYMBOL": data["data"].get("symbol", ""),
                        "NAME": data["data"].get("name", ""),
                        "DECIMALS": data["data"].get("decimals")
                    }
        except Exception as e:
            logger.error(f"Error fetching metadata for {address}: {e}")
    
    return results