# utils/token_utils.py
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Any, Optional
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)

def fetch_token_metadata_from_api(token_addresses: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    Fetch token metadata from Birdeye API for multiple tokens.
    Uses parallel requests to optimize speed.
    
    Args:
        token_addresses: List of token addresses to fetch metadata for
        
    Returns:
        Dictionary mapping token addresses to their metadata
    """
    if not token_addresses:
        return {}
    
    # Remove duplicates
    token_addresses = list(set(token_addresses))
    logger.info(f"Fetching metadata for {len(token_addresses)} tokens")
    
    results = {}
    max_workers = min(10, len(token_addresses))  # Limit concurrent requests
    
    # Get API key from Airflow connection
    try:
        connection = BaseHook.get_connection("birdeye")
        api_key = connection.extra_dejson.get("apikey", "")
        logger.info("Successfully retrieved Birdeye API key from connection")
    except Exception as e:
        logger.error(f"Failed to retrieve Birdeye API key: {str(e)}")
        api_key = ""
    
    # Define the function to fetch a single token
    def fetch_single_token(address: str) -> Optional[Dict[str, Any]]:
        try:
            url = f"https://public-api.birdeye.so/defi/v3/token/meta-data/single?address={address}"
            headers = {
                "accept": "application/json",
                "x-chain": "solana",
                "X-API-KEY": api_key
            }
            logger.debug(f"Requesting metadata for {address}")
            response = requests.get(url, headers=headers, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("success") and "data" in data:
                    token_data = data["data"]
                    # Extract and normalize the fields we need
                    return {
                        "ADDRESS": address,
                        "SYMBOL": token_data.get("symbol", ""),
                        "NAME": token_data.get("name", ""),
                        "DECIMALS": token_data.get("decimals", "NULL")  # Use "NULL" as string if None
                    }
            logger.warning(f"Failed to fetch metadata for {address}: {response.status_code}")
            logger.debug(f"Response: {response.text}")
            return None
        except Exception as e:
            logger.error(f"Error fetching metadata for {address}: {str(e)}")
            return None
    
    # Use ThreadPoolExecutor for parallel requests
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_address = {executor.submit(fetch_single_token, address): address for address in token_addresses}
        
        for future in as_completed(future_to_address):
            address = future_to_address[future]
            try:
                metadata = future.result()
                if metadata:
                    results[address] = metadata
                else:
                    # Create minimal metadata for failed requests
                    results[address] = {
                        "ADDRESS": address,
                        "SYMBOL": "",
                        "NAME": "",
                        "DECIMALS": "NULL"  # Use SQL NULL as string, not Python None
                    }
            except Exception as e:
                logger.error(f"Exception processing {address}: {str(e)}")
                # Add minimal entry for error cases
                results[address] = {
                    "ADDRESS": address,
                    "SYMBOL": "",
                    "NAME": "",
                    "DECIMALS": "NULL"  # Use SQL NULL as string, not Python None
                }
    
    return results