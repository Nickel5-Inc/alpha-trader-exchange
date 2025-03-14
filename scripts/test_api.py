#!/usr/bin/env python3
"""
Test script for the TaoStats API client.

This script tests the API client's ability to load API keys from environment
variables and make authenticated requests to the TaoStats API.

Usage:
    python scripts/test_api.py [coldkey]

Before running, make sure to:
1. Copy .env.example to .env
2. Add your actual API key to the .env file
"""

import asyncio
import logging
import sys
import json
import bittensor as bt
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from alphatrade.api.api import AlphaAPI
from alphatrade.utils.env import load_env, get_env
from alphatrade.database.models import StakeAction

# Remove logging configuration
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(levelname)s:%(name)s: %(message)s'
# )
# logger = logging.getLogger(__name__)

async def test_api():
    """Test the API client with environment variables."""
    # Load environment variables
    env_loaded = load_env()
    bt.logging.info(f"Environment variables loaded: {env_loaded}")
    
    # Get API key from environment
    api_key = get_env('TAOSTATS_API_KEY', '')
    if not api_key:
        bt.logging.warning("No API key found in environment variables. Some endpoints may not be accessible.")
    else:
        # Only show first 4 and last 4 characters of the API key
        masked_key = f"{api_key[:4]}...{api_key[-4:]}" if len(api_key) > 8 else api_key
        bt.logging.info(f"Using API key: {masked_key}")
    
    # Get coldkey from command line arguments or use default
    coldkey = sys.argv[1] if len(sys.argv) > 1 else "5CZzXqdJadV8csb3Bdvjhdfb8Wo9wqQ4aDiafAWBaGB2thb5"
    
    # Initialize API client
    async with AlphaAPI() as api:
        try:
            # Test pool data endpoint
            bt.logging.info("Testing pool data endpoint...")
            pools = await api.get_pool_data(netuid=30)
            if pools:
                bt.logging.info(f"Successfully fetched data for {len(pools)} pools")
                # Display pool data
                for pool in pools:
                    bt.logging.info(f"Pool {pool.netuid} details:")
                    bt.logging.info(f"  TAO Reserve: {pool.tao_reserve:.2f} TAO")
                    bt.logging.info(f"  Alpha Reserve: {pool.alpha_reserve:.2f} Alpha")
                    bt.logging.info(f"  Alpha Supply: {pool.alpha_supply:.2f} Alpha")
                    bt.logging.info(f"  Price: {pool.price:.6f} TAO/Alpha")
            else:
                bt.logging.warning("No pool data returned")
            
            # Test stake events endpoint
            bt.logging.info(f"\nTesting stake events endpoint for coldkey: {coldkey}")
            # Look back 90 days to increase chances of finding events
            start_time = datetime.now(timezone.utc) - timedelta(days=90)
            
            events = await api.get_stake_events(
                coldkey=coldkey, 
                start_time=start_time,
                per_page=10  # Limit to 10 per page for testing
            )
            
            if events:
                bt.logging.info(f"Successfully fetched {len(events)} stake events")
                
                # Count events by type
                delegates = sum(1 for e in events if e.action == StakeAction.DELEGATE)
                undelegates = sum(1 for e in events if e.action == StakeAction.UNDELEGATE)
                bt.logging.info(f"Events by type: DELEGATE: {delegates}, UNDELEGATE: {undelegates}")
                
                # Print first few events
                for i, event in enumerate(events[:3]):
                    bt.logging.info(f"Event {i+1}:")
                    bt.logging.info(f"  Block: {event.block_number}")
                    bt.logging.info(f"  Action: {event.action.name}")
                    bt.logging.info(f"  TAO: {event.tao_amount:.2f}")
                    bt.logging.info(f"  Alpha: {event.alpha_amount:.2f}")
                    bt.logging.info(f"  Time: {event.timestamp.isoformat()}")
                    bt.logging.info(f"  Netuid: {event.netuid}")
                    bt.logging.info(f"  Hotkey: {event.hotkey[:10]}...")
            else:
                bt.logging.warning(f"No stake events found for coldkey {coldkey}")
                bt.logging.info("Try running with a different coldkey: python scripts/test_api.py <coldkey>")
            
            bt.logging.info("\nAll API tests completed successfully!")
            
        except Exception as e:
            bt.logging.error(f"API test failed: {str(e)}")
            raise

if __name__ == "__main__":
    asyncio.run(test_api()) 