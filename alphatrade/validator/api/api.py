import asyncio
import aiohttp
import logging
import bittensor as bt
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Set
import pandas as pd

from alphatrade.validator.database.models import StakeAction, StakeEvent, ApiTrade, PoolData
from alphatrade.utils.env import get_env, load_env

class AlphaAPI:
    """Client for interacting with Alpha Token Trading APIs."""
    
    def __init__(
        self, 
        base_url: str = None,
        api_key: str = None
    ):
        """Initialize the API client.
        
        Args:
            base_url: Base URL for the API. If None, uses the TAOSTATS_API_URL environment variable
                     or falls back to the default "https://api.taostats.io".
            api_key: API key for authentication. If None, uses the TAOSTATS_API_KEY environment variable.
        """
        # Load environment variables if not already loaded
        load_env()
        
        # Set base URL from args, env var, or default
        self.base_url = base_url or get_env('TAOSTATS_API_URL', 'https://api.taostats.io')
        
        # Set API key from args or env var
        self.api_key = api_key or get_env('TAOSTATS_API_KEY', '')
        
        self.session: Optional[aiohttp.ClientSession] = None
        self._rate_limit_remaining = 100
        self._rate_limit_reset = 0
        
        # Log configuration status
        if self.api_key:
            bt.logging.info(f"API client initialized with API key: {self.api_key[:4]}...{self.api_key[-4:] if len(self.api_key) > 8 else ''}")
        else:
            bt.logging.warning("API client initialized without API key. Some endpoints may not be accessible.")

    async def __aenter__(self):
        """Create aiohttp session when entering context."""
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Close aiohttp session when exiting context."""
        if self.session:
            await self.session.close()
            self.session = None

    async def _make_request(self, endpoint: str, params: Optional[Dict] = None) -> Dict:
        """Make an API request with rate limiting and error handling.
        
        Args:
            endpoint (str): API endpoint to call
            params (Dict, optional): Query parameters
            
        Returns:
            Dict: API response data
            
        Raises:
            Exception: If API request fails
        """
        if not self.session:
            raise RuntimeError("API client not initialized. Use 'async with' context manager.")
            
        # Handle rate limiting
        if self._rate_limit_remaining == 0:
            wait_time = self._rate_limit_reset - datetime.now().timestamp()
            if wait_time > 0:
                bt.logging.info(f"Rate limit reached. Waiting {wait_time:.2f} seconds...")
                await asyncio.sleep(wait_time)
        
        url = f"{self.base_url}/{endpoint}"
        # bt.logging.info(f"Making request to {url}")
        
        # Prepare headers with API key if available
        headers = {
            "accept": "application/json"
        }
        if self.api_key:
            # Use the API key directly as the Authorization header value
            headers['Authorization'] = self.api_key
            # bt.logging.info(f"Using API key: ****{self.api_key[-8:] if len(self.api_key) > 8 else ''}")
            
        try:
            async with self.session.get(url, params=params, headers=headers) as response:
                # Update rate limit info if headers are present
                if 'X-RateLimit-Remaining' in response.headers:
                    self._rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 100))
                if 'X-RateLimit-Reset' in response.headers:
                    self._rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', 0))
                
                # Handle common error cases
                if response.status == 401:
                    error_msg = "API authentication failed. Check your API key."
                    bt.logging.error(error_msg)
                    # Try to get more details from response body
                    try:
                        error_data = await response.json()
                        if 'message' in error_data:
                            error_msg = f"{error_msg} Details: {error_data['message']}"
                    except:
                        pass
                    raise RuntimeError(error_msg)
                
                if response.status == 404:
                    error_msg = f"Endpoint not found: {endpoint}"
                    bt.logging.error(f"API request failed: {response.status}, message='{error_msg}', url='{url}'")
                    raise aiohttp.ClientResponseError(
                        response.request_info,
                        response.history,
                        status=response.status,
                        message=error_msg,
                        headers=response.headers
                    )
                
                if response.status == 429:  # Too Many Requests
                    retry_after = int(response.headers.get('Retry-After', 60))
                    bt.logging.warning(f"Rate limit exceeded. Waiting {retry_after} seconds... This is expected.")
                    await asyncio.sleep(retry_after)
                    return await self._make_request(endpoint, params)
                
                # For other errors, raise with status code
                if response.status >= 400:
                    error_msg = f"API request failed with status {response.status}"
                    try:
                        error_data = await response.json()
                        if 'message' in error_data:
                            error_msg = f"{error_msg}: {error_data['message']}"
                    except:
                        pass
                    bt.logging.error(error_msg)
                    response.raise_for_status()
                
                # Parse successful response
                data = await response.json()
                return data
                
        except aiohttp.ClientError as e:
            bt.logging.error(f"API request failed: {str(e)}")
            raise

    async def get_stake_events(
        self,
        coldkey: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        page: int = 1,
        per_page: int = 100
    ) -> List[StakeEvent]:
        """Fetch stake/unstake events for a specific coldkey.
        
        Args:
            coldkey (str): Miner's coldkey (SS58 address)
            start_time (datetime, optional): Start time for history
            end_time (datetime, optional): End time for history
            page (int): Page number for pagination
            per_page (int): Number of items per page
            
        Returns:
            List[StakeEvent]: List of stake events
        """
        params = {
            'nominator': coldkey,  # API expects 'nominator' parameter
            'page': page,
            'per_page': per_page
        }
        if start_time:
            params['start_time'] = int(start_time.timestamp())
        if end_time:
            params['end_time'] = int(end_time.timestamp())
        
        all_events = []
        try:
            # Use the correct endpoint
            endpoint = 'api/delegation/v1'
            
            data = await self._make_request(endpoint, params)
            
            # Get pagination info and data length
            pagination = data.get('pagination', {})
            total_items = pagination.get('total_items', 0)
            
            # Only log if there are actual items to process
            if total_items > 0 and page == 1:
                bt.logging.info(f"First API response for coldkey {coldkey}: {pagination}")
            
            # Process events from current page
            page_events = []
            for event_data in data.get('data', []):
                try:
                    # Safely convert values with defensive error handling
                    # Default to 0 if any value is None
                    try:
                        amount = float(event_data.get('amount', 0)) / 1e9
                    except (TypeError, ValueError):
                        amount = 0
                        
                    try:
                        alpha = float(event_data.get('alpha', 0)) / 1e9
                    except (TypeError, ValueError):
                        alpha = 0
                        
                    try:
                        alpha_price_tao = float(event_data.get('alpha_price_in_tao', 0))
                    except (TypeError, ValueError):
                        alpha_price_tao = 0
                        
                    try:
                        alpha_price_usd = float(event_data.get('alpha_price_in_usd', 0))
                    except (TypeError, ValueError):
                        alpha_price_usd = 0
                        
                    try:
                        usd_amount = float(event_data.get('usd', 0))
                    except (TypeError, ValueError):
                        usd_amount = 0
                    
                    # Safely get nominator/delegate SS58 addresses
                    nominator = event_data.get('nominator', {})
                    delegate = event_data.get('delegate', {})
                    
                    nominator_ss58 = nominator.get('ss58') if isinstance(nominator, dict) else coldkey
                    delegate_ss58 = delegate.get('ss58') if isinstance(delegate, dict) else ""
                    
                    # Map API action to our StakeAction enum
                    action_str = event_data.get('action', 'DELEGATE')
                    action = StakeAction.DELEGATE if action_str == 'DELEGATE' else StakeAction.UNDELEGATE
                    
                    # Format timestamp, defaulting to current time if missing
                    timestamp_str = event_data.get('timestamp')
                    if timestamp_str:
                        timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    else:
                        timestamp = datetime.now()
                    
                    # Get extrinsic ID - use a default if not present
                    extrinsic_id = event_data.get('extrinsic_id', f"missing-{timestamp.isoformat()}-{nominator_ss58}")
                    
                    page_events.append(StakeEvent(
                        block_number=event_data.get('block_number', 0),
                        timestamp=timestamp,
                        netuid=event_data.get('netuid', 0),
                        action=action,
                        coldkey=nominator_ss58,
                        hotkey=delegate_ss58,
                        tao_amount=amount,
                        alpha_amount=alpha,
                        extrinsic_id=extrinsic_id,
                        alpha_price_in_tao=alpha_price_tao,
                        alpha_price_in_usd=alpha_price_usd,
                        usd_amount=usd_amount
                    ))
                except Exception as e:
                    bt.logging.warning(f"Error processing event: {str(e)}")
                    continue
            
            # Add current page events to all events
            all_events.extend(page_events)
            
            # Handle pagination using a non-recursive approach
            next_page = pagination.get('next_page')
            total_pages = pagination.get('total_pages', 1)
            
            if total_items > 0 and page == 1:
                bt.logging.info(f"Fetched {len(page_events)} stake events for {coldkey} (page {page}/{total_pages})")
            
            # If there are more pages to fetch, use a non-recursive approach
            if next_page and page < total_pages and total_items > 0:
                try:
                    # Start a loop to fetch additional pages
                    current_page = next_page
                    while current_page and current_page <= total_pages:
                        # Update page parameter
                        params['page'] = current_page
                        
                        # Log progress for every 5th page or last page
                        if current_page % 5 == 0 or current_page == total_pages:
                            bt.logging.info(f"Fetching page {current_page} of {total_pages} for coldkey {coldkey}")
                        
                        # Make request for next page
                        next_data = await self._make_request(endpoint, params)
                        next_pagination = next_data.get('pagination', {})
                        
                        # Process events from this page with error handling
                        for event_data in next_data.get('data', []):
                            try:
                                # Safely convert values with defensive error handling
                                try:
                                    amount = float(event_data.get('amount', 0)) / 1e9
                                except (TypeError, ValueError):
                                    amount = 0
                                    
                                try:
                                    alpha = float(event_data.get('alpha', 0)) / 1e9
                                except (TypeError, ValueError):
                                    alpha = 0
                                    
                                try:
                                    alpha_price_tao = float(event_data.get('alpha_price_in_tao', 0))
                                except (TypeError, ValueError):
                                    alpha_price_tao = 0
                                    
                                try:
                                    alpha_price_usd = float(event_data.get('alpha_price_in_usd', 0))
                                except (TypeError, ValueError):
                                    alpha_price_usd = 0
                                    
                                try:
                                    usd_amount = float(event_data.get('usd', 0))
                                except (TypeError, ValueError):
                                    usd_amount = 0
                                
                                # Safely get nominator/delegate SS58 addresses
                                nominator = event_data.get('nominator', {})
                                delegate = event_data.get('delegate', {})
                                
                                nominator_ss58 = nominator.get('ss58') if isinstance(nominator, dict) else coldkey
                                delegate_ss58 = delegate.get('ss58') if isinstance(delegate, dict) else ""
                                
                                # Map API action to our StakeAction enum
                                action_str = event_data.get('action', 'DELEGATE')
                                action = StakeAction.DELEGATE if action_str == 'DELEGATE' else StakeAction.UNDELEGATE
                                
                                # Format timestamp, defaulting to current time if missing
                                timestamp_str = event_data.get('timestamp')
                                if timestamp_str:
                                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                                else:
                                    timestamp = datetime.now()
                                
                                # Get extrinsic ID - use a default if not present
                                extrinsic_id = event_data.get('extrinsic_id', f"missing-{timestamp.isoformat()}-{nominator_ss58}")
                                
                                all_events.append(StakeEvent(
                                    block_number=event_data.get('block_number', 0),
                                    timestamp=timestamp,
                                    netuid=event_data.get('netuid', 0),
                                    action=action,
                                    coldkey=nominator_ss58,
                                    hotkey=delegate_ss58,
                                    tao_amount=amount,
                                    alpha_amount=alpha,
                                    extrinsic_id=extrinsic_id,
                                    alpha_price_in_tao=alpha_price_tao,
                                    alpha_price_in_usd=alpha_price_usd,
                                    usd_amount=usd_amount
                                ))
                            except Exception as e:
                                bt.logging.warning(f"Error processing event on page {current_page}: {str(e)}")
                                continue
                        
                        # Move to next page if available
                        current_page = next_pagination.get('next_page')
                        
                    # Log final count for all fetched events
                    if total_items > per_page:
                        bt.logging.info(f"Completed fetching all {len(all_events)} stake events for {coldkey}")
                except Exception as e:
                    bt.logging.error(f"Error during pagination: {str(e)}")
                    # Continue with the events already fetched
        
        except Exception as e:
            bt.logging.error(f"Failed to fetch stake events: {str(e)}")
            # Return empty list instead of raising to allow other operations to continue
        
        return all_events

    async def get_active_positions_at(self, coldkey: str, timestamp: datetime) -> Set[int]:
        """Get set of netuids where the miner has active positions at a given timestamp.
        
        Args:
            coldkey (str): Miner's coldkey
            timestamp (datetime): Timestamp to check positions at
            
        Returns:
            Set[int]: Set of netuids with active positions
        """
        params = {
            'coldkey': coldkey,
            'end_time': int(timestamp.timestamp()),
            'per_page': 1000
        }
        
        data = await self._make_request('api/dtao/delegation/v1', params)
        
        # Get data length
        total_items = len(data.get('data', []))
        
        # Only log if there are actual items
        if total_items > 0:
            bt.logging.info(f"Position response for coldkey {coldkey}: total items {total_items}")
        
        # Track active positions
        active_positions = set()
        
        for event in sorted(data['data'], key=lambda x: x['block_number']):
            netuid = event['netuid']
            if event['action'] == StakeAction.STAKE_ADDED.value:
                active_positions.add(netuid)
            elif event['action'] == StakeAction.STAKE_REMOVED.value and netuid in active_positions:
                active_positions.remove(netuid)
        
        # Only log active positions if there are any
        if active_positions:
            bt.logging.info(f"Found {len(active_positions)} active positions for coldkey {coldkey}")
                
        return active_positions

    def process_events_to_trades(
        self, 
        events: List[StakeEvent],
        start_tracking_time: datetime
    ) -> List[ApiTrade]:
        """Convert stake events into trades by matching stakes and unstakes.
        Excludes any positions that existed before start_tracking_time.
        
        Args:
            events (List[StakeEvent]): List of stake events to process
            start_tracking_time (datetime): Time to start tracking from
            
        Returns:
            List[ApiTrade]: List of processed trades
        """
        # Sort events by timestamp
        events = sorted(events, key=lambda x: x.timestamp)
        
        # Group events by netuid
        netuid_events = {}
        for event in events:
            if event.netuid not in netuid_events:
                netuid_events[event.netuid] = []
            netuid_events[event.netuid].append(event)
            
        trades = []
        
        for netuid, subnet_events in netuid_events.items():
            # List of (timestamp, tao_in, alpha_received, hotkey, extrinsic_id)
            positions = []  
            
            for event in subnet_events:
                # Skip events before start_tracking_time
                if event.timestamp < start_tracking_time:
                    continue
                    
                if event.action == StakeAction.STAKE_ADDED or event.action == StakeAction.DELEGATE:
                    positions.append((
                        event.timestamp,
                        event.tao_amount,
                        event.alpha_amount,
                        event.hotkey,
                        event.extrinsic_id
                    ))
                elif (event.action == StakeAction.STAKE_REMOVED or event.action == StakeAction.UNDELEGATE) and positions:
                    # Use FIFO for cost basis
                    entry = positions.pop(0)
                    
                    # Calculate ROI
                    roi = ((event.tao_amount - entry[1]) / entry[1]) * 100 if entry[1] > 0 else 0
                    
                    trades.append(ApiTrade(
                        timestamp_enter=entry[0],
                        timestamp_exit=event.timestamp,
                        netuid=netuid,
                        coldkey=event.coldkey,
                        hotkey=entry[3],
                        tao_in=entry[1],
                        alpha_received=entry[2],
                        tao_out=event.tao_amount,
                        alpha_returned=event.alpha_amount,
                        roi_tao=roi,
                        duration=event.timestamp - entry[0],
                        extrinsic_id_enter=entry[4],
                        extrinsic_id_exit=event.extrinsic_id
                    ))
        
        # Only log trade count if there are trades            
        if trades:
            bt.logging.info(f"Processed {len(trades)} trades from {len(events)} events")
                    
        return trades

    async def get_pool_data(self, netuid: int = 30) -> List[PoolData]:
        """Fetch current pool data for all subnets.
        
        Args:
            netuid (int): Network UID to fetch data for. Defaults to 30.
            
        Returns:
            List[PoolData]: List of pool information
        """
        try:
            # Use the correct endpoint provided by the user
            endpoint = 'api/dtao/pool/latest/v1'
            params = {
                'netuid': netuid,
                'page': 1
            }
            
            data = await self._make_request(endpoint, params)
            
            # Get data length
            total_items = len(data.get('data', []))
            
            # Only log if there are items
            if total_items > 0:
                bt.logging.info(f"Pool data response for netuid {netuid}: total items {total_items}")
            
            pools = []
            for pool in data.get('data', []):
                try:
                    # Extract pool data based on the actual response format
                    pools.append(PoolData(
                        netuid=pool['netuid'],
                        tao_reserve=float(pool['total_tao']) / 1e9,  # Convert from nano-TAO
                        alpha_reserve=float(pool['alpha_in_pool']) / 1e9,  # Convert from nano-alpha
                        alpha_supply=float(pool['total_alpha']) / 1e9,  # Convert from nano-alpha
                        emission_rate=0.0,  # Not provided in the response
                        price=float(pool['price'])
                    ))
                    
                except (KeyError, ValueError) as e:
                    bt.logging.warning(f"Error processing pool data: {str(e)}")
                    continue
            
            # Only log pool count if there are pools
            if pools:
                bt.logging.info(f"Fetched data for {len(pools)} pools")
            return pools
            
        except Exception as e:
            bt.logging.error(f"Failed to fetch pool data: {str(e)}")
            # Return empty list instead of raising to allow other operations to continue
            return []

    async def get_weekly_performance(
        self,
        coldkey: str,
        start_tracking_from: datetime
    ) -> Dict[str, Any]:
        """Get weekly performance metrics for a miner.
        
        Args:
            coldkey (str): Miner's coldkey
            start_tracking_from (datetime): When to start tracking trades from
            
        Returns:
            Dict[str, Any]: Performance metrics including ROI, volume, etc.
        """
        end_time = datetime.now()
        start_time = end_time - timedelta(days=1)
        
        # Get events and process trades
        events = await self.get_stake_events(coldkey, start_time, end_time)
        trades = self.process_events_to_trades(events, start_tracking_from)
        
        # Get currently active positions (positions opened after start_tracking_from)
        active_positions = set()
        for event in events:
            if event.timestamp < start_tracking_from:
                continue
            if event.action == StakeAction.STAKE_ADDED or event.action == StakeAction.DELEGATE:
                active_positions.add(event.netuid)
            elif (event.action == StakeAction.STAKE_REMOVED or event.action == StakeAction.UNDELEGATE) and event.netuid in active_positions:
                active_positions.remove(event.netuid)
        
        result = {
            'roi_avg': 0.0,
            'trade_count': 0,
            'total_volume_tao': 0.0,
            'total_volume_alpha': 0.0,
            'avg_trade_duration': 0.0,
            'active_positions': len(active_positions)
        }
        
        if trades:
            result.update({
                'roi_avg': sum(t.roi_tao for t in trades) / len(trades),
                'trade_count': len(trades),
                'total_volume_tao': sum(t.tao_in for t in trades),
                'total_volume_alpha': sum(t.alpha_received for t in trades),
                'avg_trade_duration': sum(t.duration.total_seconds() for t in trades) / len(trades),
            })
            
            # Only log performance metrics if there are trades
            bt.logging.info(f"Weekly performance for {coldkey}: {len(trades)} trades, avg ROI: {result['roi_avg']:.2f}%")
        
        return result