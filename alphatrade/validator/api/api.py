import asyncio
import aiohttp
import logging
import bittensor as bt
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any, Tuple, Set
import pandas as pd

from alphatrade.validator.database.models import StakeAction, StakeEvent, ApiTrade, PoolData
from alphatrade.utils.env import get_env, load_env

class AlphaAPI:
    """Client for interacting with Tao App APIs."""
    
    def __init__(
        self, 
        base_url: str = None,
        api_key: str = None
    ):
        """Initialize the API client.
        
        Args:
            base_url: Base URL for the API. If None, uses the TAO_APP_API_URL environment variable
                     or falls back to the default "https://api.tao.app".
            api_key: API key for authentication. If None, uses the TAOAPP_API_KEY environment variable.
        """
        # Load environment variables if not already loaded
        load_env()
        
        # Set base URL from args, env var, or default
        self.base_url = base_url or get_env('TAO_APP_API_URL', 'https://api.tao.app')
        
        # Set API key from args or env var
        self.api_key = api_key or get_env('TAOAPP_API_KEY', '')
        
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
        
        # Prepare headers with API key
        headers = {
            "accept": "application/json",
            "X-API-Key": self.api_key
        }
            
        try:
            async with self.session.get(url, params=params, headers=headers) as response:
                # Update rate limit info if headers are present
                if 'X-RateLimit-Remaining' in response.headers:
                    self._rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 1000))
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
        per_page: int = 1000
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
            'coldkey': coldkey,
            'page': page,
            'page_size': per_page
        }
        
        # Ensure start_time and end_time are timezone-aware if provided
        if start_time and start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        
        if end_time and end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        
        all_events = []
        try:
            endpoint = 'api/beta/portfolio/transactions'
            
            data = await self._make_request(endpoint, params)
            
            # Process events from current page
            page_events = []
            for event_data in data.get('data', []):
                try:
                    # Parse timestamp
                    timestamp_str = event_data.get('timestamp')
                    if timestamp_str:
                        try:
                            # Parse timestamp and ensure it's timezone-aware
                            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                            if timestamp.tzinfo is None:  # If timezone info is missing
                                timestamp = timestamp.replace(tzinfo=timezone.utc)
                        except ValueError:
                            # Fallback if parsing fails
                            bt.logging.warning(f"Failed to parse timestamp: {timestamp_str}, using current time")
                            timestamp = datetime.now(timezone.utc)
                    else:
                        timestamp = datetime.now(timezone.utc)
                    
                    # Filter by timestamp if needed
                    if start_time and timestamp < start_time:
                        continue
                    if end_time and timestamp > end_time:
                        continue
                    
                    # Map event_id to our StakeAction enum
                    event_id = event_data.get('event_id', '').lower()
                    if event_id == 'stakeadded':
                        action = StakeAction.DELEGATE
                    elif event_id == 'stakeremoved':
                        action = StakeAction.UNDELEGATE
                    else:
                        # Skip unknown event types
                        continue
                    
                    # Convert values with defensive error handling
                    try:
                        netuid = int(event_data.get('netuid', 0))
                    except (TypeError, ValueError):
                        netuid = 0
                        
                    try:
                        # For StakeAdded: amount_in is TAO, amount_out is ALPHA
                        # For StakeRemoved: amount_in is ALPHA, amount_in is TAO
                        if action == StakeAction.DELEGATE:  # StakeAdded
                            tao_amount = float(event_data.get('amount_in', 0))
                            alpha_amount = float(event_data.get('amount_out', 0))
                        else:  # StakeRemoved
                            alpha_amount = float(event_data.get('amount_out', 0))
                            tao_amount = float(event_data.get('amount_in', 0))
                    except (TypeError, ValueError):
                        tao_amount = 0
                        alpha_amount = 0
                    
                    # Calculate price (for reference only)
                    price = 0
                    if action == StakeAction.DELEGATE and alpha_amount > 0:
                        price = tao_amount / alpha_amount
                    elif action == StakeAction.UNDELEGATE and alpha_amount > 0:
                        price = tao_amount / alpha_amount
                    
                    # Extract hotkey or use default
                    hotkey = event_data.get('hotkey', '')
                    
                    # Get extrinsic ID from the event data
                    extrinsic_id = event_data.get('extrinsics', f"missing-{timestamp.isoformat()}-{coldkey}")
                    
                    # Create StakeEvent object
                    page_events.append(StakeEvent(
                        block_number=0,
                        timestamp=timestamp,
                        netuid=netuid,
                        action=action,
                        coldkey=coldkey,
                        hotkey=hotkey,
                        tao_amount=tao_amount,
                        alpha_amount=alpha_amount,
                        extrinsic_id=extrinsic_id,
                        alpha_price_in_tao=price,
                        alpha_price_in_usd=0,  # USD price not provided in the new API
                        usd_amount=0  # USD amount not provided in the new API
                    ))
                except Exception as e:
                    bt.logging.warning(f"Error processing event: {str(e)}")
                    continue
            
            # Add current page events to all events
            all_events.extend(page_events)
            
            # Handle pagination
            next_page = data.get('next_page')
            total = data.get('total', 0)
            total_pages = (total // per_page) + (1 if total % per_page > 0 else 0) if total > 0 else 1
            
            if len(page_events) > 0:
                bt.logging.info(f"Fetched {len(page_events)} stake events for {coldkey} (page {page}/{total_pages})")
            
            # If there are more pages to fetch, use a non-recursive approach
            current_page = next_page
            while current_page and int(current_page) <= total_pages:
                # Update page parameter
                params['page'] = current_page
                
                # Log progress for every 5th page or last page
                if int(current_page) % 5 == 0 or int(current_page) == total_pages:
                    bt.logging.info(f"Fetching page {current_page} of {total_pages} for coldkey {coldkey}")
                
                # Make request for next page
                next_data = await self._make_request(endpoint, params)
                
                # Process events from this page
                next_page_events = []
                for event_data in next_data.get('data', []):
                    try:
                        # Parse timestamp
                        timestamp_str = event_data.get('timestamp')
                        if timestamp_str:
                            try:
                                # Parse timestamp and ensure it's timezone-aware
                                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                                if timestamp.tzinfo is None:  # If timezone info is missing
                                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                            except ValueError:
                                # Fallback if parsing fails
                                bt.logging.warning(f"Failed to parse timestamp: {timestamp_str}, using current time")
                                timestamp = datetime.now(timezone.utc)
                        else:
                            timestamp = datetime.now(timezone.utc)
                        
                        # Filter by timestamp if needed
                        if start_time and timestamp < start_time:
                            continue
                        if end_time and timestamp > end_time:
                            continue
                        
                        # Map event_id to our StakeAction enum
                        event_id = event_data.get('event_id', '').lower()
                        if event_id == 'stakeadded':
                            action = StakeAction.DELEGATE
                        elif event_id == 'stakeremoved':
                            action = StakeAction.UNDELEGATE
                        else:
                            # Skip unknown event types
                            continue
                        
                        # Convert values with defensive error handling
                        try:
                            netuid = int(event_data.get('netuid', 0))
                        except (TypeError, ValueError):
                            netuid = 0
                            
                        try:
                            # For StakeAdded: amount_in is TAO, amount_out is ALPHA
                            # For StakeRemoved: amount_in is ALPHA, amount_out is TAO
                            if action == StakeAction.DELEGATE:
                                tao_amount = float(event_data.get('amount_in', 0))
                                alpha_amount = float(event_data.get('amount_out', 0))
                            else:  # UNDELEGATE
                                alpha_amount = float(event_data.get('amount_out', 0))
                                tao_amount = float(event_data.get('amount_in', 0))
                        except (TypeError, ValueError):
                            tao_amount = 0
                            alpha_amount = 0
                        
                        # Calculate alpha price in TAO
                        alpha_price_tao = 0
                        if action == StakeAction.DELEGATE and alpha_amount > 0:
                            alpha_price_tao = tao_amount / alpha_amount
                        elif action == StakeAction.UNDELEGATE and alpha_amount > 0:
                            alpha_price_tao = tao_amount / alpha_amount
                        
                        # Extract hotkey from the event data or use a default
                        hotkey = event_data.get('hotkey', '')
                        
                        # Get extrinsic ID from the event data
                        extrinsic_id = event_data.get('extrinsics', f"missing-{timestamp.isoformat()}-{coldkey}")
                        
                        # Create StakeEvent object
                        next_page_events.append(StakeEvent(
                            block_number=0,  # Block number not provided in the new API
                            timestamp=timestamp,
                            netuid=netuid,
                            action=action,
                            coldkey=coldkey,
                            hotkey=hotkey,
                            tao_amount=tao_amount,
                            alpha_amount=alpha_amount,
                            extrinsic_id=extrinsic_id,
                            alpha_price_in_tao=alpha_price_tao,
                            alpha_price_in_usd=0,  # USD price not provided in the new API
                            usd_amount=0  # USD amount not provided in the new API
                        ))
                    except Exception as e:
                        bt.logging.warning(f"Error processing event on page {current_page}: {str(e)}")
                        continue
                
                # Add next page events to all events
                all_events.extend(next_page_events)
                
                # Move to next page if available
                next_page = next_data.get('next_page')
                current_page = next_page
            
            # Log final count for all fetched events
            if len(all_events) > per_page:
                bt.logging.info(f"Completed fetching all {len(all_events)} stake events for {coldkey}")
        
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
        # Ensure timestamp is timezone-aware
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
            
        params = {
            'coldkey': coldkey,
            'page': 1,
            'page_size': 1000
        }
        
        data = await self._make_request('api/beta/portfolio/transactions', params)
        
        # Get data length
        total_items = len(data.get('data', []))
        
        # Only log if there are actual items
        if total_items > 0:
            bt.logging.info(f"Position response for coldkey {coldkey}: total items {total_items}")
        
        # Track active positions
        active_positions = set()
        
        # Sort events by timestamp
        events = sorted(data.get('data', []), key=lambda x: x.get('timestamp', ''))
        
        for event in events:
            # Parse timestamp and ensure it's timezone-aware
            event_timestamp_str = event.get('timestamp', '')
            try:
                event_timestamp = datetime.fromisoformat(event_timestamp_str.replace('Z', '+00:00'))
                if event_timestamp.tzinfo is None:
                    event_timestamp = event_timestamp.replace(tzinfo=timezone.utc)
            except ValueError:
                bt.logging.warning(f"Failed to parse timestamp: {event_timestamp_str}, skipping event")
                continue
                
            # Skip events after the specified timestamp
            if event_timestamp > timestamp:
                continue
                
            try:
                netuid = int(event.get('netuid', 0))
                event_id = event.get('event_id', '').lower()
                
                if event_id == 'stakeadded':
                    active_positions.add(netuid)
                elif event_id == 'stakeremoved' and netuid in active_positions:
                    active_positions.remove(netuid)
            except (ValueError, TypeError) as e:
                bt.logging.warning(f"Error processing event for active positions: {str(e)}")
                continue
        
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
        # Ensure start_tracking_time is timezone-aware
        if start_tracking_time.tzinfo is None:
            start_tracking_time = start_tracking_time.replace(tzinfo=timezone.utc)
            
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
            # Use the correct endpoint for the new API
            endpoint = 'api/beta/network/pools'
            params = {
                'netuid': netuid
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
                        netuid=pool.get('netuid', 0),
                        tao_reserve=float(pool.get('tao_reserve', 0)),
                        alpha_reserve=float(pool.get('alpha_reserve', 0)),
                        alpha_supply=float(pool.get('alpha_supply', 0)),
                        emission_rate=float(pool.get('emission_rate', 0.0)),
                        price=float(pool.get('price', 0.0))
                    ))
                    
                except (KeyError, ValueError) as e:
                    bt.logging.warning(f"Error processing pool data: {str(e)}")
                    continue
            
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
        # Ensure start_tracking_from is timezone-aware
        if start_tracking_from.tzinfo is None:
            start_tracking_from = start_tracking_from.replace(tzinfo=timezone.utc)
            
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(days=7)  # Get one week of data
        
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