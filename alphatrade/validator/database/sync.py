"""
Unified synchronization module for the Alpha Trade Exchange subnet.

This module consolidates the metagraph synchronization and API data fetching
to eliminate redundant API calls while maintaining all functionality.

Features:
- Tracks last sync time per miner to avoid redundant API calls
- Handles both metagraph state and API data in a single pass
- Processes events with proper idempotency checks
- Maintains database consistency across sync operations
- Ignores trades for coldkeys registered on specific subnets
- Prevents duplicate API calls for the same coldkey
- Uses TaoApp API for fetching stake events
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Set, Tuple, Optional

import bittensor as bt

from alphatrade.validator.database.models import (
    StakeAction, StakeEvent, Position, Trade, PositionStatus, MinerStatus
)
from alphatrade.validator.database.manager import DatabaseManager
from alphatrade.validator.database.miner import MinerManager
from alphatrade.validator.api.api import AlphaAPI

class SyncManager:
    """Manager for synchronizing metagraph and external API data."""
    
    def __init__(
        self, 
        database: DatabaseManager,
        miner_manager: MinerManager,
        api_client: AlphaAPI,
        metagraph,
        sync_interval_minutes: int = 15,
        min_blocks_for_trade: int = 0
    ):
        """Initialize the sync manager.
        
        Args:
            database: Database manager instance
            miner_manager: Miner manager instance
            api_client: Alpha API client instance
            metagraph: Bittensor metagraph
            sync_interval_minutes: Minutes between syncs
            min_blocks_for_trade: Minimum blocks required for valid trades (set to 0)
        """
        self.database = database
        self.miner_manager = miner_manager
        self.api = api_client
        self.metagraph = metagraph
        self.sync_interval = timedelta(minutes=sync_interval_minutes)
        self.min_blocks = min_blocks_for_trade  # Set to 0
        
        # Track last sync times
        self.last_global_sync = datetime.min.replace(tzinfo=timezone.utc)
        
        # Cache for miner sync times
        self._miner_sync_cache = {}
        
        # Cache for miner coldkeys
        self._miner_coldkey_cache = {}
        
        # Track processed event extrinsic IDs to avoid duplicates
        self._processed_events = set()
        
        # Cache for events fetched per coldkey in the current sync cycle
        # Will be reset at the start of each sync cycle
        self._fetched_events_cache = {}
        
        # Track which coldkeys we've already processed in this sync cycle
        # Will be reset at the start of each sync cycle
        self._processed_coldkeys = set()
        
        # Mapping of coldkey to set of netuids where the coldkey is registered
        # Used to ignore trades for registered nodes
        self._subnet_registrations = {}
        
        # List of subnets to check for registrations
        self.subnet_range = range(1, 85)  # Subnets 1-85
        
        bt.logging.info("SyncManager initialized successfully with min_blocks=0")
    
    async def ensure_initialized(self):
        """Ensure database and miner manager are initialized."""
        if not hasattr(self.database, '_connection_initialized') or not self.database._connection_initialized:
            bt.logging.info("Database not initialized, initializing now...")
            await self.database.setup_database()
            
        if not hasattr(self.miner_manager, '_initialized') or not self.miner_manager._initialized:
            bt.logging.info("Miner manager not initialized, initializing now...")
            await self.miner_manager.ensure_database_initialized()
    
    async def should_sync(self) -> bool:
        """Check if it's time to sync based on the interval.
        
        Returns:
            bool: True if sync should run, False otherwise
        """
        current_time = datetime.now(timezone.utc)
        return (current_time - self.last_global_sync) >= self.sync_interval
        
    async def _get_registered_subnet_nodes(self) -> Dict[str, Set[int]]:
        """Fetch all registered nodes (coldkeys) on subnets 1-74 and create a mapping.
        
        Returns:
            Dict[str, Set[int]]: Mapping of coldkey to set of netuids where the coldkey is registered
        """
        registrations = {}
        
        try:
            # Use a single subtensor connection
            subtensor = self.metagraph.subtensor
            bt.logging.info(f"Fetching subnet registrations using existing subtensor connection")
            
            # Process subnets in batches to avoid overwhelming the connection
            batch_size = 5
            for i in range(0, len(self.subnet_range), batch_size):
                batch_netuids = list(self.subnet_range)[i:i+batch_size]
                bt.logging.info(f"Processing subnet batch {i//batch_size + 1}: subnets {batch_netuids}")
                
                for netuid in batch_netuids:
                    try:
                        # Create a metagraph for this subnet but reuse the existing subtensor
                        temp_metagraph = bt.metagraph(netuid=netuid, subtensor=subtensor)
                        
                        # Directly access neurons through the metagraph
                        if hasattr(temp_metagraph, 'neurons'):
                            for neuron in temp_metagraph.neurons:
                                # Skip if neuron doesn't have a coldkey
                                if not hasattr(neuron, 'coldkey') or not neuron.coldkey:
                                    continue
                                    
                                coldkey = neuron.coldkey
                                if coldkey not in registrations:
                                    registrations[coldkey] = set()
                                registrations[coldkey].add(netuid)
                        else:
                            # Fallback in case neurons isn't available - use hotkeys and coldkeys
                            for uid, hotkey in enumerate(temp_metagraph.hotkeys):
                                coldkey = None
                                if hasattr(temp_metagraph, 'coldkeys') and len(temp_metagraph.coldkeys) > uid:
                                    coldkey = temp_metagraph.coldkeys[uid]
                                
                                if not coldkey:
                                    continue
                                    
                                if coldkey not in registrations:
                                    registrations[coldkey] = set()
                                registrations[coldkey].add(netuid)
                            
                    except Exception as e:
                        bt.logging.warning(f"Error fetching registrations for subnet {netuid}: {str(e)}")
                        continue
                
                # Add a small delay between batches to avoid overwhelming the connection
                if i + batch_size < len(self.subnet_range):
                    await asyncio.sleep(0.5)
            
            bt.logging.info(f"Fetched registrations for {len(registrations)} coldkeys across subnets 1-74")
            return registrations
            
        except Exception as e:
            bt.logging.error(f"Error getting registered subnet nodes: {str(e)}")
            return {}
            
    async def sync_all(self):
        """Run a complete synchronization of metagraph and API data.
        
        This method replaces both _sync_metagraph_state and _sync_api_data,
        consolidating them into a single efficient process.
        """
        await self.ensure_initialized()
        
        current_time = datetime.now(timezone.utc)
        bt.logging.info(f"Starting unified sync at {current_time}")
        
        try:
            # Reset the processed coldkeys set for this sync cycle
            self._processed_coldkeys.clear()
            # Reset the events cache for this sync cycle
            self._fetched_events_cache.clear()
            
            # 0. Get subnet registrations to ignore trades for registered nodes
            self._subnet_registrations = await self._get_registered_subnet_nodes()
            bt.logging.info(f"Updated subnet registrations for {len(self._subnet_registrations)} coldkeys")
            
            # 1. Get current registered miners from metagraph
            current_hotkeys = self._get_miner_hotkeys_from_metagraph()
            bt.logging.info(f"Found {len(current_hotkeys)} miners in metagraph")
            
            # 2. Get existing miners from database
            db_miners = await self._get_db_miners()
            db_hotkeys = {m['hotkey'] for m in db_miners}
            
            # 3. Handle UUID reassignments or conflicts
            await self._handle_uid_conflicts(current_hotkeys)
            
            # 4. Process all miners from metagraph
            processed_hotkeys = set()
            for hotkey in current_hotkeys:
                try:
                    # Register or update miner
                    miner_uid = await self._ensure_miner_registered(hotkey)
                    if miner_uid is None:
                        bt.logging.error(f"Failed to register miner with hotkey {hotkey}")
                        continue
                    
                    # Process events for this miner
                    await self._sync_miner_events(hotkey, miner_uid)
                    processed_hotkeys.add(hotkey)
                    
                except Exception as e:
                    bt.logging.error(f"Error processing miner {hotkey}: {str(e)}")
                    # Continue with next miner
            
            # 5. Mark miners not in metagraph as inactive
            await self._mark_unregistered_miners(current_hotkeys)
            
            # 6. Update global sync time
            self.last_global_sync = current_time
            bt.logging.info(f"Unified sync completed at {current_time}, processed {len(processed_hotkeys)} miners")
            
        except Exception as e:
            bt.logging.error(f"Error during unified sync: {str(e)}")
            raise
    
    def _get_miner_hotkeys_from_metagraph(self) -> Set[str]:
        """Get the set of hotkeys for all miners in the metagraph.
        
        Returns:
            Set[str]: Set of miner hotkeys
        """
        miner_uids = set([uid for uid, dividend in enumerate(self.metagraph.dividends) if dividend == 0])
        return set([self.metagraph.hotkeys[uid] for uid in miner_uids])
    
    async def _get_db_miners(self) -> List[Dict]:
        """Get all miners from the database.
        
        Returns:
            List[Dict]: List of miner records
        """
        async with self.database.connection.get_connection() as conn:
            cursor = await conn.execute(
                "SELECT uid, hotkey, coldkey, last_active, status, tracking_start_date FROM miners"
            )
            miners = await cursor.fetchall()
            return miners
    
    async def _handle_uid_conflicts(self, current_hotkeys: Set[str]):
        """Handle UID conflicts between metagraph and database.
        
        This method ensures database UIDs match metagraph UIDs and handles any conflicts.
        
        Args:
            current_hotkeys: Set of hotkeys currently in the metagraph
        """
        bt.logging.info("Checking for UID conflicts")
        
        # Map of hotkeys to UIDs in metagraph
        metagraph_uids = {}
        for uid, hotkey in enumerate(self.metagraph.hotkeys):
            if hotkey in current_hotkeys:
                metagraph_uids[hotkey] = uid
        
        # Check database UIDs against metagraph
        async with self.database.connection.get_connection() as conn:
            for hotkey, metagraph_uid in metagraph_uids.items():
                cursor = await conn.execute(
                    "SELECT uid FROM miners WHERE hotkey = ?",
                    (hotkey,)
                )
                db_miner = await cursor.fetchone()
                
                if db_miner and db_miner['uid'] != metagraph_uid:
                    old_uid = db_miner['uid']
                    new_uid = metagraph_uid
                    
                    bt.logging.warning(f"UID conflict: Miner {hotkey} has UID {old_uid} in DB but {new_uid} in metagraph")
                    
                    # Check if there's already a miner with the new UID
                    cursor = await conn.execute(
                        "SELECT hotkey FROM miners WHERE uid = ? AND hotkey != ?",
                        (new_uid, hotkey)
                    )
                    conflict = await cursor.fetchone()
                    
                    if conflict:
                        conflict_hotkey = conflict['hotkey']
                        bt.logging.warning(f"Cannot reassign UID {new_uid} to {hotkey}, already used by {conflict_hotkey}")
                        continue
                    
                    # Handle table migration for UID change
                    try:
                        # Step 1: First check if old tables exist
                        cursor = await conn.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                            (f"miner_{old_uid}_positions",)
                        )
                        old_tables_exist = await cursor.fetchone()
                        
                        # Step 2: Check if new tables exist
                        cursor = await conn.execute(
                            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                            (f"miner_{new_uid}_positions",)
                        )
                        new_tables_exist = await cursor.fetchone()
                        
                        # Step 3: Create new tables if they don't exist
                        if not new_tables_exist:
                            bt.logging.info(f"Creating new tables for UID {new_uid}")
                            await self.miner_manager.ensure_miner_tables_exist_for_miner_uid(new_uid)
                        
                        # Step 4: Migrate data if old tables exist
                        if old_tables_exist:
                            # Copy positions
                            await conn.execute(
                                f"""
                                INSERT OR IGNORE INTO miner_{new_uid}_positions 
                                SELECT * FROM miner_{old_uid}_positions
                                """
                            )
                            
                            # Copy trades
                            await conn.execute(
                                f"""
                                INSERT OR IGNORE INTO miner_{new_uid}_trades 
                                SELECT * FROM miner_{old_uid}_trades
                                """
                            )
                            
                            # Copy position trades
                            await conn.execute(
                                f"""
                                INSERT OR IGNORE INTO miner_{new_uid}_position_trades 
                                SELECT * FROM miner_{old_uid}_position_trades
                                """
                            )
                            
                            bt.logging.info(f"Data migrated from miner_{old_uid} to miner_{new_uid}")
                            
                            # Drop old tables
                            await conn.execute(f"DROP TABLE IF EXISTS miner_{old_uid}_position_trades")
                            await conn.execute(f"DROP TABLE IF EXISTS miner_{old_uid}_positions")
                            await conn.execute(f"DROP TABLE IF EXISTS miner_{old_uid}_trades")
                        
                        # Step 5: Update miner UID in miners table
                        await conn.execute(
                            """
                            UPDATE miners 
                            SET uid = ? 
                            WHERE hotkey = ?
                            """,
                            (new_uid, hotkey)
                        )
                        
                        await conn.commit()
                        bt.logging.info(f"Successfully migrated miner {hotkey} from UID {old_uid} to {new_uid}")
                        
                    except Exception as e:
                        bt.logging.error(f"Error migrating tables for miner {hotkey}: {str(e)}")
                        await conn.rollback()
    
    async def _get_miner_coldkey(self, miner_uid: int) -> Optional[str]:
        """Get the coldkey for a miner from the database.
        
        Args:
            miner_uid: Miner's UID
            
        Returns:
            Optional[str]: Coldkey if found, None otherwise
        """
        # Check cache first
        if miner_uid in self._miner_coldkey_cache:
            return self._miner_coldkey_cache[miner_uid]
        
        try:
            async with self.database.connection.get_connection() as conn:
                cursor = await conn.execute(
                    "SELECT coldkey FROM miners WHERE uid = ?",
                    (miner_uid,)
                )
                result = await cursor.fetchone()
                
                if result and result['coldkey']:
                    # Update cache
                    coldkey = result['coldkey']
                    self._miner_coldkey_cache[miner_uid] = coldkey
                    return coldkey
                
                return None
        except Exception as e:
            bt.logging.error(f"Error getting coldkey for miner {miner_uid}: {str(e)}")
            return None
    
    async def _get_metagraph_info(self, hotkey: str) -> Tuple[Optional[int], Optional[str]]:
        """Get UID and coldkey for a hotkey from the metagraph.
        
        Args:
            hotkey: Miner's hotkey
            
        Returns:
            Tuple[Optional[int], Optional[str]]: (UID, coldkey)
        """
        try:
            # Get UID
            try:
                uid = self.metagraph.hotkeys.index(hotkey)
            except ValueError:
                bt.logging.error(f"Hotkey {hotkey} not found in metagraph")
                return None, None
            
            # Get coldkey if available
            coldkey = None
            if hasattr(self.metagraph, 'coldkeys') and len(self.metagraph.coldkeys) > uid:
                coldkey = self.metagraph.coldkeys[uid]
            
            return uid, coldkey
        except Exception as e:
            bt.logging.error(f"Error getting metagraph info for {hotkey}: {str(e)}")
            return None, None
    
    async def _ensure_miner_registered(self, hotkey: str) -> Optional[int]:
        """Register a miner if not already registered or update if exists.
        
        Args:
            hotkey: Miner's hotkey
            
        Returns:
            Optional[int]: Miner UID if successful, None if failed
        """
        try:
            # Get UID and coldkey from metagraph
            uid, metagraph_coldkey = await self._get_metagraph_info(hotkey)
            if uid is None:
                return None
            
            # Use a fallback coldkey if not available from metagraph
            coldkey = metagraph_coldkey or hotkey
            
            # Register or update miner with both hotkey and coldkey
            miner_uid = await self.miner_manager.register_miner(
                coldkey=coldkey,  # Use coldkey or hotkey as fallback
                hotkey=hotkey,
                tracking_start_date=datetime.now(timezone.utc),
                status=MinerStatus.ACTIVE,
                uid=uid
            )
            
            # Ensure miner tables exist
            await self.miner_manager.ensure_miner_tables_exist_for_miner_uid(miner_uid)
            
            return miner_uid
            
        except Exception as e:
            bt.logging.error(f"Error registering miner {hotkey}: {str(e)}")
            return None
    
    async def _get_miner_last_sync(self, miner_uid: int) -> datetime:
        """Get the last sync time for a miner.
        
        This method checks cache first, then database, and adds 
        a 'last_sync' column if it doesn't exist.
        
        Args:
            miner_uid: Miner's UID
            
        Returns:
            datetime: Last sync time or minimum datetime if never synced
        """
        # Check cache first
        if miner_uid in self._miner_sync_cache:
            return self._miner_sync_cache[miner_uid]
        
        try:
            async with self.database.connection.get_connection() as conn:
                cursor = await conn.execute(
                    "PRAGMA table_info(miners)"
                )
                columns = await cursor.fetchall()
                column_names = [col[1] for col in columns]
                
                # Add last_sync column if it doesn't exist
                if 'last_sync' not in column_names:
                    await conn.execute(
                        "ALTER TABLE miners ADD COLUMN last_sync TEXT"
                    )
                    await conn.commit()
                
                # Get last sync time
                cursor = await conn.execute(
                    "SELECT last_sync FROM miners WHERE uid = ?",
                    (miner_uid,)
                )
                row = await cursor.fetchone()
                
                if row and row['last_sync']:
                    last_sync = datetime.fromisoformat(row['last_sync'])
                else:
                    # Default to oldest possible time if never synced
                    last_sync = datetime.min.replace(tzinfo=timezone.utc)
                
                # Cache result
                self._miner_sync_cache[miner_uid] = last_sync
                return last_sync
                
        except Exception as e:
            bt.logging.error(f"Error getting last sync time for miner {miner_uid}: {str(e)}")
            return datetime.min.replace(tzinfo=timezone.utc)
    
    async def _update_miner_last_sync(self, miner_uid: int, sync_time: datetime):
        """Update the last sync time for a miner.
        
        Args:
            miner_uid: Miner's UID
            sync_time: Sync timestamp
        """
        try:
            async with self.database.connection.get_connection() as conn:
                await conn.execute(
                    "UPDATE miners SET last_sync = ? WHERE uid = ?",
                    (sync_time.isoformat(), miner_uid)
                )
                await conn.commit()
                
                # Update cache
                self._miner_sync_cache[miner_uid] = sync_time
                
        except Exception as e:
            bt.logging.error(f"Error updating last sync time for miner {miner_uid}: {str(e)}")
    
    async def _sync_miner_events(self, hotkey: str, miner_uid: int):
        """Synchronize events for a specific miner."""
        # Get last sync time for this miner
        last_sync = await self._get_miner_last_sync(miner_uid)
        
        # Get coldkey for this miner
        coldkey = await self._get_miner_coldkey(miner_uid)
        if not coldkey:
            bt.logging.error(f"No coldkey found for miner {hotkey} (UID {miner_uid}), using hotkey as fallback")
            coldkey = hotkey
        
        # Check if we've already processed this coldkey in this sync cycle
        if coldkey in self._processed_coldkeys:
            bt.logging.info(f"Already processed coldkey {coldkey} in this sync cycle, skipping API call for hotkey {hotkey}")
            return
            
        # Add to processed coldkeys set
        self._processed_coldkeys.add(coldkey)
        
        # Check if we have already fetched events for this coldkey
        if coldkey in self._fetched_events_cache:
            bt.logging.info(f"Using cached events for coldkey {coldkey}")
            events = self._fetched_events_cache[coldkey]
        else:
            # Fetch events using coldkey (not hotkey)
            events = await self._fetch_events(coldkey, last_sync)
            # Cache the events for this coldkey
            self._fetched_events_cache[coldkey] = events
        
        if not events:
            bt.logging.info(f"No events found for miner {hotkey} (UID {miner_uid}, coldkey {coldkey}) since {last_sync}")
            return
            
        bt.logging.info(f"Processing {len(events)} events for miner {hotkey} (UID {miner_uid})")
        
        # Sort events by timestamp to ensure proper order
        events.sort(key=lambda e: e.timestamp)
        
        # Group events by extrinsic_id
        extrinsic_groups = {}
        for event in events:
            if event.extrinsic_id not in extrinsic_groups:
                extrinsic_groups[event.extrinsic_id] = []
            extrinsic_groups[event.extrinsic_id].append(event)
        
        # Count events by type for reporting
        delegate_count = 0
        undelegate_count = 0
        skipped_count = 0
        invalid_count = 0
        
        # Process events by extrinsic ID group
        for extrinsic_id, events_group in extrinsic_groups.items():
            # Skip if we've already processed this extrinsic
            if extrinsic_id in self._processed_events:
                bt.logging.info(f"Already processed extrinsic {extrinsic_id}, skipping {len(events_group)} events")
                skipped_count += len(events_group)
                continue
            
            # Skip events with invalid netuid
            if all(event.netuid is None or event.netuid == 0 for event in events_group):
                bt.logging.info(f"Skipping extrinsic {extrinsic_id} with invalid netuid")
                invalid_count += len(events_group)
                self._processed_events.add(extrinsic_id)
                continue
            
            # For groups with multiple events, select the most appropriate one
            if len(events_group) > 1:
                bt.logging.info(f"Found {len(events_group)} events with same extrinsic_id {extrinsic_id}")
                
                # For same action type, prefer non-zero amounts
                event_types = set(event.action for event in events_group)
                
                if len(event_types) == 1:  # All same action type
                    action_type = list(event_types)[0]
                    
                    # For UNDELEGATE, prefer the event with non-zero amounts
                    if action_type == StakeAction.UNDELEGATE:
                        # Find event with highest amount
                        event = max(events_group, key=lambda e: (e.tao_amount or 0, e.alpha_amount or 0))
                        
                        # If all events have zero amounts, still use the first one
                        if event.tao_amount == 0 and event.alpha_amount == 0:
                            bt.logging.warning(f"All UNDELEGATE events have zero amounts for extrinsic {extrinsic_id}")
                            event = events_group[0]
                    else:
                        # For DELEGATE, use the first event
                        event = events_group[0]
                else:
                    # Mixed event types - sort by timestamp and use the latest
                    events_group.sort(key=lambda e: e.timestamp)
                    event = events_group[-1]
                    
                bt.logging.info(f"Selected event with action={event.action}, tao={event.tao_amount}, alpha={event.alpha_amount}")
            else:
                # Just one event
                event = events_group[0]
            
            # Process the selected event
            if event.netuid is None or event.netuid == 0:
                bt.logging.info(f"Skipping event with invalid netuid={event.netuid}: {extrinsic_id}")
                invalid_count += 1
                self._processed_events.add(extrinsic_id)
                continue
            
            # Process single events normally
            if event.action == StakeAction.DELEGATE:
                await self._process_stake_event(miner_uid, event)
                delegate_count += 1
            elif event.action == StakeAction.UNDELEGATE:
                # Skip events with zero amounts
                if event.tao_amount == 0 and event.alpha_amount == 0:
                    bt.logging.warning(f"Skipping UNDELEGATE event with zero amounts: {extrinsic_id}")
                    self._processed_events.add(extrinsic_id)
                    skipped_count += 1
                    continue
                    
                await self._process_unstake_event(miner_uid, event)
                undelegate_count += 1
            
            # Mark all events in this group as processed
            self._processed_events.add(extrinsic_id)
        
        # Update last sync time
        await self._update_miner_last_sync(miner_uid, datetime.now(timezone.utc))
        
        # Log summary of processed events
        bt.logging.info(f"Event processing summary for miner {hotkey}: "
                    f"DELEGATE={delegate_count}, UNDELEGATE={undelegate_count}, "
                    f"skipped={skipped_count}, invalid={invalid_count}")

    
    async def _fetch_events(self, coldkey: str, since_time: Optional[datetime] = None) -> List[StakeEvent]:
        """Fetch stake events for a coldkey with proper caching.
        
        Args:
            coldkey: Miner's coldkey
            since_time: Optional start time for fetching events
            
        Returns:
            List[StakeEvent]: List of stake events
        """
        try:
            # Set a reasonable start_time for the API call
            if since_time == datetime.min.replace(tzinfo=timezone.utc):
                # If never synced, fetch last 7 days
                start_time = datetime.now(timezone.utc) - timedelta(days=21)
            else:
                # Add a small overlap to ensure no events are missed
                start_time = since_time - timedelta(minutes=30)
            
            # Fetch events from API
            async with self.api as api_client:
                events = await api_client.get_stake_events(
                    coldkey=coldkey,
                    start_time=start_time
                )
            
            if events:
                bt.logging.info(f"Fetched {len(events)} events for coldkey {coldkey} since {start_time}")
            
            return events
            
        except Exception as e:
            bt.logging.error(f"Error fetching events for coldkey {coldkey}: {str(e)}")
            return []
    
    async def _process_stake_event(self, miner_uid: int, event: StakeEvent):
        """Process a stake event (DELEGATE action).
        
        This method creates a new position for the event with proper
        idempotency checks to avoid duplicates. It will ignore events
        for coldkeys that are registered on the subnet.
        
        Args:
            miner_uid: Miner's UID
            event: Stake event
        """
        # Skip if we've already processed this event
        if event.extrinsic_id in self._processed_events:
            bt.logging.info(f"Already processed event {event.extrinsic_id}, skipping")
            return
            
        # Check if this coldkey is registered on this netuid
        coldkey = event.coldkey
        netuid = event.netuid
        if coldkey in self._subnet_registrations and netuid in self._subnet_registrations[coldkey]:
            bt.logging.info(f"Ignoring stake event for coldkey {coldkey} on netuid {netuid} - node is registered on this subnet")
            self._processed_events.add(event.extrinsic_id)
            return
        
        # Check if position with this extrinsic_id already exists
        async with self.database.connection.get_connection() as conn:
            cursor = await conn.execute(
                f"""
                SELECT position_id FROM miner_{miner_uid}_positions
                WHERE extrinsic_id = ?
                """,
                (event.extrinsic_id,)
            )
            existing_position = await cursor.fetchone()
            
            if existing_position:
                bt.logging.info(f"Position with extrinsic_id {event.extrinsic_id} already exists. Skipping.")
                self._processed_events.add(event.extrinsic_id)
                return
        
        # Create position
        try:
            # Get hotkey from metagraph using UID for consistency
            miner_hotkey = self.metagraph.hotkeys[miner_uid]
            
            position = Position(
                position_id=None,  # Will be set by database
                miner_uid=miner_uid,
                netuid=event.netuid,
                hotkey=miner_hotkey,
                entry_block=event.block_number,
                entry_timestamp=event.timestamp,
                entry_tao=event.tao_amount,
                entry_alpha=event.alpha_amount,
                remaining_alpha=event.alpha_amount,
                status=PositionStatus.OPEN,
                extrinsic_id=event.extrinsic_id
            )
            
            # Add position to database
            await self.database.add_position(miner_uid, position)
            bt.logging.info(f"Created position for miner {miner_uid} with {event.alpha_amount} alpha, extrinsic_id: {event.extrinsic_id}")
            
            # Mark as processed
            self._processed_events.add(event.extrinsic_id)
            
        except Exception as e:
            bt.logging.error(f"Error creating position for event {event.extrinsic_id}: {str(e)}")
    
    async def _process_unstake_event(self, miner_uid: int, event: StakeEvent):
        """Process an unstake event (UNDELEGATE action).
        
        This method creates a trade and processes position closures with
        proper idempotency checks. It will ignore unstake events if there
        are no corresponding open positions or if the coldkey is registered
        on the subnet.
        
        Args:
            miner_uid: Miner's UID
            event: Stake event
        """
        # Skip if we've already processed this event
        if event.extrinsic_id in self._processed_events:
            bt.logging.info(f"Already processed event {event.extrinsic_id}, skipping")
            return
            
        # Check if this coldkey is registered on this netuid
        coldkey = event.coldkey
        netuid = event.netuid
        if coldkey in self._subnet_registrations and netuid in self._subnet_registrations[coldkey]:
            bt.logging.info(f"Ignoring unstake event for coldkey {coldkey} on netuid {netuid} - node is registered on this subnet")
            self._processed_events.add(event.extrinsic_id)
            return
        
        # Check if trade with this extrinsic_id already exists
        async with self.database.connection.get_connection() as conn:
            cursor = await conn.execute(
                f"""
                SELECT trade_id FROM miner_{miner_uid}_trades
                WHERE extrinsic_id = ?
                """,
                (event.extrinsic_id,)
            )
            existing_trade = await cursor.fetchone()
            
            if existing_trade:
                bt.logging.info(f"Trade with extrinsic_id {event.extrinsic_id} already exists. Skipping.")
                self._processed_events.add(event.extrinsic_id)
                return
        
        try:
            async with self.database.connection.get_connection() as conn:
                cursor = await conn.execute(
                    f"""
                    SELECT * FROM miner_{miner_uid}_positions
                    WHERE (status = 'open' OR status = 'partial') AND netuid = ?
                    ORDER BY entry_timestamp ASC
                    """,
                    (event.netuid,)
                )
                positions = []
                rows = await cursor.fetchall()
                
                for row in rows:
                    # Ensure timestamps are timezone-aware
                    entry_timestamp = datetime.fromisoformat(row['entry_timestamp'])
                    if entry_timestamp.tzinfo is None:
                        entry_timestamp = entry_timestamp.replace(tzinfo=timezone.utc)
                        
                    positions.append(Position(
                        position_id=row['position_id'],
                        miner_uid=miner_uid,
                        netuid=row['netuid'],
                        hotkey=row['hotkey'],
                        entry_block=row['entry_block'],
                        entry_timestamp=entry_timestamp,
                        entry_tao=row['entry_tao'],
                        entry_alpha=row['entry_alpha'],
                        remaining_alpha=row['remaining_alpha'],
                        status=PositionStatus(row['status']),
                        extrinsic_id=row['extrinsic_id']
                    ))
            
            # Check if we have any open or partial positions for this netuid
            if not positions:
                bt.logging.info(f"Ignoring unstake event {event.extrinsic_id} for netuid {event.netuid}: no open or partial positions found")
                # Mark as processed to avoid reprocessing
                self._processed_events.add(event.extrinsic_id)
                return
                    
            # Get hotkey from metagraph using UID for consistency
            miner_hotkey = self.metagraph.hotkeys[miner_uid]
            
            # For StakeRemoved (UNDELEGATE): amount_in is TAO, amount_out is ALPHA
            trade = Trade(
                trade_id=None,  # Will be set by database
                miner_uid=miner_uid,
                netuid=event.netuid,
                hotkey=miner_hotkey,
                exit_block=event.block_number,
                exit_timestamp=event.timestamp,
                exit_tao=event.tao_amount,
                exit_alpha=event.alpha_amount,
                extrinsic_id=event.extrinsic_id
            )
            
            await self.database.process_close(miner_uid, trade, positions, self.min_blocks)
            bt.logging.info(f"Closed positions for miner {miner_uid}, extrinsic_id: {event.extrinsic_id}")
            
            # Mark as processed
            self._processed_events.add(event.extrinsic_id)
                
        except Exception as e:
            bt.logging.error(f"Error processing unstake event {event.extrinsic_id}: {str(e)}")
    
    async def _mark_unregistered_miners(self, current_hotkeys: Set[str]):
        """Mark miners not in the metagraph as inactive.
        
        Args:
            current_hotkeys: Set of hotkeys currently in the metagraph
        """
        if not current_hotkeys:
            return
            
        try:
            async with self.database.connection.get_connection() as conn:
                placeholders = ','.join(['?' for _ in current_hotkeys])
                await conn.execute(
                    f"""
                    UPDATE miners 
                    SET is_registered = 0,
                        status = ?
                    WHERE hotkey NOT IN ({placeholders})
                    """,
                    (MinerStatus.INACTIVE.value, *current_hotkeys)
                )
                await conn.commit()
            
        except Exception as e:
            bt.logging.error(f"Error marking unregistered miners: {str(e)}")


# Utility function to create and initialize the manager
async def create_sync_manager(validator, sync_interval_minutes=15, min_blocks_for_trade=360):
    """Create and initialize a SyncManager instance for a validator.
    
    Args:
        validator: Validator instance with required components
        sync_interval_minutes: Minutes between syncs (default: 15)
        min_blocks_for_trade: Minimum blocks for valid trades (default: 360)
        
    Returns:
        SyncManager: Initialized sync manager
    """
    # Create manager instance
    manager = SyncManager(
        database=validator.database,
        miner_manager=validator.miner_manager,
        api_client=validator.api,
        metagraph=validator.metagraph,
        sync_interval_minutes=sync_interval_minutes,
        min_blocks_for_trade=min_blocks_for_trade
    )
    
    # Ensure initialized
    await manager.ensure_initialized()
    
    return manager